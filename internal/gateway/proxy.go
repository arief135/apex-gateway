package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"apnv.id/apex/api-gateway/internal/logger"
	"apnv.id/apex/api-gateway/internal/models"
	"apnv.id/apex/api-gateway/internal/store"
	"apnv.id/apex/api-gateway/internal/transform"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

// Proxy is the central request dispatcher. It matches every inbound request
// to a route and forwards it to the upstream destination using the correct
// protocol handler.
type Proxy struct {
	router     *Router
	mwChain    *MiddlewareChain
	engine     *transform.Engine
	loader     *transform.Loader
	httpClient *http.Client
	wsUpgrader websocket.Upgrader
}

// NewProxy constructs a ready Proxy.
func NewProxy(router *Router, cache *store.Cache, tlog *logger.TrafficLogger, engine *transform.Engine, loader *transform.Loader) *Proxy {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &Proxy{
		router:  router,
		mwChain: NewMiddlewareChain(cache, tlog),
		engine:  engine,
		loader:  loader,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   0,
		},
		wsUpgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
		},
	}
}

// ServeHTTP is the single entry point for all inbound traffic.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := p.router.Match(r)
	if route == nil {
		http.Error(w, `{"error":"no matching route"}`, http.StatusNotFound)
		return
	}

	handler := p.mwChain.Wrap(route, p.dispatchHandler(route))
	handler.ServeHTTP(w, r)
}

// dispatchHandler picks the correct protocol handler for a route.
func (p *Proxy) dispatchHandler(route *models.Route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch route.Protocol {
		case models.ProtocolWebSocket:
			p.handleWebSocket(w, r, route)
		case models.ProtocolGRPC:
			p.handleGRPC(w, r, route)
		default:
			p.handleHTTP(w, r, route)
		}
	})
}

// ── HTTP / HTTPS ───────────────────────────────────────────────────────────

func (p *Proxy) handleHTTP(w http.ResponseWriter, r *http.Request, route *models.Route) {
	target, err := url.Parse(route.Destination)
	if err != nil {
		log.Error().Err(err).Str("destination", route.Destination).Msg("invalid destination URL")
		http.Error(w, "bad gateway configuration", http.StatusBadGateway)
		return
	}

	// Build transform context once — shared by request and response transforms
	tctx := &models.TransformContext{
		Method:    r.Method,
		Path:      r.URL.Path,
		Headers:   flattenHeadersMap(r.Header),
		RouteID:   route.ID,
		RouteName: route.Name,
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = p.httpClient.Transport
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Error().Err(err).Str("route", route.Name).Msg("upstream error")
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}

	// ── Director: rewrite + REQUEST transform ─────────────────────────────
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host

		if route.StripPrefix != "" {
			req.URL.Path = strings.TrimPrefix(req.URL.Path, route.StripPrefix)
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
		}
		for k, v := range route.Headers {
			req.Header.Set(k, v)
		}
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Origin-Host", target.Host)
		if req.Header.Get("X-Forwarded-Proto") == "" {
			req.Header.Set("X-Forwarded-Proto", target.Scheme)
		}
		req.Host = target.Host

		// Apply request-direction transformers when body is JSON
		if isJSONBody(req.Header) && req.Body != nil {
			reqTransformers := p.loader.Load(route.ID, models.DirectionRequest)
			if len(reqTransformers) > 0 {
				body, readErr := io.ReadAll(req.Body)
				if readErr == nil {
					tctx.Direction = string(models.DirectionRequest)
					transformed := p.engine.ApplyChain(reqTransformers, body, tctx)
					req.Body = io.NopCloser(bytes.NewReader(transformed))
					req.ContentLength = int64(len(transformed))
					req.Header.Set("Content-Length", fmt.Sprintf("%d", len(transformed)))
				}
			}
		}
	}

	// ── ModifyResponse: RESPONSE transform ───────────────────────────────
	proxy.ModifyResponse = func(resp *http.Response) error {
		if !isJSONResponse(resp.Header) {
			return nil
		}
		respTransformers := p.loader.Load(route.ID, models.DirectionResponse)
		if len(respTransformers) == 0 {
			return nil
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			resp.Body = io.NopCloser(bytes.NewReader(body))
			return nil // fail-open
		}

		tctx.Direction = string(models.DirectionResponse)
		transformed := p.engine.ApplyChain(respTransformers, body, tctx)

		resp.Body = io.NopCloser(bytes.NewReader(transformed))
		resp.ContentLength = int64(len(transformed))
		resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(transformed)))
		resp.Header.Del("Content-Encoding") // body is no longer compressed after transform
		return nil
	}

	timeout := 30 * time.Second
	if route.TimeoutMs > 0 {
		timeout = time.Duration(route.TimeoutMs) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	proxy.ServeHTTP(w, r.WithContext(ctx))
}

// ── WebSocket ──────────────────────────────────────────────────────────────

func (p *Proxy) handleWebSocket(w http.ResponseWriter, r *http.Request, route *models.Route) {
	destURL, err := url.Parse(route.Destination)
	if err != nil {
		http.Error(w, "bad gateway configuration", http.StatusBadGateway)
		return
	}
	switch destURL.Scheme {
	case "http":
		destURL.Scheme = "ws"
	case "https":
		destURL.Scheme = "wss"
	}
	if route.StripPrefix != "" {
		destURL.Path = strings.TrimPrefix(r.URL.Path, route.StripPrefix)
	} else {
		destURL.Path = r.URL.Path
	}
	destURL.RawQuery = r.URL.RawQuery

	reqHeader := http.Header{}
	for _, key := range []string{"Authorization", "Cookie", "X-Request-ID"} {
		if v := r.Header.Get(key); v != "" {
			reqHeader.Set(key, v)
		}
	}
	for k, v := range route.Headers {
		reqHeader.Set(k, v)
	}

	upstreamConn, resp, err := websocket.DefaultDialer.Dial(destURL.String(), reqHeader)
	if err != nil {
		status := http.StatusBadGateway
		if resp != nil {
			status = resp.StatusCode
		}
		http.Error(w, fmt.Sprintf("upstream WS error: %v", err), status)
		return
	}
	defer upstreamConn.Close()

	clientConn, err := p.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Str("route", route.Name).Msg("ws upgrade failed")
		return
	}
	defer clientConn.Close()

	tctx := &models.TransformContext{
		Method: "WS", Path: r.URL.Path,
		RouteID: route.ID, RouteName: route.Name,
	}
	reqTransformers := p.loader.Load(route.ID, models.DirectionRequest)
	respTransformers := p.loader.Load(route.ID, models.DirectionResponse)

	errc := make(chan error, 2)

	// Client → Upstream (with optional request transform)
	go func() {
		for {
			msgType, msg, err := clientConn.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if msgType == websocket.TextMessage && len(reqTransformers) > 0 {
				tctx.Direction = string(models.DirectionRequest)
				msg = p.engine.ApplyChain(reqTransformers, msg, tctx)
			}
			if err := upstreamConn.WriteMessage(msgType, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	// Upstream → Client (with optional response transform)
	go func() {
		for {
			msgType, msg, err := upstreamConn.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if msgType == websocket.TextMessage && len(respTransformers) > 0 {
				tctx.Direction = string(models.DirectionResponse)
				msg = p.engine.ApplyChain(respTransformers, msg, tctx)
			}
			if err := clientConn.WriteMessage(msgType, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	if err := <-errc; err != nil && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		log.Debug().Err(err).Str("route", route.Name).Msg("ws relay ended")
	}
}

// ── gRPC ───────────────────────────────────────────────────────────────────

func (p *Proxy) handleGRPC(w http.ResponseWriter, r *http.Request, route *models.Route) {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		http.Error(w, "expected gRPC content-type", http.StatusUnsupportedMediaType)
		return
	}

	destStr := route.Destination
	destStr = strings.Replace(destStr, "grpc://", "http://", 1)
	destStr = strings.Replace(destStr, "grpcs://", "https://", 1)

	target, err := url.Parse(destStr)
	if err != nil {
		http.Error(w, "bad gateway configuration", http.StatusBadGateway)
		return
	}

	h2Transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: false},
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = h2Transport
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		for k, v := range route.Headers {
			req.Header.Set(k, v)
		}
	}
	proxy.FlushInterval = -1

	timeout := 60 * time.Second
	if route.TimeoutMs > 0 {
		timeout = time.Duration(route.TimeoutMs) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	proxy.ServeHTTP(w, r.WithContext(ctx))
}

// ── Helpers ────────────────────────────────────────────────────────────────

func isJSONBody(h http.Header) bool {
	ct := h.Get("Content-Type")
	return strings.Contains(ct, "application/json")
}

func isJSONResponse(h http.Header) bool {
	ct := h.Get("Content-Type")
	return strings.Contains(ct, "application/json")
}

func flattenHeadersMap(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		out[k] = strings.Join(vs, ", ")
	}
	return out
}
