package gateway

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"apnv.id/apex/api-gateway/internal/store"
	"apnv.id/apex/api-gateway/internal/logger"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

// Proxy is the central request dispatcher. It matches every inbound request
// to a route and forwards it to the upstream destination using the correct
// protocol handler.
type Proxy struct {
	router     *Router
	mwChain    *MiddlewareChain
	httpClient *http.Client
	wsUpgrader websocket.Upgrader
}

// NewProxy constructs a ready Proxy.
func NewProxy(router *Router, cache *store.Cache, tlog *logger.TrafficLogger) *Proxy {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false, // enforce TLS verification in production
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
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   0, // timeout set per-route via context
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

	// Apply middleware (rate limit, circuit breaker, logging) then dispatch
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
		default: // HTTP, HTTPS
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

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = p.httpClient.Transport
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Error().Err(err).Str("route", route.Name).Msg("upstream error")
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}

	// Director: rewrite request before forwarding
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host

		// Strip path prefix if configured
		if route.StripPrefix != "" {
			req.URL.Path = strings.TrimPrefix(req.URL.Path, route.StripPrefix)
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
		}

		// Inject additional upstream headers
		for k, v := range route.Headers {
			req.Header.Set(k, v)
		}

		// Standard proxy headers
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Origin-Host", target.Host)
		if req.Header.Get("X-Forwarded-Proto") == "" {
			req.Header.Set("X-Forwarded-Proto", target.Scheme)
		}
		req.Host = target.Host
	}

	// Apply per-route timeout
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
	// Build upstream WebSocket URL
	destURL, err := url.Parse(route.Destination)
	if err != nil {
		http.Error(w, "bad gateway configuration", http.StatusBadGateway)
		return
	}

	// Translate http(s) → ws(s) if needed
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

	// Propagate request headers to upstream
	reqHeader := http.Header{}
	for _, key := range []string{"Authorization", "Cookie", "X-Request-ID"} {
		if v := r.Header.Get(key); v != "" {
			reqHeader.Set(key, v)
		}
	}
	for k, v := range route.Headers {
		reqHeader.Set(k, v)
	}

	// Connect to upstream WebSocket
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

	// Upgrade the client connection
	clientConn, err := p.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Str("route", route.Name).Msg("ws upgrade failed")
		return
	}
	defer clientConn.Close()

	// Bidirectional relay
	errc := make(chan error, 2)

	// Client → Upstream
	go func() {
		for {
			msgType, msg, err := clientConn.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if err := upstreamConn.WriteMessage(msgType, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	// Upstream → Client
	go func() {
		for {
			msgType, msg, err := upstreamConn.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if err := clientConn.WriteMessage(msgType, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	// Wait for either side to close
	if err := <-errc; err != nil && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		log.Debug().Err(err).Str("route", route.Name).Msg("ws relay ended")
	}
}

// ── gRPC ───────────────────────────────────────────────────────────────────

// handleGRPC proxies gRPC traffic by treating it as HTTP/2 with binary framing.
// gRPC uses Content-Type: application/grpc — we detect and forward accordingly.
func (p *Proxy) handleGRPC(w http.ResponseWriter, r *http.Request, route *models.Route) {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		http.Error(w, "expected gRPC content-type", http.StatusUnsupportedMediaType)
		return
	}

	// Parse the gRPC destination (grpc://host:port → http://host:port for h2c)
	destStr := route.Destination
	destStr = strings.Replace(destStr, "grpc://", "http://", 1)
	destStr = strings.Replace(destStr, "grpcs://", "https://", 1)

	target, err := url.Parse(destStr)
	if err != nil {
		http.Error(w, "bad gateway configuration", http.StatusBadGateway)
		return
	}

	// Build an HTTP/2 transport for gRPC (requires h2c or TLS)
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
	proxy.FlushInterval = -1 // immediate flush for streaming RPCs

	timeout := 60 * time.Second // longer default for streaming gRPC
	if route.TimeoutMs > 0 {
		timeout = time.Duration(route.TimeoutMs) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	proxy.ServeHTTP(w, r.WithContext(ctx))
}
