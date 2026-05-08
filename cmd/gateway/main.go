package main

import (
	"context"
	"crypto/tls"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"apnv.id/apex/api-gateway/internal/admin"
	"apnv.id/apex/api-gateway/internal/config"
	"apnv.id/apex/api-gateway/internal/gateway"
	"apnv.id/apex/api-gateway/internal/logger"
	"apnv.id/apex/api-gateway/internal/store"
	
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// ── Config ─────────────────────────────────────────────────────────────
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	// ── Logging ────────────────────────────────────────────────────────────
	if cfg.Log.Format == "console" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	}
	level, _ := zerolog.ParseLevel(cfg.Log.Level)
	zerolog.SetGlobalLevel(level)

	log.Info().Str("version", "1.0.0").Msg("APEX API Gateway starting")

	// ── Context with graceful shutdown ─────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── PostgreSQL ─────────────────────────────────────────────────────────
	routeStore, err := store.NewRouteStore(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to postgres")
	}
	defer routeStore.Close()
	log.Info().Msg("postgres connected")

	if err := routeStore.Migrate(ctx); err != nil {
		log.Fatal().Err(err).Msg("schema migration failed")
	}
	log.Info().Msg("schema migrated")

	// ── Redis ──────────────────────────────────────────────────────────────
	cache, err := store.NewCache(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to redis")
	}
	defer cache.Close()
	log.Info().Str("addr", cfg.Redis.Addr).Msg("redis connected")

	// ── Traffic Logger ─────────────────────────────────────────────────────
	tlog := logger.NewTrafficLogger(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.Enabled)
	defer tlog.Close()
	if cfg.Kafka.Enabled {
		log.Info().Strs("brokers", cfg.Kafka.Brokers).Msg("kafka traffic logger enabled")
	} else {
		log.Info().Msg("kafka disabled — traffic logs buffered in-memory")
	}

	// ── Router ─────────────────────────────────────────────────────────────
	router, err := gateway.NewRouter(routeStore, cache, cfg.Gateway.RouteConfigTTL)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialise router")
	}
	defer router.Stop()
	log.Info().Dur("ttl", cfg.Gateway.RouteConfigTTL).Msg("route table loaded")

	// ── Proxy ──────────────────────────────────────────────────────────────
	proxy := gateway.NewProxy(router, cache, tlog)

	// ── Admin API ──────────────────────────────────────────────────────────
	adminHandlers := admin.NewHandlers(routeStore, cache, router, tlog)
	adminServer := admin.NewServer(&cfg.Admin, adminHandlers)

	// ── Gateway HTTP Server ────────────────────────────────────────────────
	gatewaySrv := &http.Server{
		Addr:           cfg.Gateway.Addr,
		Handler:        proxy,
		ReadTimeout:    cfg.Gateway.ReadTimeout,
		WriteTimeout:   cfg.Gateway.WriteTimeout,
		IdleTimeout:    cfg.Gateway.IdleTimeout,
		MaxHeaderBytes: cfg.Gateway.MaxHeaderBytes,
	}

	// ── TLS Server (optional) ──────────────────────────────────────────────
	var tlsSrv *http.Server
	if cfg.Gateway.TLSCertFile != "" && cfg.Gateway.TLSKeyFile != "" {
		tlsCfg := &tls.Config{
			MinVersion:               tls.VersionTLS12,
			PreferServerCipherSuites: true,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			},
		}
		tlsSrv = &http.Server{
			Addr:      cfg.Gateway.TLSAddr,
			Handler:   proxy,
			TLSConfig: tlsCfg,
		}
	}

	// ── Start all servers ──────────────────────────────────────────────────
	errCh := make(chan error, 3)

	go func() {
		log.Info().Str("addr", cfg.Gateway.Addr).Msg("gateway HTTP listening")
		if err := gatewaySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	if tlsSrv != nil {
		go func() {
			log.Info().Str("addr", cfg.Gateway.TLSAddr).Msg("gateway HTTPS listening")
			if err := tlsSrv.ListenAndServeTLS(cfg.Gateway.TLSCertFile, cfg.Gateway.TLSKeyFile); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}()
	}

	go func() {
		errCh <- adminServer.Start(ctx)
	}()

	// ── Wait for shutdown signal or fatal error ────────────────────────────
	select {
	case err := <-errCh:
		if err != nil {
			log.Fatal().Err(err).Msg("server error")
		}
	case <-ctx.Done():
		log.Info().Msg("shutdown signal received")
	}

	// Graceful shutdown — drain in-flight requests
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()

	log.Info().Msg("draining in-flight requests (30s)...")
	if err := gatewaySrv.Shutdown(shutCtx); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	}
	if tlsSrv != nil {
		if err := tlsSrv.Shutdown(shutCtx); err != nil {
			log.Error().Err(err).Msg("HTTPS server shutdown error")
		}
	}
	log.Info().Msg("shutdown complete")
}
