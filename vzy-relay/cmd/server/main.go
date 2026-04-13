package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vzy-relay/internal/config"
	"vzy-relay/internal/hub"
	"vzy-relay/internal/logger"
	"vzy-relay/internal/metrics"
	"vzy-relay/internal/store"
	"vzy-relay/internal/wal"
	"vzy-relay/internal/ws"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	log := logger.New(cfg.LogLevel)
	m := metrics.New()

	var st *store.MemoryStore
	if cfg.EnablePersistence {
		st = store.New(cfg.ReplayLimit)
	}

	w, err := wal.Open(cfg.WALPath)
	if err != nil {
		panic(err)
	}
	defer func() { _ = w.Close() }()

	h := hub.New(cfg, m, st, w, log)
	opsByRoom, seqByRoom, err := w.Recover()
	if err != nil {
		log.Error("wal recover failed", "error", err)
	} else {
		h.RestoreFromWAL(opsByRoom, seqByRoom)
		m.SetWALSize(w.Size())
	}

	server := ws.NewServer(h, cfg, log)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.Handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.Snapshot())
	})

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%s", cfg.Port),
		Handler:           recoverMiddleware(log, mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("relay server started", "port", cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	<-sigCtx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h.Shutdown(shutdownCtx)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown failed", "error", err)
	}
	log.Info("server stopped")
}

func recoverMiddleware(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic recovered in http handler", "recover", rec)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
