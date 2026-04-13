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
	"vzy-relay/internal/mesh"
	"vzy-relay/internal/metrics"
	"vzy-relay/internal/pubsub"
	"vzy-relay/internal/sequence"
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
	if cfg.SequenceMode == config.SequenceModeGlobal {
		h.SetSequenceProvider(sequence.NewRedisGlobalProvider(cfg.RedisAddr, cfg.GlobalSeqKey))
	}

	opsByRoom, seqByRoom, err := w.Recover()
	if err != nil {
		log.Error("wal recover failed", "error", err)
	} else {
		h.RestoreFromWAL(opsByRoom, seqByRoom)
		m.SetWALSize(w.Size())
	}

	var meshSvc *mesh.Service
	if cfg.EnableMesh {
		bus, err := createPubSub(cfg)
		if err != nil {
			log.Error("mesh disabled", "error", err)
		} else {
			meshSvc = mesh.NewService(cfg.NodeID, bus, m, log)
			h.SetCrossNodePublisher(meshSvc.Publish)
			if err := meshSvc.SubscribeRoom("*", h.ApplyReplicatedEvent); err != nil {
				log.Error("mesh subscribe failed", "error", err)
			}
		}
	}
	defer func() {
		if meshSvc != nil {
			_ = meshSvc.Close()
		}
	}()

	server := ws.NewServer(h, cfg, log)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.Handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/region", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(cfg.Region)) })
	mux.HandleFunc("/node-id", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(cfg.NodeID)) })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.Snapshot())
	})

	httpServer := &http.Server{Addr: fmt.Sprintf(":%s", cfg.Port), Handler: recoverMiddleware(log, mux), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Info("relay server started", "port", cfg.Port, "nodeId", cfg.NodeID, "region", cfg.Region)
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

func createPubSub(cfg config.Config) (pubsub.PubSub, error) {
	switch cfg.PubSubBackend {
	case config.PubSubNATS:
		return pubsub.NewNATS(cfg.NATSURL)
	default:
		return pubsub.NewRedis(cfg.RedisAddr), nil
	}
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
