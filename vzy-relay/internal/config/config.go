package config

import (
	"fmt"
	"os"
	"strconv"
)

type SlowClientPolicy string

const (
	SlowClientDrop       SlowClientPolicy = "drop"
	SlowClientDisconnect SlowClientPolicy = "disconnect"
)

type Config struct {
	Port                  string
	MaxConnectionsPerRoom int
	BufferSize            int
	EnablePersistence     bool
	ReplayLimit           int
	LogLevel              string
	WriteQueueSize        int
	SlowClientPolicy      SlowClientPolicy
	HeartbeatIntervalSec  int
	WALPath               string
	DedupeTTLSec          int
}

func Load() (Config, error) {
	cfg := Config{
		Port:                  getString("PORT", "8080"),
		MaxConnectionsPerRoom: getInt("MAX_CONNECTIONS_PER_ROOM", 10000),
		BufferSize:            getInt("BUFFER_SIZE", 1000),
		EnablePersistence:     getBool("ENABLE_PERSISTENCE", true),
		ReplayLimit:           getInt("REPLAY_LIMIT", 1000),
		LogLevel:              getString("LOG_LEVEL", "INFO"),
		WriteQueueSize:        getInt("WRITE_QUEUE_SIZE", 256),
		HeartbeatIntervalSec:  getInt("HEARTBEAT_INTERVAL_SEC", 20),
		WALPath:               getString("WAL_PATH", "./vzy-relay.wal"),
		DedupeTTLSec:          getInt("DEDUPE_TTL_SEC", 300),
	}

	policy := getString("SLOW_CLIENT_POLICY", string(SlowClientDrop))
	switch SlowClientPolicy(policy) {
	case SlowClientDrop, SlowClientDisconnect:
		cfg.SlowClientPolicy = SlowClientPolicy(policy)
	default:
		return Config{}, fmt.Errorf("invalid SLOW_CLIENT_POLICY: %s", policy)
	}

	if cfg.BufferSize <= 0 || cfg.ReplayLimit <= 0 || cfg.WriteQueueSize <= 0 || cfg.MaxConnectionsPerRoom <= 0 || cfg.DedupeTTLSec <= 0 {
		return Config{}, fmt.Errorf("invalid numeric configuration: values must be > 0")
	}

	return cfg, nil
}

func getString(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
