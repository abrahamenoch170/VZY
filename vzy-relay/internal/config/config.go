package config

import (
	"fmt"
	"os"
	"strconv"
)

type SlowClientPolicy string

type SequenceMode string

type PubSubBackend string

const (
	SlowClientDrop       SlowClientPolicy = "drop"
	SlowClientDisconnect SlowClientPolicy = "disconnect"

	SequenceModeLocal  SequenceMode = "local"
	SequenceModeGlobal SequenceMode = "global"

	PubSubRedis PubSubBackend = "redis"
	PubSubNATS  PubSubBackend = "nats"
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

	NodeID          string
	Region          string
	EnableMesh      bool
	PubSubBackend   PubSubBackend
	RedisAddr       string
	NATSURL         string
	SequenceMode    SequenceMode
	GlobalSeqKey    string
	DedupCapacity   int
	DedupTTLSeconds int
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

		NodeID:          getString("NODE_ID", "node-local"),
		Region:          getString("REGION", "local"),
		EnableMesh:      getBool("ENABLE_MESH", false),
		PubSubBackend:   PubSubBackend(getString("PUBSUB_BACKEND", string(PubSubRedis))),
		RedisAddr:       getString("REDIS_ADDR", "127.0.0.1:6379"),
		NATSURL:         getString("NATS_URL", "nats://127.0.0.1:4222"),
		SequenceMode:    SequenceMode(getString("SEQUENCE_MODE", string(SequenceModeLocal))),
		GlobalSeqKey:    getString("GLOBAL_SEQUENCE_KEY", "vzy:global:sequence"),
		DedupCapacity:   getInt("DEDUP_CAPACITY", 200000),
		DedupTTLSeconds: getInt("DEDUP_STORE_TTL_SEC", 3600),
	}

	policy := getString("SLOW_CLIENT_POLICY", string(SlowClientDrop))
	switch SlowClientPolicy(policy) {
	case SlowClientDrop, SlowClientDisconnect:
		cfg.SlowClientPolicy = SlowClientPolicy(policy)
	default:
		return Config{}, fmt.Errorf("invalid SLOW_CLIENT_POLICY: %s", policy)
	}

	switch cfg.SequenceMode {
	case SequenceModeLocal, SequenceModeGlobal:
	default:
		return Config{}, fmt.Errorf("invalid SEQUENCE_MODE: %s", cfg.SequenceMode)
	}

	switch cfg.PubSubBackend {
	case PubSubRedis, PubSubNATS:
	default:
		return Config{}, fmt.Errorf("invalid PUBSUB_BACKEND: %s", cfg.PubSubBackend)
	}

	if cfg.BufferSize <= 0 || cfg.ReplayLimit <= 0 || cfg.WriteQueueSize <= 0 || cfg.MaxConnectionsPerRoom <= 0 || cfg.DedupeTTLSec <= 0 || cfg.DedupCapacity <= 0 || cfg.DedupTTLSeconds <= 0 {
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
