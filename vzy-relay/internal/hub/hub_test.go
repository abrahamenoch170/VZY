package hub

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"vzy-relay/internal/config"
	"vzy-relay/internal/metrics"
	"vzy-relay/internal/protocol"
	"vzy-relay/internal/store"
	"vzy-relay/internal/wal"
)

type mockClient struct {
	id           string
	roomID       string
	lastSequence uint64
	mu           sync.Mutex
	msgs         [][]byte
	closed       bool
}

func (m *mockClient) ID() string           { return m.id }
func (m *mockClient) RoomID() string       { return m.roomID }
func (m *mockClient) LastSequence() uint64 { return m.lastSequence }
func (m *mockClient) Send(data []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.msgs = append(m.msgs, data)
	return true
}
func (m *mockClient) Close(_ int, _ string) { m.mu.Lock(); m.closed = true; m.mu.Unlock() }

func TestHubRegisterUnregisterBroadcast(t *testing.T) {
	cfg := config.Config{BufferSize: 16, MaxConnectionsPerRoom: 10, SlowClientPolicy: config.SlowClientDisconnect, DedupeTTLSec: 10}
	w, err := wal.Open(t.TempDir() + "/relay.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	h := New(cfg, metrics.New(), store.New(10), w, slog.Default())

	c1 := &mockClient{id: "c1", roomID: "r1"}
	c2 := &mockClient{id: "c2", roomID: "r1"}
	h.Register(c1)
	h.Register(c2)

	op := protocol.Operation{Type: "SET", Key: "k", Value: []byte(`1`), ClientID: "c1", Timestamp: 1, RoomID: "r1", OpID: "123e4567-e89b-42d3-a456-426614174000"}
	if err := h.Publish("r1", "c1", op); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	c2.mu.Lock()
	if len(c2.msgs) == 0 {
		t.Fatal("expected c2 to receive broadcast")
	}
	c2.mu.Unlock()

	h.Ack("r1", "c2", op.OpID)

	h.Unregister(c1)
	h.Unregister(c2)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h.Shutdown(ctx)

	if w.Size() == 0 {
		t.Fatal("expected wal to persist broadcast")
	}
}
