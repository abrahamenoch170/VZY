package test

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vzy-relay/internal/config"
	"vzy-relay/internal/hub"
	"vzy-relay/internal/metrics"
	"vzy-relay/internal/store"
	"vzy-relay/internal/wal"
	"vzy-relay/internal/ws"
)

type env struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func TestWebSocketBroadcastBetweenClients(t *testing.T) {
	cfg := config.Config{BufferSize: 64, ReplayLimit: 100, EnablePersistence: true, WriteQueueSize: 32, MaxConnectionsPerRoom: 100, SlowClientPolicy: config.SlowClientDisconnect, HeartbeatIntervalSec: 60, DedupeTTLSec: 10}
	w, err := wal.Open(t.TempDir() + "/relay.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	h := hub.New(cfg, metrics.New(), store.New(100), w, slog.Default())
	s := ws.NewServer(h, cfg, slog.Default())
	ts := httptest.NewServer(http.HandlerFunc(s.Handler))
	defer ts.Close()

	addr := strings.TrimPrefix(ts.URL, "http://")
	c1, err := dialWS(addr, "/ws?roomId=r1&clientId=c1")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := dialWS(addr, "/ws?roomId=r1&clientId=c2")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	opMsg := []byte(`{"event":"operation","data":{"type":"SET","key":"a","value":1,"clientId":"c1","timestamp":1,"roomId":"r1","opId":"123e4567-e89b-42d3-a456-426614174000"}}`)
	if err := writeClientTextFrame(c1, opMsg); err != nil {
		t.Fatal(err)
	}

	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		msg, err := readServerTextFrame(c2)
		if err != nil {
			t.Fatal(err)
		}
		var e env
		if err := json.Unmarshal(msg, &e); err != nil {
			continue
		}
		if e.Event == "operation_broadcast" {
			return
		}
	}
}

func dialWS(addr, path string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	keyRaw := make([]byte, 16)
	_, _ = rand.Read(keyRaw)
	key := base64.StdEncoding.EncodeToString(keyRaw)
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", path, addr, key)
	if _, err := conn.Write([]byte(req)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	r := bufio.NewReader(conn)
	status, err := r.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !strings.Contains(status, "101") {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected status: %s", status)
	}
	acceptExpected := computeAccept(key)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if line == "\r\n" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-accept:") && !strings.Contains(line, acceptExpected) {
			_ = conn.Close()
			return nil, fmt.Errorf("invalid accept header")
		}
	}
	return &bufferedConn{Conn: conn, r: r}, nil
}

func computeAccept(key string) string {
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h[:])
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func writeClientTextFrame(conn net.Conn, payload []byte) error {
	mask := [4]byte{1, 2, 3, 4}
	head := []byte{0x81}
	l := len(payload)
	switch {
	case l < 126:
		head = append(head, 0x80|byte(l))
	case l <= 65535:
		head = append(head, 0x80|126, byte(l>>8), byte(l))
	default:
		head = append(head, 0x80|127)
		n := uint64(l)
		ext := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			ext[i] = byte(n & 0xFF)
			n >>= 8
		}
		head = append(head, ext...)
	}
	if _, err := conn.Write(head); err != nil {
		return err
	}
	if _, err := conn.Write(mask[:]); err != nil {
		return err
	}
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	_, err := conn.Write(masked)
	return err
}

func readServerTextFrame(conn net.Conn) ([]byte, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(conn, h); err != nil {
		return nil, err
	}
	if h[0]&0x0F != 0x1 {
		return nil, fmt.Errorf("unexpected opcode: %d", h[0]&0x0F)
	}
	l := int(h[1] & 0x7F)
	if l == 126 {
		ext := make([]byte, 2)
		if _, err := io.ReadFull(conn, ext); err != nil {
			return nil, err
		}
		l = int(ext[0])<<8 | int(ext[1])
	} else if l == 127 {
		ext := make([]byte, 8)
		if _, err := io.ReadFull(conn, ext); err != nil {
			return nil, err
		}
		var n uint64
		for _, b := range ext {
			n = (n << 8) | uint64(b)
		}
		l = int(n)
	}
	p := make([]byte, l)
	_, err := io.ReadFull(conn, p)
	return p, err
}
