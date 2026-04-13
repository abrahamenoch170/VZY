package ws

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"vzy-relay/internal/config"
	"vzy-relay/internal/hub"
	"vzy-relay/internal/protocol"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Client struct {
	id           string
	roomID       string
	lastSequence uint64
	conn         net.Conn
	connMu       sync.Mutex
	rw           *bufio.ReadWriter
	send         chan []byte
	hub          *hub.Hub
	logger       *slog.Logger
	cfg          config.Config
	closeOnce    sync.Once
	closedChan   chan struct{}
}

func (c *Client) ID() string           { return c.id }
func (c *Client) RoomID() string       { return c.roomID }
func (c *Client) LastSequence() uint64 { return c.lastSequence }

func (c *Client) Send(data []byte) bool {
	select {
	case c.send <- data:
		return true
	default:
		if c.cfg.SlowClientPolicy == config.SlowClientDisconnect {
			c.Close(1011, "send queue full")
			return false
		}
		select {
		case <-c.send:
		default:
		}
		select {
		case c.send <- data:
			return true
		default:
			return false
		}
	}
}

func (c *Client) Close(_ int, _ string) {
	c.closeOnce.Do(func() {
		c.connMu.Lock()
		_ = writeFrame(c.rw, 0x8, nil)
		_ = c.conn.Close()
		c.connMu.Unlock()
		close(c.closedChan)
	})
}

type Server struct {
	hub    *hub.Hub
	cfg    config.Config
	logger *slog.Logger
}

func NewServer(h *hub.Hub, cfg config.Config, logger *slog.Logger) *Server {
	return &Server{hub: h, cfg: cfg, logger: logger}
}

func (s *Server) Handler(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("roomId")
	clientID := r.URL.Query().Get("clientId")
	lastSequence, _ := strconv.ParseUint(r.URL.Query().Get("lastSequence"), 10, 64)
	if roomID == "" || clientID == "" {
		http.Error(w, "roomId and clientId are required", http.StatusBadRequest)
		return
	}
	if !isWebSocketRequest(r) {
		http.Error(w, "websocket upgrade required", http.StatusUpgradeRequired)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		s.logger.Error("hijack failed", "error", err)
		return
	}
	if err := writeUpgradeResponse(rw, r.Header.Get("Sec-WebSocket-Key")); err != nil {
		_ = conn.Close()
		s.logger.Error("upgrade response failed", "error", err)
		return
	}

	client := &Client{id: clientID, roomID: roomID, lastSequence: lastSequence, conn: conn, rw: rw, send: make(chan []byte, s.cfg.WriteQueueSize), hub: s.hub, logger: s.logger.With("clientId", clientID, "roomId", roomID), cfg: s.cfg, closedChan: make(chan struct{})}

	s.hub.Register(client)
	s.hub.Replay(roomID, client)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go client.writeLoop(ctx)
	client.readLoop()
	s.hub.Unregister(client)
	client.Close(1000, "disconnect")
}

func (c *Client) readLoop() {
	defer c.recoverPanic("readLoop")
	for {
		opcode, payload, err := readFrame(c.rw)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				c.logger.Info("read closed", "error", err)
			}
			return
		}
		switch opcode {
		case 0x8:
			return
		case 0x1:
			if ack, ok := protocol.ParseAck(payload); ok {
				if ack.RoomID == c.roomID {
					c.hub.Ack(ack.RoomID, c.id, ack.OpID)
				}
				continue
			}
			op, err := protocol.ParseOperationEnvelope(payload)
			if err != nil {
				c.logger.Warn("invalid operation", "error", err)
				continue
			}
			if op.RoomID != c.roomID || op.ClientID != c.id {
				c.logger.Warn("room/client mismatch", "opRoom", op.RoomID, "opClient", op.ClientID)
				continue
			}
			if err := c.hub.Publish(c.roomID, c.id, op); err != nil {
				c.logger.Error("publish failed", "error", err)
			}
		}
	}
}

func (c *Client) writeLoop(ctx context.Context) {
	defer c.recoverPanic("writeLoop")
	heartbeat := time.NewTicker(time.Duration(c.cfg.HeartbeatIntervalSec) * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case msg := <-c.send:
			if err := c.write(msg); err != nil {
				return
			}
		case <-heartbeat.C:
			if err := c.write(hub.MarshalHeartbeat(c.roomID)); err != nil {
				return
			}
		case <-c.closedChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) write(payload []byte) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return writeFrame(c.rw, 0x1, payload)
}

func isWebSocketRequest(r *http.Request) bool {
	return r.Header.Get("Upgrade") == "websocket" && r.Header.Get("Sec-WebSocket-Key") != ""
}

func writeUpgradeResponse(rw *bufio.ReadWriter, secKey string) error {
	h := sha1.Sum([]byte(secKey + wsGUID))
	accept := base64.StdEncoding.EncodeToString(h[:])
	resp := fmt.Sprintf("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
	if _, err := rw.WriteString(resp); err != nil {
		return err
	}
	return rw.Flush()
}

func readFrame(rw *bufio.ReadWriter) (byte, []byte, error) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(rw, hdr); err != nil {
		return 0, nil, err
	}
	opcode := hdr[0] & 0x0F
	masked := hdr[1]&0x80 != 0
	payloadLen := int(hdr[1] & 0x7F)
	if payloadLen == 126 {
		ext := make([]byte, 2)
		if _, err := io.ReadFull(rw, ext); err != nil {
			return 0, nil, err
		}
		payloadLen = int(ext[0])<<8 | int(ext[1])
	} else if payloadLen == 127 {
		ext := make([]byte, 8)
		if _, err := io.ReadFull(rw, ext); err != nil {
			return 0, nil, err
		}
		var n uint64
		for _, b := range ext {
			n = (n << 8) | uint64(b)
		}
		payloadLen = int(n)
	}

	maskKey := make([]byte, 4)
	if masked {
		if _, err := io.ReadFull(rw, maskKey); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(rw, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := 0; i < len(payload); i++ {
			payload[i] ^= maskKey[i%4]
		}
	}
	return opcode, payload, nil
}

func writeFrame(rw *bufio.ReadWriter, opcode byte, payload []byte) error {
	finOpcode := 0x80 | opcode
	if err := rw.WriteByte(byte(finOpcode)); err != nil {
		return err
	}
	l := len(payload)
	switch {
	case l < 126:
		if err := rw.WriteByte(byte(l)); err != nil {
			return err
		}
	case l <= 65535:
		if err := rw.WriteByte(126); err != nil {
			return err
		}
		if _, err := rw.Write([]byte{byte(l >> 8), byte(l)}); err != nil {
			return err
		}
	default:
		if err := rw.WriteByte(127); err != nil {
			return err
		}
		ext := make([]byte, 8)
		n := uint64(l)
		for i := 7; i >= 0; i-- {
			ext[i] = byte(n & 0xFF)
			n >>= 8
		}
		if _, err := rw.Write(ext); err != nil {
			return err
		}
	}
	if _, err := rw.Write(payload); err != nil {
		return err
	}
	return rw.Flush()
}

func (c *Client) recoverPanic(loop string) {
	if rec := recover(); rec != nil {
		c.logger.Error("panic recovered", "loop", loop, "recover", rec)
	}
}
