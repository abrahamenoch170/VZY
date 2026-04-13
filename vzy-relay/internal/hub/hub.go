package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"vzy-relay/internal/config"
	"vzy-relay/internal/dedupe"
	"vzy-relay/internal/metrics"
	"vzy-relay/internal/protocol"
	"vzy-relay/internal/store"
	"vzy-relay/internal/wal"
)

type Client interface {
	ID() string
	RoomID() string
	LastSequence() uint64
	Send(data []byte) bool
	Close(code int, reason string)
}

type roomBroadcast struct {
	fromClientID string
	op           protocol.Operation
}

type ackMessage struct {
	clientID string
	opID     string
}

type Room struct {
	id          string
	clients     map[string]Client
	register    chan Client
	unregister  chan Client
	broadcast   chan roomBroadcast
	ack         chan ackMessage
	done        chan struct{}
	sequence    uint64
	pendingAcks map[string]map[string]protocol.Operation

	cfg     config.Config
	metrics *metrics.Metrics
	store   *store.MemoryStore
	wal     *wal.WAL
	logger  *slog.Logger
}

type Hub struct {
	mu      sync.RWMutex
	rooms   map[string]*Room
	cfg     config.Config
	metrics *metrics.Metrics
	store   *store.MemoryStore
	wal     *wal.WAL
	dedupe  *dedupe.Cache
	logger  *slog.Logger
}

func New(cfg config.Config, m *metrics.Metrics, st *store.MemoryStore, w *wal.WAL, logger *slog.Logger) *Hub {
	return &Hub{rooms: make(map[string]*Room), cfg: cfg, metrics: m, store: st, wal: w, dedupe: dedupe.New(time.Duration(cfg.DedupeTTLSec) * time.Second), logger: logger}
}

func (h *Hub) Register(c Client) {
	room := h.getOrCreateRoom(c.RoomID())
	room.register <- c
}

func (h *Hub) Unregister(c Client) {
	h.mu.RLock()
	room := h.rooms[c.RoomID()]
	h.mu.RUnlock()
	if room == nil {
		return
	}
	room.unregister <- c
}

func (h *Hub) Publish(roomID, fromClientID string, op protocol.Operation) error {
	if h.dedupe.SeenOrAdd(op.OpID) {
		return nil
	}
	h.mu.RLock()
	room := h.rooms[roomID]
	h.mu.RUnlock()
	if room == nil {
		room = h.getOrCreateRoom(roomID)
	}
	room.broadcast <- roomBroadcast{fromClientID: fromClientID, op: op}
	return nil
}

func (h *Hub) Ack(roomID, clientID, opID string) {
	h.mu.RLock()
	room := h.rooms[roomID]
	h.mu.RUnlock()
	if room == nil {
		return
	}
	room.ack <- ackMessage{clientID: clientID, opID: opID}
}

func (h *Hub) Replay(roomID string, c Client) {
	if h.store == nil {
		return
	}
	for _, op := range h.store.ReplayAfterSequence(roomID, c.LastSequence()) {
		payload, err := protocol.MarshalBroadcast(op)
		if err != nil {
			continue
		}
		if !c.Send(payload) {
			return
		}
	}
}

func (h *Hub) Shutdown(ctx context.Context) {
	h.mu.Lock()
	rooms := make([]*Room, 0, len(h.rooms))
	for _, r := range h.rooms {
		rooms = append(rooms, r)
	}
	h.mu.Unlock()

	for _, r := range rooms {
		close(r.done)
	}

	for _, r := range rooms {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
		}
	}
}

func (h *Hub) RestoreFromWAL(opsByRoom map[string][]protocol.Operation, seqByRoom map[string]uint64) {
	for roomID, ops := range opsByRoom {
		r := h.getOrCreateRoom(roomID)
		r.sequence = seqByRoom[roomID]
		if h.store != nil {
			h.store.Restore(ops)
		}
	}
}

func (h *Hub) getOrCreateRoom(roomID string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.rooms[roomID]; ok {
		return r
	}
	r := &Room{
		id:          roomID,
		clients:     make(map[string]Client),
		register:    make(chan Client),
		unregister:  make(chan Client),
		broadcast:   make(chan roomBroadcast, h.cfg.BufferSize),
		ack:         make(chan ackMessage, h.cfg.BufferSize),
		done:        make(chan struct{}),
		pendingAcks: make(map[string]map[string]protocol.Operation),
		cfg:         h.cfg,
		metrics:     h.metrics,
		store:       h.store,
		wal:         h.wal,
		logger:      h.logger.With("roomId", roomID),
	}
	h.rooms[roomID] = r
	h.metrics.SetRooms(len(h.rooms))
	go r.run(h)
	return r
}

func (r *Room) run(h *Hub) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error("room panic", "recover", rec)
		}

		h.mu.Lock()
		delete(h.rooms, r.id)
		h.metrics.SetRooms(len(h.rooms))
		h.mu.Unlock()
		close(r.done)
	}()

	for {
		select {
		case c := <-r.register:
			if len(r.clients) >= r.cfg.MaxConnectionsPerRoom {
				c.Close(1008, "room connection limit reached")
				continue
			}
			r.clients[c.ID()] = c
			r.metrics.IncConnections()
			r.pendingAcks[c.ID()] = make(map[string]protocol.Operation)
			r.sendSystem("client_joined", map[string]string{"clientId": c.ID(), "roomId": r.id}, "")
		case c := <-r.unregister:
			if _, ok := r.clients[c.ID()]; !ok {
				continue
			}
			pending := r.pendingAcks[c.ID()]
			for range pending {
				r.metrics.DecPendingAcks()
			}
			delete(r.pendingAcks, c.ID())
			delete(r.clients, c.ID())
			r.metrics.DecConnections()
			r.sendSystem("client_left", map[string]string{"clientId": c.ID(), "roomId": r.id}, "")
			if len(r.clients) == 0 {
				return
			}
		case ack := <-r.ack:
			pending := r.pendingAcks[ack.clientID]
			if pending == nil {
				continue
			}
			if _, ok := pending[ack.opID]; ok {
				delete(pending, ack.opID)
				r.metrics.DecPendingAcks()
			}
		case msg := <-r.broadcast:
			r.sequence++
			msg.op.Sequence = r.sequence
			payload, err := protocol.MarshalBroadcast(msg.op)
			if err != nil {
				continue
			}
			if r.wal != nil {
				if err := r.wal.Append(msg.op); err != nil {
					r.logger.Error("wal append failed", "error", err)
					continue
				}
				r.metrics.SetWALSize(r.wal.Size())
			}
			if r.store != nil {
				r.store.Append(msg.op)
			}
			r.metrics.IncMessagesIn()
			for id, c := range r.clients {
				if id == msg.fromClientID {
					continue
				}
				if ok := c.Send(payload); !ok {
					r.metrics.IncDroppedMessages()
					if r.cfg.SlowClientPolicy == config.SlowClientDisconnect {
						c.Close(1011, "slow consumer")
						delete(r.clients, id)
						r.metrics.DecConnections()
					}
					continue
				}
				pending := r.pendingAcks[id]
				if pending == nil {
					pending = make(map[string]protocol.Operation)
					r.pendingAcks[id] = pending
				}
				pending[msg.op.OpID] = msg.op
				r.metrics.IncPendingAcks()
				r.metrics.IncMessagesOut()
			}
		case <-r.done:
			for _, c := range r.clients {
				c.Close(1001, "server shutting down")
			}
			return
		}
	}
}

func (r *Room) sendSystem(event string, data any, skipClientID string) {
	payload, err := protocol.MarshalSystemEvent(event, data)
	if err != nil {
		r.logger.Warn("marshal system event failed", "event", event, "error", err)
		return
	}
	for id, c := range r.clients {
		if id == skipClientID {
			continue
		}
		if c.Send(payload) {
			r.metrics.IncMessagesOut()
		}
	}
}

func MarshalHeartbeat(roomID string) []byte {
	payload, _ := json.Marshal(map[string]string{"roomId": roomID, "status": "ok"})
	msg, _ := json.Marshal(protocol.Envelope{Event: "heartbeat", Data: payload})
	return msg
}
