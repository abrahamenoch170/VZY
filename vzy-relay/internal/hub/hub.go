package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"vzy-relay/internal/config"
	"vzy-relay/internal/dedupe"
	"vzy-relay/internal/ledger"
	"vzy-relay/internal/mesh"
	"vzy-relay/internal/metrics"
	"vzy-relay/internal/protocol"
	"vzy-relay/internal/sequence"
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
	isRemote     bool
	nodeID       string
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
	stopOnce    sync.Once
	sequence    uint64
	pendingAcks map[string]map[string]protocol.Operation

	cfg     config.Config
	metrics *metrics.Metrics
	store   *store.MemoryStore
	wal     *wal.WAL
	ledger  *ledger.CommitLedger
	logger  *slog.Logger
}

type Hub struct {
	mu      sync.RWMutex
	rooms   map[string]*Room
	cfg     config.Config
	metrics *metrics.Metrics
	store   *store.MemoryStore
	wal     *wal.WAL
	ledger  *ledger.CommitLedger
	dedup   dedupe.DedupStore
	seq     sequence.Provider
	logger  *slog.Logger

	publishCrossNode func(mesh.Event)
}

func New(cfg config.Config, m *metrics.Metrics, st *store.MemoryStore, w *wal.WAL, logger *slog.Logger) *Hub {
	return &Hub{rooms: make(map[string]*Room), cfg: cfg, metrics: m, store: st, wal: w, ledger: ledger.NewCommitLedger(), dedup: dedupe.NewLRUTTLStore(cfg.DedupCapacity, timeDurationSeconds(cfg.DedupTTLSeconds)), seq: sequence.LocalProvider{}, logger: logger}
}

func timeDurationSeconds(sec int) (d time.Duration) { return time.Duration(sec) * time.Second }

func (h *Hub) SetSequenceProvider(p sequence.Provider) {
	if p != nil {
		h.seq = p
	}
}

func (h *Hub) SetCrossNodePublisher(fn func(mesh.Event)) { h.publishCrossNode = fn }

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
	if entry, ok := h.ledger.Get(op.OpID); ok && entry.Committed {
		if room := h.getRoom(roomID); room != nil {
			room.sendAckToClient(fromClientID, op.OpID, entry.Sequence, true)
		}
		return nil
	}
	room := h.getRoom(roomID)
	if room == nil {
		room = h.getOrCreateRoom(roomID)
	}
	room.broadcast <- roomBroadcast{fromClientID: fromClientID, op: op}
	return nil
}

func (h *Hub) ApplyReplicatedEvent(ev mesh.Event) {
	if ev.NodeID == h.cfg.NodeID {
		return
	}
	if h.dedup.Seen(ev.OpID) {
		h.metrics.IncDedupHits()
		return
	}
	h.dedup.Mark(ev.OpID)
	if entry, ok := h.ledger.Get(ev.OpID); ok && entry.Committed {
		return
	}
	op := protocol.Operation{Type: ev.Type, Key: ev.Key, Value: ev.Value, ClientID: ev.ClientID, Timestamp: ev.Timestamp, RoomID: ev.RoomID, OpID: ev.OpID, Sequence: ev.Sequence}
	room := h.getRoom(ev.RoomID)
	if room == nil {
		room = h.getOrCreateRoom(ev.RoomID)
	}
	room.broadcast <- roomBroadcast{op: op, isRemote: true, nodeID: ev.NodeID}
	h.metrics.IncPubSubReceive()
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
		if entry, ok := h.ledger.Get(op.OpID); ok && entry.Committed {
			payload, err := protocol.MarshalBroadcast(op)
			if err != nil {
				continue
			}
			if !c.Send(payload) {
				return
			}
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
		r.stop()
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
		for _, op := range ops {
			h.ledger.Restore(op.OpID, ledger.Entry{Committed: true, Sequence: op.Sequence, ClientID: op.ClientID, RoomID: op.RoomID})
			h.dedup.Mark(op.OpID)
		}
		if h.store != nil {
			h.store.Restore(ops)
		}
	}
}

func (h *Hub) getRoom(roomID string) *Room {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rooms[roomID]
}

func (h *Hub) getOrCreateRoom(roomID string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.rooms[roomID]; ok {
		return r
	}
	r := &Room{id: roomID, clients: make(map[string]Client), register: make(chan Client), unregister: make(chan Client), broadcast: make(chan roomBroadcast, h.cfg.BufferSize), ack: make(chan ackMessage, h.cfg.BufferSize), done: make(chan struct{}), pendingAcks: make(map[string]map[string]protocol.Operation), cfg: h.cfg, metrics: h.metrics, store: h.store, wal: h.wal, ledger: h.ledger, logger: h.logger.With("roomId", roomID)}
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
		r.stop()
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
			if entry, exists := r.ledger.Get(msg.op.OpID); exists && entry.Committed {
				if !msg.isRemote {
					r.sendAckToClient(msg.fromClientID, msg.op.OpID, entry.Sequence, true)
				}
				continue
			}

			if msg.isRemote {
				r.sequence = maxU64(r.sequence, msg.op.Sequence)
			} else {
				seq, err := h.seq.Next(r.id, func() uint64 { r.sequence++; return r.sequence })
				if err != nil {
					r.metrics.IncDroppedEvents()
					continue
				}
				r.sequence = maxU64(r.sequence, seq)
				msg.op.Sequence = seq
			}

			if r.wal != nil {
				if err := r.wal.Append(msg.op); err != nil {
					r.logger.Error("wal append failed", "error", err)
					continue
				}
				r.metrics.SetWALSize(r.wal.Size())
			}
			r.ledger.MarkCommitted(msg.op.OpID, msg.op.RoomID, msg.op.ClientID, msg.op.Sequence)
			h.dedup.Mark(msg.op.OpID)
			if r.store != nil {
				r.store.Append(msg.op)
			}

			if !msg.isRemote {
				r.sendAckToClient(msg.fromClientID, msg.op.OpID, msg.op.Sequence, true)
			}
			payload, err := protocol.MarshalBroadcast(msg.op)
			if err != nil {
				continue
			}
			r.metrics.IncMessagesIn()
			for id, c := range r.clients {
				if id == msg.fromClientID && !msg.isRemote {
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

			if !msg.isRemote && h.publishCrossNode != nil {
				ev := mesh.Event{OpID: msg.op.OpID, RoomID: msg.op.RoomID, ClientID: msg.op.ClientID, NodeID: h.cfg.NodeID, Sequence: msg.op.Sequence, Type: msg.op.Type, Key: msg.op.Key, Value: msg.op.Value, Timestamp: msg.op.Timestamp}
				h.publishCrossNode(ev)
			}
		case <-r.done:
			for _, c := range r.clients {
				c.Close(1001, "server shutting down")
			}
			return
		}
	}
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (r *Room) sendAckToClient(clientID, opID string, sequence uint64, committed bool) {
	c := r.clients[clientID]
	if c == nil {
		return
	}
	payload, err := protocol.MarshalAck(opID, r.id, sequence, committed)
	if err != nil {
		return
	}
	if c.Send(payload) {
		r.metrics.IncMessagesOut()
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

func (r *Room) stop() { r.stopOnce.Do(func() { close(r.done) }) }
