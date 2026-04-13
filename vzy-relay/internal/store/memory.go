package store

import (
	"sync"

	"vzy-relay/internal/protocol"
)

type MemoryStore struct {
	mu      sync.RWMutex
	limit   int
	byRoom  map[string][]protocol.Operation
	opIndex map[string]map[string]struct{}
}

func New(limit int) *MemoryStore {
	return &MemoryStore{limit: limit, byRoom: make(map[string][]protocol.Operation), opIndex: make(map[string]map[string]struct{})}
}

func (s *MemoryStore) Append(op protocol.Operation) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.opIndex[op.RoomID]
	if !ok {
		idx = make(map[string]struct{})
		s.opIndex[op.RoomID] = idx
	}
	if _, exists := idx[op.OpID]; exists {
		return false
	}
	ops := s.byRoom[op.RoomID]
	ops = append(ops, op)
	idx[op.OpID] = struct{}{}
	if len(ops) > s.limit {
		oldest := ops[0]
		delete(idx, oldest.OpID)
		ops = ops[1:]
	}
	s.byRoom[op.RoomID] = ops
	return true
}

func (s *MemoryStore) Restore(ops []protocol.Operation) {
	for _, op := range ops {
		s.Append(op)
	}
}

func (s *MemoryStore) Replay(roomID string) []protocol.Operation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ops := s.byRoom[roomID]
	out := make([]protocol.Operation, len(ops))
	copy(out, ops)
	return out
}

func (s *MemoryStore) ReplayAfterSequence(roomID string, lastSequence uint64) []protocol.Operation {
	ops := s.Replay(roomID)
	out := make([]protocol.Operation, 0, len(ops))
	for _, op := range ops {
		if op.Sequence > lastSequence {
			out = append(out, op)
		}
	}
	return out
}
