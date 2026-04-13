package dedupe

import (
	"container/list"
	"sync"
	"time"
)

type DedupStore interface {
	Seen(opID string) bool
	Mark(opID string)
}

type lruEntry struct {
	opID      string
	expiresAt time.Time
}

type LRUTTLStore struct {
	mu       sync.Mutex
	cap      int
	ttl      time.Duration
	ll       *list.List
	items    map[string]*list.Element
	nowFn    func() time.Time
	hitCount uint64
}

func NewLRUTTLStore(capacity int, ttl time.Duration) *LRUTTLStore {
	return &LRUTTLStore{cap: capacity, ttl: ttl, ll: list.New(), items: make(map[string]*list.Element), nowFn: time.Now}
}

func (s *LRUTTLStore) Seen(opID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked()
	e, ok := s.items[opID]
	if !ok {
		return false
	}
	entry := e.Value.(lruEntry)
	if s.nowFn().After(entry.expiresAt) {
		s.removeLocked(opID, e)
		return false
	}
	s.ll.MoveToFront(e)
	return true
}

func (s *LRUTTLStore) Mark(opID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked()
	if e, ok := s.items[opID]; ok {
		e.Value = lruEntry{opID: opID, expiresAt: s.nowFn().Add(s.ttl)}
		s.ll.MoveToFront(e)
		return
	}
	e := s.ll.PushFront(lruEntry{opID: opID, expiresAt: s.nowFn().Add(s.ttl)})
	s.items[opID] = e
	for len(s.items) > s.cap {
		back := s.ll.Back()
		if back == nil {
			break
		}
		entry := back.Value.(lruEntry)
		s.removeLocked(entry.opID, back)
	}
}

func (s *LRUTTLStore) evictExpiredLocked() {
	now := s.nowFn()
	for el := s.ll.Back(); el != nil; {
		prev := el.Prev()
		entry := el.Value.(lruEntry)
		if now.After(entry.expiresAt) {
			s.removeLocked(entry.opID, el)
		}
		el = prev
	}
}

func (s *LRUTTLStore) removeLocked(opID string, e *list.Element) {
	delete(s.items, opID)
	s.ll.Remove(e)
}
