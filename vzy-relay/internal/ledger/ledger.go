package ledger

import "sync"

type Entry struct {
	Committed bool
	Sequence  uint64
	ClientID  string
	RoomID    string
}

type CommitLedger struct {
	mu      sync.RWMutex
	commits map[string]Entry
}

func NewCommitLedger() *CommitLedger {
	return &CommitLedger{commits: make(map[string]Entry)}
}

func (cl *CommitLedger) MarkCommitted(opID, roomID, clientID string, sequence uint64) Entry {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	entry := Entry{Committed: true, Sequence: sequence, ClientID: clientID, RoomID: roomID}
	cl.commits[opID] = entry
	return entry
}

func (cl *CommitLedger) Get(opID string) (Entry, bool) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	entry, ok := cl.commits[opID]
	return entry, ok
}

func (cl *CommitLedger) Restore(opID string, entry Entry) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if current, ok := cl.commits[opID]; ok && current.Sequence >= entry.Sequence {
		return
	}
	cl.commits[opID] = entry
}
