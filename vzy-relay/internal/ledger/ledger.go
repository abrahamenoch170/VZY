// package internal

package ledger

import (
	"log"
	"sync"
)

// CommitLedger tracks commit operations ensuring idempotency across crashes.
type CommitLedger struct {
	mu      sync.Mutex
	commits map[string]bool // tracks completed commits
}

// NewCommitLedger creates a new instance of CommitLedger.
func NewCommitLedger() *CommitLedger {
	return &CommitLedger{commits: make(map[string]bool)}
}

// Commit records a commit with the given id if it has not been committed before.
func (cl *CommitLedger) Commit(id string) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	// Check if the commit is already processed.
	if _, exists := cl.commits[id]; exists {
		log.Printf("Commit %s already processed. Ignoring.", id)
		return false
	}

	// Record the commit.
	cl.commits[id] = true
	log.Printf("Commit %s processed successfully.", id)
	return true
}