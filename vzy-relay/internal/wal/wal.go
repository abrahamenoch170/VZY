package wal

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"vzy-relay/internal/protocol"
)

type WAL struct {
	mu    sync.Mutex
	path  string
	file  *os.File
	size  int64
	opIDs map[string]struct{}
}

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WAL{path: path, file: f, size: st.Size(), opIDs: make(map[string]struct{})}, nil
}

func (w *WAL) Append(op protocol.Operation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.opIDs[op.OpID]; exists {
		return nil
	}

	payload, err := json.Marshal(op)
	if err != nil {
		return err
	}
	encoded := base64.StdEncoding.EncodeToString(payload)
	line := fmt.Sprintf("%s,%d,%s,%s,%s\n", op.RoomID, op.Sequence, op.OpID, op.ClientID, encoded)
	n, err := w.file.WriteString(line)
	if err != nil {
		return err
	}
	w.size += int64(n)
	w.opIDs[op.OpID] = struct{}{}
	return w.file.Sync()
}

func (w *WAL) Recover() (map[string][]protocol.Operation, map[string]uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.Open(w.path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	byRoom := make(map[string][]protocol.Operation)
	seqByRoom := make(map[string]uint64)
	seen := make(map[string]struct{})
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		parts := strings.SplitN(line, ",", 5)
		if len(parts) != 5 {
			continue
		}
		seq, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(parts[4])
		if err != nil {
			continue
		}
		var op protocol.Operation
		if err := json.Unmarshal(raw, &op); err != nil {
			continue
		}
		op.Sequence = seq
		if _, exists := seen[op.OpID]; exists {
			continue
		}
		seen[op.OpID] = struct{}{}
		byRoom[op.RoomID] = append(byRoom[op.RoomID], op)
		if seq > seqByRoom[op.RoomID] {
			seqByRoom[op.RoomID] = seq
		}
	}
	if err := s.Err(); err != nil {
		return nil, nil, err
	}
	w.opIDs = seen
	return byRoom, seqByRoom, nil
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
