package metrics

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	activeConnections atomic.Int64
	rooms             atomic.Int64
	messagesIn        atomic.Int64
	messagesOut       atomic.Int64
	droppedMessages   atomic.Int64
	pendingAcks       atomic.Int64
	walSizeBytes      atomic.Int64
	lastMsgIn         atomic.Int64
	lastTick          atomic.Int64
	messagesPerSecond atomic.Int64
}

func New() *Metrics {
	m := &Metrics{}
	m.lastTick.Store(time.Now().Unix())
	return m
}

func (m *Metrics) IncConnections()     { m.activeConnections.Add(1) }
func (m *Metrics) DecConnections()     { m.activeConnections.Add(-1) }
func (m *Metrics) SetRooms(n int)      { m.rooms.Store(int64(n)) }
func (m *Metrics) IncMessagesIn()      { m.messagesIn.Add(1); m.updateRate() }
func (m *Metrics) IncMessagesOut()     { m.messagesOut.Add(1) }
func (m *Metrics) IncDroppedMessages() { m.droppedMessages.Add(1) }
func (m *Metrics) IncPendingAcks()     { m.pendingAcks.Add(1) }
func (m *Metrics) DecPendingAcks()     { m.pendingAcks.Add(-1) }
func (m *Metrics) SetWALSize(n int64)  { m.walSizeBytes.Store(n) }

func (m *Metrics) updateRate() {
	now := time.Now().Unix()
	prevTick := m.lastTick.Load()
	if now != prevTick && m.lastTick.CompareAndSwap(prevTick, now) {
		prev := m.lastMsgIn.Swap(m.messagesIn.Load())
		m.messagesPerSecond.Store(m.messagesIn.Load() - prev)
	}
}

func (m *Metrics) Snapshot() map[string]int64 {
	m.updateRate()
	return map[string]int64{
		"active_connections":  m.activeConnections.Load(),
		"total_rooms":         m.rooms.Load(),
		"messages_in":         m.messagesIn.Load(),
		"messages_out":        m.messagesOut.Load(),
		"messages_per_second": m.messagesPerSecond.Load(),
		"dropped_messages":    m.droppedMessages.Load(),
		"pending_acks":        m.pendingAcks.Load(),
		"wal_size_bytes":      m.walSizeBytes.Load(),
	}
}
