package mesh

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"vzy-relay/internal/metrics"
	"vzy-relay/internal/pubsub"
)

type Service struct {
	nodeID string
	bus    pubsub.PubSub
	m      *metrics.Metrics
	log    *slog.Logger
	queue  chan Event
}

func NewService(nodeID string, bus pubsub.PubSub, m *metrics.Metrics, log *slog.Logger) *Service {
	s := &Service{nodeID: nodeID, bus: bus, m: m, log: log, queue: make(chan Event, 4096)}
	go s.publishLoop()
	return s
}

func (s *Service) Publish(ev Event) {
	select {
	case s.queue <- ev:
	default:
		s.m.IncDroppedEvents()
	}
}

func (s *Service) SubscribeRoom(_ string, handler func(Event)) error {
	return s.bus.Subscribe(globalChannel(), func(data []byte) {
		var ev Event
		if err := json.Unmarshal(data, &ev); err != nil {
			return
		}
		s.m.ObserveCrossNodeLatency(time.Now().UnixMilli() - ev.Timestamp)
		handler(ev)
	})
}

func (s *Service) SubscribeAll(handler func(Event)) error {
	return s.bus.Subscribe("room:*", func(data []byte) {
		var ev Event
		if err := json.Unmarshal(data, &ev); err == nil {
			handler(ev)
		}
	})
}

func (s *Service) Close() error { return s.bus.Close() }

func (s *Service) publishLoop() {
	for ev := range s.queue {
		payload, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		channel := roomChannel(ev.RoomID)
		if err := retryPublish(s.bus, channel, payload); err != nil {
			s.log.Warn("mesh publish failed", "error", err, "roomId", ev.RoomID)
			s.m.IncDroppedEvents()
			continue
		}
		s.m.IncPubSubPublish()
	}
}

func retryPublish(bus pubsub.PubSub, channel string, payload []byte) error {
	backoff := 25 * time.Millisecond
	for i := 0; i < 5; i++ {
		if err := bus.Publish(channel, payload); err == nil {
			_ = bus.Publish(globalChannel(), payload)
			return nil
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("publish retry exhausted")
}

func roomChannel(roomID string) string { return "room:" + roomID }

func globalChannel() string { return "mesh.events" }
