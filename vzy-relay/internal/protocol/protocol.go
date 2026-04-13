package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

var uuidPattern = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[1-5][a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$`)

type Operation struct {
	Type      string          `json:"type"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value,omitempty"`
	ClientID  string          `json:"clientId"`
	Timestamp int64           `json:"timestamp"`
	RoomID    string          `json:"roomId"`
	OpID      string          `json:"opId"`
	Sequence  uint64          `json:"sequence,omitempty"`
}

type Envelope struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type Ack struct {
	Type      string `json:"type"`
	OpID      string `json:"opId"`
	RoomID    string `json:"roomId"`
	Sequence  uint64 `json:"sequence"`
	Committed bool   `json:"committed"`
}

func ValidateOperation(op Operation) error {
	if op.Type != "SET" && op.Type != "DELETE" {
		return errors.New("invalid operation type")
	}
	if op.Key == "" {
		return errors.New("key is required")
	}
	if op.ClientID == "" || op.RoomID == "" || op.OpID == "" {
		return errors.New("clientId, roomId and opId are required")
	}
	if !uuidPattern.MatchString(op.OpID) {
		return fmt.Errorf("invalid opId: %s", op.OpID)
	}
	return nil
}

func ParseOperationEnvelope(payload []byte) (Operation, error) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return Operation{}, err
	}
	if env.Event != "operation" {
		return Operation{}, fmt.Errorf("unexpected event: %s", env.Event)
	}
	var op Operation
	if err := json.Unmarshal(env.Data, &op); err != nil {
		return Operation{}, err
	}
	if err := ValidateOperation(op); err != nil {
		return Operation{}, err
	}
	return op, nil
}

func MarshalBroadcast(op Operation) ([]byte, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{Event: "operation_broadcast", Data: data})
}

func MarshalSystemEvent(event string, data any) ([]byte, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{Event: event, Data: raw})
}

func ParseAck(payload []byte) (Ack, bool) {
	var ack Ack
	if err := json.Unmarshal(payload, &ack); err != nil {
		return Ack{}, false
	}
	if ack.Type != "ACK" || ack.OpID == "" || ack.RoomID == "" || ack.Sequence == 0 {
		return Ack{}, false
	}
	return ack, true
}

func MarshalAck(opID, roomID string, sequence uint64, committed bool) ([]byte, error) {
	return json.Marshal(Ack{Type: "ACK", OpID: opID, RoomID: roomID, Sequence: sequence, Committed: committed})
}
