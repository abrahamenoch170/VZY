package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

var uuidV4Regex = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$`)

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
	Type     string `json:"type"`
	OpID     string `json:"opId"`
	RoomID   string `json:"roomId"`
	Sequence uint64 `json:"sequence"`
}

func ParseOperationEnvelope(payload []byte) (Operation, error) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return Operation{}, fmt.Errorf("decode envelope: %w", err)
	}
	if env.Event != "operation" {
		return Operation{}, fmt.Errorf("unsupported event: %s", env.Event)
	}
	if len(env.Data) == 0 {
		return Operation{}, errors.New("missing data")
	}
	var op Operation
	if err := json.Unmarshal(env.Data, &op); err != nil {
		return Operation{}, fmt.Errorf("decode operation: %w", err)
	}
	if err := ValidateOperation(op); err != nil {
		return Operation{}, err
	}
	return op, nil
}

func ParseAck(payload []byte) (Ack, bool) {
	var ack Ack
	if err := json.Unmarshal(payload, &ack); err != nil {
		return Ack{}, false
	}
	if ack.Type != "ACK" || ack.OpID == "" || ack.RoomID == "" {
		return Ack{}, false
	}
	return ack, true
}

func ValidateOperation(op Operation) error {
	if op.Type != "SET" && op.Type != "DELETE" {
		return errors.New("type must be SET or DELETE")
	}
	if op.Key == "" || op.ClientID == "" || op.RoomID == "" || op.OpID == "" || op.Timestamp == 0 {
		return errors.New("missing required fields")
	}
	if !uuidV4Regex.MatchString(op.OpID) {
		return errors.New("opId must be UUIDv4")
	}
	if op.Type == "DELETE" {
		return nil
	}
	if op.Type == "SET" && len(op.Value) == 0 {
		return errors.New("value required for SET")
	}
	return nil
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
