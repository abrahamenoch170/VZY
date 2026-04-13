package protocol

import "testing"

func TestValidateOperation(t *testing.T) {
	t.Parallel()
	valid := Operation{Type: "SET", Key: "k", Value: []byte(`{"a":1}`), ClientID: "c1", Timestamp: 1, RoomID: "r1", OpID: "123e4567-e89b-42d3-a456-426614174000"}
	if err := ValidateOperation(valid); err != nil {
		t.Fatalf("expected valid op, got: %v", err)
	}

	invalid := valid
	invalid.OpID = "bad"
	if err := ValidateOperation(invalid); err == nil {
		t.Fatal("expected invalid opId error")
	}
}

func TestParseAck(t *testing.T) {
	ack, ok := ParseAck([]byte(`{"type":"ACK","opId":"abc","roomId":"r1","sequence":1}`))
	if !ok {
		t.Fatal("expected valid ack")
	}
	if ack.OpID != "abc" || ack.RoomID != "r1" {
		t.Fatal("unexpected ack payload")
	}
}
