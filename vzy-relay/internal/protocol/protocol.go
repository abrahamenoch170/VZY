// MarshalAck marshals an ACK response with opId, sequence, and roomId.
func MarshalAck(opId, sequence, roomId string) ([]byte, error) {
    ack := struct {
        OpId     string `json:"opId"`
        Sequence string `json:"sequence"`
        RoomId   string `json:"roomId"`
    }{
        OpId:     opId,
        Sequence: sequence,
        RoomId:   roomId,
    }
    return json.Marshal(ack)
}