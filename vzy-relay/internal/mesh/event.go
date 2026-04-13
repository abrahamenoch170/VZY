package mesh

type Event struct {
	OpID      string `json:"opId"`
	RoomID    string `json:"roomId"`
	ClientID  string `json:"clientId"`
	NodeID    string `json:"nodeId"`
	Sequence  uint64 `json:"sequence"`
	Type      string `json:"type"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp"`
}
