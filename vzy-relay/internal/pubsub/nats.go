package pubsub

import "errors"

type NATSPubSub struct{}

func NewNATS(_ string) (*NATSPubSub, error) { return &NATSPubSub{}, nil }

func (n *NATSPubSub) Publish(_ string, _ []byte) error {
	return errors.New("nats pubsub backend unavailable in this build")
}
func (n *NATSPubSub) Subscribe(_ string, _ func([]byte)) error {
	return errors.New("nats pubsub backend unavailable in this build")
}
func (n *NATSPubSub) Close() error { return nil }
