package pubsub

type PubSub interface {
	Publish(channel string, data []byte) error
	Subscribe(channel string, handler func([]byte)) error
	Close() error
}
