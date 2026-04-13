package pubsub

import "errors"

type RedisPubSub struct{}

func NewRedis(_ string) *RedisPubSub { return &RedisPubSub{} }

func (r *RedisPubSub) Publish(_ string, _ []byte) error {
	return errors.New("redis pubsub backend unavailable in this build")
}
func (r *RedisPubSub) Subscribe(_ string, _ func([]byte)) error {
	return errors.New("redis pubsub backend unavailable in this build")
}
func (r *RedisPubSub) Close() error { return nil }
