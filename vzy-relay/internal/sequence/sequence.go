package sequence

type Provider interface {
	Next(roomID string, localNext func() uint64) (uint64, error)
}

type LocalProvider struct{}

func (LocalProvider) Next(_ string, localNext func() uint64) (uint64, error) { return localNext(), nil }

type RedisGlobalProvider struct{}

func NewRedisGlobalProvider(_, _ string) *RedisGlobalProvider { return &RedisGlobalProvider{} }

func (p *RedisGlobalProvider) Next(_ string, localNext func() uint64) (uint64, error) {
	return localNext(), nil
}

func (p *RedisGlobalProvider) Close() error { return nil }
