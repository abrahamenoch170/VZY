# VZY Relay

## Build

```bash
go build ./cmd/server
```

## Run

```bash
PORT=8080 ENABLE_PERSISTENCE=true WAL_PATH=./vzy-relay.wal SLOW_CLIENT_POLICY=drop go run ./cmd/server
```

## Test

```bash
go test ./...
```

## Stress (optional)

```bash
go run ./test/stress -addr localhost:8080 -clients 1000 -messages 20
```
