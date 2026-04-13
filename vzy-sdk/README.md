# VZY TypeScript SDK V2

## Usage

```ts
import { createVzyClient } from "./dist/index.js";

const vzy = await createVzyClient({
  roomId: "chat-123",
  serverUrl: "wss://relay.vzy.dev"
});

await vzy.set("name", "Demi");
await vzy.connect();
```

## Features
- local-first operation log
- IndexedDB WAL persistence for pending + acked operations
- ACK-aware retry engine with exponential backoff
- reconnect replay using last applied sequence
- deterministic ordering buffer for out-of-order operations

## Install

```bash
npm install
```

## Build

```bash
npm run build
```

## Test

```bash
npm test
```
