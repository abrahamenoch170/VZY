# VZY TypeScript SDK

## Usage

```ts
import { createVzyClient } from "./dist/index.js";

const vzy = await createVzyClient({
  roomId: "chat-123",
  serverUrl: "wss://relay.vzy.dev"
});

await vzy.set("name", "Demi");
```

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
