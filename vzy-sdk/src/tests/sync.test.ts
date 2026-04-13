import { createVzyClient } from "../core/engine.js";
import { MemoryStorageAdapter } from "../storage/adapter.js";
import type { SyncStatus, Transport, VzyAck, VzyOperation, VzyReconnect } from "../types/index.js";
import { equal } from "./helpers.js";

class MockTransport implements Transport {
  public peer?: MockTransport;
  public sent: VzyOperation[] = [];
  public reconnectPayloads: VzyReconnect[] = [];
  private messageCb?: (op: VzyOperation) => void;
  private ackCb?: (ack: VzyAck) => void;
  private statusCb?: (status: SyncStatus) => void;

  async connect(): Promise<void> {
    this.statusCb?.("connected");
  }

  send(op: VzyOperation): void {
    this.sent.push(op);
    const sequence = op.sequence ?? this.sent.length;
    this.ackCb?.({ type: "ACK", opId: op.opId, roomId: op.roomId, sequence });
    this.peer?.messageCb?.({ ...op, sequence });
  }

  sendReconnect(payload: VzyReconnect): void {
    this.reconnectPayloads.push(payload);
  }

  onMessage(cb: (op: VzyOperation) => void): void {
    this.messageCb = cb;
  }

  onAck(cb: (ack: VzyAck) => void): void {
    this.ackCb = cb;
  }

  onStatusChange(cb: (status: SyncStatus) => void): void {
    this.statusCb = cb;
  }

  disconnect(): void {
    this.statusCb?.("idle");
  }
}

export async function testSync(): Promise<void> {
  const t1 = new MockTransport();
  const t2 = new MockTransport();
  t1.peer = t2;
  t2.peer = t1;

  const c1 = await createVzyClient({ roomId: "r1", serverUrl: "ws://local", clientId: "c1", storage: new MemoryStorageAdapter(), transportFactory: () => t1 });
  const c2 = await createVzyClient({ roomId: "r1", serverUrl: "ws://local", clientId: "c2", storage: new MemoryStorageAdapter(), transportFactory: () => t2 });

  await c1.connect();
  await c2.connect();
  await c1.set("name", "Demi");

  equal(c2.get("name"), "Demi", "second client should receive update");
  equal(c1.log.getAll().length, 1, "client one log length");
  equal(c2.log.getAll().length, 1, "client two log length");
  equal(t1.reconnectPayloads.length, 1, "client should emit reconnect payload on connect");

  c1.disconnect();
  c2.disconnect();

  const offlineTransport = new MockTransport();
  const client = await createVzyClient({ roomId: "offline", serverUrl: "ws://local", clientId: "c3", storage: new MemoryStorageAdapter(), transportFactory: () => offlineTransport });

  await client.set("task", { done: false });
  await client.connect();
  equal(offlineTransport.sent.length, 1, "queued op should flush on connect");
  client.disconnect();
}
