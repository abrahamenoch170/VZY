import { validateOperation } from "../core/operations.js";
import { StateStore } from "../core/store.js";
import type { AckMessage, ReconnectMessage, StorageAdapter, SyncStatus, Transport, VzyOperation, WALRecord } from "../types/index.js";
import { OfflineQueue } from "./queue.js";
import { OrderingBuffer } from "./orderingBuffer.js";
import { RetryEngine } from "./retry.js";
import { WALEngine } from "./wal.js";

export class SyncEngine {
  private status: SyncStatus = "idle";
  private readonly queue: OfflineQueue;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempts = 0;
  private readonly wal: WALEngine;
  private readonly ordering = new OrderingBuffer(0);
  private readonly pendingAcks = new Map<string, WALRecord>();
  private readonly retry: RetryEngine;

  constructor(
    private readonly transport: Transport,
    private readonly store: StateStore,
    private readonly storage: StorageAdapter,
    private readonly roomId: string,
    private readonly clientId: string,
    private readonly options: { retryBaseMs: number; maxRetryMs: number; maxOfflineQueueSize: number },
    private readonly onApplied?: (op: VzyOperation) => void
  ) {
    this.queue = new OfflineQueue(storage, options.maxOfflineQueueSize);
    this.wal = new WALEngine(storage);
    this.retry = new RetryEngine((op) => this.safeSend(op), options.maxRetryMs);

    this.transport.onMessage((op) => {
      void this.handleIncomingOperation(op);
    });

    this.transport.onAck?.((ack) => {
      void this.handleAck(ack);
    });

    this.transport.onStatusChange?.((nextStatus) => {
      this.status = nextStatus;
      if (nextStatus === "offline") this.scheduleReconnect();
    });
  }

  async init(): Promise<void> {
    await this.queue.hydrate();
    await this.wal.hydrate();
    const lastSequence = await this.wal.getLastSequence();
    this.ordering.setLastSequence(lastSequence);
    for (const record of this.wal.getPending()) {
      this.pendingAcks.set(record.opId, record);
      this.retry.schedule(record.payload);
    }
  }

  getStatus(): SyncStatus {
    return this.status;
  }

  async connect(): Promise<void> {
    this.clearReconnect();
    this.status = "connecting";
    await this.transport.connect();
    this.status = "connected";
    this.reconnectAttempts = 0;

    const reconnectMessage: ReconnectMessage = {
      type: "RECONNECT",
      roomId: this.roomId,
      clientId: this.clientId,
      lastSequence: this.ordering.getLastSequence()
    };
    this.transport.sendControl?.(reconnectMessage);

    await this.flushQueue();
    await this.retryPendingWAL();
  }

  disconnect(): void {
    this.clearReconnect();
    this.retry.clear();
    this.transport.disconnect();
    this.status = "idle";
  }

  async publish(op: VzyOperation): Promise<void> {
    await this.wal.writePending(op);
    this.pendingAcks.set(op.opId, { opId: op.opId, roomId: op.roomId, payload: op, status: "pending" });

    if (this.status !== "connected") {
      await this.queue.enqueue(op);
      this.retry.schedule(op);
      return;
    }

    this.safeSend(op);
    this.retry.schedule(op);
  }

  private async handleIncomingOperation(op: VzyOperation): Promise<void> {
    if (!validateOperation(op)) return;

    const ready = this.ordering.add(op);
    for (const candidate of ready) {
      if (candidate.clientId === this.clientId && this.pendingAcks.has(candidate.opId) && typeof candidate.sequence === "number") {
        await this.handleAck({ type: "ACK", opId: candidate.opId, roomId: candidate.roomId, sequence: candidate.sequence });
        continue;
      }

      if (!this.store.applyOperation(candidate)) continue;
      await this.storage.appendOp(candidate);
      await this.wal.persistAppliedRemote(candidate);
      this.onApplied?.(candidate);
    }
  }

  private async handleAck(ack: AckMessage): Promise<void> {
    if (ack.roomId !== this.roomId) return;
    const pending = this.pendingAcks.get(ack.opId);
    if (!pending) return;
    this.pendingAcks.delete(ack.opId);
    this.retry.cancel(ack.opId);
    this.ordering.setLastSequence(ack.sequence);
    await this.wal.markAcked(ack.opId, ack.sequence);
  }

  private safeSend(op: VzyOperation): void {
    if (this.status !== "connected") return;
    try {
      this.transport.send(op);
    } catch {
      this.status = "offline";
      this.scheduleReconnect();
    }
  }

  private async retryPendingWAL(): Promise<void> {
    for (const pending of this.pendingAcks.values()) {
      this.safeSend(pending.payload);
      this.retry.schedule(pending.payload);
    }
  }

  private async flushQueue(): Promise<void> {
    while (this.status === "connected" && this.queue.size() > 0) {
      const op = this.queue.dequeue();
      if (!op) break;
      this.safeSend(op);
    }
    await this.queue.persist();
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null || this.status === "idle") return;

    const delay = Math.min(this.options.maxRetryMs, this.options.retryBaseMs * 2 ** this.reconnectAttempts);
    this.reconnectAttempts += 1;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect().catch(() => {
        this.status = "offline";
        this.scheduleReconnect();
      });
    }, delay);
  }

  private clearReconnect(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
