import { validateOperation } from "../core/operations.js";
import { OfflineQueue } from "./queue.js";
import { OrderingBuffer } from "./orderingBuffer.js";
import { RetryEngine } from "./retry.js";
import { WALEngine } from "./wal.js";
export class SyncEngine {
    transport;
    store;
    storage;
    roomId;
    clientId;
    options;
    onApplied;
    status = "idle";
    queue;
    reconnectTimer = null;
    reconnectAttempts = 0;
    wal;
    ordering = new OrderingBuffer(0);
    pendingAcks = new Map();
    retry;
    constructor(transport, store, storage, roomId, clientId, options, onApplied) {
        this.transport = transport;
        this.store = store;
        this.storage = storage;
        this.roomId = roomId;
        this.clientId = clientId;
        this.options = options;
        this.onApplied = onApplied;
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
            if (nextStatus === "offline")
                this.scheduleReconnect();
        });
    }
    async init() {
        await this.queue.hydrate();
        await this.wal.hydrate();
        const lastSequence = await this.wal.getLastSequence();
        this.ordering.setLastSequence(lastSequence);
        for (const record of this.wal.getPending()) {
            this.pendingAcks.set(record.opId, record);
            this.retry.schedule(record.payload);
        }
    }
    getStatus() {
        return this.status;
    }
    async connect() {
        this.clearReconnect();
        this.status = "connecting";
        await this.transport.connect();
        this.status = "connected";
        this.reconnectAttempts = 0;
        const reconnectMessage = {
            type: "RECONNECT",
            roomId: this.roomId,
            clientId: this.clientId,
            lastSequence: this.ordering.getLastSequence()
        };
        this.transport.sendControl?.(reconnectMessage);
        await this.flushQueue();
        await this.retryPendingWAL();
    }
    disconnect() {
        this.clearReconnect();
        this.retry.clear();
        this.transport.disconnect();
        this.status = "idle";
    }
    async publish(op) {
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
    async handleIncomingOperation(op) {
        if (!validateOperation(op))
            return;
        const ready = this.ordering.add(op);
        for (const candidate of ready) {
            if (candidate.clientId === this.clientId && this.pendingAcks.has(candidate.opId) && typeof candidate.sequence === "number") {
                await this.handleAck({ type: "ACK", opId: candidate.opId, roomId: candidate.roomId, sequence: candidate.sequence });
                continue;
            }
            if (!this.store.applyOperation(candidate))
                continue;
            await this.storage.appendOp(candidate);
            await this.wal.persistAppliedRemote(candidate);
            this.onApplied?.(candidate);
        }
    }
    async handleAck(ack) {
        if (ack.roomId !== this.roomId)
            return;
        const pending = this.pendingAcks.get(ack.opId);
        if (!pending)
            return;
        this.pendingAcks.delete(ack.opId);
        this.retry.cancel(ack.opId);
        this.ordering.setLastSequence(ack.sequence);
        await this.wal.markAcked(ack.opId, ack.sequence);
    }
    safeSend(op) {
        if (this.status !== "connected")
            return;
        try {
            this.transport.send(op);
        }
        catch {
            this.status = "offline";
            this.scheduleReconnect();
        }
    }
    async retryPendingWAL() {
        for (const pending of this.pendingAcks.values()) {
            this.safeSend(pending.payload);
            this.retry.schedule(pending.payload);
        }
    }
    async flushQueue() {
        while (this.status === "connected" && this.queue.size() > 0) {
            const op = this.queue.dequeue();
            if (!op)
                break;
            this.safeSend(op);
        }
        await this.queue.persist();
    }
    scheduleReconnect() {
        if (this.reconnectTimer !== null || this.status === "idle")
            return;
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
    clearReconnect() {
        if (this.reconnectTimer !== null) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    }
}
//# sourceMappingURL=syncEngine.js.map