import { validateOperation } from "../core/operations.js";
import { OfflineQueue } from "./queue.js";
import { WALEngine } from "./wal.js";
export class SyncEngine {
    transport;
    store;
    options;
    identity;
    onApplied;
    status = "idle";
    queue;
    wal;
    reconnectTimer = null;
    reconnectAttempts = 0;
    retryTimers = new Map();
    seenOps = new Set();
    orderingBuffer = new Map();
    lastAppliedSequence = 0;
    constructor(transport, store, storage, options, identity, onApplied) {
        this.transport = transport;
        this.store = store;
        this.options = options;
        this.identity = identity;
        this.onApplied = onApplied;
        this.queue = new OfflineQueue(storage);
        this.wal = new WALEngine(storage);
        this.transport.onMessage((op) => {
            void this.onIncomingOperation(op);
        });
        this.transport.onAck?.((ack) => {
            void this.onAck(ack);
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
        this.lastAppliedSequence = this.wal.getLastSequence();
    }
    getStatus() {
        return this.status;
    }
    getLastSequence() {
        return this.wal.getLastSequence();
    }
    async connect() {
        this.clearReconnect();
        this.status = "connecting";
        await this.transport.connect();
        this.status = "connected";
        this.reconnectAttempts = 0;
        const reconnect = {
            type: "RECONNECT",
            roomId: this.identity.roomId,
            clientId: this.identity.clientId,
            lastSequence: this.wal.getLastSequence()
        };
        this.transport.sendReconnect?.(reconnect);
        await this.flushQueue();
        await this.flushPendingWAL();
    }
    disconnect() {
        this.clearReconnect();
        this.clearRetryTimers();
        this.transport.disconnect();
        this.status = "idle";
    }
    async publish(op) {
        await this.wal.putPending(op);
        if (this.status !== "connected") {
            await this.queue.enqueue(op);
            return;
        }
        this.sendWithRetry(op, 0);
    }
    async onAck(ack) {
        const timer = this.retryTimers.get(ack.opId);
        if (timer) {
            clearTimeout(timer);
            this.retryTimers.delete(ack.opId);
        }
        await this.wal.markAcked(ack.opId, ack.sequence);
        if (ack.sequence > this.lastAppliedSequence) {
            this.lastAppliedSequence = ack.sequence;
        }
    }
    async onIncomingOperation(op) {
        if (!validateOperation(op))
            return;
        if (this.seenOps.has(op.opId))
            return;
        if (typeof op.sequence !== "number") {
            this.seenOps.add(op.opId);
            if (!this.store.applyOperation(op))
                return;
            await this.wal.putPending(op);
            this.onApplied?.(op);
            return;
        }
        if (op.sequence <= this.lastAppliedSequence) {
            this.seenOps.add(op.opId);
            return;
        }
        if (op.sequence !== this.lastAppliedSequence + 1) {
            this.orderingBuffer.set(op.sequence, op);
            this.trimOrderingBuffer();
            return;
        }
        await this.applyOrdered(op);
        await this.drainOrderingBuffer();
    }
    async applyOrdered(op) {
        if (this.seenOps.has(op.opId))
            return;
        this.seenOps.add(op.opId);
        if (!this.store.applyOperation(op))
            return;
        if (typeof op.sequence === "number") {
            this.lastAppliedSequence = op.sequence;
            await this.wal.setLastSequence(op.sequence);
        }
        this.onApplied?.(op);
    }
    async drainOrderingBuffer() {
        while (true) {
            const next = this.orderingBuffer.get(this.lastAppliedSequence + 1);
            if (!next)
                break;
            this.orderingBuffer.delete(this.lastAppliedSequence + 1);
            await this.applyOrdered(next);
        }
    }
    async flushQueue() {
        while (this.status === "connected" && this.queue.size() > 0) {
            const op = this.queue.dequeue();
            if (!op)
                break;
            this.sendWithRetry(op, 0);
        }
        await this.queue.persist();
    }
    async flushPendingWAL() {
        for (const rec of this.wal.listPending()) {
            this.sendWithRetry(rec.payload, rec.retryCount);
        }
    }
    sendWithRetry(op, retryCount) {
        if (this.status !== "connected")
            return;
        try {
            this.transport.send(op);
            const delay = this.computeBackoff(retryCount + 1);
            void this.wal.updateRetry(op.opId, retryCount + 1, Date.now() + delay);
            const timer = setTimeout(() => {
                this.retryTimers.delete(op.opId);
                this.sendWithRetry(op, retryCount + 1);
            }, delay);
            const existing = this.retryTimers.get(op.opId);
            if (existing)
                clearTimeout(existing);
            this.retryTimers.set(op.opId, timer);
        }
        catch {
            this.status = "offline";
            void this.queue.enqueue(op);
            this.scheduleReconnect();
        }
    }
    computeBackoff(attempt) {
        const raw = this.options.retryBaseMs * 2 ** Math.max(0, attempt - 1);
        return Math.min(this.options.maxRetryMs, raw);
    }
    trimOrderingBuffer() {
        if (this.orderingBuffer.size <= 1000)
            return;
        const keys = [...this.orderingBuffer.keys()].sort((a, b) => a - b);
        while (keys.length > 1000) {
            const k = keys.shift();
            if (typeof k === "number")
                this.orderingBuffer.delete(k);
        }
    }
    scheduleReconnect() {
        if (this.reconnectTimer !== null || this.status === "idle")
            return;
        const delay = this.computeBackoff(this.reconnectAttempts + 1);
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
    clearRetryTimers() {
        for (const timer of this.retryTimers.values())
            clearTimeout(timer);
        this.retryTimers.clear();
    }
}
//# sourceMappingURL=syncEngine.js.map