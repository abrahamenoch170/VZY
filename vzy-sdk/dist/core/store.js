export class StateStore {
    data = new Map();
    opIds = new Set();
    subscribers = new Map();
    resolver;
    constructor(resolver) {
        this.resolver = resolver;
    }
    hasOp(opId) {
        return this.opIds.has(opId);
    }
    applyOperation(op) {
        if (this.opIds.has(op.opId)) {
            return false;
        }
        const existing = this.data.get(op.key);
        if (existing) {
            const localOp = {
                opId: existing.opId,
                type: "SET",
                key: op.key,
                value: existing.value,
                clientId: op.clientId,
                roomId: op.roomId,
                timestamp: existing.timestamp,
                ...(typeof existing.sequence === "number" ? { sequence: existing.sequence } : {})
            };
            const winner = this.resolver.resolve(localOp, op);
            if (winner.opId !== op.opId) {
                this.opIds.add(op.opId);
                return false;
            }
        }
        if (op.type === "DELETE") {
            this.data.delete(op.key);
            this.notify(op.key, undefined);
        }
        else {
            const record = {
                value: op.value,
                timestamp: op.timestamp,
                opId: op.opId,
                ...(typeof op.sequence === "number" ? { sequence: op.sequence } : {})
            };
            this.data.set(op.key, record);
            this.notify(op.key, op.value);
        }
        this.opIds.add(op.opId);
        return true;
    }
    get(key) {
        return this.data.get(key)?.value;
    }
    snapshot() {
        const out = {};
        for (const [key, value] of this.data.entries()) {
            out[key] = value.value;
        }
        return out;
    }
    entries() {
        return [...this.data.entries()].map(([key, stored]) => [key, stored.value]);
    }
    subscribe(key, callback) {
        const wrapped = (value) => callback(value);
        const bucket = this.subscribers.get(key) ?? new Set();
        bucket.add(wrapped);
        this.subscribers.set(key, bucket);
        return () => {
            const existing = this.subscribers.get(key);
            if (!existing)
                return;
            existing.delete(wrapped);
            if (existing.size === 0) {
                this.subscribers.delete(key);
            }
        };
    }
    notify(key, value) {
        const listeners = this.subscribers.get(key);
        if (!listeners) {
            return;
        }
        for (const listener of listeners) {
            listener(value);
        }
    }
}
//# sourceMappingURL=store.js.map