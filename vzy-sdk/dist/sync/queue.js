const QUEUE_KEY = "__vzy_offline_queue__";
const MAX_QUEUE_SIZE = 5000;
export class OfflineQueue {
    storage;
    queue = [];
    opIds = new Set();
    constructor(storage) {
        this.storage = storage;
    }
    async hydrate() {
        const persisted = (await this.storage.get(QUEUE_KEY)) ?? [];
        for (const op of persisted) {
            if (this.opIds.has(op.opId))
                continue;
            this.opIds.add(op.opId);
            this.queue.push(op);
        }
        this.trimIfNeeded();
    }
    async enqueue(op) {
        if (this.opIds.has(op.opId))
            return;
        this.queue.push(op);
        this.opIds.add(op.opId);
        this.trimIfNeeded();
        await this.persist();
    }
    dequeue() {
        const op = this.queue.shift();
        if (op)
            this.opIds.delete(op.opId);
        return op;
    }
    size() {
        return this.queue.length;
    }
    async persist() {
        await this.storage.set(QUEUE_KEY, [...this.queue]);
    }
    trimIfNeeded() {
        while (this.queue.length > MAX_QUEUE_SIZE) {
            const removed = this.queue.shift();
            if (removed)
                this.opIds.delete(removed.opId);
        }
    }
}
//# sourceMappingURL=queue.js.map