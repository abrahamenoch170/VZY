const QUEUE_KEY = "__vzy_offline_queue__";
export class OfflineQueue {
    storage;
    maxSize;
    queue = [];
    opIds = new Set();
    constructor(storage, maxSize) {
        this.storage = storage;
        this.maxSize = maxSize;
    }
    async hydrate() {
        const persisted = (await this.storage.get(QUEUE_KEY)) ?? [];
        for (const op of persisted) {
            if (this.opIds.has(op.opId))
                continue;
            this.opIds.add(op.opId);
            this.queue.push(op);
        }
    }
    async enqueue(op) {
        if (this.opIds.has(op.opId)) {
            return;
        }
        if (this.queue.length >= this.maxSize) {
            const dropped = this.queue.shift();
            if (dropped)
                this.opIds.delete(dropped.opId);
        }
        this.queue.push(op);
        this.opIds.add(op.opId);
        await this.persist();
    }
    dequeue() {
        const op = this.queue.shift();
        if (op) {
            this.opIds.delete(op.opId);
        }
        return op;
    }
    size() {
        return this.queue.length;
    }
    async persist() {
        await this.storage.set(QUEUE_KEY, [...this.queue]);
    }
}
//# sourceMappingURL=queue.js.map