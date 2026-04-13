import type { StorageAdapter, VzyOperation } from "../types/index.js";

const QUEUE_KEY = "__vzy_offline_queue__";

export class OfflineQueue {
  private readonly queue: VzyOperation[] = [];
  private readonly opIds = new Set<string>();

  constructor(private readonly storage: StorageAdapter, private readonly maxSize: number) {}

  async hydrate(): Promise<void> {
    const persisted = (await this.storage.get<VzyOperation[]>(QUEUE_KEY)) ?? [];
    for (const op of persisted) {
      if (this.opIds.has(op.opId)) continue;
      this.opIds.add(op.opId);
      this.queue.push(op);
    }
  }

  async enqueue(op: VzyOperation): Promise<void> {
    if (this.opIds.has(op.opId)) {
      return;
    }
    if (this.queue.length >= this.maxSize) {
      const dropped = this.queue.shift();
      if (dropped) this.opIds.delete(dropped.opId);
    }
    this.queue.push(op);
    this.opIds.add(op.opId);
    await this.persist();
  }

  dequeue(): VzyOperation | undefined {
    const op = this.queue.shift();
    if (op) {
      this.opIds.delete(op.opId);
    }
    return op;
  }

  size(): number {
    return this.queue.length;
  }

  async persist(): Promise<void> {
    await this.storage.set(QUEUE_KEY, [...this.queue]);
  }
}
