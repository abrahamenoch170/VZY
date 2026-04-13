import type { ConflictResolver, StoredValue, VzyOperation } from "../types/index.js";

export class StateStore {
  private readonly data = new Map<string, StoredValue>();
  private readonly opIds = new Set<string>();
  private readonly subscribers = new Map<string, Set<(value: unknown) => void>>();
  private readonly resolver: ConflictResolver;

  constructor(resolver: ConflictResolver) {
    this.resolver = resolver;
  }

  hasOp(opId: string): boolean {
    return this.opIds.has(opId);
  }

  applyOperation(op: VzyOperation): boolean {
    if (this.opIds.has(op.opId)) return false;

    const existing = this.data.get(op.key);
    if (existing) {
      if (typeof op.sequence === "number" && typeof existing.sequence === "number") {
        if (op.sequence < existing.sequence) {
          this.opIds.add(op.opId);
          return false;
        }
      } else {
        const localOp: VzyOperation = {
          opId: existing.opId,
          type: "SET",
          key: op.key,
          value: existing.value,
          clientId: op.clientId,
          roomId: op.roomId,
          timestamp: existing.timestamp
        };
        if (typeof existing.sequence === "number") localOp.sequence = existing.sequence;
        const winner = this.resolver.resolve(localOp, op);
        if (winner.opId !== op.opId) {
          this.opIds.add(op.opId);
          return false;
        }
      }
    }

    if (op.type === "DELETE") {
      this.data.delete(op.key);
      this.notify(op.key, undefined);
    } else {
      const next: StoredValue = { value: op.value, timestamp: op.timestamp, opId: op.opId };
      if (typeof op.sequence === "number") next.sequence = op.sequence;
      this.data.set(op.key, next);
      this.notify(op.key, op.value);
    }

    this.opIds.add(op.opId);
    return true;
  }

  get<T = unknown>(key: string): T | undefined {
    return this.data.get(key)?.value as T | undefined;
  }

  snapshot(): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [key, value] of this.data.entries()) out[key] = value.value;
    return out;
  }

  entries(): Array<[string, unknown]> {
    return [...this.data.entries()].map(([key, stored]) => [key, stored.value]);
  }

  subscribe<T = unknown>(key: string, callback: (value: T | undefined) => void): () => void {
    const wrapped = (value: unknown) => callback(value as T | undefined);
    const bucket = this.subscribers.get(key) ?? new Set<(value: unknown) => void>();
    bucket.add(wrapped);
    this.subscribers.set(key, bucket);

    return () => {
      const existing = this.subscribers.get(key);
      if (!existing) return;
      existing.delete(wrapped);
      if (existing.size === 0) this.subscribers.delete(key);
    };
  }

  private notify(key: string, value: unknown): void {
    const listeners = this.subscribers.get(key);
    if (!listeners) return;
    for (const listener of listeners) listener(value);
  }
}
