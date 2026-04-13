import type { StorageAdapter, VzyOperation } from "../types/index.js";

const OPS_KEY = "__vzy_ops__";

export class MemoryStorageAdapter implements StorageAdapter {
  private readonly kv = new Map<string, unknown>();
  private readonly ops: VzyOperation[] = [];

  async get<T = unknown>(key: string): Promise<T | undefined> {
    return this.kv.get(key) as T | undefined;
  }

  async set<T = unknown>(key: string, value: T): Promise<void> {
    this.kv.set(key, value);
  }

  async delete(key: string): Promise<void> {
    this.kv.delete(key);
  }

  async getAllOps(): Promise<VzyOperation[]> {
    return [...this.ops];
  }

  async appendOp(op: VzyOperation): Promise<void> {
    this.ops.push(op);
    this.kv.set(OPS_KEY, [...this.ops]);
  }
}
