import type { StorageAdapter, VzyOperation } from "../types/index.js";
import { MemoryStorageAdapter } from "./adapter.js";

const DB_NAME = "vzy-sdk";
const DB_VERSION = 1;
const KV_STORE = "kv";
const OPS_STORE = "ops";

export class IndexedDBStorageAdapter implements StorageAdapter {
  private readonly fallback = new MemoryStorageAdapter();
  private dbPromise?: Promise<IDBDatabase>;

  private get isAvailable(): boolean {
    return typeof indexedDB !== "undefined";
  }

  private async db(): Promise<IDBDatabase> {
    if (!this.isAvailable) {
      throw new Error("indexedDB unavailable");
    }

    if (!this.dbPromise) {
      this.dbPromise = new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onerror = () => reject(req.error ?? new Error("open indexedDB failed"));
        req.onupgradeneeded = () => {
          const db = req.result;
          if (!db.objectStoreNames.contains(KV_STORE)) db.createObjectStore(KV_STORE);
          if (!db.objectStoreNames.contains(OPS_STORE)) db.createObjectStore(OPS_STORE, { keyPath: "opId" });
        };
        req.onsuccess = () => resolve(req.result );
      });
    }

    return this.dbPromise;
  }

  async get<T = unknown>(key: string): Promise<T | undefined> {
    if (!this.isAvailable) return this.fallback.get<T>(key);
    const db = await this.db();
    return this.run<T | undefined>(db, KV_STORE, "readonly", (store) => store.get(key) as IDBRequest<T | undefined>);
  }

  async set<T = unknown>(key: string, value: T): Promise<void> {
    if (!this.isAvailable) {
      await this.fallback.set(key, value);
      return;
    }
    const db = await this.db();
    await this.run<unknown>(db, KV_STORE, "readwrite", (store) => store.put(value, key)).then(() => undefined);
  }

  async delete(key: string): Promise<void> {
    if (!this.isAvailable) {
      await this.fallback.delete(key);
      return;
    }
    const db = await this.db();
    await this.run<unknown>(db, KV_STORE, "readwrite", (store) => store.delete(key)).then(() => undefined);
  }

  async getAllOps(): Promise<VzyOperation[]> {
    if (!this.isAvailable) return this.fallback.getAllOps();
    const db = await this.db();
    return this.run<VzyOperation[]>(db, OPS_STORE, "readonly", (store) => store.getAll() as IDBRequest<VzyOperation[]>);
  }

  async appendOp(op: VzyOperation): Promise<void> {
    if (!this.isAvailable) {
      await this.fallback.appendOp(op);
      return;
    }
    const db = await this.db();
    await this.run<unknown>(db, OPS_STORE, "readwrite", (store) => store.put(op)).then(() => undefined);
  }

  private run<T>(db: IDBDatabase, storeName: string, mode: IDBTransactionMode, action: (store: IDBObjectStore) => IDBRequest): Promise<T> {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(storeName, mode);
      const store = tx.objectStore(storeName);
      const req = action(store);
      req.onerror = () => reject(req.error ?? new Error("indexedDB operation failed"));
      req.onsuccess = () => resolve(req.result as T);
    });
  }
}
