import { MemoryStorageAdapter } from "./adapter.js";
const DB_NAME = "vzy-sdk";
const DB_VERSION = 1;
const KV_STORE = "kv";
const OPS_STORE = "ops";
export class IndexedDBStorageAdapter {
    fallback = new MemoryStorageAdapter();
    dbPromise;
    get isAvailable() {
        return typeof indexedDB !== "undefined";
    }
    async db() {
        if (!this.isAvailable) {
            throw new Error("indexedDB unavailable");
        }
        if (!this.dbPromise) {
            this.dbPromise = new Promise((resolve, reject) => {
                const req = indexedDB.open(DB_NAME, DB_VERSION);
                req.onerror = () => reject(req.error ?? new Error("open indexedDB failed"));
                req.onupgradeneeded = () => {
                    const db = req.result;
                    if (!db.objectStoreNames.contains(KV_STORE))
                        db.createObjectStore(KV_STORE);
                    if (!db.objectStoreNames.contains(OPS_STORE))
                        db.createObjectStore(OPS_STORE, { keyPath: "opId" });
                };
                req.onsuccess = () => resolve(req.result);
            });
        }
        return this.dbPromise;
    }
    async get(key) {
        if (!this.isAvailable)
            return this.fallback.get(key);
        const db = await this.db();
        return this.run(db, KV_STORE, "readonly", (store) => store.get(key));
    }
    async set(key, value) {
        if (!this.isAvailable) {
            await this.fallback.set(key, value);
            return;
        }
        const db = await this.db();
        await this.run(db, KV_STORE, "readwrite", (store) => store.put(value, key)).then(() => undefined);
    }
    async delete(key) {
        if (!this.isAvailable) {
            await this.fallback.delete(key);
            return;
        }
        const db = await this.db();
        await this.run(db, KV_STORE, "readwrite", (store) => store.delete(key)).then(() => undefined);
    }
    async getAllOps() {
        if (!this.isAvailable)
            return this.fallback.getAllOps();
        const db = await this.db();
        return this.run(db, OPS_STORE, "readonly", (store) => store.getAll());
    }
    async appendOp(op) {
        if (!this.isAvailable) {
            await this.fallback.appendOp(op);
            return;
        }
        const db = await this.db();
        await this.run(db, OPS_STORE, "readwrite", (store) => store.put(op)).then(() => undefined);
    }
    run(db, storeName, mode, action) {
        return new Promise((resolve, reject) => {
            const tx = db.transaction(storeName, mode);
            const store = tx.objectStore(storeName);
            const req = action(store);
            req.onerror = () => reject(req.error ?? new Error("indexedDB operation failed"));
            req.onsuccess = () => resolve(req.result);
        });
    }
}
//# sourceMappingURL=indexeddb.js.map