import type { StorageAdapter, VzyOperation } from "../types/index.js";
export declare class IndexedDBStorageAdapter implements StorageAdapter {
    private readonly fallback;
    private dbPromise?;
    private get isAvailable();
    private db;
    get<T = unknown>(key: string): Promise<T | undefined>;
    set<T = unknown>(key: string, value: T): Promise<void>;
    delete(key: string): Promise<void>;
    getAllOps(): Promise<VzyOperation[]>;
    appendOp(op: VzyOperation): Promise<void>;
    private run;
}
