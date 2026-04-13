import type { StorageAdapter, VzyOperation } from "../types/index.js";
export declare class MemoryStorageAdapter implements StorageAdapter {
    private readonly kv;
    private readonly ops;
    get<T = unknown>(key: string): Promise<T | undefined>;
    set<T = unknown>(key: string, value: T): Promise<void>;
    delete(key: string): Promise<void>;
    getAllOps(): Promise<VzyOperation[]>;
    appendOp(op: VzyOperation): Promise<void>;
}
