import type { StorageAdapter, VzyOperation } from "../types/index.js";
export declare class OfflineQueue {
    private readonly storage;
    private readonly queue;
    private readonly opIds;
    constructor(storage: StorageAdapter);
    hydrate(): Promise<void>;
    enqueue(op: VzyOperation): Promise<void>;
    dequeue(): VzyOperation | undefined;
    size(): number;
    persist(): Promise<void>;
    private trimIfNeeded;
}
