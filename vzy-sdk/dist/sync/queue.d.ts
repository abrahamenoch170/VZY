import type { StorageAdapter, VzyOperation } from "../types/index.js";
export declare class OfflineQueue {
    private readonly storage;
    private readonly maxSize;
    private readonly queue;
    private readonly opIds;
    constructor(storage: StorageAdapter, maxSize: number);
    hydrate(): Promise<void>;
    enqueue(op: VzyOperation): Promise<void>;
    dequeue(): VzyOperation | undefined;
    size(): number;
    persist(): Promise<void>;
}
