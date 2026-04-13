import type { StorageAdapter, WALRecord, VzyOperation } from "../types/index.js";
export declare class WALEngine {
    private readonly storage;
    private records;
    private lastSequence;
    constructor(storage: StorageAdapter);
    hydrate(): Promise<void>;
    getLastSequence(): number;
    setLastSequence(seq: number): Promise<void>;
    putPending(op: VzyOperation): Promise<void>;
    markAcked(opId: string, sequence: number): Promise<void>;
    listPending(): WALRecord[];
    getAll(): WALRecord[];
    updateRetry(opId: string, retryCount: number, nextRetryAt: number): Promise<void>;
    private persist;
}
