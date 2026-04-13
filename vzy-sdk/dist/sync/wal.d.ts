import type { StorageAdapter, VzyOperation, WALRecord } from "../types/index.js";
export declare class WALEngine {
    private readonly storage;
    private records;
    constructor(storage: StorageAdapter);
    hydrate(): Promise<void>;
    writePending(op: VzyOperation): Promise<void>;
    markAcked(opId: string, sequence: number): Promise<void>;
    persistAppliedRemote(op: VzyOperation): Promise<void>;
    getPending(): WALRecord[];
    getAll(): WALRecord[];
    getLastSequence(): Promise<number>;
    setLastSequence(sequence: number): Promise<void>;
    private persist;
}
