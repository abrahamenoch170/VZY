import { StateStore } from "../core/store.js";
import type { StorageAdapter, SyncStatus, Transport, VzyOperation } from "../types/index.js";
export declare class SyncEngine {
    private readonly transport;
    private readonly store;
    private readonly options;
    private readonly identity;
    private readonly onApplied?;
    private status;
    private readonly queue;
    private readonly wal;
    private reconnectTimer;
    private reconnectAttempts;
    private readonly retryTimers;
    private readonly seenOps;
    private readonly orderingBuffer;
    private lastAppliedSequence;
    constructor(transport: Transport, store: StateStore, storage: StorageAdapter, options: {
        retryBaseMs: number;
        maxRetryMs: number;
    }, identity: {
        roomId: string;
        clientId: string;
    }, onApplied?: ((op: VzyOperation) => void) | undefined);
    init(): Promise<void>;
    getStatus(): SyncStatus;
    getLastSequence(): number;
    connect(): Promise<void>;
    disconnect(): void;
    publish(op: VzyOperation): Promise<void>;
    private onAck;
    private onIncomingOperation;
    private applyOrdered;
    private drainOrderingBuffer;
    private flushQueue;
    private flushPendingWAL;
    private sendWithRetry;
    private computeBackoff;
    private trimOrderingBuffer;
    private scheduleReconnect;
    private clearReconnect;
    private clearRetryTimers;
}
