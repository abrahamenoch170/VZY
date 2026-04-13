import { StateStore } from "../core/store.js";
import type { StorageAdapter, SyncStatus, Transport, VzyOperation } from "../types/index.js";
export declare class SyncEngine {
    private readonly transport;
    private readonly store;
    private readonly storage;
    private readonly roomId;
    private readonly clientId;
    private readonly options;
    private readonly onApplied?;
    private status;
    private readonly queue;
    private reconnectTimer;
    private reconnectAttempts;
    private readonly wal;
    private readonly ordering;
    private readonly pendingAcks;
    private readonly retry;
    constructor(transport: Transport, store: StateStore, storage: StorageAdapter, roomId: string, clientId: string, options: {
        retryBaseMs: number;
        maxRetryMs: number;
        maxOfflineQueueSize: number;
    }, onApplied?: ((op: VzyOperation) => void) | undefined);
    init(): Promise<void>;
    getStatus(): SyncStatus;
    connect(): Promise<void>;
    disconnect(): void;
    publish(op: VzyOperation): Promise<void>;
    private handleIncomingOperation;
    private handleAck;
    private safeSend;
    private retryPendingWAL;
    private flushQueue;
    private scheduleReconnect;
    private clearReconnect;
}
