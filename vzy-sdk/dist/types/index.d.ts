export type OperationType = "SET" | "DELETE";
export type VzyValue = unknown;
export interface VzyOperation<T = VzyValue> {
    opId: string;
    type: OperationType;
    key: string;
    value?: T;
    clientId: string;
    roomId: string;
    timestamp: number;
    sequence?: number;
}
export interface VzyAck {
    type: "ACK";
    opId: string;
    roomId: string;
    sequence: number;
}
export interface VzyReconnect {
    type: "RECONNECT";
    roomId: string;
    clientId: string;
    lastSequence: number;
}
export interface WALRecord {
    opId: string;
    roomId: string;
    payload: VzyOperation;
    status: "pending" | "acked";
    sequence?: number;
    retryCount: number;
    nextRetryAt: number;
}
export interface StoredValue<T = VzyValue> {
    value: T;
    timestamp: number;
    opId: string;
    sequence?: number;
}
export interface ConflictResolver {
    resolve(local: VzyOperation, remote: VzyOperation): VzyOperation;
}
export interface StorageAdapter {
    get<T = unknown>(key: string): Promise<T | undefined>;
    set<T = unknown>(key: string, value: T): Promise<void>;
    delete(key: string): Promise<void>;
    getAllOps(): Promise<VzyOperation[]>;
    appendOp(op: VzyOperation): Promise<void>;
}
export interface Transport {
    connect(): Promise<void>;
    send(op: VzyOperation): void;
    onMessage(cb: (op: VzyOperation) => void): void;
    onAck?(cb: (ack: VzyAck) => void): void;
    sendReconnect?(payload: VzyReconnect): void;
    onStatusChange?(cb: (status: SyncStatus) => void): void;
    disconnect(): void;
}
export type SyncStatus = "idle" | "connecting" | "connected" | "offline";
export interface VzyClientConfig {
    roomId: string;
    serverUrl: string;
    clientId?: string;
    userId?: string;
    storage?: StorageAdapter;
    transportFactory?: (params: TransportFactoryParams) => Transport;
    resolver?: ConflictResolver;
    retryBaseMs?: number;
    maxRetryMs?: number;
}
export interface TransportFactoryParams {
    roomId: string;
    clientId: string;
    serverUrl: string;
}
export interface VzyClient {
    set<T>(key: string, value: T): Promise<void>;
    delete(key: string): Promise<void>;
    get<T = unknown>(key: string): T | undefined;
    subscribe<T = unknown>(key: string, callback: (value: T | undefined) => void): () => void;
    connect(): Promise<void>;
    disconnect(): void;
    sync: {
        status(): SyncStatus;
    };
    query: {
        get<T = unknown>(key: string): T | undefined;
        where(criteria: Record<string, unknown>): Array<Record<string, unknown>>;
    };
    log: {
        getAll(): VzyOperation[];
    };
    debug: {
        dumpState(): Record<string, unknown>;
    };
}
