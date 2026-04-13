import type { SyncStatus, Transport, VzyAck, VzyOperation, VzyReconnect } from "../types/index.js";
export declare class WebSocketTransport implements Transport {
    private readonly serverUrl;
    private readonly roomId;
    private readonly clientId;
    private readonly lastSequenceProvider;
    private ws?;
    private messageCb?;
    private ackCb?;
    private statusCb?;
    constructor(serverUrl: string, roomId: string, clientId: string, lastSequenceProvider?: () => number);
    connect(): Promise<void>;
    send(op: VzyOperation): void;
    sendAck(ack: VzyAck): void;
    sendReconnect(payload: VzyReconnect): void;
    onMessage(cb: (op: VzyOperation) => void): void;
    onAck(cb: (ack: VzyAck) => void): void;
    onStatusChange(cb: (status: SyncStatus) => void): void;
    disconnect(): void;
    private handleMessage;
}
