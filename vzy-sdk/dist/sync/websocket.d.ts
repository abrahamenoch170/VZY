import type { AckMessage, ReconnectMessage, SyncStatus, Transport, VzyOperation } from "../types/index.js";
export declare class WebSocketTransport implements Transport {
    private readonly serverUrl;
    private readonly roomId;
    private readonly clientId;
    private ws?;
    private messageCb?;
    private ackCb?;
    private statusCb?;
    constructor(serverUrl: string, roomId: string, clientId: string);
    connect(): Promise<void>;
    send(op: VzyOperation): void;
    sendControl(payload: ReconnectMessage): void;
    onMessage(cb: (op: VzyOperation) => void): void;
    onAck(cb: (ack: AckMessage) => void): void;
    onStatusChange(cb: (status: SyncStatus) => void): void;
    disconnect(): void;
}
