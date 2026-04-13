import type { SyncStatus, Transport, VzyAck, VzyNode, VzyOperation, VzyReconnect } from "../types/index.js";
export declare class WebSocketTransport implements Transport {
    private readonly roomId;
    private readonly clientId;
    private readonly lastSequenceProvider;
    private ws?;
    private messageCb?;
    private ackCb?;
    private statusCb?;
    private readonly nodeManager;
    private latencyMs;
    private readonly sourceDistribution;
    private dedupHits;
    private totalEvents;
    constructor(serverUrl: string, roomId: string, clientId: string, lastSequenceProvider?: () => number);
    setNodes(nodes: VzyNode[]): void;
    getActiveNode(): VzyNode | undefined;
    switchNode(nodeId: string): Promise<void>;
    getObservability(): {
        activeNodeId?: string;
        failoverCount: number;
        crossNodeLatency: number;
        eventSourceDistribution: Record<string, number>;
        dedupRate: number;
    };
    connect(): Promise<void>;
    send(op: VzyOperation): void;
    sendAck(ack: VzyAck): void;
    sendReconnect(payload: VzyReconnect): void;
    onMessage(cb: (op: VzyOperation) => void): void;
    onAck(cb: (ack: VzyAck) => void): void;
    onStatusChange(cb: (status: SyncStatus) => void): void;
    disconnect(): void;
    private parseNodes;
    private handleMessage;
}
