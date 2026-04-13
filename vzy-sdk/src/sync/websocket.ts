import type { SyncStatus, Transport, VzyAck, VzyNode, VzyOperation, VzyReconnect } from "../types/index.js";
import { NodeManager } from "./nodeManager.js";

const OPEN_STATE = 1;

export class WebSocketTransport implements Transport {
  private ws?: WebSocket;
  private messageCb?: (op: VzyOperation) => void;
  private ackCb?: (ack: VzyAck) => void;
  private statusCb?: (status: SyncStatus) => void;
  private readonly nodeManager: NodeManager;
  private latencyMs = 0;
  private readonly sourceDistribution = new Map<string, number>();
  private dedupHits = 0;
  private totalEvents = 0;

  constructor(
    serverUrl: string,
    private readonly roomId: string,
    private readonly clientId: string,
    private readonly lastSequenceProvider: () => number = () => 0
  ) {
    const parsed = this.parseNodes(serverUrl);
    this.nodeManager = new NodeManager(parsed);
  }

  setNodes(nodes: VzyNode[]): void {
    if (nodes.length > 0) this.nodeManager.setNodes(nodes);
  }

  getActiveNode(): VzyNode | undefined {
    return this.nodeManager.active();
  }

  async switchNode(nodeId: string): Promise<void> {
    const node = this.nodeManager.switchTo(nodeId);
    if (!node) throw new Error(`unknown node: ${nodeId}`);
    this.disconnect();
    await this.connect();
  }

  getObservability(): { activeNodeId?: string; failoverCount: number; crossNodeLatency: number; eventSourceDistribution: Record<string, number>; dedupRate: number } {
    const distribution: Record<string, number> = {};
    for (const [k, v] of this.sourceDistribution.entries()) distribution[k] = v;
    return {
      ...this.nodeManager.stats(),
      crossNodeLatency: this.latencyMs,
      eventSourceDistribution: distribution,
      dedupRate: this.totalEvents === 0 ? 0 : this.dedupHits / this.totalEvents
    };
  }

  async connect(): Promise<void> {
    if (typeof WebSocket === "undefined") throw new Error("WebSocket is not available in this environment");

    this.statusCb?.("connecting");
    const node = this.nodeManager.failover();
    if (!node) throw new Error("no nodes available");

    const start = Date.now();
    const lastSeq = this.lastSequenceProvider();
    const url =
      node.url.replace(/\/+$/, "") +
      `/ws?roomId=${encodeURIComponent(this.roomId)}&clientId=${encodeURIComponent(this.clientId)}&lastSequence=${encodeURIComponent(String(lastSeq))}`;

    this.ws = new WebSocket(url);
    this.ws.onmessage = (event) => {
      this.handleMessage(String(event.data));
    };
    this.ws.onclose = () => {
      this.nodeManager.markHealth(node.nodeId, false);
      this.statusCb?.("offline");
    };
    this.ws.onerror = () => {
      this.nodeManager.markHealth(node.nodeId, false);
      this.statusCb?.("offline");
    };

    await new Promise<void>((resolve, reject) => {
      if (!this.ws) return reject(new Error("websocket not initialized"));
      this.ws.onopen = () => {
        this.latencyMs = Date.now() - start;
        this.nodeManager.markHealth(node.nodeId, true, this.latencyMs);
        this.statusCb?.("connected");
        resolve();
      };
      this.ws.onerror = () => {
        this.nodeManager.markHealth(node.nodeId, false);
        this.statusCb?.("offline");
        reject(new Error("websocket connection failed"));
      };
    });
  }

  send(op: VzyOperation): void {
    if (!this.ws || this.ws.readyState !== OPEN_STATE) throw new Error("websocket is not connected");
    const activeNode = this.nodeManager.active();
    this.ws.send(JSON.stringify({ event: "operation", data: { ...op, sourceNode: activeNode?.nodeId } }));
  }

  sendAck(ack: VzyAck): void {
    if (!this.ws || this.ws.readyState !== OPEN_STATE) return;
    this.ws.send(JSON.stringify(ack));
  }

  sendReconnect(payload: VzyReconnect): void {
    if (!this.ws || this.ws.readyState !== OPEN_STATE) return;
    const activeNode = this.nodeManager.active();
    this.ws.send(JSON.stringify({ ...payload, lastNodeId: payload.lastNodeId ?? activeNode?.nodeId }));
  }

  onMessage(cb: (op: VzyOperation) => void): void {
    this.messageCb = cb;
  }

  onAck(cb: (ack: VzyAck) => void): void {
    this.ackCb = cb;
  }

  onStatusChange(cb: (status: SyncStatus) => void): void {
    this.statusCb = cb;
  }

  disconnect(): void {
    this.ws?.close();
    this.statusCb?.("idle");
  }

  private parseNodes(serverUrl: string): VzyNode[] {
    const urls = serverUrl.split(",").map((u) => u.trim()).filter(Boolean);
    return urls.map((url, idx) => ({ nodeId: `node-${idx + 1}`, region: "unknown", url, healthy: true }));
  }

  private handleMessage(raw: string): void {
    try {
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      if (parsed.type === "ACK") {
        const ack = parsed as unknown as VzyAck;
        if (ack.opId && ack.roomId && typeof ack.sequence === "number" && ack.committed === true) {
          this.ackCb?.(ack);
        }
        return;
      }

      if (parsed.event === "operation_broadcast" && parsed.data && typeof parsed.data === "object") {
        const op = parsed.data as VzyOperation;
        this.totalEvents += 1;
        if (op.nodeId) {
          this.sourceDistribution.set(op.nodeId, (this.sourceDistribution.get(op.nodeId) ?? 0) + 1);
        }
        this.messageCb?.(op);
      }
    } catch {
      // drop malformed frame
    }
  }
}
