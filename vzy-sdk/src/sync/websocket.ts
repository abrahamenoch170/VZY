import type { SyncStatus, Transport, VzyAck, VzyOperation, VzyReconnect } from "../types/index.js";

const OPEN_STATE = 1;

export class WebSocketTransport implements Transport {
  private ws?: WebSocket;
  private messageCb?: (op: VzyOperation) => void;
  private ackCb?: (ack: VzyAck) => void;
  private statusCb?: (status: SyncStatus) => void;

  constructor(
    private readonly serverUrl: string,
    private readonly roomId: string,
    private readonly clientId: string,
    private readonly lastSequenceProvider: () => number = () => 0
  ) {}

  async connect(): Promise<void> {
    if (typeof WebSocket === "undefined") throw new Error("WebSocket is not available in this environment");

    this.statusCb?.("connecting");
    const lastSeq = this.lastSequenceProvider();
    const url =
      this.serverUrl.replace(/\/+$/, "") +
      `/ws?roomId=${encodeURIComponent(this.roomId)}&clientId=${encodeURIComponent(this.clientId)}&lastSequence=${encodeURIComponent(String(lastSeq))}`;

    this.ws = new WebSocket(url);
    this.ws.onmessage = (event) => {
      this.handleMessage(String(event.data));
    };
    this.ws.onclose = () => this.statusCb?.("offline");
    this.ws.onerror = () => this.statusCb?.("offline");

    await new Promise<void>((resolve, reject) => {
      if (!this.ws) return reject(new Error("websocket not initialized"));
      this.ws.onopen = () => {
        this.statusCb?.("connected");
        resolve();
      };
      this.ws.onerror = () => {
        this.statusCb?.("offline");
        reject(new Error("websocket connection failed"));
      };
    });
  }

  send(op: VzyOperation): void {
    if (!this.ws || this.ws.readyState !== OPEN_STATE) throw new Error("websocket is not connected");
    this.ws.send(JSON.stringify({ event: "operation", data: op }));
  }

  sendReconnect(payload: VzyReconnect): void {
    if (!this.ws || this.ws.readyState !== OPEN_STATE) return;
    this.ws.send(JSON.stringify(payload));
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

  private handleMessage(raw: string): void {
    try {
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      if (parsed.type === "ACK") {
        const ack = parsed as unknown as VzyAck;
        if (ack.opId && ack.roomId && typeof ack.sequence === "number") {
          this.ackCb?.(ack);
        }
        return;
      }

      if (parsed.event === "operation_broadcast" && parsed.data && typeof parsed.data === "object") {
        this.messageCb?.(parsed.data as VzyOperation);
      }
    } catch {
      // drop malformed frame
    }
  }
}
