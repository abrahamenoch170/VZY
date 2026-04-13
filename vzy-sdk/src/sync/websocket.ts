import type { AckMessage, ReconnectMessage, SyncStatus, Transport, VzyOperation } from "../types/index.js";

const OPEN_STATE = 1;

export class WebSocketTransport implements Transport {
  private ws?: WebSocket;
  private messageCb?: (op: VzyOperation) => void;
  private ackCb?: (ack: AckMessage) => void;
  private statusCb?: (status: SyncStatus) => void;

  constructor(
    private readonly serverUrl: string,
    private readonly roomId: string,
    private readonly clientId: string
  ) {}

  async connect(): Promise<void> {
    if (typeof WebSocket === "undefined") {
      throw new Error("WebSocket is not available in this environment");
    }

    this.statusCb?.("connecting");

    const url = this.serverUrl.replace(/\/+$/, "") + `/ws?roomId=${encodeURIComponent(this.roomId)}&clientId=${encodeURIComponent(this.clientId)}`;

    this.ws = new WebSocket(url);
    this.ws.onmessage = (event) => {
      try {
        const parsed = JSON.parse(String(event.data)) as { event?: string; data?: unknown; type?: string; opId?: string; roomId?: string; sequence?: number };
        if (parsed.type === "ACK" && parsed.opId && parsed.roomId && typeof parsed.sequence === "number") {
          this.ackCb?.({ type: "ACK", opId: parsed.opId, roomId: parsed.roomId, sequence: parsed.sequence });
          return;
        }
        if (parsed.event !== "operation_broadcast") return;
        if (!parsed.data || typeof parsed.data !== "object") return;
        this.messageCb?.(parsed.data as VzyOperation);
      } catch {
        // swallow malformed messages to keep client alive.
      }
    };

    this.ws.onclose = () => this.statusCb?.("offline");
    this.ws.onerror = () => this.statusCb?.("offline");

    await new Promise<void>((resolve, reject) => {
      if (!this.ws) {
        reject(new Error("websocket not initialized"));
        return;
      }
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
    if (!this.ws || this.ws.readyState !== OPEN_STATE) {
      throw new Error("websocket is not connected");
    }
    this.ws.send(JSON.stringify({ event: "operation", data: op }));
  }

  sendControl(payload: ReconnectMessage): void {
    if (!this.ws || this.ws.readyState !== OPEN_STATE) {
      return;
    }
    this.ws.send(JSON.stringify(payload));
  }

  onMessage(cb: (op: VzyOperation) => void): void {
    this.messageCb = cb;
  }

  onAck(cb: (ack: AckMessage) => void): void {
    this.ackCb = cb;
  }

  onStatusChange(cb: (status: SyncStatus) => void): void {
    this.statusCb = cb;
  }

  disconnect(): void {
    this.ws?.close();
    this.statusCb?.("idle");
  }
}
