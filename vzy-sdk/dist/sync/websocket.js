const OPEN_STATE = 1;
export class WebSocketTransport {
    serverUrl;
    roomId;
    clientId;
    ws;
    messageCb;
    ackCb;
    statusCb;
    constructor(serverUrl, roomId, clientId) {
        this.serverUrl = serverUrl;
        this.roomId = roomId;
        this.clientId = clientId;
    }
    async connect() {
        if (typeof WebSocket === "undefined") {
            throw new Error("WebSocket is not available in this environment");
        }
        this.statusCb?.("connecting");
        const url = this.serverUrl.replace(/\/+$/, "") + `/ws?roomId=${encodeURIComponent(this.roomId)}&clientId=${encodeURIComponent(this.clientId)}`;
        this.ws = new WebSocket(url);
        this.ws.onmessage = (event) => {
            try {
                const parsed = JSON.parse(String(event.data));
                if (parsed.type === "ACK" && parsed.opId && parsed.roomId && typeof parsed.sequence === "number") {
                    this.ackCb?.({ type: "ACK", opId: parsed.opId, roomId: parsed.roomId, sequence: parsed.sequence });
                    return;
                }
                if (parsed.event !== "operation_broadcast")
                    return;
                if (!parsed.data || typeof parsed.data !== "object")
                    return;
                this.messageCb?.(parsed.data);
            }
            catch {
                // swallow malformed messages to keep client alive.
            }
        };
        this.ws.onclose = () => this.statusCb?.("offline");
        this.ws.onerror = () => this.statusCb?.("offline");
        await new Promise((resolve, reject) => {
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
    send(op) {
        if (!this.ws || this.ws.readyState !== OPEN_STATE) {
            throw new Error("websocket is not connected");
        }
        this.ws.send(JSON.stringify({ event: "operation", data: op }));
    }
    sendControl(payload) {
        if (!this.ws || this.ws.readyState !== OPEN_STATE) {
            return;
        }
        this.ws.send(JSON.stringify(payload));
    }
    onMessage(cb) {
        this.messageCb = cb;
    }
    onAck(cb) {
        this.ackCb = cb;
    }
    onStatusChange(cb) {
        this.statusCb = cb;
    }
    disconnect() {
        this.ws?.close();
        this.statusCb?.("idle");
    }
}
//# sourceMappingURL=websocket.js.map