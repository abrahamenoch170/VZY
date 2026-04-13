const OPEN_STATE = 1;
export class WebSocketTransport {
    serverUrl;
    roomId;
    clientId;
    lastSequenceProvider;
    ws;
    messageCb;
    ackCb;
    statusCb;
    constructor(serverUrl, roomId, clientId, lastSequenceProvider = () => 0) {
        this.serverUrl = serverUrl;
        this.roomId = roomId;
        this.clientId = clientId;
        this.lastSequenceProvider = lastSequenceProvider;
    }
    async connect() {
        if (typeof WebSocket === "undefined")
            throw new Error("WebSocket is not available in this environment");
        this.statusCb?.("connecting");
        const lastSeq = this.lastSequenceProvider();
        const url = this.serverUrl.replace(/\/+$/, "") +
            `/ws?roomId=${encodeURIComponent(this.roomId)}&clientId=${encodeURIComponent(this.clientId)}&lastSequence=${encodeURIComponent(String(lastSeq))}`;
        this.ws = new WebSocket(url);
        this.ws.onmessage = (event) => {
            this.handleMessage(String(event.data));
        };
        this.ws.onclose = () => this.statusCb?.("offline");
        this.ws.onerror = () => this.statusCb?.("offline");
        await new Promise((resolve, reject) => {
            if (!this.ws)
                return reject(new Error("websocket not initialized"));
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
        if (!this.ws || this.ws.readyState !== OPEN_STATE)
            throw new Error("websocket is not connected");
        this.ws.send(JSON.stringify({ event: "operation", data: op }));
    }
    sendReconnect(payload) {
        if (!this.ws || this.ws.readyState !== OPEN_STATE)
            return;
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
    handleMessage(raw) {
        try {
            const parsed = JSON.parse(raw);
            if (parsed.type === "ACK") {
                const ack = parsed;
                if (ack.opId && ack.roomId && typeof ack.sequence === "number") {
                    this.ackCb?.(ack);
                }
                return;
            }
            if (parsed.event === "operation_broadcast" && parsed.data && typeof parsed.data === "object") {
                this.messageCb?.(parsed.data);
            }
        }
        catch {
            // drop malformed frame
        }
    }
}
//# sourceMappingURL=websocket.js.map