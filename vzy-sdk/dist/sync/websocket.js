import { NodeManager } from "./nodeManager.js";
const OPEN_STATE = 1;
export class WebSocketTransport {
    roomId;
    clientId;
    lastSequenceProvider;
    ws;
    messageCb;
    ackCb;
    statusCb;
    nodeManager;
    latencyMs = 0;
    sourceDistribution = new Map();
    dedupHits = 0;
    totalEvents = 0;
    constructor(serverUrl, roomId, clientId, lastSequenceProvider = () => 0) {
        this.roomId = roomId;
        this.clientId = clientId;
        this.lastSequenceProvider = lastSequenceProvider;
        const parsed = this.parseNodes(serverUrl);
        this.nodeManager = new NodeManager(parsed);
    }
    setNodes(nodes) {
        if (nodes.length > 0)
            this.nodeManager.setNodes(nodes);
    }
    getActiveNode() {
        return this.nodeManager.active();
    }
    async switchNode(nodeId) {
        const node = this.nodeManager.switchTo(nodeId);
        if (!node)
            throw new Error(`unknown node: ${nodeId}`);
        this.disconnect();
        await this.connect();
    }
    getObservability() {
        const distribution = {};
        for (const [k, v] of this.sourceDistribution.entries())
            distribution[k] = v;
        return {
            ...this.nodeManager.stats(),
            crossNodeLatency: this.latencyMs,
            eventSourceDistribution: distribution,
            dedupRate: this.totalEvents === 0 ? 0 : this.dedupHits / this.totalEvents
        };
    }
    async connect() {
        if (typeof WebSocket === "undefined")
            throw new Error("WebSocket is not available in this environment");
        this.statusCb?.("connecting");
        const node = this.nodeManager.failover();
        if (!node)
            throw new Error("no nodes available");
        const start = Date.now();
        const lastSeq = this.lastSequenceProvider();
        const url = node.url.replace(/\/+$/, "") +
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
        await new Promise((resolve, reject) => {
            if (!this.ws)
                return reject(new Error("websocket not initialized"));
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
    send(op) {
        if (!this.ws || this.ws.readyState !== OPEN_STATE)
            throw new Error("websocket is not connected");
        const activeNode = this.nodeManager.active();
        this.ws.send(JSON.stringify({ event: "operation", data: { ...op, sourceNode: activeNode?.nodeId } }));
    }
    sendAck(ack) {
        if (!this.ws || this.ws.readyState !== OPEN_STATE)
            return;
        this.ws.send(JSON.stringify(ack));
    }
    sendReconnect(payload) {
        if (!this.ws || this.ws.readyState !== OPEN_STATE)
            return;
        const activeNode = this.nodeManager.active();
        this.ws.send(JSON.stringify({ ...payload, lastNodeId: payload.lastNodeId ?? activeNode?.nodeId }));
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
    parseNodes(serverUrl) {
        const urls = serverUrl.split(",").map((u) => u.trim()).filter(Boolean);
        return urls.map((url, idx) => ({ nodeId: `node-${idx + 1}`, region: "unknown", url, healthy: true }));
    }
    handleMessage(raw) {
        try {
            const parsed = JSON.parse(raw);
            if (parsed.type === "ACK") {
                const ack = parsed;
                if (ack.opId && ack.roomId && typeof ack.sequence === "number" && ack.committed === true) {
                    this.ackCb?.(ack);
                }
                return;
            }
            if (parsed.event === "operation_broadcast" && parsed.data && typeof parsed.data === "object") {
                const op = parsed.data;
                this.totalEvents += 1;
                if (op.nodeId) {
                    this.sourceDistribution.set(op.nodeId, (this.sourceDistribution.get(op.nodeId) ?? 0) + 1);
                }
                this.messageCb?.(op);
            }
        }
        catch {
            // drop malformed frame
        }
    }
}
//# sourceMappingURL=websocket.js.map