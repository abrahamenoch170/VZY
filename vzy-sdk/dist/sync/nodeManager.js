export class NodeManager {
    nodes = new Map();
    activeNodeId;
    failoverCount = 0;
    constructor(initialNodes) {
        this.setNodes(initialNodes);
    }
    setNodes(nodes) {
        this.nodes.clear();
        for (const node of nodes)
            this.nodes.set(node.nodeId, { ...node });
        if (!this.activeNodeId || !this.nodes.has(this.activeNodeId)) {
            const best = this.selectBestNode();
            this.activeNodeId = best ? best.nodeId : undefined;
        }
    }
    all() {
        return [...this.nodes.values()];
    }
    markHealth(nodeId, healthy, latency) {
        const node = this.nodes.get(nodeId);
        if (!node)
            return;
        node.healthy = healthy;
        if (typeof latency === "number")
            node.latency = latency;
    }
    active() {
        if (!this.activeNodeId)
            return undefined;
        return this.nodes.get(this.activeNodeId);
    }
    selectBestNode() {
        const healthy = [...this.nodes.values()].filter((n) => n.healthy);
        if (healthy.length === 0)
            return [...this.nodes.values()][0];
        healthy.sort((a, b) => (a.latency ?? Number.MAX_SAFE_INTEGER) - (b.latency ?? Number.MAX_SAFE_INTEGER));
        return healthy[0];
    }
    failover() {
        const next = this.selectBestNode();
        if (!next)
            return undefined;
        if (next.nodeId !== this.activeNodeId) {
            this.activeNodeId = next.nodeId;
            this.failoverCount += 1;
        }
        return next;
    }
    switchTo(nodeId) {
        const node = this.nodes.get(nodeId);
        if (!node)
            return undefined;
        if (node.nodeId !== this.activeNodeId)
            this.failoverCount += 1;
        this.activeNodeId = node.nodeId;
        return node;
    }
    stats() {
        if (!this.activeNodeId)
            return { failoverCount: this.failoverCount };
        return { activeNodeId: this.activeNodeId, failoverCount: this.failoverCount };
    }
}
//# sourceMappingURL=nodeManager.js.map