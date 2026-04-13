import type { VzyNode } from "../types/index.js";
export declare class NodeManager {
    private readonly nodes;
    private activeNodeId;
    private failoverCount;
    constructor(initialNodes: VzyNode[]);
    setNodes(nodes: VzyNode[]): void;
    all(): VzyNode[];
    markHealth(nodeId: string, healthy: boolean, latency?: number): void;
    active(): VzyNode | undefined;
    selectBestNode(): VzyNode | undefined;
    failover(): VzyNode | undefined;
    switchTo(nodeId: string): VzyNode | undefined;
    stats(): {
        activeNodeId?: string;
        failoverCount: number;
    };
}
