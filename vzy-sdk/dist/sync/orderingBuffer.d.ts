import type { VzyOperation } from "../types/index.js";
export declare class OrderingBuffer {
    private lastAppliedSequence;
    private readonly pendingBySequence;
    private readonly seenOps;
    constructor(lastAppliedSequence?: number);
    getLastSequence(): number;
    setLastSequence(sequence: number): void;
    markSeen(opId: string): void;
    hasSeen(opId: string): boolean;
    add(op: VzyOperation): VzyOperation[];
}
