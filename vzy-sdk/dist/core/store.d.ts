import type { ConflictResolver, VzyOperation } from "../types/index.js";
export declare class StateStore {
    private readonly data;
    private readonly opIds;
    private readonly subscribers;
    private readonly resolver;
    constructor(resolver: ConflictResolver);
    hasOp(opId: string): boolean;
    applyOperation(op: VzyOperation): boolean;
    get<T = unknown>(key: string): T | undefined;
    snapshot(): Record<string, unknown>;
    entries(): Array<[string, unknown]>;
    subscribe<T = unknown>(key: string, callback: (value: T | undefined) => void): () => void;
    private notify;
}
