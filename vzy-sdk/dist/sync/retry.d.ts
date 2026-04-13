import type { VzyOperation } from "../types/index.js";
export declare class RetryEngine {
    private readonly sendFn;
    private readonly maxMs;
    private timers;
    private attempts;
    constructor(sendFn: (op: VzyOperation) => void, maxMs: number);
    schedule(op: VzyOperation): void;
    cancel(opId: string): void;
    clear(): void;
    private run;
}
