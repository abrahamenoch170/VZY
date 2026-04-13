import type { VzyOperation } from "../types/index.js";
export declare function createSetOperation<T>(params: {
    key: string;
    value: T;
    clientId: string;
    roomId: string;
    timestamp?: number;
}): VzyOperation<T>;
export declare function createDeleteOperation(params: {
    key: string;
    clientId: string;
    roomId: string;
    timestamp?: number;
}): VzyOperation;
export declare function validateOperation(op: VzyOperation): boolean;
