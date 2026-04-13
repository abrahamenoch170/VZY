import type { ConflictResolver, VzyOperation } from "../types/index.js";
export declare class LastWriteWinsResolver implements ConflictResolver {
    resolve(local: VzyOperation, remote: VzyOperation): VzyOperation;
}
