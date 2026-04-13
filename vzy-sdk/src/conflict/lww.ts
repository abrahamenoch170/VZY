import type { ConflictResolver, VzyOperation } from "../types/index.js";

export class LastWriteWinsResolver implements ConflictResolver {
  resolve(local: VzyOperation, remote: VzyOperation): VzyOperation {
    if (remote.timestamp > local.timestamp) {
      return remote;
    }
    if (remote.timestamp < local.timestamp) {
      return local;
    }

    if (remote.opId > local.opId) {
      return remote;
    }

    return local;
  }
}
