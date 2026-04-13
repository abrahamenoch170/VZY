import { generateOpId } from "../identity/clientId.js";
import type { VzyOperation } from "../types/index.js";

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function createSetOperation<T>(params: {
  key: string;
  value: T;
  clientId: string;
  roomId: string;
  timestamp?: number;
}): VzyOperation<T> {
  return {
    opId: generateOpId(),
    type: "SET",
    key: params.key,
    value: params.value,
    clientId: params.clientId,
    roomId: params.roomId,
    timestamp: params.timestamp ?? Date.now()
  };
}

export function createDeleteOperation(params: {
  key: string;
  clientId: string;
  roomId: string;
  timestamp?: number;
}): VzyOperation {
  return {
    opId: generateOpId(),
    type: "DELETE",
    key: params.key,
    clientId: params.clientId,
    roomId: params.roomId,
    timestamp: params.timestamp ?? Date.now()
  };
}

export function validateOperation(op: VzyOperation): boolean {
  if (!UUID_V4.test(op.opId)) return false;
  if (op.type !== "SET" && op.type !== "DELETE") return false;
  if (!op.key || !op.clientId || !op.roomId) return false;
  if (!Number.isFinite(op.timestamp) || op.timestamp <= 0) return false;
  if (op.type === "SET" && !Object.prototype.hasOwnProperty.call(op, "value")) return false;
  if (typeof op.sequence !== "undefined" && (!Number.isInteger(op.sequence) || op.sequence <= 0)) return false;
  return true;
}
