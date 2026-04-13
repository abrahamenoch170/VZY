import { createDeleteOperation, createSetOperation, validateOperation } from "../core/operations.js";
import { assert } from "./helpers.js";

export async function testOperations(): Promise<void> {
  const setOp = createSetOperation({ key: "x", value: 1, clientId: "c1", roomId: "r1" });
  const delOp = createDeleteOperation({ key: "x", clientId: "c1", roomId: "r1" });

  assert(validateOperation(setOp), "SET operation should be valid");
  assert(validateOperation(delOp), "DELETE operation should be valid");
}
