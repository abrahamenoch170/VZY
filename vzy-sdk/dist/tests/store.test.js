import { LastWriteWinsResolver } from "../conflict/lww.js";
import { StateStore } from "../core/store.js";
import { equal } from "./helpers.js";
function op(partial) {
    return {
        opId: partial.opId ?? "123e4567-e89b-42d3-a456-426614174000",
        type: partial.type ?? "SET",
        key: partial.key ?? "k",
        value: partial.value,
        clientId: partial.clientId ?? "c1",
        roomId: partial.roomId ?? "r1",
        timestamp: partial.timestamp ?? 1
    };
}
export async function testStore() {
    const store = new StateStore(new LastWriteWinsResolver());
    const first = op({ value: "a" });
    const duplicate = op({ value: "b" });
    equal(store.applyOperation(first), true, "first op should apply");
    equal(store.applyOperation(duplicate), false, "duplicate op should be ignored");
    equal(store.get("k"), "a", "value should remain first op");
    const newer = op({ value: "new", timestamp: 200, opId: "123e4567-e89b-42d3-a456-426614174001" });
    store.applyOperation(newer);
    equal(store.get("k"), "new", "newer timestamp should win");
}
//# sourceMappingURL=store.test.js.map