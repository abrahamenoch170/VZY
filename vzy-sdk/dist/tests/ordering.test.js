import { OrderingBuffer } from "../sync/orderingBuffer.js";
import { equal } from "./helpers.js";
function op(id, sequence) {
    return {
        opId: id,
        type: "SET",
        key: `k-${id}`,
        value: id,
        clientId: "c1",
        roomId: "r1",
        timestamp: sequence,
        sequence
    };
}
export async function testOrdering() {
    const buffer = new OrderingBuffer(0);
    const out1 = buffer.add(op("2", 2));
    equal(out1.length, 0, "sequence gap should buffer op");
    const out2 = buffer.add(op("1", 1));
    equal(out2.length, 2, "sequence fill should release ordered ops");
    equal(out2[0]?.sequence, 1, "first sequence should be 1");
    equal(out2[1]?.sequence, 2, "second sequence should be 2");
}
//# sourceMappingURL=ordering.test.js.map