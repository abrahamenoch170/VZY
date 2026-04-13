import { testOperations } from "./operations.test.js";
import { testStore } from "./store.test.js";
import { testSync } from "./sync.test.js";
async function main() {
    const tests = [
        ["operations", testOperations],
        ["store", testStore],
        ["sync", testSync]
    ];
    for (const [name, fn] of tests) {
        await fn();
        console.log(`PASS ${name}`);
    }
}
void main();
//# sourceMappingURL=run.js.map