import { testOperations } from "./operations.test.js";
import { testStore } from "./store.test.js";
import { testSync } from "./sync.test.js";
import { testOrdering } from "./ordering.test.js";

async function main(): Promise<void> {
  const tests: Array<[string, () => Promise<void>]> = [
    ["operations", testOperations],
    ["store", testStore],
    ["sync", testSync],
    ["ordering", testOrdering]
  ];

  for (const [name, fn] of tests) {
    await fn();
    console.log(`PASS ${name}`);
  }
}

void main();
