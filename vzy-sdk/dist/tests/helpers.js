export function assert(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}
export function equal(actual, expected, message) {
    if (actual !== expected) {
        throw new Error(`${message}: expected=${String(expected)} actual=${String(actual)}`);
    }
}
//# sourceMappingURL=helpers.js.map