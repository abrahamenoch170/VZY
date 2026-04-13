const OPS_KEY = "__vzy_ops__";
export class MemoryStorageAdapter {
    kv = new Map();
    ops = [];
    async get(key) {
        return this.kv.get(key);
    }
    async set(key, value) {
        this.kv.set(key, value);
    }
    async delete(key) {
        this.kv.delete(key);
    }
    async getAllOps() {
        return [...this.ops];
    }
    async appendOp(op) {
        this.ops.push(op);
        this.kv.set(OPS_KEY, [...this.ops]);
    }
}
//# sourceMappingURL=adapter.js.map