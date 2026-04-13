export class QueryLayer {
    store;
    constructor(store) {
        this.store = store;
    }
    get(key) {
        return this.store.get(key);
    }
    where(criteria) {
        const entries = this.store.entries();
        const results = [];
        for (const [, value] of entries) {
            if (typeof value !== "object" || value === null) {
                continue;
            }
            const row = value;
            let matches = true;
            for (const [cKey, cValue] of Object.entries(criteria)) {
                if (row[cKey] !== cValue) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                results.push(row);
            }
        }
        return results;
    }
}
//# sourceMappingURL=query.js.map