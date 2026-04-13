const WAL_KEY = "__vzy_wal_records__";
const LAST_SEQUENCE_KEY = "__vzy_last_sequence__";
export class WALEngine {
    storage;
    records = new Map();
    constructor(storage) {
        this.storage = storage;
    }
    async hydrate() {
        const persisted = (await this.storage.get(WAL_KEY)) ?? [];
        this.records = new Map(persisted.map((record) => [record.opId, record]));
    }
    async writePending(op) {
        if (!this.records.has(op.opId)) {
            this.records.set(op.opId, { opId: op.opId, roomId: op.roomId, payload: op, status: "pending" });
            await this.persist();
        }
    }
    async markAcked(opId, sequence) {
        const record = this.records.get(opId);
        if (!record)
            return;
        this.records.set(opId, { ...record, status: "acked", sequence });
        await this.persist();
        await this.setLastSequence(sequence);
    }
    async persistAppliedRemote(op) {
        if (typeof op.sequence !== "number")
            return;
        this.records.set(op.opId, {
            opId: op.opId,
            roomId: op.roomId,
            payload: op,
            status: "acked",
            sequence: op.sequence
        });
        await this.persist();
        await this.setLastSequence(op.sequence);
    }
    getPending() {
        return [...this.records.values()].filter((record) => record.status === "pending");
    }
    getAll() {
        return [...this.records.values()];
    }
    async getLastSequence() {
        return (await this.storage.get(LAST_SEQUENCE_KEY)) ?? 0;
    }
    async setLastSequence(sequence) {
        const existing = await this.getLastSequence();
        if (sequence > existing) {
            await this.storage.set(LAST_SEQUENCE_KEY, sequence);
        }
    }
    async persist() {
        await this.storage.set(WAL_KEY, [...this.records.values()]);
    }
}
//# sourceMappingURL=wal.js.map