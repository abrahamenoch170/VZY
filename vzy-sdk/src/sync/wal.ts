import type { StorageAdapter, WALRecord, VzyOperation } from "../types/index.js";

const WAL_KEY = "__vzy_wal_records__";
const LAST_SEQUENCE_KEY = "__vzy_last_sequence__";

export class WALEngine {
  private records = new Map<string, WALRecord>();
  private lastSequence = 0;

  constructor(private readonly storage: StorageAdapter) {}

  async hydrate(): Promise<void> {
    const persisted = (await this.storage.get<WALRecord[]>(WAL_KEY)) ?? [];
    for (const rec of persisted) {
      this.records.set(rec.opId, rec);
      if (typeof rec.sequence === "number" && rec.sequence > this.lastSequence) {
        this.lastSequence = rec.sequence;
      }
    }
    const storedSeq = await this.storage.get<number>(LAST_SEQUENCE_KEY);
    if (typeof storedSeq === "number" && storedSeq > this.lastSequence) {
      this.lastSequence = storedSeq;
    }
  }

  getLastSequence(): number {
    return this.lastSequence;
  }

  async setLastSequence(seq: number): Promise<void> {
    if (seq <= this.lastSequence) return;
    this.lastSequence = seq;
    await this.storage.set(LAST_SEQUENCE_KEY, seq);
  }

  async putPending(op: VzyOperation): Promise<void> {
    const existing = this.records.get(op.opId);
    if (existing?.status === "acked") return;
    const record: WALRecord = {
      opId: op.opId,
      roomId: op.roomId,
      payload: op,
      status: "pending",
      retryCount: existing?.retryCount ?? 0,
      nextRetryAt: existing?.nextRetryAt ?? Date.now(),
      timestamp: existing?.timestamp ?? op.timestamp
    };
    const nodeId = op.nodeId ?? existing?.nodeId;
    if (nodeId) record.nodeId = nodeId;
    const sourceNode = op.sourceNode ?? existing?.sourceNode;
    if (sourceNode) record.sourceNode = sourceNode;
    if (typeof existing?.sequence === "number") record.sequence = existing.sequence;
    this.records.set(op.opId, record);
    await this.persist();
  }

  async markAcked(opId: string, sequence: number): Promise<void> {
    const current = this.records.get(opId);
    if (!current) return;
    this.records.set(opId, { ...current, status: "acked", sequence, timestamp: current.timestamp ?? Date.now() });
    await this.setLastSequence(sequence);
    await this.persist();
  }

  listPending(): WALRecord[] {
    return [...this.records.values()]
      .filter((r) => r.status === "pending")
      .sort((a, b) => a.payload.timestamp - b.payload.timestamp);
  }

  getAll(): WALRecord[] {
    return [...this.records.values()];
  }

  async updateRetry(opId: string, retryCount: number, nextRetryAt: number): Promise<void> {
    const rec = this.records.get(opId);
    if (!rec || rec.status !== "pending") return;
    this.records.set(opId, { ...rec, retryCount, nextRetryAt });
    await this.persist();
  }

  private async persist(): Promise<void> {
    await this.storage.set(WAL_KEY, [...this.records.values()]);
  }
}
