import type { VzyOperation } from "../types/index.js";

export class OrderingBuffer {
  private readonly pendingBySequence = new Map<number, VzyOperation>();
  private readonly seenOps = new Set<string>();

  constructor(private lastAppliedSequence = 0) {}

  getLastSequence(): number {
    return this.lastAppliedSequence;
  }

  setLastSequence(sequence: number): void {
    this.lastAppliedSequence = Math.max(this.lastAppliedSequence, sequence);
  }

  markSeen(opId: string): void {
    this.seenOps.add(opId);
  }

  hasSeen(opId: string): boolean {
    return this.seenOps.has(opId);
  }

  add(op: VzyOperation): VzyOperation[] {
    if (this.hasSeen(op.opId)) {
      return [];
    }
    this.markSeen(op.opId);

    if (typeof op.sequence !== "number") {
      return [op];
    }

    if (op.sequence <= this.lastAppliedSequence) {
      return [];
    }

    this.pendingBySequence.set(op.sequence, op);

    const ready: VzyOperation[] = [];
    let next = this.lastAppliedSequence + 1;
    while (this.pendingBySequence.has(next)) {
      const candidate = this.pendingBySequence.get(next);
      if (!candidate) break;
      ready.push(candidate);
      this.pendingBySequence.delete(next);
      this.lastAppliedSequence = next;
      next += 1;
    }

    return ready;
  }
}
