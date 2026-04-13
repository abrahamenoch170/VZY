export class OrderingBuffer {
    lastAppliedSequence;
    pendingBySequence = new Map();
    seenOps = new Set();
    constructor(lastAppliedSequence = 0) {
        this.lastAppliedSequence = lastAppliedSequence;
    }
    getLastSequence() {
        return this.lastAppliedSequence;
    }
    setLastSequence(sequence) {
        this.lastAppliedSequence = Math.max(this.lastAppliedSequence, sequence);
    }
    markSeen(opId) {
        this.seenOps.add(opId);
    }
    hasSeen(opId) {
        return this.seenOps.has(opId);
    }
    add(op) {
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
        const ready = [];
        let next = this.lastAppliedSequence + 1;
        while (this.pendingBySequence.has(next)) {
            const candidate = this.pendingBySequence.get(next);
            if (!candidate)
                break;
            ready.push(candidate);
            this.pendingBySequence.delete(next);
            this.lastAppliedSequence = next;
            next += 1;
        }
        return ready;
    }
}
//# sourceMappingURL=orderingBuffer.js.map