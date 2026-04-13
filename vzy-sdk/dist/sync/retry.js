export class RetryEngine {
    sendFn;
    maxMs;
    timers = new Map();
    attempts = new Map();
    constructor(sendFn, maxMs) {
        this.sendFn = sendFn;
        this.maxMs = maxMs;
    }
    schedule(op) {
        this.cancel(op.opId);
        this.run(op);
    }
    cancel(opId) {
        const timer = this.timers.get(opId);
        if (timer) {
            clearTimeout(timer);
            this.timers.delete(opId);
        }
        this.attempts.delete(opId);
    }
    clear() {
        for (const timer of this.timers.values()) {
            clearTimeout(timer);
        }
        this.timers.clear();
        this.attempts.clear();
    }
    run(op) {
        const attempt = this.attempts.get(op.opId) ?? 0;
        const delay = Math.min(30_000, Math.min(this.maxMs, 1000 * 2 ** attempt));
        const timer = setTimeout(() => {
            this.sendFn(op);
            this.attempts.set(op.opId, attempt + 1);
            this.run(op);
        }, delay);
        const t = timer;
        t.unref?.();
        this.timers.set(op.opId, timer);
    }
}
//# sourceMappingURL=retry.js.map