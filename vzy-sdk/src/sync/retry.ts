import type { VzyOperation } from "../types/index.js";

export class RetryEngine {
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private attempts = new Map<string, number>();

  constructor(private readonly sendFn: (op: VzyOperation) => void, private readonly maxMs: number) {}

  schedule(op: VzyOperation): void {
    this.cancel(op.opId);
    this.run(op);
  }

  cancel(opId: string): void {
    const timer = this.timers.get(opId);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(opId);
    }
    this.attempts.delete(opId);
  }

  clear(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    this.attempts.clear();
  }

  private run(op: VzyOperation): void {
    const attempt = this.attempts.get(op.opId) ?? 0;
    const delay = Math.min(30_000, Math.min(this.maxMs, 1000 * 2 ** attempt));
    const timer = setTimeout(() => {
      this.sendFn(op);
      this.attempts.set(op.opId, attempt + 1);
      this.run(op);
    }, delay);
    const t = timer as ReturnType<typeof setTimeout> & { unref?: () => void };
    t.unref?.();
    this.timers.set(op.opId, timer);
  }
}
