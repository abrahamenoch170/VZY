import { StateStore } from "../core/store.js";

export class QueryLayer {
  constructor(private readonly store: StateStore) {}

  get<T = unknown>(key: string): T | undefined {
    return this.store.get<T>(key);
  }

  where(criteria: Record<string, unknown>): Array<Record<string, unknown>> {
    const entries = this.store.entries();
    const results: Array<Record<string, unknown>> = [];

    for (const [, value] of entries) {
      if (typeof value !== "object" || value === null) {
        continue;
      }
      const row = value as Record<string, unknown>;
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
