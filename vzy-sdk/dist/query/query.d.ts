import { StateStore } from "../core/store.js";
export declare class QueryLayer {
    private readonly store;
    constructor(store: StateStore);
    get<T = unknown>(key: string): T | undefined;
    where(criteria: Record<string, unknown>): Array<Record<string, unknown>>;
}
