import { LastWriteWinsResolver } from "../conflict/lww.js";
import { createDeleteOperation, createSetOperation, validateOperation } from "./operations.js";
import { StateStore } from "./store.js";
import { generateClientId } from "../identity/clientId.js";
import { QueryLayer } from "../query/query.js";
import { IndexedDBStorageAdapter } from "../storage/indexeddb.js";
import { SyncEngine } from "../sync/syncEngine.js";
import { WebSocketTransport } from "../sync/websocket.js";
import type { Transport, VzyClient, VzyClientConfig, VzyOperation } from "../types/index.js";

function sortOps(ops: VzyOperation[]): VzyOperation[] {
  return [...ops].sort((a, b) => {
    const aSeq = typeof a.sequence === "number" ? a.sequence : Number.MAX_SAFE_INTEGER;
    const bSeq = typeof b.sequence === "number" ? b.sequence : Number.MAX_SAFE_INTEGER;
    if (aSeq !== bSeq) return aSeq - bSeq;
    if (a.timestamp !== b.timestamp) return a.timestamp - b.timestamp;
    return a.opId.localeCompare(b.opId);
  });
}

export async function createVzyClient(config: VzyClientConfig): Promise<VzyClient> {
  const clientId = config.clientId ?? generateClientId();
  const storage = config.storage ?? new IndexedDBStorageAdapter();
  const resolver = config.resolver ?? new LastWriteWinsResolver();
  const store = new StateStore(resolver);
  const query = new QueryLayer(store);
  const opLog = new Map<string, VzyOperation>();

  let syncRef: SyncEngine | undefined;
  const transport: Transport = config.transportFactory
    ? config.transportFactory({ roomId: config.roomId, clientId, serverUrl: config.serverUrl })
    : new WebSocketTransport(config.serverUrl, config.roomId, clientId, () => syncRef?.getLastSequence() ?? 0);

  const sync = new SyncEngine(
    transport,
    store,
    storage,
    {
      retryBaseMs: config.retryBaseMs ?? 1000,
      maxRetryMs: config.maxRetryMs ?? 30_000
    },
    { roomId: config.roomId, clientId },
    (op) => {
      opLog.set(op.opId, op);
    }
  );
  syncRef = sync;

  const existingOps = sortOps(await storage.getAllOps());
  for (const op of existingOps) {
    if (!validateOperation(op)) continue;
    opLog.set(op.opId, op);
    store.applyOperation(op);
  }

  await sync.init();

  const appendAndApply = async (op: VzyOperation): Promise<void> => {
    if (!validateOperation(op)) throw new Error("invalid operation");
    const applied = store.applyOperation(op);
    opLog.set(op.opId, op);
    if (!applied) return;
    await storage.appendOp(op);
    await sync.publish(op);
  };

  return {
    async set<T>(key: string, value: T): Promise<void> {
      await appendAndApply(createSetOperation({ key, value, clientId, roomId: config.roomId }));
    },

    async delete(key: string): Promise<void> {
      await appendAndApply(createDeleteOperation({ key, clientId, roomId: config.roomId }));
    },

    get<T = unknown>(key: string): T | undefined {
      return store.get<T>(key);
    },

    subscribe<T = unknown>(key: string, callback: (value: T | undefined) => void): () => void {
      return store.subscribe<T>(key, callback);
    },

    connect(): Promise<void> {
      return sync.connect();
    },

    disconnect(): void {
      sync.disconnect();
    },

    sync: {
      status(): "idle" | "connecting" | "connected" | "offline" {
        return sync.getStatus();
      }
    },

    query: {
      get<T = unknown>(key: string): T | undefined {
        return query.get<T>(key);
      },
      where(criteria: Record<string, unknown>): Array<Record<string, unknown>> {
        return query.where(criteria);
      }
    },

    log: {
      getAll(): VzyOperation[] {
        return sortOps([...opLog.values()]);
      }
    },

    debug: {
      dumpState(): Record<string, unknown> {
        return store.snapshot();
      }
    }
  };
}
