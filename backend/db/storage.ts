/**
 * backend/db/storage.ts
 *
 * Storage — the backend-agnostic contract for everything the agent persists
 * (#822, #958): agent results, the audit log, agent state, the spending window,
 * approvals, idempotency, triggers, kill-switch state, and x402 nonces.
 *
 * Two implementations:
 *   • SqliteStorage   — the default; a local file, single instance only.
 *   • PostgresStorage — a shared database for multi-instance deployments
 *     (backend/db/postgres.ts). Select it with STORAGE_BACKEND=postgres and
 *     DATABASE_URL.
 *
 * Every method is async so both backends share one call shape, even though
 * better-sqlite3 itself is synchronous.
 */

import type { AgentResult } from '../agent';
import { config } from '../config';
import type { INonceStore } from '../nonce_store';
import { SqliteNonceStore } from '../nonce_store';
import * as persistence from '../persistence';
import type {
  ApprovalAuditRow,
  AuditEntry,
  AuditRow,
  PendingApproval,
  PersistedResult,
  SpendingRecord,
  SpendingReservation,
} from '../persistence';
import type { Trigger, TriggerSource } from '../triggers';
import { migrateSqlite } from './migrate';

export type StorageBackend = 'sqlite' | 'postgres';

/** A persisted MPP session: its channel id and JSON-serialized channel state. */
export interface MppSessionRecord {
  channelId: string;
  state: string;
}

export interface Storage {
  readonly backend: StorageBackend;

  /** Apply pending schema migrations; returns how many were applied. */
  migrate(): Promise<number>;
  /** Run a trivial query; throws when the store is unreachable. */
  probe(): Promise<void>;
  /** Release connections. */
  close(): Promise<void>;

  saveResult(result: PersistedResult): Promise<void>;
  getResults(limit?: number, offset?: number): Promise<PersistedResult[]>;

  appendAuditEntry(entry: AuditEntry): Promise<AuditRow>;
  getAuditLog(): Promise<AuditRow[]>;

  saveAgentState(key: string, value: string): Promise<void>;
  loadAgentState(key: string): Promise<string | undefined>;

  saveSpendingRecord(record: SpendingRecord): Promise<void>;
  loadSpendingRecords(sinceMs: number): Promise<SpendingRecord[]>;
  pruneSpendingRecords(cutoffMs: number): Promise<void>;
  clearSpendingRecords(): Promise<void>;
  /**
   * Atomically check the window total (records at or after `sinceMs`) and
   * record `amount` against it.
   */
  reserveSpending(
    amount: number,
    limit: number,
    sinceMs: number,
    now?: number
  ): Promise<SpendingReservation>;
  /** Release a reservation, e.g. after a failed submission. */
  releaseSpending(id: number): Promise<void>;

  /** Pending approvals (#736, #958). */
  savePendingApproval(approval: Omit<PendingApproval, 'status' | 'decidedAt'>): Promise<void>;
  getPendingApproval(id: string): Promise<PendingApproval | undefined>;
  listPendingApprovals(): Promise<PendingApproval[]>;
  decideApproval(id: string, decision: 'approved' | 'denied', now?: number): Promise<boolean>;
  expirePendingApprovals(now?: number): Promise<string[]>;
  getApprovalAuditLog(): Promise<ApprovalAuditRow[]>;

  /** Idempotency keys (#958). */
  getIdempotencyResult(key: string, payload: unknown): Promise<AgentResult | { conflict: true } | null>;
  saveIdempotencyResult(
    key: string,
    taskType: string,
    payload: unknown,
    result: AgentResult
  ): Promise<void>;
  pruneIdempotencyKeys(now?: number): Promise<number>;

  /** Triggers (#958). */
  saveTrigger(trigger: Trigger): Promise<void>;
  loadTriggers(): Promise<Trigger[]>;
  recordTriggerFired(triggerId: string, eventId: string): Promise<boolean>;
  deleteTrigger(id: string): Promise<void>;

  /** Kill-switch state (#958). */
  setPaused(paused: boolean): Promise<void>;
  isPaused(): Promise<boolean>;

  /** Nonce store sharing this storage's database. */
  nonceStore(): INonceStore;

  /** MPP Session storage: save, load, and update channel states. */
  saveMppSession(channelId: string, state: string): Promise<void>;
  loadMppSession(channelId: string): Promise<string | undefined>;
  /** Every persisted MPP session, for the expiry sweeper. */
  listMppSessions(): Promise<MppSessionRecord[]>;

  /** X402 rate limiter: save, load, prune, and clear timestamp records. */
  saveX402RateRecord(timestamp: number): Promise<void>;
  loadX402RateRecords(sinceMs: number): Promise<number[]>;
  pruneX402RateRecords(cutoffMs: number): Promise<void>;
  clearX402RateRecords(): Promise<void>;
}

// ─── SQLite ───────────────────────────────────────────────────────────────────

/**
 * SQLite storage over the connection owned by backend/persistence.ts.
 */
export class SqliteStorage implements Storage {
  readonly backend = 'sqlite' as const;

  async migrate(): Promise<number> {
    return migrateSqlite(persistence.getDb()).length;
  }

  async probe(): Promise<void> {
    persistence.probeDb();
  }

  async close(): Promise<void> {
    persistence.closeDb();
  }

  async saveResult(result: PersistedResult): Promise<void> {
    persistence.saveResult(result);
  }

  async getResults(limit?: number, offset?: number): Promise<PersistedResult[]> {
    return persistence.getResults(limit, offset);
  }

  async appendAuditEntry(entry: AuditEntry): Promise<AuditRow> {
    return persistence.appendAuditEntry(entry);
  }

  async getAuditLog(): Promise<AuditRow[]> {
    return persistence.getAuditLog();
  }

  async saveAgentState(key: string, value: string): Promise<void> {
    persistence.saveAgentState(key, value);
  }

  async loadAgentState(key: string): Promise<string | undefined> {
    return persistence.loadAgentState(key);
  }

  async saveSpendingRecord(record: SpendingRecord): Promise<void> {
    persistence.saveSpendingRecord(record);
  }

  async loadSpendingRecords(sinceMs: number): Promise<SpendingRecord[]> {
    return persistence.loadSpendingRecords(sinceMs);
  }

  async pruneSpendingRecords(cutoffMs: number): Promise<void> {
    persistence.pruneSpendingRecords(cutoffMs);
  }

  async clearSpendingRecords(): Promise<void> {
    persistence.clearSpendingRecords();
  }

  async reserveSpending(
    amount: number,
    limit: number,
    sinceMs: number,
    now?: number
  ): Promise<SpendingReservation> {
    return persistence.reserveSpendingRecord(amount, limit, sinceMs, now);
  }

  async releaseSpending(id: number): Promise<void> {
    persistence.deleteSpendingRecord(id);
  }

  async savePendingApproval(
    approval: Omit<PendingApproval, 'status' | 'decidedAt'>
  ): Promise<void> {
    persistence.savePendingApproval(approval);
  }

  async getPendingApproval(id: string): Promise<PendingApproval | undefined> {
    return persistence.getPendingApproval(id);
  }

  async listPendingApprovals(): Promise<PendingApproval[]> {
    return persistence.listPendingApprovals();
  }

  async decideApproval(
    id: string,
    decision: 'approved' | 'denied',
    now?: number
  ): Promise<boolean> {
    return persistence.decideApproval(id, decision, now);
  }

  async expirePendingApprovals(now?: number): Promise<string[]> {
    return persistence.expirePendingApprovals(now);
  }

  async getApprovalAuditLog(): Promise<ApprovalAuditRow[]> {
    return persistence.getApprovalAuditLog();
  }

  async getIdempotencyResult(
    key: string,
    payload: unknown
  ): Promise<AgentResult | { conflict: true } | null> {
    return persistence.getIdempotencyResult(key, payload);
  }

  async saveIdempotencyResult(
    key: string,
    taskType: string,
    payload: unknown,
    result: AgentResult
  ): Promise<void> {
    persistence.saveIdempotencyResult(key, taskType, payload, result);
  }

  async pruneIdempotencyKeys(now?: number): Promise<number> {
    return persistence.pruneIdempotencyKeys();
  }

  async saveTrigger(trigger: Trigger): Promise<void> {
    persistence.getDb()
      .prepare(
        `INSERT OR REPLACE INTO triggers (id, source, filter, task, enabled, created_at, last_fired_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        trigger.id,
        trigger.source,
        JSON.stringify(trigger.filter),
        JSON.stringify(trigger.task),
        trigger.enabled ? 1 : 0,
        trigger.createdAt,
        trigger.lastFiredAt ?? null
      );
  }

  async loadTriggers(): Promise<Trigger[]> {
    try {
      const rows = persistence.getDb()
        .prepare('SELECT * FROM triggers')
        .all() as Array<{
        id: string;
        source: string;
        filter: string;
        task: string;
        enabled: number;
        created_at: string;
        last_fired_at: string | null;
      }>;

      return rows.map((row) => ({
        id: row.id,
        source: row.source as TriggerSource,
        filter: JSON.parse(row.filter),
        task: JSON.parse(row.task),
        enabled: row.enabled === 1,
        createdAt: row.created_at,
        ...(row.last_fired_at !== null ? { lastFiredAt: row.last_fired_at } : {}),
      }));
    } catch {
      return [];
    }
  }

  async recordTriggerFired(triggerId: string, eventId: string): Promise<boolean> {
    const key = `trigger:${triggerId}:event:${eventId}`;
    const info = persistence.getDb()
      .prepare('INSERT OR IGNORE INTO trigger_events (key, fired_at) VALUES (?, ?)')
      .run(key, new Date().toISOString());
    return info.changes === 1;
  }

  async deleteTrigger(id: string): Promise<void> {
    persistence.getDb().prepare('DELETE FROM triggers WHERE id = ?').run(id);
  }

  async setPaused(paused: boolean): Promise<void> {
    await this.saveAgentState('paused', String(paused));
  }

  async isPaused(): Promise<boolean> {
    return (await this.loadAgentState('paused')) === 'true';
  }

  nonceStore(): INonceStore {
    return new SqliteNonceStore(persistence.getDb());
  }

  async saveMppSession(channelId: string, state: string): Promise<void> {
    persistence.saveAgentState(`mpp_session:${channelId}`, state);
  }

  async loadMppSession(channelId: string): Promise<string | undefined> {
    return persistence.loadAgentState(`mpp_session:${channelId}`);
  }

  async listMppSessions(): Promise<MppSessionRecord[]> {
    const prefix = 'mpp_session:';
    return persistence
      .listAgentStateByPrefix(prefix)
      .map(({ key, value }) => ({ channelId: key.slice(prefix.length), state: value }));
  }

  async saveX402RateRecord(timestamp: number): Promise<void> {
    persistence.saveX402RateRecord(timestamp);
  }

  async loadX402RateRecords(sinceMs: number): Promise<number[]> {
    return persistence.loadX402RateRecords(sinceMs);
  }

  async pruneX402RateRecords(cutoffMs: number): Promise<void> {
    persistence.pruneX402RateRecords(cutoffMs);
  }

  async clearX402RateRecords(): Promise<void> {
    persistence.clearX402RateRecords();
  }
}

// ─── Process-wide instance ────────────────────────────────────────────────────

let _storage: Storage | null = null;

export function getStorage(): Storage {
  if (!_storage) {
    if (config.STORAGE_BACKEND === 'postgres') {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { PostgresStorage } = require('./postgres') as typeof import('./postgres');
      _storage = PostgresStorage.fromUrl(config.DATABASE_URL as string);
    } else {
      _storage = new SqliteStorage();
    }
  }
  return _storage;
}

export function _setStorage(storage: Storage | null): void {
  _storage = storage;
}
