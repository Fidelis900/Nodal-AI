/**
 * backend/server.ts
 *
 * Health-check HTTP server for container orchestration (ECS, Kubernetes, etc.).
 *
 * Endpoints:
 *   GET /health  → 200 { status: "ok", components: { horizon, soroban, database } }
 *                  503 { status: "degraded", components: { ... } } when a dependency is down
 *   GET /status  → 200 { results: PersistedResult[] }   (paginated via ?limit=&offset=,
 *                  default last 10; Bearer auth when ADMIN_API_TOKEN is set)
 *   GET /spending → 200 { total, limit, windowMs, percentUsed, usage } (Bearer auth);
 *                  `usage` breaks the window down per asset (#733)
 *   GET /metrics → Prometheus text exposition (Bearer auth)
 *   GET /approvals → 200 { approvals: PendingApproval[] } (Bearer auth)
 *   POST /approvals/:id { "decision": "approve" | "deny" } → 200 { approval, result? } (Bearer auth)
 *   POST /stream/ticket → 200 { ticket, expiresInMs } single-use SSE ticket (Bearer auth, #965)
 *   GET /stream → SSE stream for real-time dashboard updates (Bearer auth, or ?ticket= from
 *                 POST /stream/ticket — an EventSource cannot set an Authorization header)
 *   POST /admin/pause → 200 { paused: true } halt value-moving tasks (Bearer auth)
 *   POST /admin/resume → 200 { paused: false } resume them (Bearer auth)
 *
 * ADMIN_API_TOKEN is the API bearer token. WEBHOOK_SECRET is still accepted in
 * its place when ADMIN_API_TOKEN is unset, but that fallback is deprecated.
 *
 * CORS (#966): every route answers an OPTIONS preflight, and
 * Access-Control-Allow-Origin is set when the request's Origin is listed in
 * CORS_ALLOWED_ORIGINS. Unset (default) means no CORS at all.
 *
 * Uses only the Node.js built-in `http` module — no additional dependencies.
 * Port is read from config.HEALTH_PORT (env var HEALTH_PORT, default 3000).
 */

import * as http from 'http';
import { createHash, randomUUID, timingSafeEqual } from 'crypto';
import { config, getAdminApiToken } from './config';
import { getResults, type PendingApproval } from './persistence';
import { horizonServer, sorobanServer } from './rpc_client';
import { db } from './db/client';
import { handleError } from './middleware/error_handler';
import { createLogger } from './utils/logger';
import { spendingTracker, type PayFiAgent, type AgentResult } from './agent';
import { llmUsage } from './ai/llm';
import { metricsRegistry, renderMetrics, spendingWindowTotal } from './metrics';
import { pauseAgent, resumeAgent, isPaused } from './kill_switch';
import { assetKey as makeAssetKey } from './spending_tracker';
import { stroopsToString } from './utils/amount';

const log = createLogger('health-server');

// ─── Health types & component probes ─────────────────────────────────────────

/** Reachability of a single dependency. */
export type ComponentStatus = 'up' | 'down';

export interface HealthResponse {
  status: 'ok' | 'degraded';
  components: {
    horizon: ComponentStatus;
    soroban: ComponentStatus;
    database: ComponentStatus;
  };
}

/**
 * Bound on a single probe.
 *
 * Without it an unreachable endpoint hangs until the socket's own timeout,
 * and an orchestrator's liveness probe times out first — reporting the agent
 * as dead rather than degraded, which triggers a restart instead of a
 * failover.
 */
const PROBE_TIMEOUT_MS = 3_000;

async function withTimeout<T>(work: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error('probe timed out')), PROBE_TIMEOUT_MS);
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}

/** Probe Horizon. */
export async function checkHorizon(): Promise<ComponentStatus> {
  try {
    await withTimeout(horizonServer.fetchBaseFee());
    return 'up';
  } catch (err) {
    log.warn({ msg: 'Horizon health probe failed', err: String(err) });
    return 'down';
  }
}

/**
 * Probe Soroban RPC (#233).
 *
 * Previously unchecked, so an agent whose Soroban RPC was down still reported
 * `status: "ok"` — while every `soroban_invoke` and `soroban_query` task
 * failed. Horizon being reachable says nothing about Soroban; they are
 * separate services on separate hosts.
 */
export async function checkSoroban(): Promise<ComponentStatus> {
  try {
    await withTimeout(sorobanServer.getNetwork());
    return 'up';
  } catch (err) {
    log.warn({ msg: 'Soroban RPC health probe failed', err: String(err) });
    return 'down';
  }
}

export async function checkDatabase(): Promise<ComponentStatus> {
  try {
    return (await withTimeout(db.healthCheck())) ? 'up' : 'down';
  } catch (err) {
    log.warn({ msg: 'Database health probe failed', err: String(err) });
    return 'down';
  }
}

// ─── Auth helper ──────────────────────────────────────────────────────────────

/**
 * The bearer token the HTTP API accepts: ADMIN_API_TOKEN, falling back to the
 * deprecated WEBHOOK_SECRET so existing deployments keep working. Read per
 * request so a rotated value takes effect without re-importing the module.
 */
function apiToken(): string | undefined {
  return getAdminApiToken();
}

/**
 * Constant-time token comparison. Both sides are hashed first so the buffers
 * handed to timingSafeEqual are always the same length, and the comparison
 * leaks neither the token's contents nor its length.
 */
function tokensMatch(given: string, expected: string): boolean {
  const a = createHash('sha256').update(given).digest();
  const b = createHash('sha256').update(expected).digest();
  return timingSafeEqual(a, b);
}

/**
 * Returns true if the request passes Bearer-token authentication.
 * When no API token is configured, requests are allowed on testnet/futurenet
 * only; on mainnet they are always rejected.
 *
 * Does NOT accept the token from a URL query parameter (#965) — that leaks it
 * into proxy/access logs and browser history. GET /stream, the one route an
 * EventSource (which cannot set an Authorization header) must reach, instead
 * uses a short-lived single-use ticket; see {@link isStreamAuthenticated}.
 */
function isAuthenticated(req: http.IncomingMessage): boolean {
  const secret = apiToken();
  if (!secret) return config.STELLAR_NETWORK !== 'mainnet';

  const auth = req.headers['authorization'];
  if (auth) {
    const [scheme, token] = auth.split(' ');
    if (scheme?.toLowerCase() === 'bearer' && token !== undefined && tokensMatch(token, secret)) {
      return true;
    }
  }

  return false;
}

/**
 * Stricter variant of {@link isAuthenticated} used by GET /metrics and the
 * kill-switch routes: a missing API token denies the request instead of
 * allowing it, so metrics are not world-readable and nobody can flip the kill
 * switch anonymously on a misconfigured deployment.
 */
function isStrictlyAuthenticated(req: http.IncomingMessage): boolean {
  return Boolean(apiToken()) && isAuthenticated(req);
}

// ─── SSE stream tickets (#965) ─────────────────────────────────────────────
//
// An EventSource cannot set an Authorization header, so GET /stream cannot
// use ordinary Bearer auth. POST /stream/ticket (which *can* use a header,
// since it's a normal fetch) exchanges the admin token for a short-lived,
// single-use ticket that's safe to put in the stream URL: it's useless to
// anyone who captures it from a log after STREAM_TICKET_TTL_MS, and useless
// a second time to anyone who captures it before that.

const STREAM_TICKET_TTL_MS = 60_000;
const streamTickets = new Map<string, number>(); // ticket -> expiresAt (epoch ms)

function pruneExpiredStreamTickets(now: number): void {
  for (const [ticket, expiresAt] of streamTickets) {
    if (expiresAt <= now) streamTickets.delete(ticket);
  }
}

/** Issues a new ticket, valid for one GET /stream connection within STREAM_TICKET_TTL_MS. */
function issueStreamTicket(): string {
  const now = Date.now();
  pruneExpiredStreamTickets(now);
  const ticket = randomUUID();
  streamTickets.set(ticket, now + STREAM_TICKET_TTL_MS);
  return ticket;
}

/** Consumes `ticket` if (and only if) it exists and has not expired. Always single-use. */
function consumeStreamTicket(ticket: string): boolean {
  const expiresAt = streamTickets.get(ticket);
  streamTickets.delete(ticket); // single-use: gone whether or not it was valid
  return expiresAt !== undefined && expiresAt > Date.now();
}

/**
 * Auth check for GET /stream only: a normal Bearer header (for non-browser
 * clients that can set one), or a valid `ticket` query parameter minted by
 * POST /stream/ticket. Never a raw token in the query string (#965).
 */
function isStreamAuthenticated(req: http.IncomingMessage): boolean {
  if (isAuthenticated(req)) return true;

  const url = req.url;
  if (!url) return false;
  try {
    const parsed = new URL(url, `http://${req.headers.host ?? 'localhost'}`);
    const ticket = parsed.searchParams.get('ticket');
    return !!ticket && consumeStreamTicket(ticket);
  } catch {
    return false;
  }
}

// ─── CORS (#966) ────────────────────────────────────────────────────────────
//
// Unset CORS_ALLOWED_ORIGINS (default) means no CORS at all: no
// Access-Control-* header is ever sent, so a cross-origin browser request
// simply fails the way it always has. When set, only an exact origin on the
// list gets the preflight response and the Access-Control-Allow-Origin
// header — the origin is echoed back, never "*", since this API uses
// credentials (the Bearer token).
function corsAllowedOrigins(): Set<string> {
  if (!config.CORS_ALLOWED_ORIGINS) return new Set();
  return new Set(config.CORS_ALLOWED_ORIGINS.split(',').map((o) => o.trim()));
}

/**
 * Sets CORS headers when `req`'s Origin is allowed, and answers an OPTIONS
 * preflight directly. Returns true when the caller should stop (the request
 * was an OPTIONS preflight and has already been answered).
 */
function applyCors(req: http.IncomingMessage, res: http.ServerResponse): boolean {
  const origin = req.headers.origin;
  if (origin && corsAllowedOrigins().has(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  }

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return true;
  }

  return false;
}

/** One asset's share of the rolling spending window, as reported by GET /spending. */
interface SpendingUsage {
  assetKey: string;
  /** Canonical 7-decimal string (e.g. "50.0000000"). */
  spent: string;
  /** The limit that applies to this asset, or "denied" when it may not be spent (#733). */
  limit: string;
}

/**
 * Per-asset breakdown of the window (#733): every asset spent in it, every
 * asset with a configured limit, and always the default x402 asset.
 */
function buildSpendingUsage(): SpendingUsage[] {
  const totalsMap = spendingTracker.totals();
  const limitsMap = config.SPENDING_LIMITS_MAP;
  const globalLimit = config.AGENT_SPENDING_LIMIT;

  const allKeys = new Set<string>([
    ...totalsMap.keys(),
    ...limitsMap.keys(),
    makeAssetKey(config.X402_ASSET_CODE, config.X402_ASSET_ISSUER),
  ]);

  return [...allKeys].map((key) => ({
    assetKey: key,
    spent: stroopsToString(totalsMap.get(key) ?? 0n),
    limit:
      limitsMap.size > 0
        ? (limitsMap.get(key) ?? (config.SPENDING_LIMITS_ALLOW_UNKNOWN ? globalLimit : 'denied'))
        : globalLimit,
  }));
}

const HEALTH_PATH = '/health';
const STATUS_PATH = '/status';
const SPENDING_PATH = '/spending';
const METRICS_PATH = '/metrics';
const APPROVALS_PATH = '/approvals';
const STREAM_PATH = '/stream';
const STREAM_TICKET_PATH = '/stream/ticket';
const ADMIN_PAUSE_PATH = '/admin/pause';
const ADMIN_RESUME_PATH = '/admin/resume';

/**
 * Creates and returns the health-check HTTP server.
 * Does NOT call `.listen()` — the caller (typically `index.ts`) is responsible
 * for binding to `config.HEALTH_PORT`. The approval routes need `agent`; they
 * answer 503 without one.
 */
export function createHealthServer(agent?: PayFiAgent): http.Server {
  const server = http.createServer((req, res) => {
    // ── CORS preflight (#966) ─────────────────────────────────────────────
    // Applied before any route so every path answers OPTIONS the same way.
    if (applyCors(req, res)) return;

    // ── GET /health ────────────────────────────────────────────────────────
    if (req.method === 'GET' && req.url === HEALTH_PATH) {
      // Delegate to the real implementation: it probes Horizon, Soroban and
      // the database concurrently and reports 503 / "degraded" when any
      // dependency is down. The previous inline branch hardcoded
      // `status: "ok"` and never checked anything.
      void handleHealth(req, res);
      return;
    }

    // ── GET /status ────────────────────────────────────────────────────────
    // Delegates to handleResults(), which enforces Bearer-token auth when
    // ADMIN_API_TOKEN is set and honours `limit`/`offset` query parameters.
    // The previous inline branch served persisted results with no auth guard
    // and a hardcoded `getResults(10)`.
    if (req.method === 'GET' && req.url?.split('?')[0] === STATUS_PATH) {
      const parsedUrl = new URL(
        req.url ?? STATUS_PATH,
        `http://${req.headers.host ?? 'localhost'}`
      );
      void handleResults(req, res, parsedUrl);
      return;
    }

    // ── GET /spending ──────────────────────────────────────────────────────
    if (req.method === 'GET' && req.url === SPENDING_PATH) {
      if (!isAuthenticated(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }

      const total = spendingTracker.total();
      const limit = parseFloat(config.AGENT_SPENDING_LIMIT);
      const body = JSON.stringify({
        total,
        limit,
        windowMs: config.SPENDING_WINDOW_MS,
        percentUsed: (total / limit) * 100,
        usage: buildSpendingUsage(),
      });

      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      });
      res.end(body);
      return;
    }

    // ── GET /metrics ───────────────────────────────────────────────────────
    if (req.method === 'GET' && req.url === METRICS_PATH) {
      if (!isStrictlyAuthenticated(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }

      // Prometheus scrapers send `text/plain` / OpenMetrics Accept headers;
      // only an explicit JSON request gets the legacy JSON snapshot.
      if (!req.headers['accept']?.includes('application/json')) {
        void handlePrometheusMetrics(res);
        return;
      }

      const llmMetrics = llmUsage.getMetrics();
      const spendingStatus = spendingTracker.getWindowStatus();

      const body = JSON.stringify({
        llm: llmMetrics,
        spending: {
          ...spendingStatus,
          limit: parseFloat(config.AGENT_SPENDING_LIMIT),
          percentUsed:
            spendingStatus.total > 0
              ? (spendingStatus.total / parseFloat(config.AGENT_SPENDING_LIMIT)) * 100
              : 0,
        },
      });

      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      });
      res.end(body);
      return;
    }

    // ── GET /approvals ─────────────────────────────────────────────────────
    if (req.method === 'GET' && req.url === APPROVALS_PATH) {
      if (!isAuthenticated(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }
      if (!agent) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Agent not available' }));
        return;
      }

      const body = JSON.stringify({ approvals: agent.listApprovals() });
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      });
      res.end(body);
      return;
    }

    // ── POST /approvals/:id ────────────────────────────────────────────────
    if (req.method === 'POST' && req.url?.startsWith(`${APPROVALS_PATH}/`)) {
      const id = decodeURIComponent(req.url.slice(APPROVALS_PATH.length + 1));
      void handleApprovalDecision(req, res, agent, id);
      return;
    }

    // ── POST /stream/ticket ────────────────────────────────────────────────
    // Exchanges a Bearer token (never put in a URL) for a short-lived,
    // single-use ticket that GET /stream accepts in its query string (#965).
    if (req.method === 'POST' && req.url === STREAM_TICKET_PATH) {
      if (!isAuthenticated(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }
      const body = JSON.stringify({
        ticket: issueStreamTicket(),
        expiresInMs: STREAM_TICKET_TTL_MS,
      });
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      });
      res.end(body);
      return;
    }

    // ── GET /stream ────────────────────────────────────────────────────────
    // SSE stream for real-time dashboard updates
    if (req.method === 'GET' && req.url?.split('?')[0] === STREAM_PATH) {
      if (!isStreamAuthenticated(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }
      void handleStream(req, res, agent);
      return;
    }

    // ── POST /admin/pause ──────────────────────────────────────────────────
    if (req.method === 'POST' && req.url === ADMIN_PAUSE_PATH) {
      if (!isStrictlyAuthenticated(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }
      pauseAgent('Paused via HTTP API');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ paused: true }));
      return;
    }

    // ── POST /admin/resume ─────────────────────────────────────────────────
    if (req.method === 'POST' && req.url === ADMIN_RESUME_PATH) {
      if (!isStrictlyAuthenticated(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }
      resumeAgent('Resumed via HTTP API');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ paused: false }));
      return;
    }

    // ── 404 for everything else ────────────────────────────────────────────
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
  });

  return server;
}

/**
 * Convenience: create the server and immediately start listening.
 * Returns the bound server instance.
 */
export function startHealthServer(agent?: PayFiAgent): http.Server {
  const server = createHealthServer(agent);
  const port = config.HEALTH_PORT;

  server.listen(port, () => {
    process.stdout.write(`✅ [HealthServer] Listening on port ${port}\n`);
    process.stdout.write(
      `   GET /health → { status, components }\n` +
        `   GET /status → { results: AgentResult[] }\n` +
        `   GET /metrics → Prometheus text exposition (Bearer auth)\n`
    );
  });

  return server;
}

// ─── Route handlers ───────────────────────────────────────────────────────────

async function handleHealth(_req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
  try {
    // Probed concurrently: run serially and a slow Horizon delays the Soroban
    // answer, so the endpoint's latency becomes the sum of every dependency.
    const [horizon, soroban, database] = await Promise.all([
      checkHorizon(),
      checkSoroban(),
      checkDatabase(),
    ]);

    const allUp = horizon === 'up' && soroban === 'up' && database === 'up';
    const statusCode = allUp ? 200 : 503;
    const body: HealthResponse = {
      status: allUp ? 'ok' : 'degraded',
      components: { horizon, soroban, database },
    };

    const payload = JSON.stringify(body);
    res.writeHead(statusCode, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
    });
    res.end(payload);
  } catch (err) {
    const errorResponse = handleError(err);
    log.error({
      msg: 'Unhandled request error',
      status: errorResponse.status,
      type: errorResponse.type,
    });
    const body = JSON.stringify(errorResponse);
    res.writeHead(errorResponse.status, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
      ...errorResponse.headers,
    });
    res.end(body);
  }
}

/** GET /metrics — Prometheus text exposition of the nodal_* metrics. */
async function handlePrometheusMetrics(res: http.ServerResponse): Promise<void> {
  try {
    // Gauges derived from live state are refreshed at scrape time rather than
    // on every mutation, so the value is never staler than the scrape itself.
    spendingWindowTotal.set({ asset: config.X402_ASSET_CODE }, spendingTracker.total());

    const body = await renderMetrics();
    res.writeHead(200, {
      'Content-Type': metricsRegistry.contentType,
      'Content-Length': Buffer.byteLength(body),
    });
    res.end(body);
  } catch (err) {
    const errorResponse = handleError(err);
    log.error({
      msg: 'Failed to render Prometheus metrics',
      status: errorResponse.status,
      type: errorResponse.type,
    });
    const body = JSON.stringify(errorResponse);
    res.writeHead(errorResponse.status, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
      ...errorResponse.headers,
    });
    res.end(body);
  }
}

const DEFAULT_RESULTS_LIMIT = 10;
const MAX_RESULTS_LIMIT = 100;

/** Parses a non-negative integer query parameter, falling back when absent or invalid. */
function parseNonNegativeInt(value: string | null, fallback: number): number {
  if (value === null) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

/**
 * GET /status — paginated persisted AgentResult records, newest first.
 * Requires a matching Bearer token when ADMIN_API_TOKEN is set.
 * Query parameters: `limit` (default 10, max 100) and `offset` (default 0).
 */
function handleResults(req: http.IncomingMessage, res: http.ServerResponse, url: URL): void {
  if (!isAuthenticated(req)) {
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Unauthorized' }));
    return;
  }

  try {
    const limit = Math.min(
      parseNonNegativeInt(url.searchParams.get('limit'), DEFAULT_RESULTS_LIMIT),
      MAX_RESULTS_LIMIT
    );
    const offset = parseNonNegativeInt(url.searchParams.get('offset'), 0);
    const body = JSON.stringify({ results: getResults(limit, offset) });

    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    });
    res.end(body);
  } catch (err) {
    // Route through the shared handler so a StructuredError gets the status
    // code that describes it instead of a blanket 500 - and so an internal
    // fault's message is not echoed to the caller.
    const errorResponse = handleError(err);
    log.error({
      msg: 'Failed to read persisted results',
      status: errorResponse.status,
      type: errorResponse.type,
    });
    const body = JSON.stringify({ error: 'Failed to read results', ...errorResponse });
    res.writeHead(errorResponse.status, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
      ...errorResponse.headers,
    });
    res.end(body);
  }
}

/** Reads and JSON-parses a request body; an empty body parses to `{}`. */
function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => (data += chunk));
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

/**
 * POST /approvals/:id — approve (execute once) or deny a pending task (#736).
 * Moves money, so a Bearer token is always required — even on testnet.
 */
async function handleApprovalDecision(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  agent: PayFiAgent | undefined,
  id: string
): Promise<void> {
  const send = (status: number, payload: unknown): void => {
    const body = JSON.stringify(payload);
    res.writeHead(status, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    });
    res.end(body);
  };

  if (!apiToken() || !isAuthenticated(req)) {
    send(401, { error: 'Unauthorized' });
    return;
  }
  if (!agent) {
    send(503, { error: 'Agent not available' });
    return;
  }

  let decision: unknown;
  try {
    decision = ((await readJsonBody(req)) as { decision?: unknown } | null)?.decision;
  } catch {
    send(400, { error: 'Request body must be JSON' });
    return;
  }
  if (decision !== 'approve' && decision !== 'deny') {
    send(400, { error: 'decision must be "approve" or "deny"' });
    return;
  }

  try {
    if (decision === 'approve') {
      const outcome = await agent.approve(id);
      if (!outcome) send(404, { error: 'Approval not found' });
      else send(200, outcome);
    } else {
      const approval = agent.deny(id);
      if (!approval) send(404, { error: 'Approval not found' });
      else send(200, { approval });
    }
  } catch (err) {
    const errorResponse = handleError(err);
    log.error({
      msg: 'Failed to process approval decision',
      status: errorResponse.status,
      type: errorResponse.type,
    });
    send(errorResponse.status, errorResponse);
  }
}

// ─── SSE broadcast hub (#967) ───────────────────────────────────────────────
//
// One shared poll and one agent-event subscription for the whole process,
// fanned out to every connected client — not one `setInterval` (each running
// its own `listApprovals()`, a DB query + an expiry pass) per client. Also:
//  - forwards the agent's own task:complete/task:failed events, which the
//    old polling loop never looked at, so the dashboard's task history never
//    actually streamed live.
//  - diffs approvals by the *set* of pending ids, not the count, so an
//    approve + a new approval arriving in the same tick — same count,
//    different ids — still emits.
const SSE_POLL_INTERVAL_MS = 2_000;
const SSE_RETRY_MS = 3_000;
const MAX_SSE_CLIENTS = 100;

interface SseClient {
  res: http.ServerResponse;
  nextEventId: number;
}

const sseClients = new Set<SseClient>();
let sseHubAgent: PayFiAgent | undefined;
let sseHubTimer: ReturnType<typeof setInterval> | undefined;
let sseTaskListener: ((result: AgentResult) => void) | undefined;
let lastSseSpendingTotal = 0;
let lastSseApprovals: PendingApproval[] = [];
let lastSseApprovalIds = new Set<string>();
let lastSsePaused = false;

function spendingSnapshot(): Record<string, unknown> {
  const status = spendingTracker.getWindowStatus();
  const limit = parseFloat(config.AGENT_SPENDING_LIMIT);
  return {
    ...status,
    limit,
    percentUsed: status.total > 0 ? (status.total / limit) * 100 : 0,
  };
}

function sseSend(client: SseClient, event: string, data: unknown): void {
  const id = client.nextEventId++;
  client.res.write(`id: ${id}\n`);
  client.res.write(`retry: ${SSE_RETRY_MS}\n`);
  client.res.write(`event: ${event}\n`);
  client.res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function sseBroadcast(event: string, data: unknown): void {
  for (const client of sseClients) sseSend(client, event, data);
}

function setsEqual(a: Set<string>, b: Set<string>): boolean {
  if (a.size !== b.size) return false;
  for (const value of a) if (!b.has(value)) return false;
  return true;
}

/** One shared tick: at most one `listApprovals()` / spending read, regardless of client count. */
function pollSseHub(): void {
  try {
    const currentSpendingTotal = spendingTracker.total();
    if (currentSpendingTotal !== lastSseSpendingTotal) {
      lastSseSpendingTotal = currentSpendingTotal;
      sseBroadcast('spending', { ...spendingSnapshot(), timestamp: new Date().toISOString() });
    }

    if (sseHubAgent) {
      const currentApprovals = sseHubAgent.listApprovals();
      const currentIds = new Set(currentApprovals.map((a) => a.id));
      if (!setsEqual(currentIds, lastSseApprovalIds)) {
        lastSseApprovals = currentApprovals;
        lastSseApprovalIds = currentIds;
        sseBroadcast('approvals', {
          approvals: currentApprovals,
          timestamp: new Date().toISOString(),
        });
      }
    }

    const currentPaused = isPaused();
    if (currentPaused !== lastSsePaused) {
      lastSsePaused = currentPaused;
      sseBroadcast('pause', { paused: currentPaused, timestamp: new Date().toISOString() });
    }

    sseBroadcast('heartbeat', { timestamp: new Date().toISOString() });
  } catch (err) {
    log.error({ msg: 'SSE polling error', err: String(err) });
  }
}

/** Starts the shared poll + agent subscription on the first connected client. */
function startSseHubIfNeeded(agent: PayFiAgent | undefined): void {
  if (agent) sseHubAgent = agent;

  if (sseHubAgent && !sseTaskListener) {
    sseTaskListener = (result) => {
      sseBroadcast('task_result', { result, timestamp: new Date().toISOString() });
    };
    sseHubAgent.on('task:complete', sseTaskListener);
    sseHubAgent.on('task:failed', sseTaskListener);
  }

  if (sseHubTimer) return;
  lastSseSpendingTotal = spendingTracker.total();
  lastSseApprovals = sseHubAgent?.listApprovals() ?? [];
  lastSseApprovalIds = new Set(lastSseApprovals.map((a) => a.id));
  lastSsePaused = isPaused();
  sseHubTimer = setInterval(pollSseHub, SSE_POLL_INTERVAL_MS);
}

/** Stops the shared poll + agent subscription once the last client disconnects. */
function stopSseHubIfIdle(): void {
  if (sseClients.size > 0) return;
  if (sseHubTimer) {
    clearInterval(sseHubTimer);
    sseHubTimer = undefined;
  }
  if (sseHubAgent && sseTaskListener) {
    sseHubAgent.off('task:complete', sseTaskListener);
    sseHubAgent.off('task:failed', sseTaskListener);
    sseTaskListener = undefined;
  }
}

/**
 * GET /stream — SSE stream for real-time dashboard updates.
 * Emits: task results, spending changes, approval status, and agent pause/resume,
 * via the single shared hub above rather than a per-connection poll.
 */
function handleStream(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  agent: PayFiAgent | undefined
): void {
  if (sseClients.size >= MAX_SSE_CLIENTS) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Too many stream connections' }));
    return;
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const client: SseClient = { res, nextEventId: 1 };
  sseClients.add(client);
  startSseHubIfNeeded(agent);

  try {
    sseSend(client, 'initial', {
      spending: spendingSnapshot(),
      approvals: lastSseApprovals,
      paused: lastSsePaused,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    log.error({ msg: 'Failed to send initial SSE state', err: String(err) });
  }

  req.on('close', () => {
    sseClients.delete(client);
    stopSseHubIfIdle();
    log.info('SSE client disconnected');
  });
}
