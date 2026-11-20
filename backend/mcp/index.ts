/**
 * backend/mcp/index.ts - Entry point for `npx nodal-ai mcp`.
 *
 * ORDERING CONTRACT (issue #912)
 * ──────────────────────────────
 * backend/utils/logger.ts builds the pino singleton at module-evaluation time
 * and reads MCP_TRANSPORT then.  That module is imported transitively by
 * ./server before any log line is emitted, so the singleton is already bound
 * to stderr by the time startMCPServer() runs.
 *
 * To preserve this guarantee:
 *   1. Never log to process.stdout before the imports below.
 *   2. Never set process.env.MCP_TRANSPORT after the imports below.
 *   3. Keep this file as the sole entry point for the stdio transport.
 */

// Normalise MCP_TRANSPORT before any module import so logger.ts picks up the
// canonical value.  Treat missing / empty as 'stdio' (the default transport).
if (!process.env.MCP_TRANSPORT) {
  process.env.MCP_TRANSPORT = 'stdio';
}

import { PayFiAgent } from '../agent';
import { startMCPServer, startMCPHTTPServer } from './server';

if (require.main === module) {
  const agent = new PayFiAgent();
  const transport = process.env.MCP_TRANSPORT;

  if (transport === 'http') {
    const port = parseInt(process.env.MCP_PORT || '3001', 10);
    startMCPHTTPServer(agent, port);
  } else {
    startMCPServer(agent);
  }
}
