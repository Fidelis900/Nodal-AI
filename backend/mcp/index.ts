#!/usr/bin/env node
/**
 * backend/mcp/server.ts - Add npx nodal-ai mcp command entry
 *
 * Compiled to dist/backend/mcp/index.js and exposed as the `nodal-mcp` bin.
 */

import { PayFiAgent } from '../agent';
import { startMCPServer, startMCPHTTPServer } from './server';

if (require.main === module) {
  const agent = new PayFiAgent();
  const transport = process.env.MCP_TRANSPORT || 'stdio';

  if (transport === 'http') {
    const port = parseInt(process.env.MCP_PORT || '3001', 10);
    startMCPHTTPServer(agent, port);
  } else {
    startMCPServer(agent);
  }
}
