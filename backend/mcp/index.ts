#!/usr/bin/env node
/**
 * backend/mcp/server.ts - Add npx nodal-ai mcp command entry
 *
 * Compiled to dist/backend/mcp/index.js and exposed as the `nodal-mcp` bin.
 */

import { PayFiAgent } from '../agent';
import { config } from '../config';
import { startMCPServer, startMCPHTTPServer } from './server';

if (require.main === module) {
  const agent = new PayFiAgent();
  if (config.MCP_TRANSPORT === 'http') {
    startMCPHTTPServer(agent, config.MCP_PORT);
  } else {
    startMCPServer(agent);
  }
}
