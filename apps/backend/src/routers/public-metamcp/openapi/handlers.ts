import { RequestOptions } from "@modelcontextprotocol/sdk/shared/protocol.js";
import {
  CallToolResult,
  CompatibilityCallToolResultSchema,
  ListToolsResultSchema,
  Tool,
} from "@modelcontextprotocol/sdk/types.js";

import { configService } from "../../../lib/config.service";
import { ConnectedClient } from "../../../lib/metamcp";
import { getMcpServers } from "../../../lib/metamcp/fetch-metamcp";
import { mcpServerPool } from "../../../lib/metamcp/mcp-server-pool";
import {
  createFilterCallToolMiddleware,
  createFilterListToolsMiddleware,
} from "../../../lib/metamcp/metamcp-middleware/filter-tools.functional";
import {
  CallToolHandler,
  compose,
  ListToolsHandler,
  MetaMCPHandlerContext,
} from "../../../lib/metamcp/metamcp-middleware/functional-middleware";
import { sanitizeName } from "../../../lib/metamcp/utils";

/**
 * Enhanced session acquisition with retry logic for OpenAPI handlers
 */
async function getSessionWithRetryOpenAPI(
  sessionId: string,
  serverUuid: string,
  params: ServerParameters,
  maxRetries: number = 3,
  baseDelay: number = 100,
): Promise<ConnectedClient | undefined> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const session = await mcpServerPool.getSession(sessionId, serverUuid, params);
      if (session) {
        return session;
      }
      
      if (attempt === maxRetries) {
        console.error(
          `OpenAPI: Failed to acquire session for server ${serverUuid} after ${maxRetries} attempts`,
        );
        return undefined;
      }
      
      const delay = baseDelay * Math.pow(2, attempt - 1);
      console.log(
        `OpenAPI: Session acquisition failed for server ${serverUuid}, retrying in ${delay}ms (attempt ${attempt}/${maxRetries})`,
      );
      await new Promise(resolve => setTimeout(resolve, delay));
      
    } catch (error) {
      console.error(
        `OpenAPI: Error during session acquisition for server ${serverUuid} (attempt ${attempt}/${maxRetries}):`,
        error,
      );
      
      if (attempt === maxRetries) {
        return undefined;
      }
      
      const delay = baseDelay * Math.pow(2, attempt - 1);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  return undefined;
}

// Original List Tools Handler (adapted from metamcp-proxy.ts)
export const createOriginalListToolsHandler = (
  includeInactiveServers: boolean = false,
): ListToolsHandler => {
  return async (request, context) => {
    const startTime = Date.now();
    const serverParams = await getMcpServers(
      context.namespaceUuid,
      includeInactiveServers,
    );
    const allTools: Tool[] = [];
    const serverMetrics = {
      total: 0,
      successful: 0,
      failed: 0,
      skipped: 0,
      toolsRetrieved: 0,
    };

    // Warmup the namespace to pre-create idle sessions and reduce race conditions
    try {
      await mcpServerPool.warmupNamespace(serverParams);
    } catch (error) {
      console.warn(
        `OpenAPI warmup failed for namespace ${context.namespaceUuid}, continuing without warmup:`,
        error,
      );
    }

    // Log pool metrics for monitoring
    const poolMetrics = mcpServerPool.getPoolMetrics();
    console.log(
      `OpenAPI pool metrics for namespace ${context.namespaceUuid}: idle=${poolMetrics.totalIdle}, active=${poolMetrics.totalActive}, reserved=${poolMetrics.totalReserved}, creating=${poolMetrics.totalCreating}`,
    );

    serverMetrics.total = Object.keys(serverParams).length;

    await Promise.allSettled(
      Object.entries(serverParams).map(async ([mcpServerUuid, params]) => {
        const session = await getSessionWithRetryOpenAPI(
          context.sessionId,
          mcpServerUuid,
          params,
        );
        if (!session) {
          console.warn(
            `OpenAPI: Failed to get session for server ${mcpServerUuid} (${params.name}), skipping tools from this server`,
          );
          serverMetrics.failed++;
          return;
        }

        const capabilities = session.client.getServerCapabilities();
        if (!capabilities?.tools) {
          serverMetrics.skipped++;
          return;
        }

        // Use name assigned by user, fallback to name from server
        const serverName =
          params.name || session.client.getServerVersion()?.name || "";
        try {
          // Get configurable timeout values to bypass MCP SDK default enforcement
          const resetTimeoutOnProgress =
            await configService.getMcpResetTimeoutOnProgress();
          const timeout = await configService.getMcpTimeout();
          const maxTotalTimeout = await configService.getMcpMaxTotalTimeout();

          const mcpRequestOptions: RequestOptions = {
            resetTimeoutOnProgress,
            timeout,
            maxTotalTimeout,
          };

          const result = await session.client.request(
            {
              method: "tools/list",
              params: { _meta: request.params?._meta },
            },
            ListToolsResultSchema,
            mcpRequestOptions,
          );

          const toolsWithSource =
            result.tools?.map((tool) => {
              const toolName = `${sanitizeName(serverName)}__${tool.name}`;
              return {
                ...tool,
                name: toolName,
                description: tool.description,
              };
            }) || [];

          allTools.push(...toolsWithSource);
          serverMetrics.successful++;
          serverMetrics.toolsRetrieved += toolsWithSource.length;
        } catch (error) {
          console.error(`OpenAPI: Error fetching tools from: ${serverName}`, error);
          serverMetrics.failed++;
        }
      }),
    );

    const duration = Date.now() - startTime;
    
    // Log comprehensive metrics for monitoring
    console.log(
      `OpenAPI tools/list completed for namespace ${context.namespaceUuid} in ${duration}ms:`,
      `servers=${serverMetrics.total}, successful=${serverMetrics.successful}, failed=${serverMetrics.failed}, skipped=${serverMetrics.skipped},`,
      `tools=${serverMetrics.toolsRetrieved}, sessionId=${context.sessionId}`,
    );

    // Log warning if we have significant failures
    if (serverMetrics.failed > 0 && serverMetrics.failed / serverMetrics.total > 0.2) {
      console.warn(
        `OpenAPI high failure rate for namespace ${context.namespaceUuid}: ${serverMetrics.failed}/${serverMetrics.total} servers failed (${Math.round((serverMetrics.failed / serverMetrics.total) * 100)}%)`,
      );
    }

    // Graceful degradation: return what tools we could retrieve, even if some servers failed
    if (serverMetrics.failed > 0 && allTools.length === 0) {
      console.error(
        `OpenAPI all servers failed for namespace ${context.namespaceUuid}, returning empty tools list`,
      );
    } else if (serverMetrics.failed > 0) {
      console.warn(
        `OpenAPI partial success for namespace ${context.namespaceUuid}: ${allTools.length} tools retrieved despite ${serverMetrics.failed} server failures`,
      );
    }

    return { tools: allTools };
  };
};

// Original Call Tool Handler (adapted from metamcp-proxy.ts)
export const createOriginalCallToolHandler = (): CallToolHandler => {
  const toolToClient: Record<string, ConnectedClient> = {};
  const toolToServerUuid: Record<string, string> = {};

  return async (request, context) => {
    const { name, arguments: args } = request.params;

    // Extract the original tool name by removing the server prefix
    const firstDoubleUnderscoreIndex = name.indexOf("__");
    if (firstDoubleUnderscoreIndex === -1) {
      throw new Error(`Invalid tool name format: ${name}`);
    }

    const serverPrefix = name.substring(0, firstDoubleUnderscoreIndex);
    const originalToolName = name.substring(firstDoubleUnderscoreIndex + 2);

    // Get server parameters and find the right session for this tool
    const serverParams = await getMcpServers(context.namespaceUuid);
    let targetSession = null;

    for (const [mcpServerUuid, params] of Object.entries(serverParams)) {
      const session = await mcpServerPool.getSession(
        context.sessionId,
        mcpServerUuid,
        params,
      );
      if (!session) continue;

      const capabilities = session.client.getServerCapabilities();
      if (!capabilities?.tools) continue;

      // Use name assigned by user, fallback to name from server
      const serverName =
        params.name || session.client.getServerVersion()?.name || "";

      if (sanitizeName(serverName) === serverPrefix) {
        targetSession = session;
        toolToClient[name] = session;
        toolToServerUuid[name] = mcpServerUuid;
        break;
      }
    }

    if (!targetSession) {
      throw new Error(`Unknown tool: ${name}`);
    }

    try {
      // Get configurable timeout values to bypass MCP SDK default enforcement
      const resetTimeoutOnProgress =
        await configService.getMcpResetTimeoutOnProgress();
      const timeout = await configService.getMcpTimeout();
      const maxTotalTimeout = await configService.getMcpMaxTotalTimeout();

      const mcpRequestOptions: RequestOptions = {
        resetTimeoutOnProgress,
        timeout,
        maxTotalTimeout,
      };

      // Use the correct schema for tool calls with timeout options
      const result = await targetSession.client.request(
        {
          method: "tools/call",
          params: {
            name: originalToolName,
            arguments: args || {},
            _meta: {
              progressToken: request.params._meta?.progressToken,
            },
          },
        },
        CompatibilityCallToolResultSchema,
        mcpRequestOptions,
      );

      // Cast the result to CallToolResult type
      return result as CallToolResult;
    } catch (error) {
      console.error(
        `Error calling tool "${name}" through ${
          targetSession.client.getServerVersion()?.name || "unknown"
        }:`,
        error,
      );
      throw error;
    }
  };
};

// Helper function to create middleware-enabled handlers
export const createMiddlewareEnabledHandlers = (
  sessionId: string,
  namespaceUuid: string,
) => {
  // Create the handler context
  const handlerContext: MetaMCPHandlerContext = {
    namespaceUuid,
    sessionId,
  };

  // Create original handlers
  const originalListToolsHandler = createOriginalListToolsHandler();
  const originalCallToolHandler = createOriginalCallToolHandler();

  // Compose middleware with handlers
  const listToolsWithMiddleware = compose(
    createFilterListToolsMiddleware({ cacheEnabled: true }),
    // Add more middleware here as needed
    // createLoggingMiddleware(),
    // createRateLimitingMiddleware(),
  )(originalListToolsHandler);

  const callToolWithMiddleware = compose(
    createFilterCallToolMiddleware({
      cacheEnabled: true,
      customErrorMessage: (toolName, reason) =>
        `Access denied to tool "${toolName}": ${reason}`,
    }),
    // Add more middleware here as needed
    // createAuditingMiddleware(),
    // createAuthorizationMiddleware(),
  )(originalCallToolHandler);

  return {
    handlerContext,
    listToolsWithMiddleware,
    callToolWithMiddleware,
  };
};
