import { ServerParameters } from "@repo/zod-types";

import { ConnectedClient, connectMetaMcpClient } from "./client";

export interface McpServerPoolStatus {
  idle: number;
  active: number;
  activeSessionIds: string[];
  idleServerUuids: string[];
}

export class McpServerPool {
  // Singleton instance
  private static instance: McpServerPool | null = null;

  // Idle sessions: serverUuid -> ConnectedClient (no sessionId assigned yet)
  private idleSessions: Record<string, ConnectedClient> = {};

  // Active sessions: sessionId -> Record<serverUuid, ConnectedClient>
  private activeSessions: Record<string, Record<string, ConnectedClient>> = {};

  // Mapping: sessionId -> Set<serverUuid> for cleanup tracking
  private sessionToServers: Record<string, Set<string>> = {};

  // Server parameters cache: serverUuid -> ServerParameters
  private serverParamsCache: Record<string, ServerParameters> = {};

  // Track ongoing idle session creation to prevent duplicates
  private creatingIdleSessions: Set<string> = new Set();

  // Track promises for ongoing session creation: serverUuid -> Promise<ConnectedClient | undefined>
  private creatingSessionPromises: Map<string, Promise<ConnectedClient | undefined>> = new Map();

  // Default number of idle sessions per server UUID
  private readonly defaultIdleCount: number;

  // Default timeout for waiting for session creation (30 seconds)
  private readonly SESSION_CREATION_TIMEOUT = 30000;

  private constructor(defaultIdleCount: number = 1) {
    this.defaultIdleCount = defaultIdleCount;
  }

  /**
   * Get the singleton instance
   */
  static getInstance(defaultIdleCount: number = 1): McpServerPool {
    if (!McpServerPool.instance) {
      McpServerPool.instance = new McpServerPool(defaultIdleCount);
    }
    return McpServerPool.instance;
  }

  /**
   * Get or create a session for a specific MCP server
   */
  async getSession(
    sessionId: string,
    serverUuid: string,
    params: ServerParameters,
  ): Promise<ConnectedClient | undefined> {
    // Update server params cache
    this.serverParamsCache[serverUuid] = params;

    // Check if we already have an active session for this sessionId and server
    if (this.activeSessions[sessionId]?.[serverUuid]) {
      return this.activeSessions[sessionId][serverUuid];
    }

    // Initialize session if it doesn't exist
    if (!this.activeSessions[sessionId]) {
      this.activeSessions[sessionId] = {};
      this.sessionToServers[sessionId] = new Set();
    }

    // Check if we have an idle session for this server that we can convert
    const idleClient = this.idleSessions[serverUuid];
    if (idleClient) {
      // Convert idle session to active session
      delete this.idleSessions[serverUuid];
      this.activeSessions[sessionId][serverUuid] = idleClient;
      this.sessionToServers[sessionId].add(serverUuid);

      console.log(
        `Converted idle session to active for server ${serverUuid}, session ${sessionId}`,
      );

      // Create a new idle session to replace the one we just used (ASYNC - NON-BLOCKING)
      this.createIdleSessionAsync(serverUuid, params);

      return idleClient;
    }

    // Check if a session is currently being created for this server
    if (this.creatingIdleSessions.has(serverUuid)) {
      console.log(
        `Waiting for idle session creation for server ${serverUuid}, session ${sessionId}`,
      );
      
      // Wait for the existing creation promise with timeout
      const existingPromise = this.creatingSessionPromises.get(serverUuid);
      if (existingPromise) {
        try {
          const client = await this.waitWithTimeout(
            existingPromise,
            this.SESSION_CREATION_TIMEOUT,
            `Session creation timeout for server ${serverUuid}`,
          );
          
          if (client) {
            // Check again if we now have an idle session
            const idleClientAfterWait = this.idleSessions[serverUuid];
            if (idleClientAfterWait) {
              // Convert idle session to active session
              delete this.idleSessions[serverUuid];
              this.activeSessions[sessionId][serverUuid] = idleClientAfterWait;
              this.sessionToServers[sessionId].add(serverUuid);

              console.log(
                `Converted newly created idle session to active for server ${serverUuid}, session ${sessionId}`,
              );

              // Create a new idle session to replace the one we just used
              this.createIdleSessionAsync(serverUuid, params);

              return idleClientAfterWait;
            }
          }
        } catch (error) {
          console.error(
            `Error waiting for idle session creation for server ${serverUuid}:`,
            error,
          );
        }
      }
    }

    // No idle session available and none being created, create a new connection
    const newClient = await this.createNewConnection(params);
    if (!newClient) {
      return undefined;
    }

    this.activeSessions[sessionId][serverUuid] = newClient;
    this.sessionToServers[sessionId].add(serverUuid);

    console.log(
      `Created new active session for server ${serverUuid}, session ${sessionId}`,
    );

    // Also create an idle session for future use (ASYNC - NON-BLOCKING)
    this.createIdleSessionAsync(serverUuid, params);

    return newClient;
  }

  /**
   * Create a new connection for a server
   */
  private async createNewConnection(
    params: ServerParameters,
  ): Promise<ConnectedClient | undefined> {
    const connectedClient = await connectMetaMcpClient(params);
    if (!connectedClient) {
      return undefined;
    }

    return connectedClient;
  }

  /**
   * Create an idle session for a server (blocking version for initial setup)
   */
  private async createIdleSession(
    serverUuid: string,
    params: ServerParameters,
  ): Promise<void> {
    // Don't create if we already have an idle session for this server
    if (this.idleSessions[serverUuid]) {
      return;
    }

    const newClient = await this.createNewConnection(params);
    if (newClient) {
      this.idleSessions[serverUuid] = newClient;
      console.log(`Created idle session for server ${serverUuid}`);
    }
  }

  /**
   * Wait for a promise with a timeout
   */
  private async waitWithTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    timeoutMessage: string,
  ): Promise<T> {
    const timeoutPromise = new Promise<never>((_, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(timeoutMessage));
      }, timeoutMs);
      
      // Cleanup the timer if the promise resolves first
      promise.then(() => clearTimeout(timer)).catch(() => clearTimeout(timer));
    });

    return Promise.race([promise, timeoutPromise]);
  }

  /**
   * Create an idle session for a server asynchronously (non-blocking)
   */
  private createIdleSessionAsync(
    serverUuid: string,
    params: ServerParameters,
  ): void {
    // Don't create if we already have an idle session or are already creating one
    if (
      this.idleSessions[serverUuid] ||
      this.creatingIdleSessions.has(serverUuid)
    ) {
      return;
    }

    // Mark that we're creating an idle session for this server
    this.creatingIdleSessions.add(serverUuid);

    // Create the session promise and store it
    const creationPromise = this.createNewConnection(params)
      .then((newClient) => {
        if (newClient && !this.idleSessions[serverUuid]) {
          this.idleSessions[serverUuid] = newClient;
          console.log(
            `Created background idle session for server [${params.name}] ${serverUuid}`,
          );
          return newClient;
        } else if (newClient) {
          // We already have an idle session, cleanup the extra one
          newClient.cleanup().catch((error) => {
            console.error(
              `Error cleaning up extra idle session for ${serverUuid}:`,
              error,
            );
          });
        }
        return undefined;
      })
      .catch((error) => {
        console.error(
          `Error creating background idle session for ${serverUuid}:`,
          error,
        );
        return undefined;
      })
      .finally(() => {
        // Remove from creating set and promise map
        this.creatingIdleSessions.delete(serverUuid);
        this.creatingSessionPromises.delete(serverUuid);
      });

    // Store the promise so other requests can wait for it
    this.creatingSessionPromises.set(serverUuid, creationPromise);
  }

  /**
   * Ensure idle sessions exist for all servers
   */
  async ensureIdleSessions(
    serverParams: Record<string, ServerParameters>,
  ): Promise<void> {
    const promises = Object.entries(serverParams).map(
      async ([uuid, params]) => {
        if (!this.idleSessions[uuid]) {
          await this.createIdleSession(uuid, params);
        }
      },
    );

    await Promise.allSettled(promises);
  }

  /**
   * Warmup the pool by pre-creating idle sessions for all servers in a namespace
   * This helps avoid the race condition where initial requests get empty tool lists
   */
  async warmupNamespace(
    serverParams: Record<string, ServerParameters>,
  ): Promise<void> {
    console.log(
      `Starting warmup for namespace with ${Object.keys(serverParams).length} servers`,
    );
    
    const startTime = Date.now();
    const promises = Object.entries(serverParams).map(
      async ([uuid, params]) => {
        // Check if we already have an idle session or are creating one
        if (this.idleSessions[uuid] || this.creatingIdleSessions.has(uuid)) {
          return;
        }

        try {
          // Create idle session synchronously for warmup
          await this.createIdleSession(uuid, params);
        } catch (error) {
          console.error(
            `Failed to warmup server ${uuid} (${params.name}):`,
            error,
          );
        }
      },
    );

    const results = await Promise.allSettled(promises);
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    const failCount = results.filter(r => r.status === 'rejected').length;
    const duration = Date.now() - startTime;
    
    console.log(
      `Namespace warmup completed in ${duration}ms: ${successCount} successful, ${failCount} failed`,
    );
  }

  /**
   * Cleanup a session by sessionId
   */
  async cleanupSession(sessionId: string): Promise<void> {
    const activeSession = this.activeSessions[sessionId];
    if (!activeSession) {
      return;
    }

    // Cleanup all connections for this session
    await Promise.allSettled(
      Object.entries(activeSession).map(async ([_serverUuid, client]) => {
        await client.cleanup();
      }),
    );

    // Remove from active sessions
    delete this.activeSessions[sessionId];

    // Clean up session to servers mapping
    const serverUuids = this.sessionToServers[sessionId];
    if (serverUuids) {
      // For each server this session was using, create new idle sessions if needed (ASYNC - NON-BLOCKING)
      Array.from(serverUuids).forEach((serverUuid) => {
        const params = this.serverParamsCache[serverUuid];
        if (params) {
          this.createIdleSessionAsync(serverUuid, params);
        }
      });

      delete this.sessionToServers[sessionId];
    }

    console.log(`Cleaned up MCP server pool session ${sessionId}`);
  }

  /**
   * Cleanup all sessions
   */
  async cleanupAll(): Promise<void> {
    // Cleanup all active sessions
    const activeSessionIds = Object.keys(this.activeSessions);
    await Promise.allSettled(
      activeSessionIds.map((sessionId) => this.cleanupSession(sessionId)),
    );

    // Cleanup all idle sessions
    await Promise.allSettled(
      Object.entries(this.idleSessions).map(async ([_uuid, client]) => {
        await client.cleanup();
      }),
    );

    // Clear all state
    this.idleSessions = {};
    this.activeSessions = {};
    this.sessionToServers = {};
    this.serverParamsCache = {};
    this.creatingIdleSessions.clear();

    console.log("Cleaned up all MCP server pool sessions");
  }

  /**
   * Get pool status for monitoring
   */
  getPoolStatus(): McpServerPoolStatus {
    const idle = Object.keys(this.idleSessions).length;
    const active = Object.keys(this.activeSessions).reduce(
      (total, sessionId) =>
        total + Object.keys(this.activeSessions[sessionId]).length,
      0,
    );

    return {
      idle,
      active,
      activeSessionIds: Object.keys(this.activeSessions),
      idleServerUuids: Object.keys(this.idleSessions),
    };
  }

  /**
   * Get active session connections for a specific session (for debugging/monitoring)
   */
  getSessionConnections(
    sessionId: string,
  ): Record<string, ConnectedClient> | undefined {
    return this.activeSessions[sessionId];
  }

  /**
   * Get all active session IDs (for debugging/monitoring)
   */
  getActiveSessionIds(): string[] {
    return Object.keys(this.activeSessions);
  }

  /**
   * Invalidate and refresh idle session for a specific server
   * This should be called when a server's parameters (command, args, etc.) change
   */
  async invalidateIdleSession(
    serverUuid: string,
    params: ServerParameters,
  ): Promise<void> {
    console.log(`Invalidating idle session for server ${serverUuid}`);

    // Update server params cache
    this.serverParamsCache[serverUuid] = params;

    // Cleanup existing idle session if it exists
    const existingIdleSession = this.idleSessions[serverUuid];
    if (existingIdleSession) {
      try {
        await existingIdleSession.cleanup();
        console.log(
          `Cleaned up existing idle session for server ${serverUuid}`,
        );
      } catch (error) {
        console.error(
          `Error cleaning up existing idle session for server ${serverUuid}:`,
          error,
        );
      }
      delete this.idleSessions[serverUuid];
    }

    // Remove from creating set if it's in progress
    this.creatingIdleSessions.delete(serverUuid);

    // Create a new idle session with updated parameters
    await this.createIdleSession(serverUuid, params);
  }

  /**
   * Invalidate and refresh idle sessions for multiple servers
   */
  async invalidateIdleSessions(
    serverParams: Record<string, ServerParameters>,
  ): Promise<void> {
    const promises = Object.entries(serverParams).map(([serverUuid, params]) =>
      this.invalidateIdleSession(serverUuid, params),
    );

    await Promise.allSettled(promises);
  }

  /**
   * Clean up idle session for a specific server without creating a new one
   * This should be called when a server is being deleted
   */
  async cleanupIdleSession(serverUuid: string): Promise<void> {
    console.log(`Cleaning up idle session for server ${serverUuid}`);

    // Cleanup existing idle session if it exists
    const existingIdleSession = this.idleSessions[serverUuid];
    if (existingIdleSession) {
      try {
        await existingIdleSession.cleanup();
        console.log(`Cleaned up idle session for server ${serverUuid}`);
      } catch (error) {
        console.error(
          `Error cleaning up idle session for server ${serverUuid}:`,
          error,
        );
      }
      delete this.idleSessions[serverUuid];
    }

    // Remove from creating set if it's in progress
    this.creatingIdleSessions.delete(serverUuid);

    // Remove from server params cache
    delete this.serverParamsCache[serverUuid];
  }

  /**
   * Ensure idle session exists for a newly created server
   * This should be called when a new server is created
   */
  async ensureIdleSessionForNewServer(
    serverUuid: string,
    params: ServerParameters,
  ): Promise<void> {
    console.log(`Ensuring idle session exists for new server ${serverUuid}`);

    // Update server params cache
    this.serverParamsCache[serverUuid] = params;

    // Only create if we don't already have one
    if (
      !this.idleSessions[serverUuid] &&
      !this.creatingIdleSessions.has(serverUuid)
    ) {
      await this.createIdleSession(serverUuid, params);
    }
  }
}

// Create a singleton instance
export const mcpServerPool = McpServerPool.getInstance();
