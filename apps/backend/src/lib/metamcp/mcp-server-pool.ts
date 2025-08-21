import { ServerParameters } from "@repo/zod-types";

import { ConnectedClient, connectMetaMcpClient } from "./client";

export interface McpServerPoolStatus {
  idle: number;
  active: number;
  activeSessionIds: string[];
  idleServerUuids: string[];
}

export interface SessionReservation {
  reservedAt: number;
  sessionId: string;
  client: ConnectedClient;
}

export interface SessionPoolMetrics {
  totalIdle: number;
  totalActive: number;
  totalReserved: number;
  totalCreating: number;
  servers: Record<string, {
    idle: number;
    active: number;
    reserved: number;
    creating: boolean;
  }>;
}

export class McpServerPool {
  // Singleton instance
  private static instance: McpServerPool | null = null;

  // Idle sessions: serverUuid -> ConnectedClient[] (pool of available sessions)
  private idleSessions: Record<string, ConnectedClient[]> = {};

  // Reserved sessions: serverUuid -> SessionReservation[] (temporarily held sessions)
  private reservedSessions: Record<string, SessionReservation[]> = {};

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

  // Waiting queues for sessions: serverUuid -> Array of resolve functions
  private waitingQueues: Map<string, Array<(client: ConnectedClient | null) => void>> = new Map();

  // Default number of idle sessions per server UUID
  private readonly defaultIdleCount: number;

  // Minimum pool size per server (always maintain this many idle sessions)
  private readonly minPoolSize: number = 2;

  // Default timeout for waiting for session creation (30 seconds)
  private readonly SESSION_CREATION_TIMEOUT = 30000;

  // Reservation timeout (5 seconds)
  private readonly RESERVATION_TIMEOUT = 5000;

  // Session waiting poll interval (100ms)
  private readonly WAIT_POLL_INTERVAL = 100;

  private constructor(defaultIdleCount: number = 1) {
    this.defaultIdleCount = defaultIdleCount;
    
    // Start cleanup timer for expired reservations
    this.startReservationCleanupTimer();
  }

  /**
   * Start periodic cleanup of expired reservations
   */
  private startReservationCleanupTimer(): void {
    setInterval(() => {
      this.cleanupExpiredReservations();
    }, this.RESERVATION_TIMEOUT);
  }

  /**
   * Clean up expired session reservations
   */
  private cleanupExpiredReservations(): void {
    const now = Date.now();
    
    Object.entries(this.reservedSessions).forEach(([serverUuid, reservations]) => {
      const validReservations = reservations.filter(reservation => {
        const isExpired = now - reservation.reservedAt > this.RESERVATION_TIMEOUT;
        if (isExpired) {
          // Return expired reservation back to idle pool
          if (!this.idleSessions[serverUuid]) {
            this.idleSessions[serverUuid] = [];
          }
          this.idleSessions[serverUuid].push(reservation.client);
          console.log(
            `Released expired reservation for server ${serverUuid}, session ${reservation.sessionId}`,
          );
        }
        return !isExpired;
      });
      
      this.reservedSessions[serverUuid] = validReservations;
    });
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
   * Get or create a session for a specific MCP server with enhanced race condition protection
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

    // Initialize session tracking if it doesn't exist
    if (!this.activeSessions[sessionId]) {
      this.activeSessions[sessionId] = {};
      this.sessionToServers[sessionId] = new Set();
    }

    // Try to get a session with reservation system (prevents race conditions)
    const client = await this.getOrWaitForSession(sessionId, serverUuid, params);
    
    if (client) {
      // Successfully got a session, convert to active
      this.activeSessions[sessionId][serverUuid] = client;
      this.sessionToServers[sessionId].add(serverUuid);
      
      console.log(
        `Acquired session for server ${serverUuid}, session ${sessionId}`,
      );
      
      // Ensure minimum pool size is maintained
      this.ensureMinimumPoolSize(serverUuid, params);
      
      return client;
    }

    console.error(
      `Failed to acquire session for server ${serverUuid}, session ${sessionId}`,
    );
    return undefined;
  }

  /**
   * Get or wait for a session with comprehensive race condition protection
   */
  private async getOrWaitForSession(
    sessionId: string,
    serverUuid: string,
    params: ServerParameters,
  ): Promise<ConnectedClient | undefined> {
    // Try to reserve an idle session immediately
    const reservedClient = this.tryReserveIdleSession(sessionId, serverUuid);
    if (reservedClient) {
      return reservedClient;
    }

    // No idle session available, check if we're creating one
    if (this.creatingIdleSessions.has(serverUuid)) {
      console.log(
        `Waiting for session creation for server ${serverUuid}, session ${sessionId}`,
      );
      
      // Wait for the session to be created
      const client = await this.waitForSessionCreation(serverUuid);
      if (client) {
        return client;
      }
    }

    // No session available and none being created, create one synchronously
    console.log(
      `Creating new session for server ${serverUuid}, session ${sessionId}`,
    );
    
    const newClient = await this.createNewConnection(params);
    if (newClient) {
      console.log(
        `Created new session for server ${serverUuid}, session ${sessionId}`,
      );
    }
    
    return newClient;
  }

  /**
   * Try to reserve an idle session atomically
   */
  private tryReserveIdleSession(
    sessionId: string,
    serverUuid: string,
  ): ConnectedClient | undefined {
    const idlePool = this.idleSessions[serverUuid];
    if (!idlePool || idlePool.length === 0) {
      return undefined;
    }

    // Atomically remove from idle pool and add to reservations
    const client = idlePool.pop()!;
    
    if (!this.reservedSessions[serverUuid]) {
      this.reservedSessions[serverUuid] = [];
    }
    
    this.reservedSessions[serverUuid].push({
      reservedAt: Date.now(),
      sessionId,
      client,
    });

    console.log(
      `Reserved idle session for server ${serverUuid}, session ${sessionId}`,
    );

    return client;
  }

  /**
   * Wait for session creation with exponential backoff and timeout
   */
  private async waitForSessionCreation(
    serverUuid: string,
  ): Promise<ConnectedClient | undefined> {
    return new Promise((resolve) => {
      const startTime = Date.now();
      let attempts = 0;
      const maxAttempts = Math.ceil(this.SESSION_CREATION_TIMEOUT / this.WAIT_POLL_INTERVAL);

      const checkForSession = () => {
        attempts++;
        const elapsed = Date.now() - startTime;

        // Check if timeout exceeded
        if (elapsed > this.SESSION_CREATION_TIMEOUT || attempts > maxAttempts) {
          console.warn(
            `Session wait timeout for server ${serverUuid} after ${elapsed}ms`,
          );
          resolve(undefined);
          return;
        }

        // Check if session is no longer being created (creation completed or failed)
        if (!this.creatingIdleSessions.has(serverUuid)) {
          // Try to reserve a newly created session
          const idlePool = this.idleSessions[serverUuid];
          if (idlePool && idlePool.length > 0) {
            const client = idlePool.pop()!;
            console.log(
              `Got newly created session for server ${serverUuid} after ${elapsed}ms`,
            );
            resolve(client);
            return;
          }
        }

        // Calculate next wait interval with exponential backoff (cap at 1000ms)
        const backoffInterval = Math.min(
          this.WAIT_POLL_INTERVAL * Math.pow(1.2, Math.floor(attempts / 5)),
          1000,
        );

        setTimeout(checkForSession, backoffInterval);
      };

      // Start checking
      checkForSession();
    });
  }

  /**
   * Ensure minimum pool size is maintained for a server
   */
  private ensureMinimumPoolSize(
    serverUuid: string,
    params: ServerParameters,
  ): void {
    const currentPoolSize = (this.idleSessions[serverUuid]?.length || 0);
    const neededSessions = this.minPoolSize - currentPoolSize;
    
    if (neededSessions > 0 && !this.creatingIdleSessions.has(serverUuid)) {
      console.log(
        `Creating ${neededSessions} sessions to maintain minimum pool size for server ${serverUuid}`,
      );
      
      for (let i = 0; i < neededSessions; i++) {
        this.createIdleSessionAsync(serverUuid, params);
      }
    }
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
    const newClient = await this.createNewConnection(params);
    if (newClient) {
      if (!this.idleSessions[serverUuid]) {
        this.idleSessions[serverUuid] = [];
      }
      this.idleSessions[serverUuid].push(newClient);
      console.log(`Created idle session for server ${serverUuid} (pool size: ${this.idleSessions[serverUuid].length})`);
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
    // Don't create if we're already creating one (allow multiple idle sessions)
    if (this.creatingIdleSessions.has(serverUuid)) {
      return;
    }

    // Mark that we're creating an idle session for this server
    this.creatingIdleSessions.add(serverUuid);

    // Create the session promise and store it
    const creationPromise = this.createNewConnection(params)
      .then((newClient) => {
        if (newClient) {
          if (!this.idleSessions[serverUuid]) {
            this.idleSessions[serverUuid] = [];
          }
          this.idleSessions[serverUuid].push(newClient);
          console.log(
            `Created background idle session for server [${params.name}] ${serverUuid} (pool size: ${this.idleSessions[serverUuid].length})`,
          );
          
          // Notify any waiting sessions
          this.notifyWaitingQueue(serverUuid, newClient);
          
          return newClient;
        }
        return undefined;
      })
      .catch((error) => {
        console.error(
          `Error creating background idle session for ${serverUuid}:`,
          error,
        );
        
        // Notify waiting queue about the failure
        this.notifyWaitingQueue(serverUuid, null);
        
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
   * Notify waiting queue when a session is available
   */
  private notifyWaitingQueue(
    serverUuid: string,
    client: ConnectedClient | null,
  ): void {
    const queue = this.waitingQueues.get(serverUuid);
    if (queue && queue.length > 0) {
      const waiter = queue.shift();
      if (waiter) {
        waiter(client);
        console.log(
          `Notified waiting queue for server ${serverUuid} (${queue.length} remaining)`,
        );
      }
    }
  }

  /**
   * Ensure idle sessions exist for all servers (maintains minimum pool size)
   */
  async ensureIdleSessions(
    serverParams: Record<string, ServerParameters>,
  ): Promise<void> {
    const promises = Object.entries(serverParams).map(
      async ([uuid, params]) => {
        const currentPoolSize = this.idleSessions[uuid]?.length || 0;
        const neededSessions = this.minPoolSize - currentPoolSize;
        
        for (let i = 0; i < neededSessions; i++) {
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
        const currentPoolSize = this.idleSessions[uuid]?.length || 0;
        const neededSessions = this.minPoolSize - currentPoolSize;
        
        // Only create if we need more sessions
        if (neededSessions <= 0) {
          return;
        }

        try {
          // Create minimum pool size for warmup
          for (let i = 0; i < neededSessions; i++) {
            await this.createIdleSession(uuid, params);
          }
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
   * Get current pool metrics for monitoring
   */
  getPoolMetrics(): SessionPoolMetrics {
    const servers: Record<string, { idle: number; active: number; reserved: number; creating: boolean }> = {};
    let totalIdle = 0;
    let totalActive = 0;
    let totalReserved = 0;
    let totalCreating = 0;

    // Calculate idle sessions
    Object.entries(this.idleSessions).forEach(([serverUuid, sessions]) => {
      const idleCount = sessions.length;
      totalIdle += idleCount;
      servers[serverUuid] = { idle: idleCount, active: 0, reserved: 0, creating: false };
    });

    // Calculate active sessions
    Object.values(this.activeSessions).forEach(sessionServers => {
      Object.keys(sessionServers).forEach(serverUuid => {
        totalActive++;
        if (!servers[serverUuid]) {
          servers[serverUuid] = { idle: 0, active: 0, reserved: 0, creating: false };
        }
        servers[serverUuid].active++;
      });
    });

    // Calculate reserved sessions
    Object.entries(this.reservedSessions).forEach(([serverUuid, reservations]) => {
      const reservedCount = reservations.length;
      totalReserved += reservedCount;
      if (!servers[serverUuid]) {
        servers[serverUuid] = { idle: 0, active: 0, reserved: 0, creating: false };
      }
      servers[serverUuid].reserved = reservedCount;
    });

    // Mark creating sessions
    this.creatingIdleSessions.forEach(serverUuid => {
      totalCreating++;
      if (!servers[serverUuid]) {
        servers[serverUuid] = { idle: 0, active: 0, reserved: 0, creating: false };
      }
      servers[serverUuid].creating = true;
    });

    return {
      totalIdle,
      totalActive,
      totalReserved,
      totalCreating,
      servers,
    };
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
      Object.entries(this.idleSessions).flatMap(([_uuid, clients]) =>
        clients.map(client => client.cleanup())
      ),
    );

    // Cleanup all reserved sessions
    await Promise.allSettled(
      Object.entries(this.reservedSessions).flatMap(([_uuid, reservations]) =>
        reservations.map(reservation => reservation.client.cleanup())
      ),
    );

    // Clear all state
    this.idleSessions = {};
    this.reservedSessions = {};
    this.activeSessions = {};
    this.sessionToServers = {};
    this.serverParamsCache = {};
    this.creatingIdleSessions.clear();
    this.creatingSessionPromises.clear();
    this.waitingQueues.clear();

    console.log("Cleaned up all MCP server pool sessions");
  }

  /**
   * Get pool status for monitoring (legacy method - use getPoolMetrics for detailed info)
   */
  getPoolStatus(): McpServerPoolStatus {
    const idle = Object.values(this.idleSessions).reduce((total, clients) => total + clients.length, 0);
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
   * Stress test the session pool to verify race condition fixes
   */
  async stressTest(
    serverParams: Record<string, ServerParameters>,
    concurrentRequests: number = 10,
    iterations: number = 5,
  ): Promise<{
    totalRequests: number;
    successfulRequests: number;
    failedRequests: number;
    averageResponseTime: number;
    raceConditionsDetected: number;
  }> {
    console.log(
      `Starting stress test: ${concurrentRequests} concurrent requests, ${iterations} iterations`,
    );

    let totalRequests = 0;
    let successfulRequests = 0;
    let failedRequests = 0;
    let totalResponseTime = 0;
    let raceConditionsDetected = 0;

    for (let iteration = 0; iteration < iterations; iteration++) {
      console.log(`Stress test iteration ${iteration + 1}/${iterations}`);

      const promises = Array.from({ length: concurrentRequests }, async (_, requestIndex) => {
        const startTime = Date.now();
        const sessionId = `stress-test-session-${iteration}-${requestIndex}`;
        const serverEntries = Object.entries(serverParams);
        
        let requestSuccessful = true;
        let requestSessions = 0;

        for (const [serverUuid, params] of serverEntries) {
          try {
            const session = await this.getSession(sessionId, serverUuid, params);
            if (session) {
              requestSessions++;
            } else {
              console.warn(`Stress test: Failed to get session for ${serverUuid}`);
              requestSuccessful = false;
            }
          } catch (error) {
            console.error(`Stress test: Error getting session for ${serverUuid}:`, error);
            requestSuccessful = false;
          }
        }

        const responseTime = Date.now() - startTime;
        totalResponseTime += responseTime;

        // Check for potential race conditions (empty sessions when we expect them)
        if (requestSessions === 0 && serverEntries.length > 0) {
          raceConditionsDetected++;
          console.warn(
            `Potential race condition detected: No sessions acquired for ${sessionId}`,
          );
        }

        return {
          successful: requestSuccessful,
          responseTime,
          sessionId,
          sessionsAcquired: requestSessions,
        };
      });

      const results = await Promise.allSettled(promises);
      
      results.forEach((result) => {
        totalRequests++;
        if (result.status === 'fulfilled') {
          if (result.value.successful) {
            successfulRequests++;
          } else {
            failedRequests++;
          }
        } else {
          failedRequests++;
          console.error('Stress test promise failed:', result.reason);
        }
      });

      // Cleanup sessions from this iteration
      for (let requestIndex = 0; requestIndex < concurrentRequests; requestIndex++) {
        const sessionId = `stress-test-session-${iteration}-${requestIndex}`;
        try {
          await this.cleanupSession(sessionId);
        } catch (error) {
          console.error(`Failed to cleanup session ${sessionId}:`, error);
        }
      }

      // Small delay between iterations
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const averageResponseTime = totalResponseTime / totalRequests;

    const results = {
      totalRequests,
      successfulRequests,
      failedRequests,
      averageResponseTime,
      raceConditionsDetected,
    };

    console.log('Stress test completed:', results);
    return results;
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
