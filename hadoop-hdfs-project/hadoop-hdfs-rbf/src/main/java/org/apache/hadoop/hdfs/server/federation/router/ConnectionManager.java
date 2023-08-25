/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a pool of connections for the {@link Router} to be able to open
 * many connections to many Namenodes.
 */
public class ConnectionManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConnectionManager.class);

  /** Configuration for the connection manager, pool and sockets. */
  private final Configuration conf;

  /** Min number of connections per user + nn. */
  private final int minSize = 1;
  /** Max number of connections per user + nn. */
  private final int maxSize;
  /** Min ratio of active connections per user + nn. */
  private final float minActiveRatio;

  /** How often we close a pool for a particular user + nn. */
  private final long poolCleanupPeriodMs;
  /** How often we close a connection in a pool. */
  private final long connectionCleanupPeriodMs;

  /** Map of connection pools, one pool per user + NN. */
  private final Map<ConnectionPoolId, ConnectionPool> pools;
  /** Lock for accessing pools. */
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  /** Queue for creating new connections. */
  private final BlockingQueue<ConnectionPool> creatorQueue;
  /**
   * Global federated namespace context for router.
   */
  private final RouterStateIdContext routerStateIdContext;
  /** Max size of queue for creating new connections. */
  private final int creatorQueueMaxSize;

  /** Create new connections asynchronously. */
  private final ConnectionCreator creator;
  /** Periodic executor to remove stale connection pools. */
  private final ScheduledThreadPoolExecutor cleaner =
      new ScheduledThreadPoolExecutor(1);

  /** If the connection manager is running. */
  private boolean running = false;

  public ConnectionManager(Configuration config) {
    this(config, new RouterStateIdContext(config));
  }

  /**
   * Creates a proxy client connection pool manager.
   *
   * @param config Configuration for the connections.
   * @param routerStateIdContext Federated namespace context for router.
   */
  public ConnectionManager(Configuration config, RouterStateIdContext routerStateIdContext) {
    this.conf = config;
    this.routerStateIdContext = routerStateIdContext;
    // Configure minimum, maximum and active connection pools
    this.maxSize = this.conf.getInt(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_POOL_SIZE,
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_POOL_SIZE_DEFAULT);
    this.minActiveRatio = this.conf.getFloat(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_MIN_ACTIVE_RATIO,
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_MIN_ACTIVE_RATIO_DEFAULT);

    // Map with the connections indexed by UGI and Namenode
    this.pools = new HashMap<>();

    // Create connections in a thread asynchronously
    this.creatorQueueMaxSize = this.conf.getInt(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_CREATOR_QUEUE_SIZE,
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_CREATOR_QUEUE_SIZE_DEFAULT
        );
    this.creatorQueue = new ArrayBlockingQueue<>(this.creatorQueueMaxSize);
    this.creator = new ConnectionCreator(this.creatorQueue);
    this.creator.setDaemon(true);

    // Cleanup periods
    this.poolCleanupPeriodMs = this.conf.getLong(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_POOL_CLEAN,
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_POOL_CLEAN_DEFAULT);
    LOG.info("Cleaning connection pools every {} seconds",
        TimeUnit.MILLISECONDS.toSeconds(this.poolCleanupPeriodMs));
    this.connectionCleanupPeriodMs = this.conf.getLong(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_CLEAN_MS,
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_CLEAN_MS_DEFAULT);
    LOG.info("Cleaning connections every {} seconds",
        TimeUnit.MILLISECONDS.toSeconds(this.connectionCleanupPeriodMs));
  }

  /**
   * Start the connection manager.
   */
  public void start() {
    // Start the thread that creates connections asynchronously
    this.creator.start();

    // Schedule a task to remove stale connection pools and sockets
    long recycleTimeMs = Math.min(
        poolCleanupPeriodMs, connectionCleanupPeriodMs);
    LOG.info("Cleaning every {} seconds",
        TimeUnit.MILLISECONDS.toSeconds(recycleTimeMs));
    this.cleaner.scheduleAtFixedRate(
        new CleanupTask(), 0, recycleTimeMs, TimeUnit.MILLISECONDS);

    // Mark the manager as running
    this.running = true;
  }

  /**
   * Stop the connection manager by closing all the pools.
   */
  public void close() {
    this.creator.shutdown();
    this.cleaner.shutdown();
    this.running = false;

    writeLock.lock();
    try {
      for (ConnectionPool pool : this.pools.values()) {
        pool.close();
      }
      this.pools.clear();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Fetches the next available proxy client in the pool. Each client connection
   * is reserved for a single user and cannot be reused until free.
   *
   * @param ugi User group information.
   * @param nnAddress Namenode address for the connection.
   * @param protocol Protocol for the connection.
   * @param nsId Nameservice identity.
   * @return Proxy client to connect to nnId as UGI.
   * @throws IOException If the connection cannot be obtained.
   */
  public ConnectionContext getConnection(UserGroupInformation ugi,
      String nnAddress, Class<?> protocol, String nsId) throws IOException {
    // Check if the manager is shutdown
    if (!this.running) {
      LOG.error(
          "Cannot get a connection to {} because the manager isn't running",
          nnAddress);
      return null;
    }

    // Try to get the pool if created
    ConnectionPoolId connectionId =
        new ConnectionPoolId(ugi, nnAddress, protocol);
    ConnectionPool pool = null;
    readLock.lock();
    try {
      pool = this.pools.get(connectionId);
    } finally {
      readLock.unlock();
    }

    // Create the pool if not created before
    if (pool == null) {
      writeLock.lock();
      try {
        pool = this.pools.get(connectionId);
        if (pool == null) {
          pool = new ConnectionPool(
              this.conf, nnAddress, ugi, this.minSize, this.maxSize,
              this.minActiveRatio, protocol,
              new PoolAlignmentContext(this.routerStateIdContext, nsId));
          this.pools.put(connectionId, pool);
        }
      } finally {
        writeLock.unlock();
      }
    }

    long clientStateId = RouterStateIdContext.getClientStateIdFromCurrentCall(nsId);
    pool.getPoolAlignmentContext().advanceClientStateId(clientStateId);

    ConnectionContext conn = pool.getConnection();

    // Add a new connection to the pool if it wasn't usable
    if (conn == null || !conn.isUsable()) {
      if (!this.creatorQueue.offer(pool)) {
        LOG.error("Cannot add more than {} connections at the same time",
            this.creatorQueueMaxSize);
      }
    }

    if (conn != null && conn.isClosed()) {
      LOG.error("We got a closed connection from {}", pool);
      conn = null;
    }

    return conn;
  }

  /**
   * Get the number of connection pools.
   *
   * @return Number of connection pools.
   */
  public int getNumConnectionPools() {
    readLock.lock();
    try {
      return pools.size();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get number of open connections.
   *
   * @return Number of open connections.
   */
  public int getNumConnections() {
    int total = 0;
    readLock.lock();
    try {
      for (ConnectionPool pool : this.pools.values()) {
        total += pool.getNumConnections();
      }
    } finally {
      readLock.unlock();
    }
    return total;
  }

  /**
   * Get number of active connections.
   *
   * @return Number of active connections.
   */
  public int getNumActiveConnections() {
    int total = 0;
    readLock.lock();
    try {
      for (ConnectionPool pool : this.pools.values()) {
        total += pool.getNumActiveConnections();
      }
    } finally {
      readLock.unlock();
    }
    return total;
  }

  /**
   * Get number of idle connections.
   *
   * @return Number of active connections.
   */
  public int getNumIdleConnections() {
    int total = 0;
    readLock.lock();
    try {
      for (ConnectionPool pool : this.pools.values()) {
        total += pool.getNumIdleConnections();
      }
    } finally {
      readLock.unlock();
    }
    return total;
  }

  /**
   * Get number of recently active connections.
   *
   * @return Number of recently active connections.
   */
  public int getNumActiveConnectionsRecently() {
    int total = 0;
    readLock.lock();
    try {
      for (ConnectionPool pool : this.pools.values()) {
        total += pool.getNumActiveConnectionsRecently();
      }
    } finally {
      readLock.unlock();
    }
    return total;
  }

  /**
   * Get the number of connections to be created.
   *
   * @return Number of connections to be created.
   */
  public int getNumCreatingConnections() {
    return this.creatorQueue.size();
  }

  /**
   * Get a JSON representation of the connection pool.
   *
   * @return JSON representation of all the connection pools.
   */
  public String getJSON() {
    final Map<String, String> info = new TreeMap<>();
    readLock.lock();
    try {
      for (Entry<ConnectionPoolId, ConnectionPool> entry :
          this.pools.entrySet()) {
        ConnectionPoolId connectionPoolId = entry.getKey();
        ConnectionPool pool = entry.getValue();
        info.put(connectionPoolId.toString(), pool.getJSON());
      }
    } finally {
      readLock.unlock();
    }
    return JSON.toString(info);
  }

  @VisibleForTesting
  Map<ConnectionPoolId, ConnectionPool> getPools() {
    return this.pools;
  }

  /**
   * Clean the unused connections for this pool.
   *
   * @param pool Connection pool to cleanup.
   */
  @VisibleForTesting
  void cleanup(ConnectionPool pool) {
    if (pool.getNumConnections() > pool.getMinSize()) {
      // Check if the pool hasn't been active in a while or not 50% are used
      long timeSinceLastActive = Time.now() - pool.getLastActiveTime();
      int total = pool.getNumConnections();
      // Active is a transient status in many cases for a connection since
      // the handler thread uses the connection very quickly. Thus, the number
      // of connections with handlers using at the call time is constantly low.
      // Recently active is more lasting status, and it shows how many
      // connections have been used with a recent time period. (i.e. 30 seconds)
      int active = pool.getNumActiveConnectionsRecently();
      float poolMinActiveRatio = pool.getMinActiveRatio();
      if (timeSinceLastActive > connectionCleanupPeriodMs ||
          active < poolMinActiveRatio * total) {
        // Be greedy here to close as many connections as possible in one shot
        // The number should at least be 1
        int targetConnectionsCount = Math.max(1,
            (int)(poolMinActiveRatio * total) - active);
        List<ConnectionContext> connections =
            pool.removeConnections(targetConnectionsCount);
        for (ConnectionContext conn : connections) {
          conn.close();
        }
        LOG.debug("Removed connection {} used {} seconds ago. " +
                "Pool has {}/{} connections", pool.getConnectionPoolId(),
            TimeUnit.MILLISECONDS.toSeconds(timeSinceLastActive),
            pool.getNumConnections(), pool.getMaxSize());
      }
    }
  }

  /**
   * Removes stale connections not accessed recently from the pool. This is
   * invoked periodically.
   */
  private class CleanupTask implements Runnable {

    @Override
    public void run() {
      long currentTime = Time.now();
      List<ConnectionPoolId> toRemove = new LinkedList<>();

      // Look for stale pools
      readLock.lock();
      try {
        for (Entry<ConnectionPoolId, ConnectionPool> entry : pools.entrySet()) {
          ConnectionPool pool = entry.getValue();
          long lastTimeActive = pool.getLastActiveTime();
          boolean isStale =
              currentTime > (lastTimeActive + poolCleanupPeriodMs);
          if (lastTimeActive > 0 && isStale) {
            // Remove this pool
            LOG.debug("Closing and removing stale pool {}", pool);
            pool.close();
            ConnectionPoolId poolId = entry.getKey();
            toRemove.add(poolId);
          } else {
            // Keep this pool but clean connections inside
            LOG.debug("Cleaning up {}", pool);
            cleanup(pool);
          }
        }
      } finally {
        readLock.unlock();
      }

      // Remove stale pools
      if (!toRemove.isEmpty()) {
        writeLock.lock();
        try {
          for (ConnectionPoolId poolId : toRemove) {
            pools.remove(poolId);
          }
        } finally {
          writeLock.unlock();
        }
      }
    }
  }

  /**
   * Thread that creates connections asynchronously.
   */
  static class ConnectionCreator extends Thread {
    /** If the creator is running. */
    private boolean running = true;
    /** Queue to push work to. */
    private BlockingQueue<ConnectionPool> queue;

    ConnectionCreator(BlockingQueue<ConnectionPool> blockingQueue) {
      super("Connection creator");
      this.queue = blockingQueue;
    }

    @Override
    public void run() {
      while (this.running) {
        try {
          ConnectionPool pool = this.queue.take();
          try {
            int total = pool.getNumConnections();
            int active = pool.getNumActiveConnectionsRecently();
            float poolMinActiveRatio = pool.getMinActiveRatio();
            if (pool.getNumConnections() < pool.getMaxSize() &&
                active >= poolMinActiveRatio * total) {
              ConnectionContext conn = pool.newConnection();
              pool.addConnection(conn);
            } else {
              LOG.debug("Cannot add more than {} connections to {}",
                  pool.getMaxSize(), pool);
            }
          } catch (IOException e) {
            LOG.error("Cannot create a new connection for {} {}", pool, e);
          }
        } catch (InterruptedException e) {
          LOG.error("The connection creator was interrupted");
          this.running = false;
        } catch (Throwable e) {
          LOG.error("Fatal error caught by connection creator ", e);
        }
      }
    }

    /**
     * Stop this connection creator.
     */
    public void shutdown() {
      this.running = false;
      this.interrupt();
    }
  }
}
