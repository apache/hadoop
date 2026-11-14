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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;

/**
 * KeepAliveCache manages pooled HTTP connections for Azure Blob File System (ABFS) clients.
 * <p>
 * It maintains separate caches for default and cluster hosts, using thread-safe queues to store connections.
 * The cache supports concurrent access, eviction, and safe closure of connections.
 * <ul>
 *   <li>Connections are pooled per host, with limits for default and cluster hosts.</li>
 *   <li>Eviction is handled atomically to prevent race conditions.</li>
 *   <li>Thread pools are used for cache refresh and warmup, with daemon threads to avoid JVM hang.</li>
 *   <li>Cache closure ensures all connections and resources are released safely.</li>
 * </ul>
 * <p>
 * Typical usage involves putting and getting connections, and closing the cache when done.
 * <p>
 * <b>Thread Safety:</b> All operations are thread-safe, using concurrent collections and atomic variables.
 * <p>
 * <b>Testing:</b> Exposes methods and inner classes for testing cache size, host queues, and connection limits.
 */
class KeepAliveCache implements Closeable {

  /**
   * Logger instance.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      KeepAliveCache.class);

  /**
   * Indicates whether the cache has been closed.
   * Used to prevent further operations after closure.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Maximum number of connections allowed in the default host cache.
   */
  private final int maxDefaultConnections;

  /**
   * Maximum number of connections allowed in the cluster host cache.
   */
  private final int maxClusterConnections;

  /**
   * The account name path associated with this cache instance.
   */
  private final String accountNamePath;

  /**
   * Executor service for single-threaded cache refresh operations.
   */
  private ExecutorService singleThreadPool;

  /**
   * Executor service for fixed-threaded cache warmup and refresh operations.
   */
  private ExecutorService fixedThreadPool;

  /**
   * Represents a pooled HTTP connection with a usage flag and cache key.
   * <p>
   * Each instance wraps a {@link HttpClientConnection} and tracks whether it is currently in use.
   * The {@code cacheKey} identifies the host this connection is associated with.
   * The {@code inUse} flag is used to prevent race conditions during eviction or retrieval.
   */
  @VisibleForTesting
  public static final class PooledConnection {

    final HttpClientConnection conn;

    final String cacheKey;

    final AtomicBoolean inUse = new AtomicBoolean(false); // New Flag

    PooledConnection(HttpClientConnection conn, String cacheKey) {
      this.conn = conn;
      this.cacheKey = cacheKey;
    }
  }

  /**
   * HostQueue is a thread-safe queue for storing pooled HTTP connections per host.
   * <p>
   * Uses a ConcurrentLinkedDeque to allow concurrent access and lock-free operations.
   * Each HostQueue instance manages the connections for a specific host in the cache.
   */
  @VisibleForTesting
  public static final class HostQueue {

    // ConcurrentLinkedDeque is thread-safe; no manual lock needed
    final ConcurrentLinkedDeque<PooledConnection> queue
        = new ConcurrentLinkedDeque<>();
  }

  private final ConcurrentHashMap<String, HostQueue> hostCaches
      = new ConcurrentHashMap<>();

  private final ConcurrentLinkedQueue<PooledConnection> clusterQueue
      = new ConcurrentLinkedQueue<>();

  private final AtomicInteger defaultSize = new AtomicInteger(0);

  private final AtomicInteger clusterSize = new AtomicInteger(0);

  /**
   * Checks if the cache is closed.
   *
   * @return true if the cache is closed, false otherwise
   */
  boolean isClosed() {
    return closed.get();
  }

  /**
   * Gets the current size of the default host cache.
   *
   * @return the number of connections cached for the default host
   */
  int getCachedDefaultSize() {
    return defaultSize.get();
  }

  /**
   * Constructs a KeepAliveCache instance with the specified configuration.
   *
   * @param abfsConfiguration the ABFS configuration
   */
  KeepAliveCache(AbfsConfiguration abfsConfiguration) {
    this.accountNamePath = abfsConfiguration.getAccountName();
    this.maxDefaultConnections
        = abfsConfiguration.getApacheMaxDefaultCacheSize();
    this.maxClusterConnections
        = abfsConfiguration.getApacheMaxNonDefaultCacheSize();

    // Fix: Always use Daemon threads to prevent JVM hang on exit
    if (abfsConfiguration.getApacheCacheRefreshCount() > 0) {
      this.singleThreadPool = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "CacheRefreshThread");
        t.setDaemon(true);
        return t;
      });
    }

    int warmup = abfsConfiguration.getApacheCacheWarmupCount();
    int refresh = abfsConfiguration.getApacheCacheRefreshCount();
    if (warmup > 0 || refresh > 0) {
      this.fixedThreadPool = Executors.newFixedThreadPool(
          Math.min(5, Math.max(warmup, refresh)), r -> {
            Thread t = new Thread(r, "AsyncCacheConnectionThread");
            t.setDaemon(true);
            return t;
          });
    }
  }

  /**
   * Retrieves a pooled HTTP connection for the specified host.
   * <p>
   * If the cache is closed, throws ClosedIOException. If no connection is available, returns null.
   * Decrements the appropriate counter and closes stale connections.
   *
   * @param host the host to retrieve a connection for
   * @param isDefaultHost true if the host is the default host
   * @return a valid HttpClientConnection or null if unavailable
   * @throws IOException if the cache is closed
   */
  public HttpClientConnection get(String host, boolean isDefaultHost)
      throws IOException {
    if (closed.get()) {
      throw new ClosedIOException(accountNamePath, KEEP_ALIVE_CACHE_CLOSED);
    }
    HostQueue hq = hostCaches.get(host);
    if (hq == null) {
      return null;
    }

    PooledConnection pooled;
    // poll() is now an atomic, non-blocking operation
    while ((pooled = hq.queue.poll()) != null) {
      if (!pooled.inUse.compareAndSet(false, true)) {
        continue; // Already claimed by eviction
      }

      if (!isDefaultHost) {
        clusterSize.decrementAndGet();
      } else {
        defaultSize.decrementAndGet();
      }

      if (pooled.conn.isOpen() && !pooled.conn.isStale()) {
        return pooled.conn;
      }
      closeQuietly(pooled.conn);
    }
    return null;
  }

  /**
   * Adds a connection to the cache for the specified host type.
   * <p>
   * Only valid, open, non-stale connections are cached. Defensive casting ensures only
   * AbfsManagedApacheHttpConnection instances are accepted. Returns true if the connection
   * was successfully cached, false otherwise.
   *
   * @param conn the connection to cache
   * @param isDefaultHost true if the host is the default host
   * @return true if the connection was cached, false otherwise
   */
  public boolean put(HttpClientConnection conn, boolean isDefaultHost) {
    if (conn == null || closed.get() || !conn.isOpen() || conn.isStale()) {
      closeQuietly(conn);
      return false;
    }

    // Defensive casting check
    if (!(conn instanceof AbfsManagedApacheHttpConnection)) {
      closeQuietly(conn);
      return false;
    }

    String host = ((AbfsManagedApacheHttpConnection) conn).getTargetHost()
        .toHostString();
    PooledConnection pooled = new PooledConnection(conn, host);
    HostQueue hq = hostCaches.computeIfAbsent(host, k -> new HostQueue());

    return isDefaultHost ? putDefault(hq, pooled) : putCluster(hq, pooled);
  }

  /**
   * Adds a connection to the default host cache, evicting the oldest if the cache exceeds its limit.
   *
   * @param hq the HostQueue for the default host
   * @param pooled the pooled connection to add
   * @return true if the connection was cached, false otherwise
   */
  private boolean putDefault(HostQueue hq, PooledConnection pooled) {
    if (maxDefaultConnections <= 0) {
      closeQuietly(pooled.conn);
      return false;
    }

    hq.queue.offer(pooled);
    if (defaultSize.incrementAndGet() > maxDefaultConnections) {
      PooledConnection evicted = hq.queue.poll(); // Evict oldest
      if (evicted != null) {
        // Double-check 'inUse' to ensure we don't close a connection just handed out
        if (evicted.inUse.compareAndSet(false, true)) {
          defaultSize.decrementAndGet();
          closeQuietly(evicted.conn);
        } else {
          // If we couldn't claim it for eviction, it was just grabbed by get()
          // defaultSize was already decremented by get(), so just move on.
        }
      }
    }
    return true;
  }

  /**
   * Adds a connection to the cluster host cache, evicting connections if the cache exceeds its limit.
   *
   * @param hq the HostQueue for the cluster host
   * @param pooled the pooled connection to add
   * @return true if the connection was cached, false otherwise
   */
  private boolean putCluster(HostQueue hq, PooledConnection pooled) {
    if (maxClusterConnections <= 0) {
      closeQuietly(pooled.conn);
      return false;
    }

    while (clusterSize.get() >= maxClusterConnections) {
      PooledConnection candidate = clusterQueue.poll();
      if (candidate == null) {break;}

      if (candidate.inUse.compareAndSet(false, true)) {
        clusterSize.decrementAndGet();
        HostQueue evictHq = hostCaches.get(candidate.cacheKey);
        if (evictHq != null) {
          // remove() on ConcurrentLinkedDeque is thread-safe and lock-free
          evictHq.queue.remove(candidate);
        }
        closeQuietly(candidate.conn);
      }
    }

    hq.queue.offer(pooled);
    clusterQueue.offer(pooled);
    clusterSize.incrementAndGet();
    return true;
  }

  /**
   * Closes the given HttpClientConnection quietly, ignoring any IOException.
   *
   * @param conn the connection to close
   */
  private void closeQuietly(HttpClientConnection conn) {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (IOException ignored) {
      // Ignore
    }
  }

  /**
   * Gets the maximum number of connections that can be cached for the default host.
   *
   * @return the maximum default host cache size
   */
  @VisibleForTesting
  public int getMaxCacheConnections() {
    return maxDefaultConnections;
  }

  /**
   * Gets the maximum number of connections that can be cached for cluster hosts.
   *
   * @return the maximum cluster host cache size
   */
  @VisibleForTesting
  public int getMaxCacheConnectionsForHost() {
    return maxClusterConnections;
  }

  /**
   * Gets the current number of connections cached for the default host.
   *
   * @return the default host cache size
   */
  @VisibleForTesting
  public int getDefaultConnectionsSize() {
    return defaultSize.get();
  }

  /**
   * Gets the current number of connections cached for cluster hosts.
   *
   * @return the cluster host cache size
   */
  @VisibleForTesting
  public int getClusterConnectionsSize() {
    return clusterSize.get();
  }

  /**
   * Gets the HostQueue for the specified host.
   *
   * @param host the host name
   * @return the HostQueue for the host, or null if not present
   */
  @VisibleForTesting
  public HostQueue getHostQueue(String host) {
    return hostCaches.get(host);
  }

  /**
   * Gets the fixed thread pool used for cache warmup and refresh operations.
   *
   * @return the fixed thread pool ExecutorService, or null if not initialized
   */
  ExecutorService getFixedThreadPool() {
    return fixedThreadPool;
  }

  /**
   * Gets the single thread pool used for cache refresh operations.
   *
   * @return the single thread pool ExecutorService, or null if not initialized
   */
  ExecutorService getSingleThreadPool() {
    return singleThreadPool;
  }

  /**
   * Closes the cache and all connections within it, shutting down thread pools and releasing resources.
   */
  @Override
  public void close() {
    // 1. Atomic check to ensure close only runs once
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    // 2. Shutdown thread pools immediately to stop background refresh/warmup
    if (singleThreadPool != null) {
      singleThreadPool.shutdownNow();
    }
    if (fixedThreadPool != null) {
      fixedThreadPool.shutdownNow();
    }

    // 3. Close all connections across all host queues
    hostCaches.values().forEach(hq -> {
      PooledConnection pooled;
      // poll() ensures we "own" the connection during the close process
      // preventing any race conditions with late-running threads
      while ((pooled = hq.queue.poll()) != null) {
        // Attempt to claim it. If get() already took it, it will handle closing or reuse.
        if (pooled.inUse.compareAndSet(false, true)) {
          closeQuietly(pooled.conn);
        }
      }
    });

    // 4. Clear the maps and queues to release memory
    hostCaches.clear();
    clusterQueue.clear();

    // 5. Reset counters
    clusterSize.set(0);
    defaultSize.set(0);

    LOG.debug("KeepAliveCache closed for account: {}", accountNamePath);
  }

  /**
   * Returns a string representation of the KeepAliveCache instance.
   *
   * @return a string describing the cache state
   */
  @Override
  public String toString() {
    return String.format("KeepAliveCache[closed=%s, size=%d, max=%d]",
        closed.get(), defaultSize.get() + clusterSize.get(),
        getMaxCacheConnections() + getMaxCacheConnectionsForHost());
  }
}
