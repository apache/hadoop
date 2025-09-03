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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;

/**
 * Connection-pooling heuristics used by {@link AbfsConnectionManager}. Each
 * instance of FileSystem has its own KeepAliveCache.
 * <p>
 * Why this implementation is required in comparison to {@link org.apache.http.impl.conn.PoolingHttpClientConnectionManager}
 * connection-pooling:
 * <ol>
 * <li>PoolingHttpClientConnectionManager heuristic caches all the reusable connections it has created.
 * JDK's implementation only caches a limited number of connections. The limit is given by JVM system
 * property "http.maxConnections". If there is no system-property, it defaults to 5.</li>
 * <li>In PoolingHttpClientConnectionManager, it expects the application to provide setMaxPerRoute and setMaxTotal,
 * which the implementation uses as the total number of connections it can create. For application using ABFS, it is not
 * feasible to provide a value in the initialisation of the connectionManager. JDK's implementation has no cap on the
 * number of connections it can create.</li>
 * </ol>
 */
class KeepAliveCache extends LinkedBlockingDeque<HttpClientConnection>
    implements Closeable {

  /**
   * Logger instance.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      KeepAliveCache.class);

  /**
   * Flag to indicate if the cache is closed.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * Maximum number of connections that can be cached.
   */
  private final int maxCacheConnections;

  /**
   * Account name for which the cache is created. To be used only in exception
   * messages.
   */
  private final String accountNamePath;

  /**
   * Executor server to trigger connection refresh from cache manager.
   */
  private ExecutorService singleThreadPool = null;

  /**
   * Executor service to trigger async cache warmup.
   */
  private ExecutorService fixedThreadPool = null;


  /**
   * Creates an {@link KeepAliveCache} instance using filesystem's configuration.
   * <p>
   * The size of the cache is determined by the configuration
   * {@value org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys#FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE}.
   * If the configuration is not set, the default value is
   * {@value org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations#DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE}.
   * <p>.
   */
  KeepAliveCache(AbfsConfiguration abfsConfiguration) {
    this.accountNamePath =
        abfsConfiguration.getAccountName();
    this.maxCacheConnections =
        abfsConfiguration.getApacheMaxCacheSize();
    // Initialise singleThreadPool if cache refresh is enabled.
    if (abfsConfiguration.getApacheCacheRefreshCount() > 0) {
      this.singleThreadPool = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setName("CacheRefreshThread");
        thread.setDaemon(true);
        return thread;
      });
    }

    // Initialise fixedThreadPool if cache warmup or cache refresh is enabled.
    if (abfsConfiguration.getApacheCacheWarmupCount() > 0
        || abfsConfiguration.getApacheCacheRefreshCount() > 0) {
      this.fixedThreadPool = Executors.newFixedThreadPool(Math.min(5,
          Math.max(abfsConfiguration.getApacheCacheWarmupCount(),
              abfsConfiguration.getApacheCacheRefreshCount())), r -> {
        Thread thread = new Thread(r);
        thread.setName("AsyncCacheConnectionThread");
        thread.setDaemon(true);
        return thread;
      });
    }
  }

  /**
   * Safe close of the HttpClientConnection.
   *
   * @param hc HttpClientConnection to be closed
   */
  private void closeHttpClientConnection(final HttpClientConnection hc) {
    try {
      hc.close();
    } catch (IOException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Close failed for connection: {}", hc, ex);
      }
    }
  }

  /**
   * Close all connections in cache.
   */
  @Override
  public void close() {
    boolean closed = isClosed.getAndSet(true);
    if (closed) {
      return;
    }
    closeInternal();
    if (singleThreadPool != null && !singleThreadPool.isShutdown()) {
      singleThreadPool.shutdownNow();
    }

    if (fixedThreadPool != null && !fixedThreadPool.isShutdown()) {
      fixedThreadPool.shutdownNow();
    }
  }

  /**
   * @return true if the cache is closed, false otherwise.
   */
  public boolean getIsClosed() {
    return isClosed.get();
  }

  /**
   * @return ExecutorService to trigger connection refresh from cache manager.
   */
  public ExecutorService getSingleThreadPool() {
    return singleThreadPool;
  }

  /**
   * @return ExecutorService to trigger async create connections and put in cache.
   */
  public ExecutorService getFixedThreadPool() {
    return fixedThreadPool;
  }

  /**
   * Internal close method to close all connections in the cache.
   * This method does not change the state of isClosed.
   * It is expected that the caller of this method has set the isClosed flag.
   */
  @VisibleForTesting
  void closeInternal() {
    while (size() != 0) {
      closeHttpClientConnection(pollFirst());
    }
  }

  /**
   * Gets the oldest added HttpClientConnection from the cache. The returned connection
   * is open.
   * The cache follows the FIFO strategy. If the connection is not open, it will
   * be closed and the next connection is checked. Once a valid connection is found,
   * it is returned.
   * @return HttpClientConnection: if a valid connection is found, else null.
   * @throws IOException if the cache is closed.
   */
  public HttpClientConnection get() throws IOException {
    if (getIsClosed()) {
      LOG.debug("Attempt to get connection from closed cache for account: {}",
          accountNamePath);
      throw new ClosedIOException(accountNamePath, KEEP_ALIVE_CACHE_CLOSED);
    }
    HttpClientConnection httpClientConnection;
    while ((httpClientConnection = pollFirst()) != null) {
      if (!httpClientConnection.isOpen() || httpClientConnection.isStale()) {
        closeHttpClientConnection(httpClientConnection);
        continue;
      }
      return httpClientConnection;
    }
    LOG.debug("No valid connection found in cache for account: {}",
        accountNamePath);
    return null;
  }

  /**
   * Puts the HttpClientConnection in the cache. If the size of cache is equal to
   * maxConn, the oldest connection is closed and removed from the cache, which
   * will make space for the new connection. If the cache is closed or of zero size,
   * the connection is closed and not added to the cache.
   *
   * @param conn HttpClientConnection to be cached
   * @return true if the HttpClientConnection is added in active cache, false otherwise.
   */
  public boolean add(HttpClientConnection conn) {
    if (conn == null) {
      LOG.warn(
          "Attempt to add null HttpClientConnection to the cache for account: {}",
          accountNamePath);
      return false;
    }
    if (getIsClosed() || getMaxCacheConnections() <= 0
        || !conn.isOpen() || conn.isStale()) {
      LOG.debug(
          "Not adding connection to cache. closed: {}, "
              + "maxCacheSize: {}, isOpen: {}, isStale: {} for account: {}",
          getIsClosed(), getMaxCacheConnections(), conn.isOpen(),
          conn.isStale(), accountNamePath);
      closeHttpClientConnection(conn);
      return false;
    }
    while (size() >= getMaxCacheConnections()) {
      HttpClientConnection httpClientConnection = pollFirst();
      if (httpClientConnection != null) {
        closeHttpClientConnection(httpClientConnection);
      } else {
        break;
      }
    }
    return offerLast(conn);
  }

  /**
   * @return maximum number of connections that can be cached.
   */
  @VisibleForTesting
  public int getMaxCacheConnections() {
    return maxCacheConnections;
  }

  /**
   * @return String representation of the KeepAliveCache instance.
   */
  @Override
  public String toString() {
    return String.format("KeepAliveCache[closed=%s, size=%d, max=%d]",
        getIsClosed(), size(), getMaxCacheConnections());
  }
}
