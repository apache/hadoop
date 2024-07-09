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
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_CONNECTION_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS;

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
 * <li>In PoolingHttpClientConnectionManager, it expects the application to provide `setMaxPerRoute` and `setMaxTotal`,
 * which the implementation uses as the total number of connections it can create. For application using ABFS, it is not
 * feasible to provide a value in the initialisation of the connectionManager. JDK's implementation has no cap on the
 * number of connections it can create.</li>
 * </ol>
 */
class KeepAliveCache extends Stack<KeepAliveCache.KeepAliveEntry>
    implements
    Closeable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(KeepAliveCache.class);

  /**
   * Scheduled timer that evicts idle connections.
   */
  private final transient Timer timer;

  /**
   * Task provided to the timer that owns eviction logic.
   */
  private final transient TimerTask timerTask;

  /**
   * Flag to indicate if the cache is closed.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * Counter to keep track of the number of KeepAliveCache instances created.
   */
  private static final AtomicInteger KAC_COUNTER = new AtomicInteger(0);

  /**
   * Maximum number of connections that can be cached.
   */
  private final int maxConn;

  /**
   * Time-to-live for an idle connection.
   */
  private final long connectionIdleTTL;

  /**
   * Flag to indicate if the eviction thread is paused.
   */
  private final AtomicBoolean isPaused = new AtomicBoolean(false);

  /**
   * Account name for which the cache is created. To be used only in exception
   * messages.
   */
  private final String accountNamePath;

  @VisibleForTesting
  synchronized void pauseThread() {
    isPaused.set(true);
  }

  @VisibleForTesting
  synchronized void resumeThread() {
    isPaused.set(false);
  }

  /**
   * @return connectionIdleTTL.
   */
  @VisibleForTesting
  public long getConnectionIdleTTL() {
    return connectionIdleTTL;
  }

  /**
   * Creates an {@link KeepAliveCache} instance using filesystem's configuration.
   * <p>
   * The size of the cache is determined by the configuration
   * {@value org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys#FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_CONNECTION_SIZE}.
   * If the configuration is not set, the system-property {@value org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants#HTTP_MAX_CONN_SYS_PROP}.
   * If the system-property is not set or set to 0, the default value
   * {@value org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations#DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS} is used.
   * <p>
   * This schedules an eviction thread to run every connectionIdleTTL milliseconds
   * given by the configuration {@link AbfsConfiguration#getMaxApacheHttpClientConnectionIdleTime()}.
   * @param abfsConfiguration Configuration of the filesystem.
   */
  KeepAliveCache(AbfsConfiguration abfsConfiguration) {
    accountNamePath = abfsConfiguration.getAccountName();
    this.timer = new Timer("abfs-kac-" + KAC_COUNTER.getAndIncrement(), true);

    int sysPropMaxConn = Integer.parseInt(System.getProperty(HTTP_MAX_CONN_SYS_PROP, "0"));
    final int defaultMaxConn;
    if (sysPropMaxConn > 0) {
      defaultMaxConn = sysPropMaxConn;
    } else {
      defaultMaxConn = DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS;
    }
    this.maxConn = abfsConfiguration.getInt(
        FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_CONNECTION_SIZE,
        defaultMaxConn);

    this.connectionIdleTTL
        = abfsConfiguration.getMaxApacheHttpClientConnectionIdleTime();
    this.timerTask = new TimerTask() {
      @Override
      public void run() {
          if (isPaused.get() || isClosed.get()) {
            return;
          }
          evictIdleConnection();
      }
    };
    timer.schedule(timerTask, 0, connectionIdleTTL);
  }

  /**
   * Iterate over the cache and evict the idle connections. An idle connection is
   * one that has been in the cache for more than connectionIdleTTL milliseconds.
   */
  synchronized void evictIdleConnection() {
    long currentTime = System.currentTimeMillis();
    int i;
    for (i = 0; i < size(); i++) {
      KeepAliveEntry e = elementAt(i);
      if ((currentTime - e.idleStartTime) > connectionIdleTTL
          || e.httpClientConnection.isStale()) {
        HttpClientConnection hc = e.httpClientConnection;
        closeHttpClientConnection(hc);
      } else {
        break;
      }
    }
    subList(0, i).clear();
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
   * Close all connections in cache and cancel the eviction timer.
   */
  @Override
  public synchronized void close() {
    boolean closed = isClosed.getAndSet(true);
    if (closed) {
      return;
    }
    closeInternal();
  }

  @VisibleForTesting
  void closeInternal() {
    timerTask.cancel();
    timer.purge();
    while (!empty()) {
      KeepAliveEntry e = pop();
      closeHttpClientConnection(e.httpClientConnection);
    }
  }

  /**
   * <p>
   * Gets the latest added HttpClientConnection from the cache. The returned connection
   * is non-stale and has been in the cache for less than connectionIdleTTL milliseconds.
   * <p>
   * The cache is checked from the top of the stack. If the connection is stale or has been
   * in the cache for more than connectionIdleTTL milliseconds, it is closed and the next
   * connection is checked. Once a valid connection is found, it is returned.
   * @return HttpClientConnection: if a valid connection is found, else null.
   * @throws IOException if the cache is closed.
   */
  public synchronized HttpClientConnection get()
      throws IOException {
    if (isClosed.get()) {
      throw new ClosedIOException(accountNamePath, KEEP_ALIVE_CACHE_CLOSED);
    }
    if (empty()) {
      return null;
    }
    HttpClientConnection hc = null;
    long currentTime = System.currentTimeMillis();
    do {
      KeepAliveEntry e = pop();
      if ((currentTime - e.idleStartTime) > connectionIdleTTL
          || e.httpClientConnection.isStale()) {
        closeHttpClientConnection(e.httpClientConnection);
      } else {
        hc = e.httpClientConnection;
      }
    } while ((hc == null) && (!empty()));
    return hc;
  }

  /**
   * Puts the HttpClientConnection in the cache. If the size of cache is equal to
   * maxConn, the oldest connection is closed and removed from the cache, which
   * will make space for the new connection. If the cache is closed or of zero size,
   * the connection is closed and not added to the cache.
   *
   * @param httpClientConnection HttpClientConnection to be cached
   * @return true if the HttpClientConnection is added in active cache, false otherwise.
   */
  public synchronized boolean put(HttpClientConnection httpClientConnection) {
    if (isClosed.get() || maxConn == 0) {
      closeHttpClientConnection(httpClientConnection);
      return false;
    }
    if (size() == maxConn) {
      closeHttpClientConnection(get(0).httpClientConnection);
      subList(0, 1).clear();
    }
    KeepAliveEntry entry = new KeepAliveEntry(httpClientConnection,
        System.currentTimeMillis());
    push(entry);
    return true;
  }

  @Override
  public synchronized boolean equals(final Object o) {
    return super.equals(o);
  }

  @Override
  public synchronized int hashCode() {
    return super.hashCode();
  }

  /**
   * Entry data-structure in the cache.
   */
  static class KeepAliveEntry {

    /**HttpClientConnection in the cache entry.*/
    private final HttpClientConnection httpClientConnection;

    /**Time at which the HttpClientConnection was added to the cache.*/
    private final long idleStartTime;

    KeepAliveEntry(HttpClientConnection hc, long idleStartTime) {
      this.httpClientConnection = hc;
      this.idleStartTime = idleStartTime;
    }
  }
}
