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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;

/**
 * Thread-safe, bounded connection cache used by {@link AbfsConnectionManager}.
 * Supports reuse of open, non-stale Apache HttpClient connections.
 * Replaces oldest connection if capacity is full.
 * <p>
 * Backed by a LinkedBlockingDeque, with bounded capacity.
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
class KeepAliveCache implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KeepAliveCache.class);

  private final LinkedBlockingDeque<HttpClientConnection> deque;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final int maxCacheConnections;
  private final String accountNamePath;

  KeepAliveCache(AbfsConfiguration abfsConfiguration) {
    this.accountNamePath = abfsConfiguration.getAccountName();
    this.maxCacheConnections = abfsConfiguration.getMaxApacheHttpClientCacheConnections();
    this.deque = new LinkedBlockingDeque<>(maxCacheConnections);
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
      LOG.debug("Failed to close connection: {}", hc, ex);
    }
  }

  /**
   * Check if the connection is stale or closed.
   *
   * @param conn HttpClientConnection to check
   * @return true if stale or closed, false otherwise
   */
  private boolean isConnectionStale(HttpClientConnection conn) {
    try {
      return !conn.isOpen() || conn.isStale();
    } catch (Exception e) {
      return true;
    }
  }

  /**
   * Close the cache and all cached connections.
   */
  @Override
  public void close() {
    if (isClosed.getAndSet(true)) {
      return;
    }
    closeInternal();
  }

  @VisibleForTesting
  void closeInternal() {
    HttpClientConnection conn;
    while ((conn = deque.pollFirst()) != null) {
      closeHttpClientConnection(conn);
    }
  }

  /**
   * Get the oldest usable connection from the cache.
   *
   * @return HttpClientConnection if available and valid, otherwise null.
   * @throws IOException if the cache is closed
   */
  public HttpClientConnection get() throws IOException {
    if (isClosed.get()) {
      throw new ClosedIOException(accountNamePath, KEEP_ALIVE_CACHE_CLOSED);
    }
    HttpClientConnection conn;
    while ((conn = deque.pollFirst()) != null) {
      if (isConnectionStale(conn)) {
        closeHttpClientConnection(conn);
        continue;
      }
      LOG.debug("Reusing cached connection: {}", conn);
      return conn;
    }
    return null;
  }

  /**
   * Attempt to add a connection to the cache.
   * If full, evicts the oldest one first.
   *
   * @param conn connection to add
   * @return true if added, false if closed or rejected
   */
  public boolean add(HttpClientConnection conn) {
    if (isClosed.get() || maxCacheConnections <= 0 || isConnectionStale(conn)) {
      closeHttpClientConnection(conn);
      return false;
    }

    // Remove oldest if full
    if (!deque.offerLast(conn)) {
      HttpClientConnection evicted = deque.pollFirst();
      if (evicted != null) {
        closeHttpClientConnection(evicted);
      }
      boolean offered = deque.offerLast(conn);
      if (!offered) {
        closeHttpClientConnection(conn);
        return false;
      }
    }
    LOG.debug("Cached new connection: {}", conn);
    return true;
  }

  public int size() {
    return deque.size();
  }

  public void clear() {
    if (isClosed.get()) {
      return;
    }
    closeInternal();
    deque.clear();
  }

  @VisibleForTesting
  public int getMaxCacheConnections() {
    return maxCacheConnections;
  }

  @Override
  public String toString() {
    return String.format("KeepAliveCache[closed=%s, size=%d, max=%d]",
        isClosed.get(), deque.size(), maxCacheConnections);
  }
}
