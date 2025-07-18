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

import java.io.IOException;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.config.Registry;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpClientConnectionOperator;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.DefaultHttpClientConnectionOperator;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.protocol.HttpContext;

/**
 * AbfsConnectionManager is a custom implementation of {@code HttpClientConnectionManager}.
 * This implementation manages connection-pooling heuristics and custom implementation
 * of {@link ManagedHttpClientConnectionFactory}.
 */
class AbfsConnectionManager implements HttpClientConnectionManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsConnectionManager.class);

  /**
   * Connection pool for the ABFS managed connections.
   */
  private final KeepAliveCache kac;

  /**
   * Factory to create new connections.
   */
  private final AbfsHttpClientConnectionFactory httpConnectionFactory;

  /**
   * Operator to manage the network connection state of ABFS managed connections.
   */
  private final HttpClientConnectionOperator connectionOperator;

  /**
   * Number of connections to be created during cache refresh.
   */
  private final int cacheRefreshConnections;

  /**
   * Connection timeout for establishing a new connection.
   */
  private final int connectionTimeout;

  private final AtomicBoolean isCaching = new AtomicBoolean(false);

  private final Object connectionLock = new Object();

  private HttpHost baseHost;// lock for waiting threads

  AbfsConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry,
      AbfsHttpClientConnectionFactory connectionFactory, KeepAliveCache kac,
      final AbfsConfiguration abfsConfiguration, final URL baseUrl) {
    this.httpConnectionFactory = connectionFactory;
    this.connectionTimeout = abfsConfiguration.getHttpConnectionTimeout();
    this.kac = kac;
    this.connectionOperator = new DefaultHttpClientConnectionOperator(
        socketFactoryRegistry, null, null);
    if (abfsConfiguration.getCacheWarmupConnections() > 0) {
      // Warm up the cache with connections.
      LOG.debug("Warming up the KeepAliveCache with {} connections",
          abfsConfiguration.getCacheWarmupConnections());
      this.baseHost = new HttpHost(baseUrl.getHost(),
          baseUrl.getDefaultPort(), baseUrl.getProtocol());
      HttpRoute route = new HttpRoute(baseHost, null, true);
      cacheExtraConnection(route, abfsConfiguration.getCacheWarmupConnections());
    }
    this.cacheRefreshConnections = abfsConfiguration.getCacheRefreshConnections();
  }

  /**
   * Returns a custom implementation of connection request for the given route.
   * The implementation would return a connection from the {@link KeepAliveCache} if available,
   * else it would create a new non-connected {@link AbfsManagedApacheHttpConnection}.
   */
  @Override
  public ConnectionRequest requestConnection(final HttpRoute route,
      final Object state) {
    return new ConnectionRequest() {

      /**
       * Synchronously gets a connection from the {@link KeepAliveCache} or
       * creates a new un-connected instance of {@link AbfsManagedApacheHttpConnection}.
       */
      @Override
      public HttpClientConnection get(final long timeout,
          final TimeUnit timeUnit) throws ExecutionException {
        String requestId = UUID.randomUUID().toString();
        logDebug("Connection requested for request {}", requestId);
        if (!route.getTargetHost().equals(baseHost)) {
          // If the route target host does not match the base host, create a new connection
          logDebug("Route target host {} does not match base host {}, creating new connection",
              route.getTargetHost(), baseHost);
          return createNewConnection();
        }
        try {
          HttpClientConnection conn = kac.get();

          // If a valid connection is available, return it and trigger background warm-up if needed
          if (conn != null) {
            triggerConnectionWarmupIfNeeded();
            return conn;
          }

          // No connection available — wait up to timeout for one to appear
          synchronized (connectionLock) {
            triggerConnectionWarmupIfNeeded();

            final long timeoutMs = 500L;
            final long deadline = System.currentTimeMillis() + timeoutMs;

            while ((conn = kac.get()) == null
                && System.currentTimeMillis() < deadline) {
              long waitTime = deadline - System.currentTimeMillis();
              if (waitTime <= 0) break;

              try {
                connectionLock.wait(waitTime);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
              }
            }

            if (conn != null) {
              logDebug("Connection retrieved from KAC: {} for requestId: {}",
                  conn, requestId);
              return conn;
            }

            // Timed out — create a new connection
            logDebug("Creating new connection for requestId: {}", requestId);
            return createNewConnection();
          }
        } catch (IOException ex) {
          throw new ExecutionException(ex);
        }
      }

      @Override
      public boolean cancel() {
        return false;
      }

      /**
       * Trigger a background warm-up of the connection cache if needed.
       * This method checks if the cache size is small and if caching is not already in progress.
       * If so, it starts a new thread to cache extra connections.
       */
      private void triggerConnectionWarmupIfNeeded() {
        if (kac.size() <= 2 && !isCaching.get()) {
          // Use a single-threaded executor or thread pool instead of raw thread
          new Thread(() -> cacheExtraConnection(route, cacheRefreshConnections)).start();
        }
      }

      /**
       * Creates new Http Client Connection.
       * @return HttpClientConnection a new connection instance
       */
      private HttpClientConnection createNewConnection() {
        return httpConnectionFactory.create(route, null);
      }
    };
  }

  /**
   * Releases a connection for reuse. It can be reused only if validDuration is greater than 0.
   * This method is called by {@link org.apache.http.impl.execchain} internal class `ConnectionHolder`.
   * If it wants to reuse the connection, it will send a non-zero validDuration, else it will send 0.
   * @param conn the connection to release
   * @param newState the new state of the connection
   * @param validDuration the duration for which the connection is valid
   * @param timeUnit the time unit for the validDuration
   */
  @Override
  public void releaseConnection(final HttpClientConnection conn,
      final Object newState,
      final long validDuration,
      final TimeUnit timeUnit) {
    if (validDuration == 0) {
      return;
    }
    addConnectionToCache(conn);
  }

  /**{@inheritDoc}*/
  @Override
  public void connect(final HttpClientConnection conn,
      final HttpRoute route,
      final int connectTimeout,
      final HttpContext context) throws IOException {
    long start = System.currentTimeMillis();
    logDebug("Connecting {} to {}", conn, route.getTargetHost());
    connectionOperator.connect((AbfsManagedApacheHttpConnection) conn,
        route.getTargetHost(), route.getLocalSocketAddress(),
        connectTimeout, SocketConfig.DEFAULT, context);
    logDebug("Connection established: {}", conn);
    if (context instanceof AbfsManagedHttpClientContext) {
      ((AbfsManagedHttpClientContext) context).setConnectTime(
          System.currentTimeMillis() - start);
    }
  }

  /**{@inheritDoc}*/
  @Override
  public void upgrade(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {
    connectionOperator.upgrade((AbfsManagedApacheHttpConnection) conn,
        route.getTargetHost(), context);
  }

  /**{@inheritDoc}*/
  @Override
  public void routeComplete(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {

  }

  /**{@inheritDoc}*/
  @Override
  public void closeIdleConnections(final long idletime,
      final TimeUnit timeUnit) {
    // Do nothing, as we are not managing idle connections
  }

  /**{@inheritDoc}*/
  @Override
  public void closeExpiredConnections() {
    // Do nothing, as we are not managing expired connections
  }

  /**{@inheritDoc}*/
  @Override
  public void shutdown() {
    kac.close();
  }

  private void logDebug(String message, Object... args) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message, args);
    }
  }

  /**
   * Caches extra connections in the {@link KeepAliveCache} to warm it up.
   * This method is called during initialization and when the cache is empty.
   *
   * @param route the HTTP route for which connections are created
   * @param numberOfConnections the number of connections to create
   */
  private void cacheExtraConnection(final HttpRoute route, final int numberOfConnections) {
    if (!isCaching.getAndSet(true)) { // Only one thread allowed at a time
      ExecutorService executorService = Executors.newFixedThreadPool(Math.min(numberOfConnections, 5));

      for (int i = 0; i < numberOfConnections; i++) {
        executorService.submit(() -> {
          try {
            HttpClientConnection conn = httpConnectionFactory.create(route, null);
            connect(conn, route, connectionTimeout, new AbfsManagedHttpClientContext());
            addConnectionToCache(conn);
          } catch (Exception e) {
            LOG.debug("Error creating connection: {}", e.getMessage());
          }
        });
      }

      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      } finally {
        isCaching.set(false);
      }
    } else {
      LOG.debug("Skipping connection warmup — another thread is already caching.");
    }
  }

  /**
   * Adds a connection to the cache if it is open and not stale.
   * If the connection is added to the cache, it notifies one waiting thread.
   *
   * @param conn the connection to add to the cache
   */
  private void addConnectionToCache(HttpClientConnection conn) {
    if (conn instanceof AbfsManagedApacheHttpConnection) {
      if (((AbfsManagedApacheHttpConnection) conn).getTargetHost().equals(baseHost)) {
        boolean connAddedInKac = kac.add(conn);
        synchronized (connectionLock) {
          connectionLock.notify(); // wake up one thread only
        }
        if (connAddedInKac) {
          logDebug("Connection cached: {}", conn);
        } else {
          logDebug("Connection not cached, and is released: {}", conn);
        }
      }
    }
  }
}
