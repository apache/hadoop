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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

  /**
   * Logger instance for logging in this class.
   */
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
   * AbfsConfiguration instance to get configuration values.
   */
  private final AbfsConfiguration abfsConfiguration;

  /**
   * Atomic boolean to ensure only one thread can trigger cache refresh at a time.
   */
  private final AtomicBoolean isCacheRefreshInProgress = new AtomicBoolean(
      false);

  /**
   * Lock object for synchronizing connection retrieval and caching.
   */
  private final Object connectionLock = new Object();

  /**
   * The base host for which connections are managed.
   */
  private final HttpHost baseHost;

  AbfsConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry,
      AbfsHttpClientConnectionFactory connectionFactory,
      KeepAliveCache kac,
      final AbfsConfiguration abfsConfiguration,
      final URL baseUrl,
      final boolean isCacheWarmupNeeded) {
    this.httpConnectionFactory = connectionFactory;
    this.kac = kac;
    this.connectionOperator = new DefaultHttpClientConnectionOperator(
        socketFactoryRegistry, null, null);
    this.abfsConfiguration = abfsConfiguration;
    this.baseHost = new HttpHost(baseUrl.getHost(),
        baseUrl.getDefaultPort(), baseUrl.getProtocol());
    if (isCacheWarmupNeeded && abfsConfiguration.getApacheCacheWarmupCount() > 0
        && kac.getFixedThreadPool() != null) {
      // Warm up the cache with connections.
      LOG.debug("Warming up the KeepAliveCache with {} connections",
          abfsConfiguration.getApacheCacheWarmupCount());
      HttpRoute route = new HttpRoute(baseHost, null, true);
      int totalConnectionsCreated = cacheExtraConnection(route,
          abfsConfiguration.getApacheCacheWarmupCount());
      if (totalConnectionsCreated == 0) {
        AbfsApacheHttpClient.registerFallback();
      } else {
        AbfsApacheHttpClient.setUsable();
      }
    }
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
        LOG.debug("Connection requested for request {}", requestId);
        long start = System.nanoTime();
        try {
          if (!route.getTargetHost().equals(baseHost)) {
            // If the route target host does not match the base host, create a new connection
            LOG.debug(
                "Route target host {} does not match base host {}, creating new connection",
                route.getTargetHost(), baseHost);
            return createNewConnection();
          }
          try {
            HttpClientConnection conn = kac.get();

            // If a valid connection is available, return it and trigger background refresh if needed
            if (conn != null) {
              triggerConnectionRefreshIfNeeded();
              return conn;
            }

            // No connection available — wait up to timeout for one to appear
            synchronized (connectionLock) {
              triggerConnectionRefreshIfNeeded();

              final long deadline = System.nanoTime()
                  + TimeUnit.MILLISECONDS.toNanos(
                  abfsConfiguration.getApacheMaxRefreshWaitTimeInMillis());

              while ((conn = kac.get()) == null
                  && System.nanoTime() < deadline) {
                long waitTime = deadline - System.nanoTime();
                if (waitTime <= 0) {
                  break;
                }

                try {
                  connectionLock.wait(TimeUnit.NANOSECONDS.toMillis(waitTime));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return null;
                }
              }

              if (conn != null) {
                LOG.debug("Connection retrieved from KAC: {} for requestId: {}",
                    conn, requestId);
                return conn;
              }

              // Timed out — create a new connection
              LOG.debug("Creating new connection for requestId: {}", requestId);
              return createNewConnection();
            }
          } catch (IOException ex) {
            throw new ExecutionException(ex);
          }
        } finally {
          LOG.debug("Connection request for requestId: {} completed in {} ms",
              requestId, elapsedTimeMillis(start));
        }
      }

      @Override
      public boolean cancel() {
        return false;
      }

      /**
       * Trigger a background refresh of the connection cache if needed.
       * This method checks if the cache size is small and if caching is not already in progress.
       * If so, it starts a new thread to cache extra connections.
       */
      private void triggerConnectionRefreshIfNeeded() {
        if (!isCacheRefreshInProgress.get() && !kac.getIsClosed()
            && kac.getFixedThreadPool() != null
            && kac.getSingleThreadPool() != null
            && kac.size()
            <= abfsConfiguration.getApacheMinTriggerRefreshCount()) {
          // Use a single-threaded executor or thread pool instead of raw thread
          try {
            kac.getSingleThreadPool().submit(() ->
                cacheExtraConnection(route,
                    abfsConfiguration.getApacheCacheRefreshCount()));
          } catch (RejectedExecutionException e) {
            LOG.debug("Task rejected for connection refresh: {}",
                e.getMessage());
          }
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
    long start = System.nanoTime();
    try {
      if (validDuration == 0) {
        return;
      }
      addConnectionToCache(conn);
    } finally {
      LOG.debug("Connection released: {} in {} ms", conn,
          elapsedTimeMillis(start));
    }
  }

  /**{@inheritDoc}*/
  @Override
  public void connect(final HttpClientConnection conn,
      final HttpRoute route,
      final int connectTimeout,
      final HttpContext context) throws IOException {
    long start = System.nanoTime();
    LOG.debug("Connecting {} to {}", conn, route.getTargetHost());
    connectionOperator.connect((AbfsManagedApacheHttpConnection) conn,
        route.getTargetHost(), route.getLocalSocketAddress(),
        connectTimeout, SocketConfig.DEFAULT, context);
    LOG.debug("Connection established: {}", conn);
    if (context instanceof AbfsManagedHttpClientContext) {
      ((AbfsManagedHttpClientContext) context).setConnectTime(
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
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

  /**
   * Caches extra connections in the {@link KeepAliveCache} to warm it up.
   * This method is called during initialization and when the cache is empty.
   *
   * @param route the HTTP route for which connections are created
   * @param numberOfConnections the number of connections to create
   */
  private int cacheExtraConnection(final HttpRoute route,
      final int numberOfConnections) {
    AtomicInteger totalConnectionCreated = new AtomicInteger(0);
    if (!isCacheRefreshInProgress.getAndSet(true)) {
      long start = System.nanoTime();
      CountDownLatch latch = new CountDownLatch(numberOfConnections);

      for (int i = 0; i < numberOfConnections; i++) {
        try {
          kac.getFixedThreadPool().submit(() -> {
            HttpClientConnection conn = null;
            try {
              conn = httpConnectionFactory.create(route, null);
              connect(conn, route, abfsConfiguration.getHttpConnectionTimeout(),
                  new AbfsManagedHttpClientContext());
              addConnectionToCache(conn);
              totalConnectionCreated.incrementAndGet();
            } catch (Exception e) {
              LOG.debug("Error creating connection: {}", e.getMessage());
              if (conn != null) {
                try {
                  conn.close();
                } catch (IOException ioException) {
                  LOG.debug("Error closing connection: {}",
                      ioException.getMessage());
                }
              }
            } finally {
              latch.countDown();
            }
          });
        } catch (RejectedExecutionException e) {
          LOG.debug("Task rejected for connection creation: {}",
              e.getMessage());
          return -1;
        }
      }

      try {
        // Wait for all connections to be created before releasing the lock
        boolean result = latch.await(
            abfsConfiguration.getApacheWarmupCacheTimeoutInMillis(),
            TimeUnit.MILLISECONDS);
        if (!result) {
          LOG.debug("Timeout waiting for connections to be created");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();  // Handle interruption
      } finally {
        isCacheRefreshInProgress.set(false);
        LOG.debug("Connection refresh completed in {} ms",
            elapsedTimeMillis(start));
      }
    }
    return totalConnectionCreated.get();
  }

  /**
   * Adds a connection to the cache if it is open and not stale.
   * If the connection is added to the cache, it notifies one waiting thread.
   *
   * @param conn the connection to add to the cache
   */
  private void addConnectionToCache(HttpClientConnection conn) {
    if (conn instanceof AbfsManagedApacheHttpConnection) {
      if (((AbfsManagedApacheHttpConnection) conn).getTargetHost()
          .equals(baseHost)) {
        boolean connAddedInKac = kac.add(conn);
        if (connAddedInKac) {
          synchronized (connectionLock) {
            connectionLock.notify(); // wake up one thread only
          }
          LOG.debug("Connection cached: {}", conn);
        } else {
          LOG.debug("Connection not cached, and is released: {}", conn);
        }
      }
    }
  }

  /**
   * Calculates the elapsed time in milliseconds since the given start time.
   *
   * @param startTime the start time in nanoseconds
   * @return the elapsed time in milliseconds
   */
  private static long elapsedTimeMillis(long startTime) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
  }
}
