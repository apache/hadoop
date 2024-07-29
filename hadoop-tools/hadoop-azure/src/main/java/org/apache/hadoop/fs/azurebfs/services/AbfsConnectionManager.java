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
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpClientConnection;
import org.apache.http.config.Registry;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpClientConnectionOperator;
import org.apache.http.conn.ManagedHttpClientConnection;
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

  AbfsConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry,
      AbfsHttpClientConnectionFactory connectionFactory, KeepAliveCache kac) {
    this.httpConnectionFactory = connectionFactory;
    this.kac = kac;
    this.connectionOperator = new DefaultHttpClientConnectionOperator(
        socketFactoryRegistry, null, null);
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
          final TimeUnit timeUnit)
          throws InterruptedException, ExecutionException,
          ConnectionPoolTimeoutException {
        String requestId = UUID.randomUUID().toString();
        logDebug("Connection requested for request {}", requestId);
        try {
          HttpClientConnection clientConn = kac.get();
          if (clientConn != null) {
            logDebug("Connection retrieved from KAC: {} for requestId: {}",
                clientConn, requestId);
            return clientConn;
          }
          logDebug("Creating new connection for requestId: {}", requestId);
          ManagedHttpClientConnection conn = httpConnectionFactory.create(route,
              null);
          logDebug("Connection created: {} for requestId: {}", conn, requestId);
          return conn;
        } catch (IOException ex) {
          throw new ExecutionException(ex);
        }
      }

      @Override
      public boolean cancel() {
        return false;
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
    if (conn.isOpen() && conn instanceof AbfsManagedApacheHttpConnection) {
      boolean connAddedInKac = kac.put(conn);
      if (connAddedInKac) {
        logDebug("Connection cached: {}", conn);
      } else {
        logDebug("Connection not cached, and is released: {}", conn);
      }
    }
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
    kac.evictIdleConnection();
  }

  /**{@inheritDoc}*/
  @Override
  public void closeExpiredConnections() {
    kac.evictIdleConnection();
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
}
