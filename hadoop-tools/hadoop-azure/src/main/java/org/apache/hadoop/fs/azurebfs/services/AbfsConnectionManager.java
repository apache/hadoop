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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.azurebfs.services.kac.KeepAliveCache;
import org.apache.http.HttpClientConnection;
import org.apache.http.config.Registry;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpClientConnectionOperator;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.DefaultHttpClientConnectionOperator;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Asserts;

/**
 * AbfsConnectionManager is a custom implementation of {@link HttpClientConnectionManager}.
 * This implementation manages connection-pooling heuristics and custom implementation
 * of {@link ManagedHttpClientConnectionFactory}.
 */
public class AbfsConnectionManager implements HttpClientConnectionManager {

  private final KeepAliveCache kac = KeepAliveCache.getInstance();

  private final AbfsConnFactory httpConnectionFactory;

  private final HttpClientConnectionOperator connectionOperator;

  public AbfsConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry,
      AbfsConnFactory connectionFactory) {
    this.httpConnectionFactory = connectionFactory;
    connectionOperator = new DefaultHttpClientConnectionOperator(
        socketFactoryRegistry, null, null);
  }

  @Override
  public ConnectionRequest requestConnection(final HttpRoute route,
      final Object state) {
    return new ConnectionRequest() {
      @Override
      public HttpClientConnection get(final long timeout,
          final TimeUnit timeUnit)
          throws InterruptedException, ExecutionException,
          ConnectionPoolTimeoutException {
        try {
          HttpClientConnection client = kac.get(route);
          if (client != null && client.isOpen()) {
            return client;
          }
          return httpConnectionFactory.create(route, null);
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
      HttpRoute route = ((AbfsManagedApacheHttpConnection) conn).getHttpRoute();
      if (route != null) {
        kac.put(route, conn);
      }
    }
  }

  @Override
  public void connect(final HttpClientConnection conn,
      final HttpRoute route,
      final int connectTimeout,
      final HttpContext context) throws IOException {
    Asserts.check(conn instanceof AbfsManagedApacheHttpConnection,
        "Connection not obtained from this manager");
    long start = System.currentTimeMillis();
    connectionOperator.connect((AbfsManagedApacheHttpConnection) conn,
        route.getTargetHost(), route.getLocalSocketAddress(),
        connectTimeout, SocketConfig.DEFAULT, context);
    if (context instanceof AbfsManagedHttpContext) {
      ((AbfsManagedHttpContext) context).setConnectTime(
          System.currentTimeMillis() - start);
    }
  }

  @Override
  public void upgrade(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {
    Asserts.check(conn instanceof AbfsManagedApacheHttpConnection,
        "Connection not obtained from this manager");
    connectionOperator.upgrade((AbfsManagedApacheHttpConnection) conn,
        route.getTargetHost(), context);
  }

  @Override
  public void routeComplete(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {
    Asserts.check(conn instanceof AbfsManagedApacheHttpConnection,
        "Connection not obtained from this manager");
  }

  @Override
  public void closeIdleConnections(final long idletime,
      final TimeUnit timeUnit) {

  }

  @Override
  public void closeExpiredConnections() {

  }

  @Override
  public void shutdown() {

  }
}
