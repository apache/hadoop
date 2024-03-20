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
import java.util.HashMap;
import java.util.Map;
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
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.DefaultHttpClientConnectionOperator;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Asserts;

public class AbfsConnectionManager implements HttpClientConnectionManager {

  KeepAliveCache kac = KeepAliveCache.INSTANCE;

  private final Registry<ConnectionSocketFactory> socketFactoryRegistry;
  private final ManagedHttpClientConnectionFactory
      httpConnectionFactory;

  private final HttpClientConnectionOperator connectionOperator;


  private final Map<HttpClientConnection, HttpRoute> map = new HashMap<>();

  private ManagedHttpClientConnection managedHttpClientConnection;

  public AbfsConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry,
      ManagedHttpClientConnectionFactory connectionFactory) {
    this.socketFactoryRegistry = socketFactoryRegistry;
    this.httpConnectionFactory = connectionFactory;
    connectionOperator = new DefaultHttpClientConnectionOperator(socketFactoryRegistry, null, null);
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
          if(client != null && client.isOpen()) {
             return  client;
          }
          managedHttpClientConnection = httpConnectionFactory.create(route, null);
          return managedHttpClientConnection;
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

  @Override
  public void releaseConnection(final HttpClientConnection conn,
      final Object newState,
      final long validDuration,
      final TimeUnit timeUnit) {
    if(validDuration == 0) {
      return;
    }
    if(conn.isOpen() && conn instanceof AbfsManagedApacheHttpConnection) {
      HttpRoute route = ((AbfsManagedApacheHttpConnection) conn).httpRoute;
      if(route != null) {
        kac.put(route, conn);
      }
    }
  }

  @Override
  public void connect(final HttpClientConnection conn,
      final HttpRoute route,
      final int connectTimeout,
      final HttpContext context) throws IOException {
    Asserts.check(conn == this.managedHttpClientConnection, "Connection not obtained from this manager");
    long start = System.currentTimeMillis();
    connectionOperator.connect(managedHttpClientConnection, route.getTargetHost(), route.getLocalSocketAddress(),
        connectTimeout, SocketConfig.DEFAULT, context);
    if(context instanceof AbfsManagedHttpContext) {
      ((AbfsManagedHttpContext)context).connectTime = (System.currentTimeMillis() - start);
    }
  }

  @Override
  public void upgrade(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {
    Asserts.check(conn == this.managedHttpClientConnection, "Connection not obtained from this manager");
    connectionOperator.upgrade(managedHttpClientConnection, route.getTargetHost(), context);
  }

  @Override
  public void routeComplete(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {
    Asserts.check(conn == this.managedHttpClientConnection, "Connection not obtained from this manager");

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
