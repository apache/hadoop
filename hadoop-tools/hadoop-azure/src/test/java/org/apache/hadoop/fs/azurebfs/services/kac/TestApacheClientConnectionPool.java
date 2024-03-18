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

package org.apache.hadoop.fs.azurebfs.services.kac;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KAC_CONN_TTL;

public class TestApacheClientConnectionPool extends
    AbstractAbfsTestWithTimeout {

  public TestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Test
  public void testBasicPool() throws IOException {
    System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    validatePoolSize(DEFAULT_MAX_CONN_SYS_PROP);
  }

  @Test
  public void testSysPropAppliedPool() throws IOException {
    final String customPoolSize = "10";
    System.setProperty(HTTP_MAX_CONN_SYS_PROP, customPoolSize);
    validatePoolSize(Integer.parseInt(customPoolSize));
  }

  private void validatePoolSize(int size) throws IOException {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    final HttpClientConnection[] connections = new HttpClientConnection[size * 2];

    for (int i = 0; i < size * 2; i++) {
      connections[i] = Mockito.mock(HttpClientConnection.class);
    }

    for (int i = 0; i < size * 2; i++) {
      keepAliveCache.put(routes, connections[i]);
    }

    for (int i = size; i < size * 2; i++) {
      Mockito.verify(connections[i], Mockito.times(1)).close();
    }

    for (int i = 0; i < size * 2; i++) {
      if (i < size) {
        Assert.assertNotNull(keepAliveCache.get(routes));
      } else {
        Assert.assertNull(keepAliveCache.get(routes));
      }
    }
    System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    keepAliveCache.close();
  }

  @Test
  public void testKeepAliveCache() throws IOException {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);

    keepAliveCache.put(routes, connection);

    Assert.assertNotNull(keepAliveCache.get(routes));
    keepAliveCache.put(routes, connection);

    final HttpRoute routes1 = new HttpRoute(new HttpHost("localhost1"));
    Assert.assertNull(keepAliveCache.get(routes1));
    keepAliveCache.close();
  }

  @Test
  public void testKeepAliveCacheCleanup() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    keepAliveCache.put(routes, connection);

    Thread.sleep(2 * KAC_CONN_TTL);
    Mockito.verify(connection, Mockito.times(1)).close();
    Assert.assertNull(keepAliveCache.get(routes));
    Mockito.verify(connection, Mockito.times(1)).close();
    keepAliveCache.close();
  }

  @Test
  public void testKeepAliveCacheCleanupWithConnections() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    keepAliveCache.pauseThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    keepAliveCache.put(routes, connection);

    Thread.sleep(2 * KAC_CONN_TTL);
    Mockito.verify(connection, Mockito.times(0)).close();
    Assert.assertNull(keepAliveCache.get(routes));
    Mockito.verify(connection, Mockito.times(1)).close();
    keepAliveCache.close();
  }
}

