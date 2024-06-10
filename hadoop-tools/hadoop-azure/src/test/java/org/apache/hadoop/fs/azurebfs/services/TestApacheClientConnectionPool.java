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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;

import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS;

public class TestApacheClientConnectionPool extends
    AbstractAbfsTestWithTimeout {

  public TestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Test
  public void testBasicPool() throws Exception {
    System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    validatePoolSize(DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS);
  }

  @Test
  public void testSysPropAppliedPool() throws Exception {
    final String customPoolSize = "10";
    System.setProperty(HTTP_MAX_CONN_SYS_PROP, customPoolSize);
    validatePoolSize(Integer.parseInt(customPoolSize));
  }

  private void validatePoolSize(int size) throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), ""))) {
      keepAliveCache.clear();
      final HttpClientConnection[] connections = new HttpClientConnection[size
          * 2];

      for (int i = 0; i < size * 2; i++) {
        connections[i] = Mockito.mock(HttpClientConnection.class);
      }

      for (int i = 0; i < size * 2; i++) {
        keepAliveCache.put(connections[i]);
      }

      for (int i = size; i < size * 2; i++) {
        Mockito.verify(connections[i], Mockito.times(1)).close();
      }

      for (int i = 0; i < size * 2; i++) {
        if (i < size) {
          Assert.assertNotNull(keepAliveCache.get());
        } else {
          Assert.assertNull(keepAliveCache.get());
        }
      }
      System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    }
  }

  @Test
  public void testKeepAliveCache() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), ""))) {
      keepAliveCache.clear();
      final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);

      keepAliveCache.put(connection);

      Assert.assertNotNull(keepAliveCache.get());
      keepAliveCache.put(connection);
    }
  }

  @Test
  public void testKeepAliveCacheCleanup() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), ""))) {
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);
      keepAliveCache.put(connection);

      Thread.sleep(2 * keepAliveCache.getConnectionIdleTTL());
      Mockito.verify(connection, Mockito.times(1)).close();
      Assert.assertNull(keepAliveCache.get());
      Mockito.verify(connection, Mockito.times(1)).close();
    }
  }

  @Test
  public void testKeepAliveCacheCleanupWithConnections() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), ""))) {
      keepAliveCache.pauseThread();
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);
      keepAliveCache.put(connection);

      Thread.sleep(2 * keepAliveCache.getConnectionIdleTTL());
      Mockito.verify(connection, Mockito.times(0)).close();
      Assert.assertNull(keepAliveCache.get());
      Mockito.verify(connection, Mockito.times(1)).close();
      keepAliveCache.resumeThread();
    }
  }

  @Test
  public void testKeepAliveCacheConnectionRecache() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), ""))) {
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);
      keepAliveCache.put(connection);

      Assert.assertNotNull(keepAliveCache.get());
      keepAliveCache.put(connection);
      Assert.assertNotNull(keepAliveCache.get());
    }
  }

  @Test
  public void testKeepAliveCacheRemoveStaleConnection() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), ""))) {
      keepAliveCache.clear();
      HttpClientConnection[] connections = new HttpClientConnection[5];

      // Fill up the cache.
      for (int i = 0;
          i < DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS;
          i++) {
        connections[i] = Mockito.mock(HttpClientConnection.class);
        keepAliveCache.put(connections[i]);
      }

      // Mark all but the last two connections as stale.
      for (int i = 0;
          i < DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS - 2;
          i++) {
        Mockito.doReturn(true).when(connections[i]).isStale();
      }

      // Verify that the stale connections are removed.
      for (int i = DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS - 1;
          i >= 0;
          i--) {
        // The last two connections are not stale and would be returned.
        if (i >= (DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS - 2)) {
          Assert.assertNotNull(keepAliveCache.get());
        } else {
          // Stale connections are closed and removed.
          Assert.assertNull(keepAliveCache.get());
          Mockito.verify(connections[i], Mockito.times(1)).close();
        }
      }
    }
  }
}
