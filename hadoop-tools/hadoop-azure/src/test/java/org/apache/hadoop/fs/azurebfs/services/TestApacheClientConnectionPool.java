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
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;

import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APACHE_HTTP_CLIENT_IDLE_CONNECTION_TTL;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_CONNECTION_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_HTTP_CLIENT_CONN_MAX_IDLE_TIME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestApacheClientConnectionPool extends
    AbstractAbfsTestWithTimeout {

  public TestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Override
  protected int getTestTimeoutMillis() {
    return (int) DEFAULT_HTTP_CLIENT_CONN_MAX_IDLE_TIME * 4;
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

  @Test
  public void testPoolWithZeroSysProp() throws Exception {
    final String customPoolSize = "0";
    System.setProperty(HTTP_MAX_CONN_SYS_PROP, customPoolSize);
    validatePoolSize(DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS);
  }

  @Test
  public void testEmptySizePool() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_CONNECTION_SIZE,
        "0");
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        EMPTY_STRING);
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        abfsConfiguration)) {
      assertCachePutFail(keepAliveCache,
          Mockito.mock(HttpClientConnection.class));
      assertCacheGetIsNull(keepAliveCache);
    }
  }

  private void assertCacheGetIsNull(final KeepAliveCache keepAliveCache)
      throws IOException {
    Assertions.assertThat(keepAliveCache.get())
        .describedAs("cache.get()")
        .isNull();
  }

  private void assertCacheGetIsNonNull(final KeepAliveCache keepAliveCache)
      throws IOException {
    Assertions.assertThat(keepAliveCache.get())
        .describedAs("cache.get()")
        .isNotNull();
  }

  private void assertCachePutFail(final KeepAliveCache keepAliveCache,
      final HttpClientConnection mock) {
    Assertions.assertThat(keepAliveCache.put(mock))
        .describedAs("cache.put()")
        .isFalse();
  }

  private void assertCachePutSuccess(final KeepAliveCache keepAliveCache,
      final HttpClientConnection connections) {
    Assertions.assertThat(keepAliveCache.put(connections))
        .describedAs("cache.put()")
        .isTrue();
  }

  private void validatePoolSize(int size) throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      final HttpClientConnection[] connections = new HttpClientConnection[size
          * 2];

      for (int i = 0; i < size * 2; i++) {
        connections[i] = Mockito.mock(HttpClientConnection.class);
      }

      for (int i = 0; i < size; i++) {
        assertCachePutSuccess(keepAliveCache, connections[i]);
        Mockito.verify(connections[i], Mockito.times(0)).close();
      }

      for (int i = size; i < size * 2; i++) {
        assertCachePutSuccess(keepAliveCache, connections[i]);
        Mockito.verify(connections[i - size], Mockito.times(1)).close();
      }

      for (int i = 0; i < size * 2; i++) {
        if (i < size) {
          assertCacheGetIsNonNull(keepAliveCache);
        } else {
          assertCacheGetIsNull(keepAliveCache);
        }
      }
      System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    }
  }

  @Test
  public void testKeepAliveCache() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);

      keepAliveCache.put(connection);

      assertCacheGetIsNonNull(keepAliveCache);
    }
  }

  @Test
  public void testKeepAliveCacheCleanup() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(FS_AZURE_APACHE_HTTP_CLIENT_IDLE_CONNECTION_TTL,
        HUNDRED + EMPTY_STRING);
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(configuration, EMPTY_STRING))) {
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);


      // Eviction thread would close the TTL-elapsed connection and remove it from cache.
      AtomicBoolean isConnClosed = new AtomicBoolean(false);
      Mockito.doAnswer(closeInvocation -> {
        isConnClosed.set(true);
        return null;
      }).when(connection).close();
      keepAliveCache.put(connection);

      while (!isConnClosed.get()) {
        Thread.sleep(HUNDRED);
      }

      // Assert that the closed connection is removed from the cache.
      assertCacheGetIsNull(keepAliveCache);
      Mockito.verify(connection, Mockito.times(1)).close();
    }
  }

  @Test
  public void testKeepAliveCacheCleanupWithConnections() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(FS_AZURE_APACHE_HTTP_CLIENT_IDLE_CONNECTION_TTL,
        HUNDRED + EMPTY_STRING);
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(configuration, EMPTY_STRING))) {
      keepAliveCache.pauseThread();
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);
      keepAliveCache.put(connection);

      Thread.sleep(2 * keepAliveCache.getConnectionIdleTTL());
      /*
       * Eviction thread is switched off, the get() on the cache would close and
       * remove the TTL-elapsed connection.
       */
      Mockito.verify(connection, Mockito.times(0)).close();
      assertCacheGetIsNull(keepAliveCache);
      Mockito.verify(connection, Mockito.times(1)).close();
      keepAliveCache.resumeThread();
    }
  }

  @Test
  public void testKeepAliveCacheConnectionRecache() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);
      keepAliveCache.put(connection);

      assertCacheGetIsNonNull(keepAliveCache);
      keepAliveCache.put(connection);
      assertCacheGetIsNonNull(keepAliveCache);
    }
  }

  @Test
  public void testKeepAliveCacheRemoveStaleConnection() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
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
          assertCacheGetIsNonNull(keepAliveCache);
        } else {
          // Stale connections are closed and removed.
          assertCacheGetIsNull(keepAliveCache);
          Mockito.verify(connections[i], Mockito.times(1)).close();
        }
      }
    }
  }

  @Test
  public void testKeepAliveCacheClosed() throws Exception {
    KeepAliveCache keepAliveCache = Mockito.spy(new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING)));
    keepAliveCache.put(Mockito.mock(HttpClientConnection.class));
    keepAliveCache.close();
    intercept(ClosedIOException.class,
        KEEP_ALIVE_CACHE_CLOSED,
        () -> keepAliveCache.get());

    HttpClientConnection conn = Mockito.mock(HttpClientConnection.class);
    assertCachePutFail(keepAliveCache, conn);
    Mockito.verify(conn, Mockito.times(1)).close();
    keepAliveCache.close();
    Mockito.verify(keepAliveCache, Mockito.times(1)).closeInternal();
  }
}
