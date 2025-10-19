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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestApacheClientConnectionPool extends
    AbstractAbfsTestWithTimeout {

  public TestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Test
  public void testPoolSizeWithNotConfigured() throws Exception {
    Configuration configuration = new Configuration();
    configuration.unset(FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        EMPTY_STRING);
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        abfsConfiguration)) {
      Assertions.assertThat(keepAliveCache.getMaxCacheConnections())
          .describedAs("In case configured cache size is 0, "
              + "the pool size should be minimum possible value")
          .isEqualTo(DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE);

      assertCachePutSuccess(keepAliveCache, getValidMockConnection());
      assertCacheGetIsNonNull(keepAliveCache);
    }
  }

  @Test
  public void testEmptySizePool() throws Exception {
    Configuration configuration = new Configuration();
    // In case the max cache size is set to 0,
    // the pool will set the sze to minimum possible value (which is 5).
    configuration.set(FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE, "0");
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        EMPTY_STRING);
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        abfsConfiguration)) {
      Assertions.assertThat(keepAliveCache.getMaxCacheConnections())
          .describedAs("In case configured cache size is 0, "
              + "the pool size should be minimum possible value")
          .isEqualTo(MIN_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE);

      assertCachePutSuccess(keepAliveCache, getValidMockConnection());
      assertCacheGetIsNonNull(keepAliveCache);
    }
  }

  private HttpClientConnection getValidMockConnection() {
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    Mockito.doReturn(true).when(connection).isOpen();
    return connection;
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
    Assertions.assertThat(keepAliveCache.add(mock))
        .describedAs("cache.put()")
        .isFalse();
  }

  private void assertCachePutSuccess(final KeepAliveCache keepAliveCache,
      final HttpClientConnection connections) {
    Assertions.assertThat(keepAliveCache.add(connections))
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
        connections[i] = getValidMockConnection();
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
    }
  }

  @Test
  public void testKeepAliveCache() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();

      keepAliveCache.add(getValidMockConnection());

      assertCacheGetIsNonNull(keepAliveCache);
    }
  }

  @Test
  public void testKeepAliveCacheCleanupWithConnections() throws Exception {
    Configuration configuration = new Configuration();
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(configuration, EMPTY_STRING))) {
      keepAliveCache.clear();
      HttpClientConnection connection = getValidMockConnection();
      keepAliveCache.add(connection);
      Mockito.verify(connection, Mockito.times(0)).close();
      Mockito.doReturn(false).when(connection).isOpen();
      assertCacheGetIsNull(keepAliveCache);
      Mockito.verify(connection, Mockito.times(1)).close();
    }
  }

  @Test
  public void testKeepAliveCacheConnectionRecache() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      HttpClientConnection connection = Mockito.mock(
          HttpClientConnection.class);
      Mockito.doReturn(true).when(connection).isOpen();
      keepAliveCache.add(connection);

      assertCacheGetIsNonNull(keepAliveCache);
      keepAliveCache.add(connection);
      assertCacheGetIsNonNull(keepAliveCache);
    }
  }

  @Test
  public void testKeepAliveCacheRemoveStaleConnection() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      HttpClientConnection[] connections =
          new HttpClientConnection[DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE];

      // Fill up the cache.
      for (int i = 0;
          i < DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE;
          i++) {
        connections[i] = getValidMockConnection();
        keepAliveCache.add(connections[i]);
      }

      // Mark all but the last two connections as stale.
      for (int i = 0;
          i < DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE - 2;
          i++) {
        Mockito.doReturn(true).when(connections[i]).isStale();
      }

      // Verify that the stale connections are removed.
      for (int i = DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE - 1;
          i >= 0;
          i--) {
        // The last two connections are not stale and would be returned.
        if (i >= (DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE - 2)) {
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
    keepAliveCache.add(Mockito.mock(HttpClientConnection.class));
    keepAliveCache.close();
    intercept(ClosedIOException.class, KEEP_ALIVE_CACHE_CLOSED, keepAliveCache::get);
    HttpClientConnection conn = Mockito.mock(HttpClientConnection.class);
    assertCachePutFail(keepAliveCache, conn);
    Mockito.verify(conn, Mockito.times(1)).close();
    keepAliveCache.close();
    Mockito.verify(keepAliveCache, Mockito.times(1)).closeInternal();
  }

  /**
   * Tests that the KeepAliveCache removes stale connections when adding a new
   * connection.
   */
  @Test
  public void keepAliveCacheShouldRemoveStaleConnectionsOnAdd() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      // This will ensure that the connection is open
      HttpClientConnection staleConnection = getValidMockConnection();
      Mockito.doReturn(true).when(staleConnection).isStale();
      keepAliveCache.add(staleConnection);
      Assertions.assertThat(keepAliveCache.get())
          .describedAs(
              "Getting from cache after adding stale connection should return null")
          .isNull();
      Mockito.verify(staleConnection, Mockito.times(1)).close();
    }
  }

  /**
   * Tests that the KeepAliveCache can be closed multiple times without throwing
   * an exception.
   */
  @Test
  public void keepAliveCacheClosedTwiceShouldNotThrowException() throws Exception {
    KeepAliveCache keepAliveCache = Mockito.spy(new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING)));
    keepAliveCache.close();
    keepAliveCache.close();
    // Verify that closeInternal is called only once even if close is called multiple times.
    Mockito.verify(keepAliveCache, Mockito.times(1)).closeInternal();
  }

  /**
   * Tests that the KeepAliveCache can handle null connections gracefully.
   */
  @Test
  public void testKeepAliveCacheShouldHandleNullConnectionsGracefully() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      Assertions.assertThat(keepAliveCache.add(null))
          .describedAs("Adding null connection should return false")
          .isFalse();
      Assertions.assertThat(keepAliveCache.get())
          .describedAs(
              "Getting from cache with no valid connections should return null")
          .isNull();
    }
  }

  /**
   * Tests that the KeepAliveCache does not add closed connections.
   * Closed connections should be cleaned up and not added to the cache.
   */
  @Test
  public void testKeepAliveCacheShouldNotAddClosedConnections() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      // This will ensure that the connection is open.
      HttpClientConnection connection = getValidMockConnection();
      Mockito.doReturn(false).when(connection).isOpen();
      Assertions.assertThat(keepAliveCache.add(connection))
          .describedAs("Adding closed connection should return false")
          .isFalse();
      Mockito.verify(connection, Mockito.times(1)).close();
    }
  }

  /**
   * Tests that the KeepAliveCache closes all connections when closed.
   * This is to ensure that all connections are properly cleaned up.
   */
  @Test
  public void testKeepAliveCacheCloseWithMultipleConnections() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();
      HttpClientConnection[] connections = new HttpClientConnection[10];

      // Add multiple connections to the cache.
      for (int i = 0; i < connections.length; i++) {
        connections[i] = getValidMockConnection();
        keepAliveCache.add(connections[i]);
      }

      Assertions.assertThat(keepAliveCache.getSingleThreadPool().isShutdown())
          .describedAs("singleThreadPool should not be shutdown")
          .isFalse();
      Assertions.assertThat(keepAliveCache.getFixedThreadPool().isShutdown())
          .describedAs("fixedThreadPool should not be shutdown")
          .isFalse();

      // Close the cache and verify all connections are closed.
      keepAliveCache.close();
      for (HttpClientConnection connection : connections) {
        Mockito.verify(connection, Mockito.times(1)).close();
      }

      // Verify the cache size is 0.
      Assertions.assertThat(keepAliveCache.size())
          .describedAs("Cache should be empty after closing")
          .isEqualTo(0);

      // Attempt to get a connection after closing the cache.
      // This should throw a ClosedIOException.
      LambdaTestUtils.intercept(ClosedIOException.class,
          KEEP_ALIVE_CACHE_CLOSED, keepAliveCache::get);

      Assertions.assertThat(keepAliveCache.getSingleThreadPool().isShutdown())
          .describedAs("singleThreadPool should be shutdown after close()")
          .isTrue();
      Assertions.assertThat(keepAliveCache.getFixedThreadPool().isShutdown())
          .describedAs("fixedThreadPool should be shutdown after close()")
          .isTrue();
    }
  }

  /**
   * Tests that the KeepAliveCache handles stale connections correctly.
   * If a connection becomes stale, it should not be returned from the cache.
   */
  @Test
  public void testKeepAliveCacheStaleConnectionHandling() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();

      // Create a valid connection and add it to the cache.
      HttpClientConnection connection = getValidMockConnection();
      keepAliveCache.add(connection);

      // Verify size of the cache is 1.
      Assertions.assertThat(keepAliveCache.size())
          .describedAs("Cache size should be 1 after adding a connection")
          .isEqualTo(1);

      // Simulate the connection becoming stale.
      Mockito.doReturn(true).when(connection).isStale();

      // Verify that the stale connection is not returned.
      Assertions.assertThat(keepAliveCache.get())
          .describedAs("Getting from cache after connection becomes stale should return null")
          .isNull();

      // Verify that the stale connection is closed.
      Mockito.verify(connection, Mockito.times(1)).close();
    }
  }

  /**
   * Tests that the KeepAliveCache does not contain connections exceeding the maximum size.
   */
  @Test
  public void testKeepAliveCacheMaxSizeLimit() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {
      keepAliveCache.clear();

      // Add connections up to the maximum size.
      HttpClientConnection[] connections =
          new HttpClientConnection[DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE + 1];
      for (int i = 0; i < connections.length; i++) {
        connections[i] = getValidMockConnection();
        keepAliveCache.add(connections[i]);
      }

      // Verify that the cache size does not exceed the maximum size.
      Assertions.assertThat(keepAliveCache.size())
          .describedAs("Cache size should not exceed the maximum allowed size")
          .isEqualTo(DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE);

      // Verify that the oldest connection is closed when the cache exceeds the maximum size.
      Mockito.verify(connections[0], Mockito.times(1)).close();
    }
  }
}
