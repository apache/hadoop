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
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APACHE_HTTP_CLIENT_MAX_DEFAULT_CACHE_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APACHE_HTTP_CLIENT_MAX_NON_DEFAULT_CACHE_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests for the {@link KeepAliveCache} connection pool behavior in Azure Blob File System.
 * <p>
 * These tests verify correct handling of connection pool sizing, cleanup, eviction, stale connection removal,
 * closed cache behavior, and cluster/global capacity isolation for Apache HTTP client connections.
 */
public class TestApacheClientConnectionPool
    extends AbstractAbfsTestWithTimeout {

  private static final String TEST_HOST = "test.blob.core.windows.net";

  public TestApacheClientConnectionPool() throws Exception {
    super();
  }

  /**
   * Tests that the KeepAliveCache uses the default max cache size when not configured.
   * Verifies that put and get operations succeed for a valid connection.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testPoolSizeWithNotConfigured() throws Exception {
    Configuration configuration = new Configuration();
    configuration.unset(FS_AZURE_APACHE_HTTP_CLIENT_MAX_DEFAULT_CACHE_SIZE);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        EMPTY_STRING);

    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        abfsConfiguration)) {
      Assertions.assertThat(keepAliveCache.getMaxCacheConnections())
          .isEqualTo(DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE);

      assertCachePutSuccess(keepAliveCache, getValidMockConnection(TEST_HOST),
          true);
      assertCacheGetIsNonNull(keepAliveCache, true);
    }
  }

  /**
   * Tests that the KeepAliveCache uses the minimum cache size when configured to zero.
   * Verifies that put and get operations succeed for a valid connection.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testEmptySizePool() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(FS_AZURE_APACHE_HTTP_CLIENT_MAX_DEFAULT_CACHE_SIZE, "0");
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        EMPTY_STRING);

    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        abfsConfiguration)) {
      Assertions.assertThat(keepAliveCache.getMaxCacheConnections())
          .isEqualTo(MIN_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE);

      assertCachePutSuccess(keepAliveCache, getValidMockConnection(TEST_HOST),
          true);
      assertCacheGetIsNonNull(keepAliveCache, true);
    }
  }

  /**
   * Tests basic put and get operations for the KeepAliveCache.
   * Verifies that a valid connection can be stored and retrieved.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testKeepAliveCache() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {

      keepAliveCache.put(getValidMockConnection(TEST_HOST), true);
      assertCacheGetIsNonNull(keepAliveCache, true);
    }
  }

  /**
   * Tests cleanup of KeepAliveCache when connections become invalid.
   * Verifies that get returns null for closed connections and that invalid connections are closed.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testKeepAliveCacheCleanupWithConnections() throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {

      HttpClientConnection connection = getValidMockConnection(TEST_HOST);
      keepAliveCache.put(connection, true);

      Mockito.doReturn(false).when(connection).isOpen();
      assertCacheGetIsNull(keepAliveCache, true);

      // Lock-free get() closes invalid connections immediately
      Mockito.verify(connection, Mockito.atLeastOnce()).close();
    }
  }

  /**
   * Tests removal of stale connections from the KeepAliveCache.
   * Verifies that healthy connections are returned first, and stale connections are closed and not returned.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testKeepAliveCacheRemoveStaleConnection() throws Exception {
    int max = DEFAULT_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE;
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {

      HttpClientConnection[] connections = new HttpClientConnection[max];

      for (int i = 0; i < max; i++) {
        connections[i] = getValidMockConnection(TEST_HOST);
        keepAliveCache.put(connections[i], true);
      }

      // Mark all but last 2 as stale
      for (int i = 0; i < max - 2; i++) {
        Mockito.doReturn(true).when(connections[i]).isStale();
      }

      // Verify healthy ones are returned first
      assertCacheGetIsNonNull(keepAliveCache, true);
      assertCacheGetIsNonNull(keepAliveCache, true);

      // Next gets should return null because remaining are stale
      assertCacheGetIsNull(keepAliveCache, true);
      Mockito.verify(connections[0], Mockito.atLeastOnce()).close();
    }
  }

  /**
   * Tests KeepAliveCache behavior after being closed.
   * Verifies that get throws ClosedIOException and put fails, closing the connection.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testKeepAliveCacheClosed() throws Exception {
    KeepAliveCache keepAliveCache = Mockito.spy(new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING)));

    keepAliveCache.close();

    intercept(ClosedIOException.class, KEEP_ALIVE_CACHE_CLOSED,
        () -> keepAliveCache.get(TEST_HOST, true));

    HttpClientConnection conn = Mockito.mock(HttpClientConnection.class);
    assertCachePutFail(keepAliveCache, conn, true);
    Mockito.verify(conn, Mockito.times(1)).close();
  }

  /**
   * Tests KeepAliveCache close operation with multiple connections.
   * Verifies that all connections are closed and the cache size returns to zero.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testKeepAliveCacheCloseWithMultipleConnections()
      throws Exception {
    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(new Configuration(), EMPTY_STRING))) {

      HttpClientConnection[] connections = new HttpClientConnection[10];
      for (int i = 0; i < connections.length; i++) {
        connections[i] = getValidMockConnection(TEST_HOST);
        keepAliveCache.put(connections[i], true);
      }

      keepAliveCache.close();

      for (HttpClientConnection connection : connections) {
        Mockito.verify(connection, Mockito.atLeastOnce()).close();
      }

      Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
          .isEqualTo(0);
    }
  }

  /**
   * Tests that KeepAliveCache enforces the max size limit and evicts the oldest connection when exceeded.
   * Verifies that the oldest connection is closed upon eviction.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testKeepAliveCacheMaxSizeLimit() throws Exception {
    Configuration conf = new Configuration();
    int maxSize = 5;
    conf.setInt(FS_AZURE_APACHE_HTTP_CLIENT_MAX_DEFAULT_CACHE_SIZE, maxSize);

    try (KeepAliveCache keepAliveCache = new KeepAliveCache(
        new AbfsConfiguration(conf, EMPTY_STRING))) {
      HttpClientConnection[] connections = new HttpClientConnection[maxSize
          + 1];

      for (int i = 0; i < connections.length; i++) {
        connections[i] = getValidMockConnection(TEST_HOST);
        // Mock target host for cluster logic if necessary, or use default
        keepAliveCache.put(connections[i], true);
      }

      Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
          .isEqualTo(maxSize);

      // In lock-free put, the oldest is evicted and closed when capacity is exceeded
      Mockito.verify(connections[0], Mockito.atLeastOnce()).close();
    }
  }

  /**
   * Tests cluster/global capacity and host isolation in KeepAliveCache.
   * Verifies that connections for different hosts are managed independently and eviction occurs as expected.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testClusterGlobalCapacityAndHostIsolation() throws Exception {
    Configuration conf = new Configuration();
    int maxCluster = 5;
    conf.setInt(FS_AZURE_APACHE_HTTP_CLIENT_MAX_NON_DEFAULT_CACHE_SIZE,
        maxCluster);

    try (KeepAliveCache cache = new KeepAliveCache(
        new AbfsConfiguration(conf, EMPTY_STRING))) {
      String hostA = "account1.dfs.core.windows.net";
      String hostB = "account2.dfs.core.windows.net";

      // 1. Fill Host A to capacity
      for (int i = 0; i < maxCluster; i++) {
        cache.put(getValidMockConnection(hostA), false);
        // Note: isDefault = false triggers cluster logic
      }

      Assertions.assertThat(cache.getClusterConnectionsSize())
          .describedAs("Cluster size should be at max")
          .isEqualTo(maxCluster);

      // 2. Put a connection for Host B (triggers eviction of oldest Host A connection)
      HttpClientConnection hostBConn = getValidMockConnection(hostB);
      cache.put(hostBConn, false);

      Assertions.assertThat(cache.getClusterConnectionsSize())
          .isEqualTo(maxCluster);

      // 3. Verify Host B get works, and Host A has lost one
      Assertions.assertThat(cache.get(hostB, false))
          .describedAs("Should retrieve the connection specifically for Host B")
          .isEqualTo(hostBConn);
    }
  }

  private HttpClientConnection getValidMockConnection(String host) {
    AbfsManagedApacheHttpConnection mockConn = Mockito.mock(
        AbfsManagedApacheHttpConnection.class);
    Mockito.when(mockConn.isOpen()).thenReturn(true);
    Mockito.when(mockConn.isStale()).thenReturn(false);
    HttpHost httpHost = new HttpHost(host);
    Mockito.when(mockConn.getTargetHost()).thenReturn(httpHost);
    return mockConn;
  }

  private void assertCacheGetIsNull(KeepAliveCache keepAliveCache,
      boolean isDefault) throws IOException {
    Assertions.assertThat(keepAliveCache.get(TEST_HOST, isDefault))
        .isNull();
  }

  private void assertCacheGetIsNonNull(KeepAliveCache keepAliveCache,
      boolean isDefault) throws IOException {
    Assertions.assertThat(keepAliveCache.get(TEST_HOST, isDefault))
        .isNotNull();
  }

  private void assertCachePutFail(KeepAliveCache keepAliveCache,
      HttpClientConnection mock,
      boolean isDefault) {
    Assertions.assertThat(keepAliveCache.put(mock, isDefault))
        .isFalse();
  }

  private void assertCachePutSuccess(KeepAliveCache keepAliveCache,
      HttpClientConnection connections,
      boolean isDefault) {
    Assertions.assertThat(keepAliveCache.put(connections, isDefault))
        .isTrue();
  }
}
