/*
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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;
import org.apache.hadoop.fs.s3a.impl.EncryptionS3ClientFactory;
import org.apache.hadoop.fs.s3a.impl.LazySharedThreadPoolHolder;
import org.apache.hadoop.test.GenericTestUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.s3a.Constants.AWS_CLIENT_SHARED_THREADPOOL_KEEPALIVE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CLIENT_SHARED_THREADPOOL_SIZE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.AWS_KMS_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_KMS_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_KMS_CLIENT_SHARED_THREADPOOL_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_STS_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_STS_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_STS_CLIENT_SHARED_THREADPOOL_SIZE;

/**
 * Tests for the shared scheduled executors in DefaultS3ClientFactory.
 */
public class TestSharedScheduledExecutor {

  @Test
  public void testLazyHolderDisabledByDefault() {
    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        "test.enabled", "test.size", "test.keepalive", "test-disabled");
    Configuration conf = new Configuration();
    ScheduledExecutorService executor = holder.get(conf);
    Assertions.assertThat(executor)
        .as("Executor should be null when disabled")
        .isNull();
  }

  @Test
  public void testCreateScheduledExecutorConfiguration() {
    ScheduledExecutorService executor =
        LazySharedThreadPoolHolder.createScheduledExecutor("test-scheduler", 10, 30);
    Assertions.assertThat(executor)
        .as("Executor should be a ScheduledThreadPoolExecutor")
        .isInstanceOf(ScheduledThreadPoolExecutor.class);

    ScheduledThreadPoolExecutor poolExecutor = (ScheduledThreadPoolExecutor) executor;
    Assertions.assertThat(poolExecutor.getCorePoolSize())
        .as("Core pool size should be 10")
        .isEqualTo(10);
    Assertions.assertThat(poolExecutor.getKeepAliveTime(TimeUnit.SECONDS))
        .as("Keepalive should be 30 seconds")
        .isEqualTo(30);
    Assertions.assertThat(poolExecutor.allowsCoreThreadTimeOut())
        .as("Core threads should be allowed to time out")
        .isTrue();

    executor.shutdown();
  }

  @Test
  public void testCreateScheduledExecutorThreadsAreDaemon() throws Exception {
    ScheduledExecutorService executor =
        LazySharedThreadPoolHolder.createScheduledExecutor("test-daemon", 5, 60);
    final boolean[] isDaemon = new boolean[1];
    executor.submit(() -> {
      isDaemon[0] = Thread.currentThread().isDaemon();
    }).get();
    Assertions.assertThat(isDaemon[0])
        .as("Executor threads should be daemon threads")
        .isTrue();
    executor.shutdown();
  }

  @Test
  public void testCreateScheduledExecutorThreadName() throws Exception {
    ScheduledExecutorService executor =
        LazySharedThreadPoolHolder.createScheduledExecutor("custom-prefix", 5, 60);
    final String[] threadName = new String[1];
    executor.submit(() -> {
      threadName[0] = Thread.currentThread().getName();
    }).get();
    Assertions.assertThat(threadName[0])
        .as("Thread name should match custom prefix")
        .startsWith("custom-prefix");
    executor.shutdown();
  }

  @Test
  public void testLazyHolderRejectsNegativePoolSize() {
    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        "test.enabled", "test.size", "test.keepalive", "test-pool");
    Configuration conf = new Configuration();
    conf.setBoolean("test.enabled", true);
    conf.setInt("test.size", -1);
    conf.setInt("test.keepalive", 60);
    Assertions.assertThatThrownBy(() -> holder.get(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("test.size")
        .hasMessageContaining("below the minimum value");
  }

  @Test
  public void testLazyHolderRejectsZeroPoolSize() {
    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        "test.enabled", "test.size", "test.keepalive", "test-pool");
    Configuration conf = new Configuration();
    conf.setBoolean("test.enabled", true);
    conf.setInt("test.size", 0);
    conf.setInt("test.keepalive", 60);
    Assertions.assertThatThrownBy(() -> holder.get(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("test.size")
        .hasMessageContaining("below the minimum value");
  }

  @Test
  public void testLazyHolderRejectsNegativeKeepAlive() {
    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        "test.enabled", "test.size", "test.keepalive", "test-pool");
    Configuration conf = new Configuration();
    conf.setBoolean("test.enabled", true);
    conf.setInt("test.size", 5);
    conf.setInt("test.keepalive", -1);
    Assertions.assertThatThrownBy(() -> holder.get(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("test.keepalive")
        .hasMessageContaining("below the minimum value");
  }

  @Test
  public void testLazyHolderRejectsZeroKeepAlive() {
    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        "test.enabled", "test.size", "test.keepalive", "test-pool");
    Configuration conf = new Configuration();
    conf.setBoolean("test.enabled", true);
    conf.setInt("test.size", 5);
    conf.setInt("test.keepalive", 0);
    Assertions.assertThatThrownBy(() -> holder.get(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("test.keepalive")
        .hasMessageContaining("below the minimum value");
  }

  @Test
  public void testLazyHolderAcceptsValidConfig() {
    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        "test.enabled", "test.size", "test.keepalive", "test-valid");
    Configuration conf = new Configuration();
    conf.setBoolean("test.enabled", true);
    conf.setInt("test.size", 5);
    conf.setInt("test.keepalive", 30);
    ScheduledExecutorService executor = holder.get(conf);
    Assertions.assertThat(executor)
        .as("Executor should be created with valid config")
        .isNotNull()
        .isInstanceOf(ScheduledThreadPoolExecutor.class);

    ScheduledThreadPoolExecutor poolExecutor =
        (ScheduledThreadPoolExecutor) executor;
    Assertions.assertThat(poolExecutor.getCorePoolSize())
        .as("Core pool size should match configured value")
        .isEqualTo(5);
    Assertions.assertThat(poolExecutor.getKeepAliveTime(TimeUnit.SECONDS))
        .as("Keepalive should match configured value")
        .isEqualTo(30);
    executor.shutdown();
  }

  /**
   * Count threads matching the given prefix.
   * @param prefix thread name prefix to match
   * @return count of matching threads
   */
  private int countThreadsWithPrefix(String prefix) {
    return GenericTestUtils.countThreadsMatching(
        Pattern.compile(Pattern.quote(prefix) + ".*"));
  }

  /**
   * Test that without shared pool, each holder creates its own threads.
   * This demonstrates the thread growth problem.
   */
  @Test
  public void testWithoutSharedPoolThreadsGrow() throws Exception {
    final String prefix = "test-growth-";
    final int poolSize = 3;
    final int numHolders = 5;
    List<ScheduledExecutorService> executors = new ArrayList<>();

    int initialCount = countThreadsWithPrefix(prefix);

    for (int i = 0; i < numHolders; i++) {
      ScheduledExecutorService executor =
          LazySharedThreadPoolHolder.createScheduledExecutor(prefix + i, poolSize, 60);
      executors.add(executor);
      executor.submit(() -> {}).get();
    }

    int afterCount = countThreadsWithPrefix(prefix);
    int newThreads = afterCount - initialCount;

    Assertions.assertThat(newThreads)
        .as("Without shared pool, thread count should grow with each executor")
        .isGreaterThanOrEqualTo(numHolders);

    for (ScheduledExecutorService executor : executors) {
      executor.shutdown();
    }
  }

  /**
   * Test that with shared pool enabled, thread count is bounded.
   * This demonstrates the fix for thread leak.
   */
  @Test
  public void testWithSharedPoolThreadCountBounded() throws Exception {
    final String prefix = "test-shared-";
    final int poolSize = 5;
    final int numCalls = 10;

    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE,
        prefix);

    Configuration conf = new Configuration();
    conf.setBoolean(AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED, true);
    conf.setInt(AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE, poolSize);
    conf.setInt(AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE, 60);

    int initialCount = countThreadsWithPrefix(prefix);

    for (int i = 0; i < numCalls; i++) {
      ScheduledExecutorService executor = holder.get(conf);
      Assertions.assertThat(executor)
          .as("Should return same executor instance")
          .isNotNull();
      executor.submit(() -> {}).get();
    }

    int afterCount = countThreadsWithPrefix(prefix);
    int newThreads = afterCount - initialCount;

    Assertions.assertThat(newThreads)
        .as("With shared pool, thread count should be bounded by pool size")
        .isLessThanOrEqualTo(poolSize);

    holder.get(conf).shutdown();
  }

  /**
   * Test that holder returns the same executor instance on repeated calls.
   */
  @Test
  public void testHolderReturnsSameInstance() {
    LazySharedThreadPoolHolder holder = new LazySharedThreadPoolHolder(
        "test.enabled", "test.size", "test.keepalive", "test-same");
    Configuration conf = new Configuration();
    conf.setBoolean("test.enabled", true);
    conf.setInt("test.size", 5);
    conf.setInt("test.keepalive", 60);

    ScheduledExecutorService first = holder.get(conf);
    ScheduledExecutorService second = holder.get(conf);
    ScheduledExecutorService third = holder.get(conf);

    Assertions.assertThat(first)
        .as("Holder should return same instance on repeated calls")
        .isSameAs(second)
        .isSameAs(third);

    first.shutdown();
  }

  /**
   * Verify the production shared-thread-pool config keys for every AWS client
   * type are well-formed: within each client the enabled/size/keepalive keys
   * share a single base and differ only by the expected suffix, and all keys
   * are globally unique. This guards against the copy/paste or swapped-argument
   * mistakes that are easy to make across the near-identical client
   * definitions, where the holder constructor takes only strings.
   */
  @Test
  public void testProductionThreadPoolConfigKeysWellFormed() {
    String[][] clients = {
        {AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED,
            AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE,
            AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE},
        {AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED,
            AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE,
            AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_KEEPALIVE},
        {AWS_STS_CLIENT_SHARED_THREADPOOL_ENABLED,
            AWS_STS_CLIENT_SHARED_THREADPOOL_SIZE,
            AWS_STS_CLIENT_SHARED_THREADPOOL_KEEPALIVE},
        {AWS_KMS_CLIENT_SHARED_THREADPOOL_ENABLED,
            AWS_KMS_CLIENT_SHARED_THREADPOOL_SIZE,
            AWS_KMS_CLIENT_SHARED_THREADPOOL_KEEPALIVE},
    };

    List<String> allKeys = new ArrayList<>();
    for (String[] client : clients) {
      String enabled = client[0];
      String size = client[1];
      String keepAlive = client[2];

      Assertions.assertThat(enabled)
          .as("Enabled key should end with .threadpool.enabled")
          .endsWith(".threadpool.enabled");
      String base =
          enabled.substring(0, enabled.length() - ".enabled".length());
      Assertions.assertThat(size)
          .as("Size key should be base + .size for base %s", base)
          .isEqualTo(base + ".size");
      Assertions.assertThat(keepAlive)
          .as("Keepalive key should be base + .keepalive.seconds for base %s",
              base)
          .isEqualTo(base + ".keepalive.seconds");

      allKeys.add(enabled);
      allKeys.add(size);
      allKeys.add(keepAlive);
    }

    Assertions.assertThat(allKeys)
        .as("All shared thread pool config keys should be unique")
        .doesNotHaveDuplicates();
  }

  /**
   * Verify the shared-thread-pool defaults declared in core-default.xml match
   * the constants used by the lazy holders: every pool is disabled by default
   * and the size/keepalive defaults are the documented values. This catches a
   * typo in a core-default.xml name or a mismatch between Constants, the holder
   * defaults and core-default.xml.
   */
  @Test
  public void testProductionThreadPoolDefaultsInCoreDefaultXml() {
    Configuration conf = new Configuration();
    String[] enabledKeys = {
        AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_STS_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_KMS_CLIENT_SHARED_THREADPOOL_ENABLED,
    };
    String[] sizeKeys = {
        AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_STS_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_KMS_CLIENT_SHARED_THREADPOOL_SIZE,
    };
    String[] keepAliveKeys = {
        AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_KEEPALIVE,
        AWS_STS_CLIENT_SHARED_THREADPOOL_KEEPALIVE,
        AWS_KMS_CLIENT_SHARED_THREADPOOL_KEEPALIVE,
    };

    for (String key : enabledKeys) {
      Assertions.assertThat(conf.getBoolean(key, true))
          .as("%s should default to false in core-default.xml", key)
          .isFalse();
    }
    for (String key : sizeKeys) {
      Assertions.assertThat(conf.getInt(key, -1))
          .as("%s should match the documented default in core-default.xml", key)
          .isEqualTo(AWS_CLIENT_SHARED_THREADPOOL_SIZE_DEFAULT);
    }
    for (String key : keepAliveKeys) {
      Assertions.assertThat(conf.getInt(key, -1))
          .as("%s should match the documented default in core-default.xml", key)
          .isEqualTo(AWS_CLIENT_SHARED_THREADPOOL_KEEPALIVE_DEFAULT);
    }
  }

  /**
   * Verify each AWS client factory wires its shared-thread-pool holder with the
   * config keys for that client, in the correct slots. The holder constructor
   * takes only strings, so a swapped or mis-pasted key would otherwise be
   * silent; this asserts the enabled/size/keepalive keys land where intended.
   */
  @Test
  public void testFactoryExecutorHoldersWiredWithExpectedKeys() {
    assertHolderKeys(DefaultS3ClientFactory.s3SyncExecutorHolder(),
        AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE);
    assertHolderKeys(DefaultS3ClientFactory.s3AsyncExecutorHolder(),
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_KEEPALIVE);
    assertHolderKeys(STSClientFactory.stsExecutorHolder(),
        AWS_STS_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_STS_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_STS_CLIENT_SHARED_THREADPOOL_KEEPALIVE);
    assertHolderKeys(EncryptionS3ClientFactory.kmsExecutorHolder(),
        AWS_KMS_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_KMS_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_KMS_CLIENT_SHARED_THREADPOOL_KEEPALIVE);
  }

  /**
   * Assert a holder is wired with the expected enabled/size/keepalive keys.
   * @param holder the holder under test
   * @param enabledKey expected enabled key
   * @param sizeKey expected size key
   * @param keepAliveKey expected keepalive key
   */
  private static void assertHolderKeys(LazySharedThreadPoolHolder holder,
      String enabledKey, String sizeKey, String keepAliveKey) {
    Assertions.assertThat(holder.getEnabledKey())
        .as("enabled key")
        .isEqualTo(enabledKey);
    Assertions.assertThat(holder.getSizeKey())
        .as("size key")
        .isEqualTo(sizeKey);
    Assertions.assertThat(holder.getKeepAliveKey())
        .as("keepalive key")
        .isEqualTo(keepAliveKey);
  }
}
