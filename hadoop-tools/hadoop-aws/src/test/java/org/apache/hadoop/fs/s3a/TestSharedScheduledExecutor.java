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
import org.apache.hadoop.fs.s3a.impl.LazySharedThreadPoolHolder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE;

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
        .hasMessageContaining("must be positive");
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
        .hasMessageContaining("must be positive");
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
        .hasMessageContaining("must be positive");
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
        .hasMessageContaining("must be positive");
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
        .isNotNull();
    executor.shutdown();
  }

  /**
   * Count threads matching the given prefix.
   * @param prefix thread name prefix to match
   * @return count of matching threads
   */
  private int countThreadsWithPrefix(String prefix) {
    int count = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().startsWith(prefix)) {
        count++;
      }
    }
    return count;
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
}
