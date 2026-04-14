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
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

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
}
