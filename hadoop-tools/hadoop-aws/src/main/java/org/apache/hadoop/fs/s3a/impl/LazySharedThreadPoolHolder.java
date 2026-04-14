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

package org.apache.hadoop.fs.s3a.impl;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.AWS_CLIENT_SHARED_THREADPOOL_KEEPALIVE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CLIENT_SHARED_THREADPOOL_SIZE_DEFAULT;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Holder for a lazily initialized shared ScheduledExecutorService.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LazySharedThreadPoolHolder {

  private static final Logger LOG =
      LoggerFactory.getLogger(LazySharedThreadPoolHolder.class);

  private final String enabledKey;
  private final String sizeKey;
  private final String keepAliveKey;
  private final String namePrefix;

  private volatile Optional<ScheduledExecutorService> executor;

  /**
   * Create a holder for a lazy shared thread pool.
   * @param enabledKey config key to enable the shared pool
   * @param sizeKey config key for pool size
   * @param keepAliveKey config key for thread keep-alive in seconds
   * @param namePrefix thread name prefix for debugging
   */
  public LazySharedThreadPoolHolder(String enabledKey, String sizeKey,
      String keepAliveKey, String namePrefix) {
    this.enabledKey = enabledKey;
    this.sizeKey = sizeKey;
    this.keepAliveKey = keepAliveKey;
    this.namePrefix = namePrefix;
  }

  /**
   * Get the shared executor, creating it on first call if enabled.
   * @param conf configuration
   * @return the executor, or null if not enabled
   */
  public synchronized ScheduledExecutorService get(Configuration conf) {
    if (executor == null) {
      if (conf.getBoolean(enabledKey, false)) {
        int poolSize = conf.getInt(sizeKey, AWS_CLIENT_SHARED_THREADPOOL_SIZE_DEFAULT);
        int keepAlive = conf.getInt(keepAliveKey, AWS_CLIENT_SHARED_THREADPOOL_KEEPALIVE_DEFAULT);
        checkArgument(poolSize > 0,
            "Value of %s must be positive, got: %s", sizeKey, poolSize);
        checkArgument(keepAlive > 0,
            "Value of %s must be positive, got: %s", keepAliveKey, keepAlive);
        executor = Optional.of(createScheduledExecutor(namePrefix, poolSize, keepAlive));
      } else {
        executor = Optional.empty();
      }
    }
    return executor.orElse(null);
  }

  /**
   * Create a scheduled executor with idle thread timeout.
   * @param namePrefix thread name prefix for debugging
   * @param poolSize core pool size
   * @param keepAliveSeconds keepalive time in seconds
   * @return the executor
   */
  public static ScheduledExecutorService createScheduledExecutor(
      String namePrefix, int poolSize, int keepAliveSeconds) {
    ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(poolSize,
        runnable -> {
          Thread t = new Thread(runnable, namePrefix);
          t.setDaemon(true);
          return t;
        });
    pool.setKeepAliveTime(keepAliveSeconds, TimeUnit.SECONDS);
    pool.allowCoreThreadTimeOut(true);
    LOG.debug("Created shared executor '{}' with pool size {} and keepalive {}s",
        namePrefix, poolSize, keepAliveSeconds);
    return pool;
  }
}
