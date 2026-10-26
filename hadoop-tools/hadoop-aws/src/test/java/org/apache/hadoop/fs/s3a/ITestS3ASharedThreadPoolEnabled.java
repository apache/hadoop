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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE;

/**
 * Integration test demonstrating that the shared scheduled thread pool bounds
 * thread growth across many S3AFileSystem instances.
 * <p>
 * Background (HADOOP-19862): each S3AFileSystem builds its own AWS SDK clients,
 * and each client otherwise creates a 5-thread sdk-ScheduledExecutor pool, so
 * creating many instances grows threads without bound. With the shared pool
 * enabled, all clients of a type reuse a single pool, so the thread count stays
 * bounded regardless of how many instances are created.
 * <p>
 * This test proves the fix direction (pool enabled, threads bounded);
 * {@link ITestS3ASharedThreadPoolDisabled} is the control proving the opposite
 * (pool disabled, threads grow). The two must be separate classes: the holders
 * are private static final and memoize on first use, so the first configuration
 * to reach them wins for the JVM. They therefore cannot share a fork; each must
 * run in its own JVM. hadoop-aws sets reuseForks=false, which these tests
 * require (do not enable fork reuse for them); do not merge them into one class.
 * <p>
 * Exact thread counts depend on AWS SDK internals, so the assertion is on the
 * bound (not exceeding the pool sizes), not an exact number.
 */
public class ITestS3ASharedThreadPoolEnabled extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ASharedThreadPoolEnabled.class);

  private static final int POOL_SIZE = 5;

  /** Number of distinct filesystem instances to create. */
  private static final int INSTANCES = 20;

  /** Thread name prefixes used by the shared pools (see the factories). */
  private static final String[] SHARED_POOL_PREFIXES = {
      "s3a-s3-sync-scheduler",
      "s3a-s3-async-scheduler",
  };

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // Strip any base or per-bucket overrides for these keys so the values set
    // below win: a bucket override would otherwise take precedence and
    // silently invalidate the test.
    S3ATestUtils.removeBaseAndBucketOverrides(
        S3ATestUtils.getTestBucketName(conf), conf,
        AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE);
    // Enable the shared pools before any client is built, so the static
    // holders memoize in the enabled state for this JVM.
    conf.setBoolean(AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED, true);
    conf.setBoolean(AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED, true);
    conf.setInt(AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE, POOL_SIZE);
    conf.setInt(AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE, POOL_SIZE);
    return conf;
  }

  @Test
  public void testSharedPoolBoundsThreadsAcrossManyInstances() throws Exception {
    int shared = S3ATestUtils.countSchedulerThreadsAfterAbandoningFilesystems(
        getFileSystem().getUri(), getConfiguration(), INSTANCES,
        SHARED_POOL_PREFIXES);
    LOG.info("Shared scheduler threads after abandoning {} S3AFileSystem "
        + "instances and forcing GC: {}", INSTANCES, shared);

    // With sharing, the single static pool survives instance abandonment + GC
    // and stays bounded by the two pools' core sizes, regardless of count.
    Assertions.assertThat(shared)
        .as("Total shared scheduler threads after abandoning %s S3AFileSystem "
            + "instances should be bounded by the pool sizes", INSTANCES)
        .isLessThanOrEqualTo(2 * POOL_SIZE);
  }
}
