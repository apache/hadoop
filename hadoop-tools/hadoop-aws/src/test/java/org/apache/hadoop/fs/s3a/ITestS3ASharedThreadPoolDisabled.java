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
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED;

/**
 * Control case for {@link ITestS3ASharedThreadPoolEnabled}: with the shared
 * pool disabled (the default), each S3AFileSystem's AWS SDK clients create their
 * own sdk-ScheduledExecutor pool, so thread count grows with the number of
 * instances. Must run in its own JVM: the static pool holder is single-state
 * per JVM, so this control (pools off) and {@link ITestS3ASharedThreadPoolEnabled}
 * (pools on) cannot share a fork. hadoop-aws sets reuseForks=false, which these
 * tests require; do not enable fork reuse for them.
 * <p>
 * The sdk-ScheduledExecutor pools are created lazily as the clients schedule
 * work, so the exact count varies; the assertion is only that it clearly
 * exceeds a single shared pool.
 */
public class ITestS3ASharedThreadPoolDisabled extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ASharedThreadPoolDisabled.class);

  private static final int INSTANCES = 20;

  /** Pool size the shared pools would otherwise cap at, per type. */
  private static final int SHARED_POOL_SIZE = 5;

  /** Thread name prefix the AWS SDK uses for its per-client default pool. */
  private static final String SDK_POOL_PREFIX = "sdk-ScheduledExecutor";

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // Strip any base/per-bucket override, then pin the pools explicitly off, so
    // this control is deterministic regardless of the default: it must run with
    // the shared pools disabled to exercise the per-client leak.
    S3ATestUtils.removeBaseAndBucketOverrides(
        S3ATestUtils.getTestBucketName(conf), conf,
        AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED);
    conf.setBoolean(AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED, false);
    conf.setBoolean(AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED, false);
    return conf;
  }

  @Test
  public void testWithoutSharedPoolThreadsLeakPastGc() throws Exception {
    // Abandon many uncached instances and force GC: without sharing, each
    // client's own scheduled pool is leaked (not shut down on eviction, and
    // rooted by its own running threads), so the threads survive collection.
    int sdkThreads = S3ATestUtils.countSchedulerThreadsAfterAbandoningFilesystems(
        getFileSystem().getUri(), getConfiguration(), INSTANCES,
        SDK_POOL_PREFIX);
    LOG.info("SDK scheduler threads surviving GC after abandoning {} "
        + "S3AFileSystem instances without the shared pool: {}", INSTANCES,
        sdkThreads);

    // The leaked per-client pools persist past GC, far exceeding a single
    // shared pool.
    Assertions.assertThat(sdkThreads)
        .as("Without the shared pool, abandoned sdk-ScheduledExecutor threads "
            + "should leak past GC, exceeding a single shared pool after %s "
            + "instances", INSTANCES)
        .isGreaterThan(2 * SHARED_POOL_SIZE);

    // Intentionally not cleaned up: the abandoned uncached instances leave no
    // handle to close, and that unreclaimability is the point. The daemon
    // threads are reaped when this class's own fork exits (reuseForks=false).
  }
}
