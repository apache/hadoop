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

/**
 * Control case for {@link ITestS3ASharedThreadPoolEnabled}: explicitly disable
 * the shared pools so each S3AFileSystem's AWS SDK clients create their own
 * sdk-ScheduledExecutor pool. Thread count grows with the number of unclosed
 * instances.
 * The holders memoize the first configuration used in each JVM. Each measurement
 * therefore runs in a fresh child JVM through {@link SharedPoolForkWorker},
 * independently of test-runner fork reuse and execution order.
 * <p>
 * Pool threads start only when the clients schedule work, so the exact count
 * varies; the assertion is only that it exceeds the combined size of both
 * shared pools.
 */
public class ITestS3ASharedThreadPoolDisabled extends AbstractSharedThreadPoolTest {

  /** Thread name prefix the AWS SDK uses for its per-client default pool. */
  private static final String SDK_POOL_PREFIX = "sdk-ScheduledExecutor";

  @Override
  protected boolean sharedPoolsEnabled() {
    return false;
  }

  @Test
  public void testWithoutSharedPoolThreadsLeakPastGc() throws Exception {
    // Abandon many uncached instances and force GC: without sharing, each
    // client's own scheduled pool is leaked (never shut down, because the
    // instances are never closed, and rooted by its own running threads), so
    // the threads survive collection.
    int sdkThreads = measureThreads(SDK_POOL_PREFIX);
    LOG.info("SDK scheduler threads surviving GC after abandoning {} "
        + "S3AFileSystem instances without the shared pool: {}", INSTANCES,
        sdkThreads);

    // The leaked per-client pools persist past GC, exceeding both shared
    // pools combined.
    Assertions.assertThat(sdkThreads)
        .as("Without the shared pool, abandoned sdk-ScheduledExecutor threads "
            + "should leak past GC, exceeding both shared pools combined after "
            + "%s instances", INSTANCES)
        .isGreaterThan(2 * POOL_SIZE);

    // The child JVM exits after the measurement, so intentionally abandoned
    // clients cannot affect later tests.
  }
}
