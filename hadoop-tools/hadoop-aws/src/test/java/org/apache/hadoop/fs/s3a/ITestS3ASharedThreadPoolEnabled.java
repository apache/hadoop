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
 * Integration test demonstrating that the shared scheduled thread pool bounds
 * thread growth across many S3AFileSystem instances.
 * <p>
 * Background (HADOOP-19862): each S3AFileSystem builds its own AWS SDK clients,
 * and each client otherwise creates its own sdk-ScheduledExecutor pool of up to
 * 5 threads, so threads grow with every unclosed instance. With the shared pool
 * enabled, all clients of a type reuse a single pool, so the thread count stays
 * bounded regardless of how many instances are created.
 * <p>
 * This test checks bounded growth with sharing enabled;
 * {@link ITestS3ASharedThreadPoolDisabled} is the control with sharing disabled.
 * The holders memoize the first configuration used in each JVM. Each measurement
 * therefore runs in a fresh child JVM through {@link SharedPoolForkWorker},
 * independently of test-runner fork reuse and execution order.
 * <p>
 * Exact thread counts depend on AWS SDK internals, so the assertion is on the
 * bound (not exceeding the pool sizes), not an exact number.
 */
public class ITestS3ASharedThreadPoolEnabled extends AbstractSharedThreadPoolTest {

  /** Thread name prefixes used by the shared pools (see the factories). */
  private static final String[] SHARED_POOL_PREFIXES = {
      "s3a-s3-sync-scheduler",
      "s3a-s3-async-scheduler",
  };

  @Override
  protected boolean sharedPoolsEnabled() {
    return true;
  }

  @Test
  public void testSharedPoolBoundsThreadsAcrossManyInstances() throws Exception {
    int shared = measureThreads(SHARED_POOL_PREFIXES);
    LOG.info("Shared scheduler threads after abandoning {} S3AFileSystem "
        + "instances and forcing GC: {}", INSTANCES, shared);

    // With sharing, the two static pools survive instance abandonment + GC
    // and stay bounded by their combined core sizes, regardless of count.
    Assertions.assertThat(shared)
        .as("Total shared scheduler threads after abandoning %s S3AFileSystem "
            + "instances should be bounded by the pool sizes", INSTANCES)
        .isPositive()
        .isLessThanOrEqualTo(2 * POOL_SIZE);
  }
}
