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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE;

/**
 * Shared setup for the shared-pool integration tests. Each subclass chooses
 * whether the shared pools are enabled, supplies its thread name prefixes and
 * makes its own assertion.
 */
public abstract class AbstractSharedThreadPoolTest extends AbstractS3ATestBase {

  /** Number of distinct filesystem instances to create. */
  protected static final int INSTANCES = 20;

  /** Core size of each shared pool when enabled. */
  protected static final int POOL_SIZE = 5;

  private static final int KEEP_ALIVE_SECONDS = 60;

  /**
   * Whether the shared S3 sync and async pools are enabled for this test.
   * @return true to enable the shared pools
   */
  protected abstract boolean sharedPoolsEnabled();

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
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE,
        AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE,
        AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_KEEPALIVE);
    // Pin the mode transferred to the fresh measurement JVM.
    conf.setBoolean(AWS_S3_CLIENT_SHARED_THREADPOOL_ENABLED, sharedPoolsEnabled());
    conf.setBoolean(AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_ENABLED, sharedPoolsEnabled());
    conf.setInt(AWS_S3_CLIENT_SHARED_THREADPOOL_SIZE, POOL_SIZE);
    conf.setInt(AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_SIZE, POOL_SIZE);
    conf.setInt(AWS_S3_CLIENT_SHARED_THREADPOOL_KEEPALIVE, KEEP_ALIVE_SECONDS);
    conf.setInt(AWS_S3_ASYNC_CLIENT_SHARED_THREADPOOL_KEEPALIVE, KEEP_ALIVE_SECONDS);
    return conf;
  }

  /**
   * Count threads whose names match the prefixes, measured in a fresh child JVM
   * after abandoning {@link #INSTANCES} filesystem instances and forcing GC.
   * @param prefixes thread name prefixes to count
   * @return the number of matching threads
   * @throws IOException if the measurement fails
   */
  protected int measureThreads(String... prefixes) throws IOException {
    return SharedPoolForkWorker.countThreads(getFileSystem().getUri(),
        getConfiguration(), INSTANCES, getTestTimeoutMillis(), prefixes);
  }
}
