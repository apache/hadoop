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

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.io.retry.RetryPolicies.exponentialBackoffRetry;

/**
 * A Retry policy whose throttling comes from the S3Guard config options.
 */
public class S3GuardDataAccessRetryPolicy extends S3ARetryPolicy {

  public S3GuardDataAccessRetryPolicy(final Configuration conf) {
    super(conf);
  }

  protected RetryPolicy createThrottleRetryPolicy(final Configuration conf) {
    return exponentialBackoffRetry(
        conf.getInt(S3GUARD_DDB_MAX_RETRIES, S3GUARD_DDB_MAX_RETRIES_DEFAULT),
        conf.getTimeDuration(S3GUARD_DDB_THROTTLE_RETRY_INTERVAL,
            S3GUARD_DDB_THROTTLE_RETRY_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }
}
