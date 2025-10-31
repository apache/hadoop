/**
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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.TAIL_LATENCY_TIMEOUT_RETRY_POLICY_ABBREVIATION;

/**
 * Retry policy for tail latency request timeout scenarios.
 */
public class TailLatencyRequestTimeoutRetryPolicy extends AbfsRetryPolicy{

  /**
   * Initializes a new instance of the {@link TailLatencyRequestTimeoutRetryPolicy} class.
   * @param conf The {@link AbfsConfiguration} from which to retrieve retry configuration.
   */
  public TailLatencyRequestTimeoutRetryPolicy(AbfsConfiguration conf) {
    super(conf.getTailLatencyMaxRetryCount(), TAIL_LATENCY_TIMEOUT_RETRY_POLICY_ABBREVIATION);
  }

  /**
   * Returns a constant backoff interval independent of retry count;
   *
   * @param retryCount The current retry attempt count.
   * @return backoff Interval time
   */
  @Override
  public long getRetryInterval(final int retryCount) {
    return 0;
  }
}
