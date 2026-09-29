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

package org.apache.hadoop.fs.bos;

import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * A retry policy that always fails on the first attempt.
 * Used as the default policy for methods not explicitly
 * configured with a retry policy.
 */
public class BosTryOnceThenFail implements RetryPolicy {

  /**
   * Constructs a BosTryOnceThenFail retry policy.
   */
  public BosTryOnceThenFail() {
  }

  /**
   * Always returns a FAIL action, indicating no retry.
   *
   * @param e the exception that caused the failure
   * @param retries the number of retries attempted
   * @param failovers the number of failovers attempted
   * @param isIdempotentOrAtMostOnce whether the operation
   *        is idempotent or at-most-once
   * @return a retry action indicating failure
   * @throws Exception if an error occurs
   */
  public RetryAction shouldRetry(
      Exception e, int retries, int failovers,
      boolean isIdempotentOrAtMostOnce)
      throws Exception {
    return new RetryAction(
        RetryAction.RetryDecision.FAIL);
  }
}
