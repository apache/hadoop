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

#include "common/retry_policy.h"

namespace hdfs {

RetryAction FixedDelayRetryPolicy::ShouldRetry(
    const Status &s, uint64_t retries, uint64_t failovers,
    bool isIdempotentOrAtMostOnce) const {
  (void)s;
  (void)isIdempotentOrAtMostOnce;
  if (retries + failovers >= max_retries_) {
    return RetryAction::fail(
        "Failovers (" + std::to_string(retries + failovers) +
        ") exceeded maximum retries (" + std::to_string(max_retries_) + ")");
  } else {
    return RetryAction::retry(delay_);
  }
}

RetryAction NoRetryPolicy::ShouldRetry(
    const Status &s, uint64_t retries, uint64_t failovers,
    bool isIdempotentOrAtMostOnce) const {
  (void)s;
  (void)retries;
  (void)failovers;
  (void)isIdempotentOrAtMostOnce;
  return RetryAction::fail("No retry");
}

}
