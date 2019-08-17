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
#include "common/logging.h"

#include <sstream>

namespace hdfs {

RetryAction FixedDelayRetryPolicy::ShouldRetry(
    const Status &s, uint64_t retries, uint64_t failovers,
    bool isIdempotentOrAtMostOnce) const {
  LOG_TRACE(kRPC, << "FixedDelayRetryPolicy::ShouldRetry(retries=" << retries << ", failovers=" << failovers << ")");
  (void)isIdempotentOrAtMostOnce;
  if (retries + failovers >= max_retries_) {
    return RetryAction::fail(
        "Failovers and retries(" + std::to_string(retries + failovers) +
        ") exceeded maximum retries (" + std::to_string(max_retries_) + "), Status: " +
        s.ToString());
  } else {
    return RetryAction::retry(delay_);
  }
}


RetryAction NoRetryPolicy::ShouldRetry(
    const Status &s, uint64_t retries, uint64_t failovers,
    bool isIdempotentOrAtMostOnce) const {
  LOG_TRACE(kRPC, << "NoRetryPolicy::ShouldRetry(retries=" << retries << ", failovers=" << failovers << ")");
  (void)retries;
  (void)failovers;
  (void)isIdempotentOrAtMostOnce;
  return RetryAction::fail("No retry, Status: " + s.ToString());
}


RetryAction FixedDelayWithFailover::ShouldRetry(const Status &s, uint64_t retries,
    uint64_t failovers,
    bool isIdempotentOrAtMostOnce) const {
  (void)isIdempotentOrAtMostOnce;
  (void)max_failover_conn_retries_;
  LOG_TRACE(kRPC, << "FixedDelayWithFailover::ShouldRetry(retries=" << retries << ", failovers=" << failovers << ")");

  if(failovers < max_failover_retries_ && (s.code() == ::asio::error::timed_out || s.get_server_exception_type() == Status::kStandbyException) )
  {
    // Try connecting to another NN in case this one keeps timing out
    // Can add the backoff wait specified by dfs.client.failover.sleep.base.millis here
    if(failovers == 0) {
      // No delay on first failover if it looks like the NN was bad.
      return RetryAction::failover(0);
    } else {
      return RetryAction::failover(delay_);
    }
  }

  if(retries < max_retries_ && failovers < max_failover_retries_) {
    LOG_TRACE(kRPC, << "FixedDelayWithFailover::ShouldRetry: retries < max_retries_ && failovers < max_failover_retries_");
    return RetryAction::retry(delay_);
  } else if (retries >= max_retries_ && failovers < max_failover_retries_) {
    LOG_TRACE(kRPC, << "FixedDelayWithFailover::ShouldRetry: retries >= max_retries_ && failovers < max_failover_retries_");
    return RetryAction::failover(delay_);
  } else if (retries <= max_retries_ && failovers == max_failover_retries_) {
    LOG_TRACE(kRPC, << "FixedDelayWithFailover::ShouldRetry: retries <= max_retries_ && failovers == max_failover_retries_");
    // 1 last retry on new connection
    return RetryAction::retry(delay_);
  }

  return RetryAction::fail("Retry and failover didn't work, Status: " + s.ToString());
}

}
