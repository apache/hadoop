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
#ifndef LIB_COMMON_RETRY_POLICY_H_
#define LIB_COMMON_RETRY_POLICY_H_

#include "common/util.h"

#include <string>
#include <stdint.h>

namespace hdfs {

class RetryAction {
 public:
  enum RetryDecision { FAIL, RETRY, FAILOVER_AND_RETRY };

  RetryDecision action;
  uint64_t delayMillis;
  std::string reason;

  RetryAction(RetryDecision in_action, uint64_t in_delayMillis,
              const std::string &in_reason)
      : action(in_action), delayMillis(in_delayMillis), reason(in_reason) {}

  static RetryAction fail(const std::string &reason) {
    return RetryAction(FAIL, 0, reason);
  }
  static RetryAction retry(uint64_t delay) {
    return RetryAction(RETRY, delay, "");
  }
  static RetryAction failover() {
    return RetryAction(FAILOVER_AND_RETRY, 0, "");
  }
};

class RetryPolicy {
 public:
  /*
   * If there was an error in communications, responds with the configured
   * action to take.
   */
  virtual RetryAction ShouldRetry(const Status &s, uint64_t retries,
                                            uint64_t failovers,
                                            bool isIdempotentOrAtMostOnce) const = 0;

  virtual ~RetryPolicy() {}
};

/*
 * Returns a fixed delay up to a certain number of retries
 */
class FixedDelayRetryPolicy : public RetryPolicy {
 public:
  FixedDelayRetryPolicy(uint64_t delay, uint64_t max_retries)
      : delay_(delay), max_retries_(max_retries) {}

  RetryAction ShouldRetry(const Status &s, uint64_t retries,
                          uint64_t failovers,
                          bool isIdempotentOrAtMostOnce) const override;
 private:
  uint64_t delay_;
  uint64_t max_retries_;
};

/*
 * Never retries
 */
class NoRetryPolicy : public RetryPolicy {
 public:
  RetryAction ShouldRetry(const Status &s, uint64_t retries,
                          uint64_t failovers,
                          bool isIdempotentOrAtMostOnce) const override;
};
}

#endif
