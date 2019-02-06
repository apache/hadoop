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
  static RetryAction failover(uint64_t delay) {
    return RetryAction(FAILOVER_AND_RETRY, delay, "");
  }

  std::string decision_str() const {
    switch(action) {
      case FAIL: return "FAIL";
      case RETRY: return "RETRY";
      case FAILOVER_AND_RETRY: return "FAILOVER_AND_RETRY";
      default: return "UNDEFINED ACTION";
    }
  };
};

class RetryPolicy {
 protected:
  uint64_t delay_;
  uint64_t max_retries_;
  RetryPolicy(uint64_t delay, uint64_t max_retries) :
              delay_(delay), max_retries_(max_retries) {}

 public:
  RetryPolicy() {};

  virtual ~RetryPolicy() {}
  /*
   * If there was an error in communications, responds with the configured
   * action to take.
   */
  virtual RetryAction ShouldRetry(const Status &s, uint64_t retries,
                                            uint64_t failovers,
                                            bool isIdempotentOrAtMostOnce) const = 0;

  virtual std::string str() const { return "Base RetryPolicy"; }
};


/*
 * Overview of how the failover retry policy works:
 *
 * 1) Acts the same as FixedDelayRetryPolicy in terms of connection retries against a single NN
 *    with two differences:
 *      a) If we have retried more than the maximum number of retries we will failover to the
 *         other node and reset the retry counter rather than error out.  It will begin the same
 *         routine on the other node.
 *      b) If an attempted connection times out and max_failover_conn_retries_ is less than the
 *         normal number of retries it will failover sooner.  The connection timeout retry limit
 *         defaults to zero; the idea being that if a node is unresponsive it's better to just
 *         try the secondary rather than incur the timeout cost multiple times.
 *
 * 2) Keeps track of the failover count in the same way that the retry count is tracked.  If failover
 *    is triggered more than a set number (dfs.client.failover.max.attempts) of times then the operation
 *    will error out in the same way that a non-HA operation would error if it ran out of retries.
 *
 * 3) Failover between namenodes isn't instantaneous so the RPC retry delay is reused to add a small
 *    delay between failover attempts.  This helps prevent the client from quickly using up all of
 *    its failover attempts while thrashing between namenodes that are both temporarily marked standby.
 *    Note: The java client implements exponential backoff here with a base other than the rpc delay,
 *    and this will do the same here in the future. This doesn't do any sort of exponential backoff
 *    and the name can be changed to ExponentialDelayWithFailover when backoff is implemented.
 */
class FixedDelayWithFailover : public RetryPolicy {
 public:
  FixedDelayWithFailover(uint64_t delay, uint64_t max_retries,
                         uint64_t max_failover_retries,
                         uint64_t max_failover_conn_retries)
      : RetryPolicy(delay, max_retries), max_failover_retries_(max_failover_retries),
        max_failover_conn_retries_(max_failover_conn_retries) {}

  RetryAction ShouldRetry(const Status &s, uint64_t retries,
                          uint64_t failovers,
                          bool isIdempotentOrAtMostOnce) const override;

  std::string str() const override { return "FixedDelayWithFailover"; }

 private:
  // Attempts to fail over
  uint64_t max_failover_retries_;
  // Attempts to fail over if connection times out rather than
  // tring to connect and wait for the timeout delay failover_retries_
  // times.
  uint64_t max_failover_conn_retries_;
};


/*
 * Returns a fixed delay up to a certain number of retries
 */
class FixedDelayRetryPolicy : public RetryPolicy {
 public:
  FixedDelayRetryPolicy(uint64_t delay, uint64_t max_retries)
      : RetryPolicy(delay, max_retries) {}

  RetryAction ShouldRetry(const Status &s, uint64_t retries,
                          uint64_t failovers,
                          bool isIdempotentOrAtMostOnce) const override;

  std::string str() const override { return "FixedDelayRetryPolicy"; }
};

/*
 * Never retries
 */
class NoRetryPolicy : public RetryPolicy {
 public:
  NoRetryPolicy() {};
  RetryAction ShouldRetry(const Status &s, uint64_t retries,
                          uint64_t failovers,
                          bool isIdempotentOrAtMostOnce) const override;

  std::string str() const override { return "NoRetryPolicy"; }
};
}

#endif
