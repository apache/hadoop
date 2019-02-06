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

#include <gmock/gmock.h>

using namespace hdfs;

TEST(RetryPolicyTest, TestNoRetry) {
  NoRetryPolicy policy;
  EXPECT_EQ(RetryAction::FAIL, policy.ShouldRetry(Status::Unimplemented(), 0, 0, true).action);
}

TEST(RetryPolicyTest, TestFixedDelay) {
  static const uint64_t DELAY = 100;
  FixedDelayRetryPolicy policy(DELAY, 10);

  // No error
  RetryAction result = policy.ShouldRetry(Status::Unimplemented(), 0, 0, true);
  EXPECT_EQ(RetryAction::RETRY, result.action);
  EXPECT_EQ(DELAY, result.delayMillis);

  // Few errors
  result = policy.ShouldRetry(Status::Unimplemented(), 2, 2, true);
  EXPECT_EQ(RetryAction::RETRY, result.action);
  EXPECT_EQ(DELAY, result.delayMillis);

  result = policy.ShouldRetry(Status::Unimplemented(), 9, 0, true);
  EXPECT_EQ(RetryAction::RETRY, result.action);
  EXPECT_EQ(DELAY, result.delayMillis);

  // Too many errors
  result = policy.ShouldRetry(Status::Unimplemented(), 10, 0, true);
  EXPECT_EQ(RetryAction::FAIL, result.action);
  EXPECT_TRUE(result.reason.size() > 0);  // some error message

  result = policy.ShouldRetry(Status::Unimplemented(), 0, 10, true);
  EXPECT_EQ(RetryAction::FAIL, result.action);
  EXPECT_TRUE(result.reason.size() > 0);  // some error message
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
