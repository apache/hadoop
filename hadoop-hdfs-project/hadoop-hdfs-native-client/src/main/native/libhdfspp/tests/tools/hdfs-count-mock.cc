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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "hdfs-count-mock.h"
#include "hdfs-tool-tests.h"

namespace hdfs::tools::test {
CountMock::~CountMock() = default;

void CountMock::SetExpectations(
    std::function<std::unique_ptr<CountMock>()> test_case,
    const std::vector<std::string> &args) const {
  // Get the pointer to the function that defines the test case
  const auto test_case_func =
      test_case.target<std::unique_ptr<CountMock> (*)()>();
  ASSERT_NE(test_case_func, nullptr);

  // Set the expected method calls and their corresponding arguments for each
  // test case
  if (*test_case_func == &CallHelp<CountMock>) {
    EXPECT_CALL(*this, HandleHelp()).Times(1).WillOnce(testing::Return(true));
    return;
  }

  if (*test_case_func == &PassAPath<CountMock>) {
    const auto arg1 = args[0];
    EXPECT_CALL(*this, HandlePath(false, arg1))
        .Times(1)
        .WillOnce(testing::Return(true));
  }

  if (*test_case_func == &PassQOptAndPath<CountMock>) {
    const auto arg1 = args[0];
    const auto arg2 = args[1];
    EXPECT_CALL(*this, HandlePath(true, arg2))
        .Times(1)
        .WillOnce(testing::Return(true));
  }
}
} // namespace hdfs::tools::test
