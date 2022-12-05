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

#include "hdfs-ls-mock.h"
#include "hdfs-tool-tests.h"

namespace hdfs::tools::test {
LsMock::~LsMock() = default;

void LsMock::SetExpectations(std::function<std::unique_ptr<LsMock>()> test_case,
                             const std::vector<std::string> &args) const {
  // Get the pointer to the function that defines the test case
  const auto test_case_func = test_case.target<std::unique_ptr<LsMock> (*)()>();
  ASSERT_NE(test_case_func, nullptr);

  // Set the expected method calls and their corresponding arguments for each
  // test case
  if (*test_case_func == &CallHelp<LsMock>) {
    EXPECT_CALL(*this, HandleHelp()).Times(1).WillOnce(testing::Return(true));
    return;
  }

  if (*test_case_func == &PassAPath<LsMock>) {
    const auto arg1 = args[0];
    EXPECT_CALL(*this, HandlePath(arg1, false))
        .Times(1)
        .WillOnce(testing::Return(true));
  }

  if (*test_case_func == &PassRecursivePath<LsMock>) {
    const auto arg1 = args[0];
    const auto arg2 = args[1];
    ASSERT_EQ(arg1, "-R");
    EXPECT_CALL(*this, HandlePath(arg2, true))
        .Times(1)
        .WillOnce(testing::Return(true));
  }

  if (*test_case_func == &PassRecursive<LsMock>) {
    const auto arg1 = args[0];
    ASSERT_EQ(arg1, "-R");
  }
}
} // namespace hdfs::tools::test
