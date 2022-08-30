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

#include "hdfs-find-mock.h"
#include "hdfs-tool-tests.h"
#include "hdfspp/hdfspp.h"

namespace hdfs::tools::test {
FindMock::~FindMock() = default;

void FindMock::SetExpectations(
    std::function<std::unique_ptr<FindMock>()> test_case,
    const std::vector<std::string> &args) const {
  // Get the pointer to the function that defines the test case
  const auto test_case_func =
      test_case.target<std::unique_ptr<FindMock> (*)()>();
  ASSERT_NE(test_case_func, nullptr);

  // Set the expected method calls and their corresponding arguments for each
  // test case
  if (*test_case_func == &CallHelp<FindMock>) {
    EXPECT_CALL(*this, HandleHelp()).Times(1).WillOnce(testing::Return(true));
    return;
  }

  if (*test_case_func == &PassAPath<FindMock>) {
    const auto arg1 = args[0];
    EXPECT_CALL(*this, HandlePath(arg1, "*",
                                  hdfs::FileSystem::GetDefaultFindMaxDepth()))
        .Times(1)
        .WillOnce(testing::Return(true));
  }

  if (*test_case_func == &PassNOptAndAPath<FindMock>) {
    const auto arg1 = args[0];
    const auto arg2 = args[1];
    const auto arg3 = args[2];
    ASSERT_EQ(arg1, "-n");
    EXPECT_CALL(*this, HandlePath(arg3, arg2,
                                  hdfs::FileSystem::GetDefaultFindMaxDepth()))
        .Times(1)
        .WillOnce(testing::Return(true));
  }

  if (*test_case_func == &PassMOptPermissionsAndAPath<FindMock>) {
    const auto arg1 = args[0];
    const auto arg2 = args[1];
    const auto arg3 = args[2];
    ASSERT_EQ(arg1, "-m");
    EXPECT_CALL(*this,
                HandlePath(arg3, "*", static_cast<uint32_t>(std::stoi(arg2))))
        .Times(1)
        .WillOnce(testing::Return(true));
  }

  if (*test_case_func == &PassNStrMNumAndAPath<FindMock>) {
    const auto arg1 = args[0];
    const auto arg2 = args[1];
    const auto arg3 = args[2];
    const auto arg4 = args[3];
    const auto arg5 = args[4];
    ASSERT_EQ(arg1, "-n");
    ASSERT_EQ(arg3, "-m");
    EXPECT_CALL(*this,
                HandlePath(arg5, arg2, static_cast<uint32_t>(std::stoi(arg4))))
        .Times(1)
        .WillOnce(testing::Return(true));
  }
}
} // namespace hdfs::tools::test
