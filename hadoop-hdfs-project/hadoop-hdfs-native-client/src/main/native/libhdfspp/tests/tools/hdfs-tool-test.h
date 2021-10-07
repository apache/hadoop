/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#ifndef LIBHDFSPP_TOOLS_HDFS_TOOL_TEST
#define LIBHDFSPP_TOOLS_HDFS_TOOL_TEST

#include <functional>
#include <memory>
#include <string>
#include <tuple>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "hdfs-tool.h"

/**
 * {@class HdfsToolBasicTest} is a fixture that houses basic tests on {@class
 * hdfs::tools::HdfsTool} interface. It contains the "Happy path" tests which
 * covers the scenarios where {@class hdfs::tools::HdfsTool} is expected to
 * work just fine.
 *
 * {@class HdfsToolBasicTest} is parameterized on a lambda returning an instance
 * of {@class hdfs::tools::HdfsTool} wrapped in a std::unique_ptr. We then run
 * the tests on this instance. Each test runs in isolation. So, a new instance
 * is created for each test.
 */
class HdfsToolBasicTest
    : public testing::TestWithParam<
          std::function<std::unique_ptr<hdfs::tools::HdfsTool>()>> {
public:
  // Abiding to the rule of 5
  HdfsToolBasicTest() = default;
  HdfsToolBasicTest(const HdfsToolBasicTest &) = delete;
  HdfsToolBasicTest(HdfsToolBasicTest &&) = delete;
  HdfsToolBasicTest &operator=(const HdfsToolBasicTest &) = delete;
  HdfsToolBasicTest &operator=(HdfsToolBasicTest &&) = delete;
  ~HdfsToolBasicTest() override;

protected:
  void SetUp() override { hdfs_tool_ = GetParam()(); }

  std::unique_ptr<hdfs::tools::HdfsTool> hdfs_tool_{nullptr};
};

/**
 * {@class HdfsToolNegativeTestThrows} is a fixture that houses negative tests
 * on {@class hdfs::tools::HdfsTool} interface. It covers the tests where
 * unfavorable inputs are presented to the {@class hdfs::tools::HdfsTool}
 * instance and is expected to throw exceptions. Regardless, the tool is not
 * expected to crash and ensures that the thrown exceptions are handled
 * gracefully.
 */
class HdfsToolNegativeTestThrows : public HdfsToolBasicTest {
public:
  // Abiding to the rule of 5
  HdfsToolNegativeTestThrows() = default;
  HdfsToolNegativeTestThrows(const HdfsToolNegativeTestThrows &) = delete;
  HdfsToolNegativeTestThrows(HdfsToolNegativeTestThrows &&) = delete;
  HdfsToolNegativeTestThrows &
  operator=(const HdfsToolNegativeTestThrows &) = delete;
  HdfsToolNegativeTestThrows &operator=(HdfsToolNegativeTestThrows &&) = delete;
  ~HdfsToolNegativeTestThrows() override;
};

/**
 * {@class HdfsToolNegativeTestReturnsFalse} is a fixture that houses negative
 * tests on {@class hdfs::tools::HdfsTool} interface. It covers the tests where
 * unfavorable inputs are presented to the {@class hdfs::tools::HdfsTool}
 * instance and is not expected to throw exceptions. The tool is not
 * expected to crash and ensures that the unfavorable inputs are handled
 * gracefully.
 */
class HdfsToolNegativeTestReturnsFalse : public HdfsToolBasicTest {
public:
  // Abiding to the rule of 5
  HdfsToolNegativeTestReturnsFalse() = default;
  HdfsToolNegativeTestReturnsFalse(const HdfsToolNegativeTestReturnsFalse &) =
      delete;
  HdfsToolNegativeTestReturnsFalse(HdfsToolNegativeTestReturnsFalse &&) =
      delete;
  HdfsToolNegativeTestReturnsFalse &
  operator=(const HdfsToolNegativeTestReturnsFalse &) = delete;
  HdfsToolNegativeTestReturnsFalse &
  operator=(HdfsToolNegativeTestReturnsFalse &&) = delete;
  ~HdfsToolNegativeTestReturnsFalse() override;
};

TEST_P(HdfsToolBasicTest, RunTool) { EXPECT_TRUE(this->hdfs_tool_->Do()); }

TEST_P(HdfsToolNegativeTestReturnsFalse, RunTool) {
  EXPECT_FALSE(this->hdfs_tool_->Do());
}

TEST_P(HdfsToolNegativeTestThrows, RunTool) {
  EXPECT_ANY_THROW({ std::ignore = this->hdfs_tool_->Do(); });
}

template <class T> std::unique_ptr<T> PassAPath(const bool expect_call = true) {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("a/b/c");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  if (expect_call) {
    EXPECT_CALL(*hdfs_tool, HandlePath(arg1))
        .Times(1)
        .WillOnce(testing::Return(true));
  }
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> CallHelp(const bool expect_call = true) {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-h");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  if (expect_call) {
    EXPECT_CALL(*hdfs_tool, HandleHelp())
        .Times(1)
        .WillOnce(testing::Return(true));
  }
  return hdfs_tool;
}

template <class T>
std::unique_ptr<T> Pass2Paths(const bool expect_call = false) {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("a/b/c");
  static std::string arg2("d/e/f");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  if (expect_call) {
    EXPECT_CALL(*hdfs_tool, HandleSnapshot(arg1, arg2))
        .Times(1)
        .WillOnce(testing::Return(true));
  }
  return hdfs_tool;
}

#endif
