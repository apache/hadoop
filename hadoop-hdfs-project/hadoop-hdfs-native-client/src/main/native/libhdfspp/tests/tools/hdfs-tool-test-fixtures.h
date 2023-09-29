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

#ifndef LIBHDFSPP_TOOLS_HDFS_TOOL_TEST_FIXTURES
#define LIBHDFSPP_TOOLS_HDFS_TOOL_TEST_FIXTURES

#include <functional>
#include <memory>

#include <gtest/gtest.h>

#include "hdfs-tool.h"

/**
 * {@class HdfsToolBasicTest} is a fixture that houses basic tests on {@link
 * hdfs::tools::HdfsTool} interface. It contains the "Happy path" tests which
 * covers the scenarios where {@link hdfs::tools::HdfsTool} is expected to
 * work just fine.
 *
 * {@class HdfsToolBasicTest} is parameterized on a lambda returning an instance
 * of {@link hdfs::tools::HdfsTool} wrapped in a std::unique_ptr. We then run
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
 * on {@link hdfs::tools::HdfsTool} interface. It covers the tests where
 * unfavorable inputs are presented to the {@link hdfs::tools::HdfsTool}
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
 * {@class HdfsToolNegativeTestNoThrow} is a fixture that houses negative
 * tests on {@link hdfs::tools::HdfsTool} interface. It covers the tests where
 * unfavorable inputs are presented to the {@link hdfs::tools::HdfsTool}
 * instance and is not expected to throw exceptions and returns false instead.
 * The tool is not expected to crash and ensures that the unfavorable inputs are
 * handled gracefully.
 */
class HdfsToolNegativeTestNoThrow : public HdfsToolBasicTest {
public:
  // Abiding to the rule of 5
  HdfsToolNegativeTestNoThrow() = default;
  HdfsToolNegativeTestNoThrow(const HdfsToolNegativeTestNoThrow &) = delete;
  HdfsToolNegativeTestNoThrow(HdfsToolNegativeTestNoThrow &&) = delete;
  HdfsToolNegativeTestNoThrow &
  operator=(const HdfsToolNegativeTestNoThrow &) = delete;
  HdfsToolNegativeTestNoThrow &
  operator=(HdfsToolNegativeTestNoThrow &&) = delete;
  ~HdfsToolNegativeTestNoThrow() override;
};

#endif
