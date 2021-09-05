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

#include <gtest/gtest.h>

#include "hdfs-tool.h"

class HdfsToolBasicTest
    : public testing::TestWithParam<
          std::function<std::unique_ptr<hdfs::tools::HdfsTool>()>> {
protected:
  void SetUp() override { hdfs_tool_ = GetParam()(); }

  std::unique_ptr<hdfs::tools::HdfsTool> hdfs_tool_{nullptr};
};

TEST_P(HdfsToolBasicTest, RunTool) { EXPECT_TRUE(this->hdfs_tool_->Do()); }

class HdfsToolNegativeTest : public HdfsToolBasicTest {};

TEST_P(HdfsToolNegativeTest, RunTool) {
  EXPECT_ANY_THROW({ std::ignore = this->hdfs_tool_->Do(); });
}

template <class T> std::unique_ptr<hdfs::tools::HdfsTool> PassAPath() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("a/b/c");

  static char *argv[] = {exe.data(), arg1.data()};
  return std::make_unique<T>(argc, argv);
}

template <class T> std::unique_ptr<hdfs::tools::HdfsTool> CallHelp() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-h");

  static char *argv[] = {exe.data(), arg1.data()};
  return std::make_unique<T>(argc, argv);
}

template <class T> std::unique_ptr<hdfs::tools::HdfsTool> Pass2Paths() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("a/b/c");
  static std::string arg2("d/e/f");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};
  return std::make_unique<T>(argc, argv);
}

#endif
