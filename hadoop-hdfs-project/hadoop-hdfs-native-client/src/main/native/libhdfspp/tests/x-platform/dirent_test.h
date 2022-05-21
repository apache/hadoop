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

#ifndef LIBHDFSPP_CROSS_PLATFORM_DIRENT_TEST
#define LIBHDFSPP_CROSS_PLATFORM_DIRENT_TEST

#include <filesystem>
#include <string>
#include <unordered_set>

#include <gtest/gtest.h>

class DirentTest : public ::testing::Test {
protected:
  void SetUp() override;
  void TearDown() override;

  [[nodiscard]] std::string CreateTempDir(const std::string &pattern) const;

  [[nodiscard]] std::unordered_set<std::string>
  CreateTempDirAndFiles(std::size_t num_dirs, std::size_t num_files) const;

  [[nodiscard]] std::unordered_set<std::string>
  ListDirAndFiles(const std::string &path) const;

  std::filesystem::path tmp_root_;
};

#endif