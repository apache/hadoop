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

#include <cassert>
#include <cerrno>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

#include "x-platform/c-api/dirent.h"
#include "x-platform/c-api/dirent_test.h"

std::unordered_set<std::string>
DirentCApiTest::ListDirAndFiles(const std::string &path) const {
  std::stringstream err_msg;
  std::unordered_set<std::string> paths;

  DIR *dir = opendir(path.c_str());
  if (dir == nullptr) {
    err_msg << "Unable to open directory " << path;
    throw std::runtime_error(err_msg.str());
  }

  errno = 0;
  for (struct dirent *file; (file = readdir(dir)) != nullptr; errno = 0) {
    std::filesystem::path absolute_path{path};
    absolute_path = absolute_path / file->d_name;
    paths.emplace(absolute_path.string());
  }

  if (errno != 0) {
    err_msg << "Expected errno to be 0, instead it is " << errno;
    throw std::runtime_error(err_msg.str());
  }

  if (const auto result = closedir(dir); result != 0) {
    err_msg << "Expected the return value of closedir() to be 0, instead it is "
            << result;
    throw std::runtime_error(err_msg.str());
  }
  return paths;
}

TEST_F(DirentCApiTest, TestEmptyFolder) {
  const auto expected = CreateTempDirAndFiles(0, 0);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentCApiTest, TestOneFolder) {
  const auto expected = CreateTempDirAndFiles(1, 0);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentCApiTest, TestOneFile) {
  const auto expected = CreateTempDirAndFiles(0, 1);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentCApiTest, TestMultipleFolders) {
  const auto expected = CreateTempDirAndFiles(10, 0);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentCApiTest, TestMultipleFiles) {
  const auto expected = CreateTempDirAndFiles(0, 10);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentCApiTest, TestOneFileAndFolder) {
  const auto expected = CreateTempDirAndFiles(1, 1);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentCApiTest, TestMultipleFilesAndFolders) {
  const auto expected = CreateTempDirAndFiles(10, 10);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}
