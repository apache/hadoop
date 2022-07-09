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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_set>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "dirent_test.h"
#include "x-platform/dirent.h"
#include "x-platform/syscall.h"

void DirentTest::SetUp() {
  tmp_root_ = std::filesystem::temp_directory_path() / GetTempName();
  if (!std::filesystem::create_directories(tmp_root_)) {
    std::stringstream err_msg;
    err_msg << "Unable to create temp directory " << tmp_root_.string();
    throw std::runtime_error(err_msg.str());
  }
}

void DirentTest::TearDown() { std::filesystem::remove_all(tmp_root_); }

std::string DirentTest::GetTempName(const std::string &pattern) const {
  std::vector pattern_raw(pattern.begin(), pattern.end());
  if (!XPlatform::Syscall::CreateTempDir(pattern_raw)) {
    std::stringstream err_msg;
    err_msg << "Creating temp dir failed" << std::endl;
    throw std::runtime_error(err_msg.str());
  }

  std::string tmp_dir_path(pattern_raw.data());
  std::filesystem::remove(tmp_dir_path);
  return tmp_dir_path;
}

std::unordered_set<std::string>
DirentTest::CreateTempDirAndFiles(std::size_t num_dirs,
                                  std::size_t num_files) const {
  std::unordered_set<std::string> paths;
  for (std::size_t i = 0; i < num_dirs; ++i) {
    const auto tmp_dir_absolute_path = tmp_root_ / std::to_string(i);
    if (std::error_code err;
        !std::filesystem::create_directories(tmp_dir_absolute_path, err)) {
      std::stringstream err_msg;
      err_msg << "Unable to create the temp dir "
              << tmp_dir_absolute_path.string() << " reason: " << err.message();
      throw std::runtime_error(err_msg.str());
    }
    paths.emplace(tmp_dir_absolute_path.string());
  }

  for (std::size_t i = 0; i < num_files; ++i) {
    std::stringstream tmp_filename;
    tmp_filename << i << ".txt";
    const auto tmp_file_absolute_path = tmp_root_ / tmp_filename.str();
    std::ofstream{tmp_file_absolute_path};
    paths.emplace(tmp_file_absolute_path.string());
  }
  return paths;
}

std::unordered_set<std::string>
DirentTest::ListDirAndFiles(const std::string &path) const {
  std::unordered_set<std::string> paths;

  XPlatform::Dirent dirent(path);
  for (auto dir_entry = dirent.NextFile();
       !std::holds_alternative<std::monostate>(dir_entry);
       dir_entry = dirent.NextFile()) {
    if (std::holds_alternative<std::error_code>(dir_entry)) {
      std::stringstream err_msg;
      const auto err = std::get<std::error_code>(dir_entry);
      err_msg << "Error in listing directory " << path
              << " cause: " << err.message();
      throw std::runtime_error(err_msg.str());
    }

    if (std::holds_alternative<std::filesystem::directory_entry>(dir_entry)) {
      const auto entry = std::get<std::filesystem::directory_entry>(dir_entry);
      paths.emplace(entry.path().string());
    }
  }
  return paths;
}

TEST_F(DirentTest, TestEmptyFolder) {
  const auto expected = CreateTempDirAndFiles(0, 0);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentTest, TestOneFolder) {
  const auto expected = CreateTempDirAndFiles(1, 0);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentTest, TestOneFile) {
  const auto expected = CreateTempDirAndFiles(0, 1);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentTest, TestMultipleFolders) {
  const auto expected = CreateTempDirAndFiles(10, 0);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentTest, TestMultipleFiles) {
  const auto expected = CreateTempDirAndFiles(0, 10);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentTest, TestOneFileAndFolder) {
  const auto expected = CreateTempDirAndFiles(1, 1);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}

TEST_F(DirentTest, TestMultipleFilesAndFolders) {
  const auto expected = CreateTempDirAndFiles(10, 10);
  const auto actual = ListDirAndFiles(tmp_root_.string());
  EXPECT_EQ(expected, actual);
}
