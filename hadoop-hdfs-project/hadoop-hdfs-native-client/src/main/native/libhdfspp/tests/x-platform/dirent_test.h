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

#include "x-platform/dirent.h"

/**
 * Test fixture for testing {@link XPlatform::Dirent}.
 */
class DirentTest : public ::testing::Test {
protected:
  void SetUp() override;
  void TearDown() override;

  /**
   * Gets a name for creating temporary file or folder. This also ensures that
   * the temporary file or folder does not exist.
   *
   * @param pattern The pattern to use for naming the temporary directory.
   * @return The temporary file or folder name that can be used for creating the
   * same.
   */
  [[nodiscard]] std::string
  GetTempName(const std::string &pattern = "test_XXXXXX") const;

  /**
   * Creates the given number of temporary files and directories under the
   * {@link DirentTest#tmp_root_}.
   *
   * @param num_dirs The number of temporary directories to create.
   * @param num_files The number of temporary files to create.
   * @return An {@link std::unordered_set> of the absolute paths of all the
   * temporary files and folders that were created.
   */
  [[nodiscard]] std::unordered_set<std::string>
  CreateTempDirAndFiles(std::size_t num_dirs, std::size_t num_files) const;

  /**
   * Lists all the children of the given path.
   *
   * @param path The path whose children must be listed.
   * @return An {@link std::unordered_set} containing the absolute paths of all
   * the children of the given path.
   */
  [[nodiscard]] virtual std::unordered_set<std::string>
  ListDirAndFiles(const std::string &path) const;

  /**
   * The root in temp folder under which the rest of all the temporary files and
   * folders will be created for the purpose of testing.
   */
  std::filesystem::path tmp_root_;
};

#endif