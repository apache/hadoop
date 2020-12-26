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

#include "x-platform/utils.h"

#include <gtest/gtest.h>

#include <string>

TEST(XPlatformUtils, BasenameEmpty) {
  const std::string expected(".");
  const auto actual = XPlatform::Utils::Basename("");
  EXPECT_EQ(expected, actual);
}

TEST(XPlatformUtils, BasenameRelativePath) {
  const std::string expected("x");
  const auto actual = XPlatform::Utils::Basename("x");
  EXPECT_EQ(expected, actual);
}

TEST(XPlatformUtils, BasenameRoot) {
  const std::string win_expected(R"(\)");
  const auto win_actual_1 = XPlatform::Utils::Basename(R"(\)");
  EXPECT_EQ(win_expected, win_actual_1);

  const auto win_actual_2 = XPlatform::Utils::Basename(R"(C:\)");
  EXPECT_EQ(win_expected, win_actual_2);

  const std::string nix_expected("/");
  const auto nix_actual = XPlatform::Utils::Basename("/");
  EXPECT_EQ(nix_expected, nix_actual);
}

TEST(XPlatformUtils, BasenameSpecialFiles) {
  const std::string current_dir_expected(".");
  const auto current_dir_actual = XPlatform::Utils::Basename(".");
  EXPECT_EQ(current_dir_expected, current_dir_actual);

  const std::string parent_dir_expected("..");
  const auto parent_dir_actual = XPlatform::Utils::Basename("..");
  EXPECT_EQ(parent_dir_expected, parent_dir_actual);
}

TEST(XPlatformUtils, BasenameTrailingSlash) {
  const std::string expected("def");

  const std::string win_path(R"(C:\abc\def\)");
  const auto win_actual = XPlatform::Utils::Basename(win_path);
  EXPECT_EQ(expected, win_actual);

  const std::string nix_path("/abc/def/");
  const auto nix_actual = XPlatform::Utils::Basename(nix_path);
  EXPECT_EQ(expected, nix_actual);
}

TEST(XPlatformUtils, BasenameBasic) {
  const std::string expected("def");

  const std::string win_path(R"(C:\abc\def)");
  const auto win_actual = XPlatform::Utils::Basename(win_path);
  EXPECT_EQ(expected, win_actual);

  const std::string nix_path("/abc/def");
  const auto nix_actual = XPlatform::Utils::Basename(nix_path);
  EXPECT_EQ(expected, nix_actual);
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
