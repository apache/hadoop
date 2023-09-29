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

#include <gtest/gtest.h>

#include <string>

#include "x-platform/utils.h"

TEST(XPlatformUtils, BasenameRoot) {
  const std::string win_expected(R"(\)");
  const auto win_actual = XPlatform::Utils::Basename(R"(\)");
  EXPECT_EQ(win_expected, win_actual);
}

TEST(XPlatformUtils, BasenameRootLabel) {
  const std::string win_expected(R"(C:\)");
  const auto win_actual = XPlatform::Utils::Basename(R"(C:\)");
  EXPECT_EQ(win_expected, win_actual);
}

TEST(XPlatformUtils, BasenameTrailingSlash) {
  const std::string expected("def");
  const std::string win_path(R"(C:\abc\def\)");
  const auto win_actual = XPlatform::Utils::Basename(win_path);
  EXPECT_EQ(expected, win_actual);
}

TEST(XPlatformUtils, BasenameBasic) {
  const std::string expected("def");
  const std::string win_path(R"(C:\abc\def)");
  const auto win_actual = XPlatform::Utils::Basename(win_path);
  EXPECT_EQ(expected, win_actual);
}
