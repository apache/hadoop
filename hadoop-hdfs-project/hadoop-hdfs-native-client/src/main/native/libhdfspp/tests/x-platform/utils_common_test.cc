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

TEST(XPlatformUtils, BasenameSpecialFiles) {
  const std::string current_dir_expected(".");
  const auto current_dir_actual = XPlatform::Utils::Basename(".");
  EXPECT_EQ(current_dir_expected, current_dir_actual);

  const std::string parent_dir_expected("..");
  const auto parent_dir_actual = XPlatform::Utils::Basename("..");
  EXPECT_EQ(parent_dir_expected, parent_dir_actual);
}
