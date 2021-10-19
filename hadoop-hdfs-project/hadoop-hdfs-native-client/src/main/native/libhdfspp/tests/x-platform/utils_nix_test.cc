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
  const std::string nix_expected("/");
  const auto nix_actual = XPlatform::Utils::Basename("/");
  EXPECT_EQ(nix_expected, nix_actual);
}

TEST(XPlatformUtils, BasenameTrailingSlash) {
  const std::string expected("def");
  const std::string nix_path("/abc/def/");
  const auto nix_actual = XPlatform::Utils::Basename(nix_path);
  EXPECT_EQ(expected, nix_actual);
}

TEST(XPlatformUtils, BasenameBasic) {
  const std::string expected("def");
  const std::string nix_path("/abc/def");
  const auto nix_actual = XPlatform::Utils::Basename(nix_path);
  EXPECT_EQ(expected, nix_actual);
}
