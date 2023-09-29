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

#include <numeric>
#include <string>
#include <vector>

#include "x-platform/syscall.h"

TEST(XPlatformSyscall, FnMatchBasicAsterisk) {
  const std::string pattern("a*.doc");
  const std::string str("abcd.doc");
  EXPECT_TRUE(XPlatform::Syscall::FnMatch(pattern, str));
}

TEST(XPlatformSyscall, FnMatchBasicQuestionMark) {
  const std::string pattern("a?.doc");
  const std::string str("ab.doc");
  EXPECT_TRUE(XPlatform::Syscall::FnMatch(pattern, str));
}

TEST(XPlatformSyscall, FnMatchNegativeAsterisk) {
  const std::string pattern("a*.doc");
  const std::string str("bcd.doc");
  EXPECT_FALSE(XPlatform::Syscall::FnMatch(pattern, str));
}

TEST(XPlatformSyscall, FnMatchNegativeQuestionMark) {
  const std::string pattern("a?.doc");
  const std::string str("abc.doc");
  EXPECT_FALSE(XPlatform::Syscall::FnMatch(pattern, str));
}

TEST(XPlatformSyscall, ClearBufferSafelyChars) {
  std::vector<char> alphabets(26);
  std::iota(alphabets.begin(), alphabets.end(), 'a');

  XPlatform::Syscall::ClearBufferSafely(alphabets.data(), alphabets.size());
  for (const auto alphabet : alphabets) {
    EXPECT_EQ(alphabet, '\0');
  }
}

TEST(XPlatformSyscall, ClearBufferSafelyNumbers) {
  std::vector<int> numbers(200);
  std::iota(numbers.begin(), numbers.end(), 0);

  XPlatform::Syscall::ClearBufferSafely(numbers.data(),
                                        numbers.size() * sizeof(int));
  for (const auto number : numbers) {
    EXPECT_EQ(number, 0);
  }
}

TEST(XPlatformSyscall, StringCompareIgnoreCaseBasic) {
  EXPECT_TRUE(XPlatform::Syscall::StringCompareIgnoreCase("aBcDeF", "AbCdEf"));
  EXPECT_TRUE(XPlatform::Syscall::StringCompareIgnoreCase("a1B2c3D4e5F",
                                                          "A1b2C3d4E5f"));
  EXPECT_TRUE(XPlatform::Syscall::StringCompareIgnoreCase(
      "a!1@B#2$c%3^D&4*e(5)F", "A!1@b#2$C%3^d&4*E(5)f"));
  EXPECT_TRUE(XPlatform::Syscall::StringCompareIgnoreCase(
      "a<!>1@B#2$c%3^D&4*e(5)F?:", "A<!>1@b#2$C%3^d&4*E(5)f?:"));
  EXPECT_TRUE(XPlatform::Syscall::StringCompareIgnoreCase("12345", "12345"));
  EXPECT_TRUE(XPlatform::Syscall::StringCompareIgnoreCase("", ""));
}

TEST(XPlatformSyscall, StringCompareIgnoreCaseNegative) {
  EXPECT_FALSE(XPlatform::Syscall::StringCompareIgnoreCase("abcd", "abcde"));
  EXPECT_FALSE(XPlatform::Syscall::StringCompareIgnoreCase("12345", "abcde"));
}

TEST(XPlatformSyscall, CreateAndOpenTempFileBasic) {
  std::string pattern("tmp-XXXXXX");
  std::vector<char> pattern_vec(pattern.begin(), pattern.end());

  const auto fd = XPlatform::Syscall::CreateAndOpenTempFile(pattern_vec);
  EXPECT_GT(fd, -1);
  EXPECT_TRUE(XPlatform::Syscall::CloseFile(fd));
}

TEST(XPlatformSyscall, CreateAndOpenTempFileNegative) {
  std::string pattern("does-not-adhere-to-pattern");
  std::vector<char> pattern_vec(pattern.begin(), pattern.end());

  const auto fd = XPlatform::Syscall::CreateAndOpenTempFile(pattern_vec);
  EXPECT_EQ(fd, -1);
  EXPECT_FALSE(XPlatform::Syscall::CloseFile(fd));
}

TEST(XPlatformSyscall, CreateTempDirBasic) {
  std::string pattern("/tmp/tmp-XXXXXX");
  std::vector<char> pattern_vec(pattern.begin(), pattern.end());
  EXPECT_TRUE(XPlatform::Syscall::CreateTempDir(pattern_vec));
}

TEST(XPlatformSyscall, CreateTempDirNegative) {
  std::string pattern("does-not-adhere-to-pattern");
  std::vector<char> pattern_vec(pattern.begin(), pattern.end());
  EXPECT_FALSE(XPlatform::Syscall::CreateTempDir(pattern_vec));
}
