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

#include <string>
#include <vector>

#include <ftw.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include "utils/temp-dir.h"
#include "x-platform/syscall.h"

namespace TestUtils {
/*
 * Callback to remove a directory in the nftw visitor.
 */
int nftw_remove(const char *fpath, const struct stat *sb, int typeflag,
                struct FTW *ftwbuf);

TempDir::TempDir() {
  std::vector<char> path_pattern(path_.begin(), path_.end());
  is_path_init_ = XPlatform::Syscall::CreateTempDir(path_pattern);
  EXPECT_TRUE(is_path_init_);
  path_.assign(path_pattern.data());
}

TempDir::TempDir(TempDir &&other) noexcept : path_{std::move(other.path_)} {}

TempDir &TempDir::operator=(const TempDir &other) {
  if (&other != this) {
    path_ = other.path_;
  }
  return *this;
}

TempDir &TempDir::operator=(TempDir &&other) noexcept {
  if (&other != this) {
    path_ = std::move(other.path_);
  }
  return *this;
}

TempDir::~TempDir() {
  if (is_path_init_) {
    nftw(path_.c_str(), nftw_remove, 64, FTW_DEPTH | FTW_PHYS);
  }
}

int nftw_remove(const char *fpath, const struct stat *sb, int typeflag,
                FTW *ftwbuf) {
  (void)sb;
  (void)typeflag;
  (void)ftwbuf;

  int rv = remove(fpath);
  EXPECT_EQ(0, rv);
  return rv;
}
} // namespace TestUtils