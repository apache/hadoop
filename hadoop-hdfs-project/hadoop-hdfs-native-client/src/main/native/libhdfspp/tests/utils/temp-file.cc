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

#include <gtest/gtest.h>

#include "utils/temp-file.h"
#include "x-platform/syscall.h"

namespace TestUtils {
TempFile::TempFile() {
  std::vector tmp_buf(filename_.begin(), filename_.end());
  fd_ = XPlatform::Syscall::CreateAndOpenTempFile(tmp_buf);
  EXPECT_NE(fd_, -1);
  filename_.assign(tmp_buf.data());
}

TempFile::TempFile(std::string filename) : filename_(std::move(filename)) {}

TempFile::TempFile(TempFile &&other) noexcept
    : filename_{std::move(other.filename_)}, fd_{other.fd_} {}

TempFile &TempFile::operator=(const TempFile &other) {
  if (&other != this) {
    filename_ = other.filename_;
    fd_ = other.fd_;
  }
  return *this;
}

TempFile &TempFile::operator=(TempFile &&other) noexcept {
  if (&other != this) {
    filename_ = std::move(other.filename_);
    fd_ = other.fd_;
  }
  return *this;
}

TempFile::~TempFile() {
  if (-1 != fd_) {
    EXPECT_NE(XPlatform::Syscall::CloseFile(fd_), -1);
  }

  unlink(filename_.c_str());
}
} // namespace TestUtils
