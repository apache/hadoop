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
#ifndef TESTS_CONFIGURATION_H_
#define TESTS_CONFIGURATION_H_

#include "hdfspp/config_parser.h"
#include "common/configuration.h"
#include "common/configuration_loader.h"
#include "x-platform/syscall.h"

#include <cstdio>
#include <fstream>
#include <istream>
#include <string>
#include <utility>
#include <vector>

#include <ftw.h>
#include <unistd.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace hdfs {

template <typename T, typename U>
void simpleConfigStreamProperty(std::stringstream& out, T key, U value) {
  out << "<property>"
      << "<name>" << key << "</name>"
      << "<value>" << value << "</value>"
      << "</property>";
}

template <typename T, typename U, typename... Args>
void simpleConfigStreamProperty(std::stringstream& out, T key, U value,
                                Args... args) {
  simpleConfigStreamProperty(out, key, value);
  simpleConfigStreamProperty(out, args...);
}

template <typename... Args>
void simpleConfigStream(std::stringstream& out, Args... args) {
  out << "<configuration>";
  simpleConfigStreamProperty(out, args...);
  out << "</configuration>";
}

template <typename T, typename U>
void damagedConfigStreamProperty(std::stringstream& out, T key, U value) {
  out << "<propertyy>"
      << "<name>" << key << "</name>"
      << "<value>" << value << "</value>"
      << "</property>";
}

template <typename T, typename U, typename... Args>
void damagedConfigStreamProperty(std::stringstream& out, T key, U value,
                                Args... args) {
  damagedConfigStreamProperty(out, key, value);
  damagedConfigStreamProperty(out, args...);
}

template <typename... Args>
void damagedConfigStream(std::stringstream& out, Args... args) {
  out << "<configuration>";
  damagedConfigStreamProperty(out, args...);
  out << "</configuration>";
}

template <typename... Args>
optional<Configuration> simpleConfig(Args... args) {
  std::stringstream stream;
  simpleConfigStream(stream, args...);
  ConfigurationLoader config_loader;
  config_loader.ClearSearchPath();
  optional<Configuration> parse = config_loader.Load<Configuration>(stream.str());
  EXPECT_TRUE((bool)parse);

  return parse;
}

template <typename... Args>
void writeSimpleConfig(const std::string& filename, Args... args) {
  std::stringstream stream;
  simpleConfigStream(stream, args...);

  std::ofstream out;
  out.open(filename);
  out << stream.rdbuf();
}

template <typename... Args>
void writeDamagedConfig(const std::string& filename, Args... args) {
  std::stringstream stream;
  damagedConfigStream(stream, args...);

  std::ofstream out;
  out.open(filename);
  out << stream.rdbuf();
}

// TempDir: is deleted on destruction
class TempFile {
 public:
  TempFile() {
    std::vector<char> tmp_buf(filename_.begin(), filename_.end());
    fd_ = XPlatform::Syscall::CreateAndOpenTempFile(tmp_buf);
    EXPECT_NE(fd_, -1);
    filename_.assign(tmp_buf.data());
  }

  TempFile(std::string fn) : filename_(std::move(fn)) {}

  TempFile(const TempFile& other) = default;

  TempFile(TempFile&& other) noexcept
      : filename_{std::move(other.filename_)}, fd_{other.fd_} {}

  TempFile& operator=(const TempFile& other) {
    if (&other != this) {
      filename_ = other.filename_;
      fd_ = other.fd_;
    }
    return *this;
  }

  TempFile& operator=(TempFile&& other) noexcept {
    if (&other != this) {
      filename_ = std::move(other.filename_);
      fd_ = other.fd_;
    }
    return *this;
  }

  [[nodiscard]] const std::string& GetFileName() const { return filename_; }

  ~TempFile() {
    if (-1 != fd_) {
      EXPECT_NE(XPlatform::Syscall::CloseFile(fd_), -1);
    }

    unlink(filename_.c_str());
  }

 private:
  std::string filename_{"/tmp/test_XXXXXXXXXX"};
  int fd_{-1};
};

// Callback to remove a directory in the nftw visitor
int nftw_remove(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
  (void)sb; (void)typeflag; (void)ftwbuf;

  int rv = remove(fpath);
  EXPECT_EQ(0, rv);
  return rv;
}

// TempDir: is created in ctor and recursively deletes in dtor
class TempDir {
public:
  std::string path;
  TempDir() {
    char        fn_buffer[128];
    strncpy(fn_buffer, "/tmp/test_dir_XXXXXXXXXX", sizeof(fn_buffer));
    const char * returned_path = mkdtemp(fn_buffer);
    EXPECT_NE(nullptr, returned_path);
    path = returned_path;
  }
  ~TempDir() {
    if(!path.empty())
      nftw(path.c_str(), nftw_remove, 64, FTW_DEPTH | FTW_PHYS);
  }
};


}

#endif
