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

#include "common/configuration.h"
#include "common/configuration_loader.h"
#include <cstdio>
#include <fstream>
#include <istream>
#include <ftw.h>
#include <gmock/gmock.h>

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

template <typename... Args>
optional<Configuration> simpleConfig(Args... args) {
  std::stringstream stream;
  simpleConfigStream(stream, args...);
  optional<Configuration> parse = ConfigurationLoader().Load<Configuration>(stream.str());
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

// TempDir: is deleted on destruction
class TempFile {
public:
  std::string filename;
  char        fn_buffer[128];
  int         tempFileHandle;
  TempFile() : tempFileHandle(-1) {
    strncpy(fn_buffer, "/tmp/test_XXXXXXXXXX", sizeof(fn_buffer));
    tempFileHandle = mkstemp(fn_buffer);
    EXPECT_NE(-1, tempFileHandle);
    filename = fn_buffer;
  }
  TempFile(const std::string & fn) : filename(fn), tempFileHandle(-1) {
    strncpy(fn_buffer, fn.c_str(), sizeof(fn_buffer));
    fn_buffer[sizeof(fn_buffer)-1] = 0;
  }
  ~TempFile() { if(-1 != tempFileHandle) close(tempFileHandle); unlink(fn_buffer); }
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
