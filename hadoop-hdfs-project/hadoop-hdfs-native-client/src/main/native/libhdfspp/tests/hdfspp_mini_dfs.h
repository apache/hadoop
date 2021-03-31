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

#include "hdfs/hdfs.h"
#include "hdfspp/hdfspp.h"
#include <native_mini_dfs.h>

#include <google/protobuf/io/coded_stream.h>
#include <gmock/gmock.h>

#include <string>
#include <atomic>

#define TO_STR_HELPER(X) #X
#define TO_STR(X) TO_STR_HELPER(X)

#define TEST_BLOCK_SIZE 134217728

namespace hdfs {


static std::atomic<int> dirnum;
static std::atomic<int> filenum;


class FSHandle {
public:
  FSHandle() : fs(nullptr) {}
  FSHandle(FileSystem * fs_in) : fs(fs_in) {}


  FileSystem * handle()       { return fs.get(); }
  operator     FileSystem *() { return fs.get(); }
protected:
  std::shared_ptr<FileSystem> fs;
};


/**
 * For tests going through the C API to libhdfs++
 */

class HdfsHandle {
public:
    HdfsHandle() : fs(nullptr) {
    }

    HdfsHandle(hdfsFS fs_in) : fs(fs_in) {
    }

    ~HdfsHandle () {
      if (fs)  {
        EXPECT_EQ(0, hdfsDisconnect(fs));
      }
    }

  std::string newDir(const std::string & parent_dir = "/") {
    int newDirNum = dirnum++;

    std::string path = parent_dir;
    if (path.back() != '/')
      path += "/";
    path += "dir" + std::to_string(newDirNum) + "/";

    EXPECT_EQ(0, hdfsCreateDirectory(*this, path.c_str()));
    return path;
  }

  std::string newFile(const std::string & dir = "/", size_t size = 1024) {
    int newFileNum = filenum++;

    std::string path = dir;
    if (path.back() != '/')
      path += "/";
    path += "file" + std::to_string(newFileNum);

    hdfsFile file = hdfsOpenFile(*this, path.c_str(), O_WRONLY, 0, 0, 0);
    EXPECT_NE(nullptr, file);
    void * buf = malloc(size);
    explicit_bzero(buf, size);
    EXPECT_EQ(1024, hdfsWrite(*this, file, buf, size));
    EXPECT_EQ(0, hdfsCloseFile(*this, file));
    free(buf);

    return path;
  }

  std::string newFile(size_t size) {
    return newFile("/", size);
  }

  hdfsFS   handle() { return fs; }
  operator hdfsFS() { return fs; }
private:
  hdfsFS fs;
};


class MiniCluster  {
public:
  MiniCluster() : io_service(IoService::MakeShared()) {
    struct NativeMiniDfsConf conf = {
        1, /* doFormat */
        0, /* webhdfs */
        -1, /* webhdfs port */
        1  /* shortcircuit */
    };
    clusterInfo = nmdCreate(&conf);
    EXPECT_NE(nullptr, clusterInfo);
    EXPECT_EQ(0, nmdWaitClusterUp(clusterInfo));

    //TODO: Write some files for tests to read/check
  }

  virtual ~MiniCluster() {
    if (clusterInfo) {
        EXPECT_EQ(0, nmdShutdown(clusterInfo));
    }
    nmdFree(clusterInfo);
  }

  // Connect via the C++ API
  FSHandle connect(const std::string username) {
    Options options;

    unsigned int worker_count = io_service->InitDefaultWorkers();
    EXPECT_NE(0, worker_count);

    FileSystem * fs = FileSystem::New(io_service, username, options);
    EXPECT_NE(nullptr, fs);
    FSHandle result(fs);

    tPort port = (tPort)nmdGetNameNodePort(clusterInfo);
    EXPECT_NE(0, port);
    Status status = fs->Connect("localhost", std::to_string(port));
    EXPECT_EQ(true, status.ok());
    return result;
  }

  FSHandle connect() {
    return connect("");
  }

  // Connect via the C API
  HdfsHandle connect_c(const std::string & username) {
    tPort port;
    hdfsFS hdfs;
    struct hdfsBuilder *bld;

    port = (tPort)nmdGetNameNodePort(clusterInfo);
    bld = hdfsNewBuilder();
    EXPECT_NE(nullptr, bld);
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetNameNode(bld, "localhost");
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderConfSetStr(bld, "dfs.block.size",
                          TO_STR(TEST_BLOCK_SIZE));
    hdfsBuilderConfSetStr(bld, "dfs.blocksize",
                          TO_STR(TEST_BLOCK_SIZE));
    if (!username.empty()) {
        hdfsBuilderSetUserName(bld, username.c_str());
    }
    hdfs = hdfsBuilderConnect(bld);
    EXPECT_NE(nullptr, hdfs);

    return HdfsHandle(hdfs);
  }

  // Connect via the C API
  HdfsHandle connect_c() {
    return connect_c("");
  }

protected:
  struct NativeMiniDfsCluster* clusterInfo;
  std::shared_ptr<IoService> io_service;
};

} // namespace
