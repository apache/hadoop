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

#ifndef LIBHDFSPP_BINDINGS_HDFSCPP_H
#define LIBHDFSPP_BINDINGS_HDFSCPP_H

#include <cstdint>
#include <thread>
#include <vector>
#include <mutex>
#include <chrono>
#include <iostream>

#include "libhdfspp/hdfs.h"
#include "fs/bad_datanode_tracker.h"
#include <hdfs/hdfs.h>

namespace hdfs {

/**
 * Implement a very simple 'it just works' interface in C++
 * that provides posix-like file operations + extra stuff for hadoop.
 * Then provide very thin C wrappers over each method.
 */

class HadoopFileSystem;

class FileHandle {
 public:
  virtual ~FileHandle(){};
  /**
   * Note:  The nbyte argument for Read and Pread as well as the
   * offset argument for Seek are in/out parameters.
   *
   * For Read and Pread the value referenced by nbyte should
   * be set to the number of bytes to read. Before returning
   * the value referenced will be set by the callee to the number
   * of bytes that was successfully read.
   *
   * For Seek the value referenced by offset should be the number
   * of bytes to shift from the specified whence position.  The
   * referenced value will be set to the new offset before returning.
   **/
  Status Pread(void *buf, size_t *nbyte, off_t offset);
  Status Read(void *buf, size_t *nbyte);
  Status Seek(off_t *offset, std::ios_base::seekdir whence);
  bool IsOpenForRead();

 private:
  /* handle should only be created by fs */
  friend class HadoopFileSystem;
  FileHandle(InputStream *is);
  bool CheckSeekBounds(ssize_t desired_position);
  std::unique_ptr<InputStream> input_stream_;
  off_t offset_;
};

class HadoopFileSystem {
 public:
  HadoopFileSystem() : service_(IoService::New()) {}
  virtual ~HadoopFileSystem();

  /* attempt to connect to namenode, return false on failure */
  Status Connect(const char *nn, tPort port, unsigned int threads = 1);

  /* how many worker threads are servicing asio requests */
  int WorkerThreadCount() { return worker_threads_.size(); }

  /* add a new thread to handle asio requests, return number of threads in pool
   */
  int AddWorkerThread();

  Status OpenFileForRead(const std::string &path, FileHandle **handle);

 private:
  std::unique_ptr<IoService> service_;
  /* std::thread needs to join before deletion */
  struct WorkerDeleter {
    void operator()(std::thread *t) {
      t->join();
      delete t;
    }
  };
  typedef std::unique_ptr<std::thread, WorkerDeleter> WorkerPtr;
  std::vector<WorkerPtr> worker_threads_;
  std::unique_ptr<FileSystem> file_system_;
};
}

#endif
