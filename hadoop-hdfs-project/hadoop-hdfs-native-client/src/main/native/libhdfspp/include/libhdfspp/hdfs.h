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
#ifndef LIBHDFSPP_HDFS_H_
#define LIBHDFSPP_HDFS_H_

#include "libhdfspp/options.h"
#include "libhdfspp/status.h"
#include "common/cancelable.h"

#include <functional>
#include <set>

namespace hdfs {

/**
 * An IoService manages a queue of asynchronous tasks. All libhdfs++
 * operations are filed against a particular IoService.
 *
 * When an operation is queued into an IoService, the IoService will
 * run the callback handler associated with the operation. Note that
 * the IoService must be stopped before destructing the objects that
 * file the operations.
 *
 * From an implementation point of view the IoService object wraps the
 * ::asio::io_service objects. Please see the related documentation
 * for more details.
 **/
class IoService {
public:
  static IoService *New();
  /**
   * Run the asynchronous tasks associated with this IoService.
   **/
  virtual void Run() = 0;
  /**
   * Stop running asynchronous tasks associated with this IoService.
   **/
  virtual void Stop() = 0;
  virtual ~IoService();
};

/**
 * Applications opens a FileHandle to read files in HDFS.
 **/
class FileHandle {
public:
  /**
   * Read data from a specific position. The current implementation
   * stops at the block boundary.
   *
   * @param buf the pointer to the buffer
   * @param nbyte the size of the buffer
   * @param offset the offset the file
   * @param excluded_datanodes the UUID of the datanodes that should
   * not be used in this read
   *
   * The handler returns the datanode that serves the block and the number of
   * bytes has read.
   **/
  virtual CancelHandle
  PositionRead(void *buf, size_t nbyte, uint64_t offset,
               const std::function<void(const Status &, size_t)> &handler) = 0;

  virtual size_t PositionRead(void *buf, size_t nbyte, off_t offset) = 0;

  virtual ~FileHandle();
};

/**
 * FileSystem implements APIs to interact with HDFS.
 **/
class FileSystem {
public:
  /**
   * Create a new instance of the FileSystem object. The call
   * initializes the RPC connections to the NameNode and returns an
   * FileSystem object.
   **/
  static void
  New(IoService *io_service, const Options &options, const std::string &server,
      const std::string &service,
      const std::function<void(const Status &, FileSystem *)> &handler);

  /* Synchronous call of New*/
  static FileSystem *
  New(IoService *io_service, const Options &options, const std::string &server,
      const std::string &service);

  /**
   * Open a file on HDFS. The call issues an RPC to the NameNode to
   * gather the locations of all blocks in the file and to return a
   * new instance of the @ref InputStream object.
   **/
  virtual void
  Open(const std::string &path,
       const std::function<void(const Status &, FileHandle *)> &handler) = 0;
  virtual Status Open(const std::string &path, FileHandle **handle);

  virtual ~FileSystem() {};

};
}

#endif
