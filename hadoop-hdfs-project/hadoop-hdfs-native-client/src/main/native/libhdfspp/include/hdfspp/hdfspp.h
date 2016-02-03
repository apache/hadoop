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
#ifndef LIBHDFSPP_HDFSPP_H_
#define LIBHDFSPP_HDFSPP_H_

#include "hdfspp/options.h"
#include "hdfspp/status.h"

#include <functional>
#include <memory>
#include <set>
#include <iostream>

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
 * A node exclusion rule provides a simple way of testing if the
 * client should attempt to connect to a node based on the node's
 * UUID.  The FileSystem and FileHandle use the BadDataNodeTracker
 * by default.  AsyncPreadSome takes an optional NodeExclusionRule
 * that will override the BadDataNodeTracker.
 **/
class NodeExclusionRule {
 public:
  virtual ~NodeExclusionRule(){};
  virtual bool IsBadNode(const std::string &node_uuid) = 0;
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
   *
   * The handler returns the datanode that serves the block and the number of
   * bytes has read.
   **/
  virtual void
  PositionRead(void *buf, size_t nbyte, uint64_t offset,
               const std::function<void(const Status &, size_t)> &handler) = 0;

  virtual Status PositionRead(void *buf, size_t *nbyte, off_t offset) = 0;
  virtual Status Read(void *buf, size_t *nbyte) = 0;
  virtual Status Seek(off_t *offset, std::ios_base::seekdir whence) = 0;

  /**
   * Cancel outstanding file operations.  This is not reversable, once called
   * the handle should be disposed of.
   **/
  virtual void CancelOperations(void) = 0;

  /**
   * Determine if a datanode should be excluded from future operations
   * based on the return Status.
   *
   * @param status the Status object returned by InputStream::PositionRead
   * @return true if the status indicates a failure that is not recoverable
   * by the client and false otherwise.
   **/
  static bool ShouldExclude(const Status &status);

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
   *
   * If user_name is blank, the current user will be used for a default.
   **/
  static FileSystem * New(
      IoService *&io_service, const std::string &user_name, const Options &options);

  virtual void Connect(const std::string &server,
      const std::string &service,
      const std::function<void(const Status &, FileSystem *)> &&handler) = 0;

  /* Synchronous call of Connect */
  virtual Status Connect(const std::string &server,
      const std::string &service) = 0;

  /**
   * Open a file on HDFS. The call issues an RPC to the NameNode to
   * gather the locations of all blocks in the file and to return a
   * new instance of the @ref InputStream object.
   **/
  virtual void
  Open(const std::string &path,
       const std::function<void(const Status &, FileHandle *)> &handler) = 0;
  virtual Status Open(const std::string &path, FileHandle **handle) = 0;

  /**
   * Note that it is an error to destroy the filesystem from within a filesystem
   * callback.  It will lead to a deadlock and the termination of the process.
   */
  virtual ~FileSystem() {};

};
}

#endif
