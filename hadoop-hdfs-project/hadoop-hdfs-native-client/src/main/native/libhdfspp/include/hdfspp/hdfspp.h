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
#include "hdfspp/events.h"
#include "hdfspp/block_location.h"
#include "hdfspp/statinfo.h"
#include "hdfspp/fsinfo.h"

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


  /**
   * Sets an event callback for file-level event notifications (such as connecting
   * to the DataNode, communications errors, etc.)
   *
   * Many events are defined in hdfspp/events.h; the consumer should also expect
   * to be called with many private events, which can be ignored.
   *
   * @param callback The function to call when a reporting event occurs.
   */
  virtual void SetFileEventCallback(file_event_callback callback) = 0;

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
      const std::function<void(const Status &, FileSystem *)> &handler) = 0;

  /* Synchronous call of Connect */
  virtual Status Connect(const std::string &server,
      const std::string &service) = 0;


  /**
   * Connects to the hdfs instance indicated by the defaultFs value of the
   * Options structure.
   *
   * If no defaultFs is defined, returns an error.
   */
  virtual void ConnectToDefaultFs(
      const std::function<void(const Status &, FileSystem *)> &handler) = 0;
  virtual Status ConnectToDefaultFs() = 0;

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
   * Returns metadata about the file if the file/directory exists.
   **/
  virtual void
  GetFileInfo(const std::string &path,
                  const std::function<void(const Status &, const StatInfo &)> &handler) = 0;
  virtual Status GetFileInfo(const std::string &path, StatInfo & stat_info) = 0;

  /**
   * Retrieves the file system information as a whole, such as the total raw size of all files in the filesystem
   * and the raw capacity of the filesystem
   *
   *  @param FsInfo      struct to be populated by GetFsStats
   **/
  virtual void GetFsStats(
      const std::function<void(const Status &, const FsInfo &)> &handler) = 0;
  virtual Status GetFsStats(FsInfo & fs_info) = 0;

  /**
   * Retrieves the files contained in a directory and returns the metadata
   * for each of them.
   *
   * The asynchronous method will return batches of files; the consumer must
   * return true if they want more files to be delivered.  The final bool
   * parameter in the callback will be set to true if this is the final
   * batch of files.
   *
   * The synchronous method will return all files in the directory.
   *
   * Path must be an absolute path in the hdfs filesytem (e.g. /tmp/foo/bar)
   **/
  virtual void
  GetListing(const std::string &path,
                  const std::function<bool(const Status &, std::shared_ptr<std::vector<StatInfo>> &, bool)> &handler) = 0;
  virtual Status GetListing(const std::string &path,
                            std::shared_ptr<std::vector<StatInfo>> & stat_infos) = 0;

  /**
   * Returns the locations of all known blocks for the indicated file, or an error
   * if the information clould not be found
   */
  virtual void GetBlockLocations(const std::string & path,
    const std::function<void(const Status &, std::shared_ptr<FileBlockLocation> locations)> ) = 0;
  virtual Status GetBlockLocations(const std::string & path,
    std::shared_ptr<FileBlockLocation> * locations) = 0;

  /**
   * Creates a new directory
   *
   *  @param path           Path to the directory to be created (must be non-empty)
   *  @param permissions    Permissions for the new directory   (negative value for the default permissions)
   *  @param createparent   Create parent directories if they do not exist (may not be empty)
   */
  virtual void Mkdirs(const std::string & path, long permissions, bool createparent,
      std::function<void(const Status &)> handler) = 0;
  virtual Status Mkdirs(const std::string & path, long permissions, bool createparent) = 0;

  /**
   *  Delete the given file or directory from the file system.
   *  <p>
   *  same as delete but provides a way to avoid accidentally
   *  deleting non empty directories programmatically.
   *  @param path existing name (must be non-empty)
   *  @param recursive if true deletes a non empty directory recursively
   */
  virtual void Delete(const std::string &path, bool recursive,
      const std::function<void(const Status &)> &handler) = 0;
  virtual Status Delete(const std::string &path, bool recursive) = 0;

  /**
   *  Rename - Rename file.
   *  @param oldPath The path of the source file.       (must be non-empty)
   *  @param newPath The path of the destination file.  (must be non-empty)
   *  @return Returns 0 on success, -1 on error.
   */
  virtual void Rename(const std::string &oldPath, const std::string &newPath,
      const std::function<void(const Status &)> &handler) = 0;
  virtual Status Rename(const std::string &oldPath, const std::string &newPath) = 0;

  /*****************************************************************************
   *                    FILE SYSTEM SNAPSHOT FUNCTIONS
   ****************************************************************************/

  /**
   * Creates a snapshot of a snapshottable directory specified by path
   *
   *  @param path    Path to the directory to be snapshotted (must be non-empty)
   *  @param name    Name to be given to the created snapshot (may be empty)
   **/
  virtual void CreateSnapshot(const std::string &path, const std::string &name,
      const std::function<void(const Status &)> &handler) = 0;
  virtual Status CreateSnapshot(const std::string &path,
      const std::string &name) = 0;

  /**
   * Deletes the directory snapshot specified by path and name
   *
   *  @param path    Path to the snapshotted directory (must be non-empty)
   *  @param name    Name of the snapshot to be deleted (must be non-empty)
   **/
  virtual void DeleteSnapshot(const std::string &path, const std::string &name,
      const std::function<void(const Status &)> &handler) = 0;
  virtual Status DeleteSnapshot(const std::string &path,
      const std::string &name) = 0;

  /**
   * Allows snapshots to be made on the specified directory
   *
   *  @param path    Path to the directory to be made snapshottable (must be non-empty)
   **/
  virtual void AllowSnapshot(const std::string &path,
      const std::function<void(const Status &)> &handler) = 0;
  virtual Status AllowSnapshot(const std::string &path) = 0;

  /**
   * Disallows snapshots to be made on the specified directory
   *
   *  @param path    Path to the directory to be made non-snapshottable (must be non-empty)
   **/
  virtual void DisallowSnapshot(const std::string &path,
      const std::function<void(const Status &)> &handler) = 0;
  virtual Status DisallowSnapshot(const std::string &path) = 0;

  /**
   * Note that it is an error to destroy the filesystem from within a filesystem
   * callback.  It will lead to a deadlock and the termination of the process.
   */
  virtual ~FileSystem() {};


  /**
   * Sets an event callback for fs-level event notifications (such as connecting
   * to the NameNode, communications errors with the NN, etc.)
   *
   * Many events are defined in hdfspp/events.h; the consumer should also expect
   * to be called with many private events, which can be ignored.
   *
   * @param callback The function to call when a reporting event occurs.
   */
  virtual void SetFsEventCallback(fs_event_callback callback) = 0;
};
}

#endif
