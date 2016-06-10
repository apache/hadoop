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
#ifndef LIBHDFSPP_LIB_FS_FILESYSTEM_H_
#define LIBHDFSPP_LIB_FS_FILESYSTEM_H_

#include "filehandle.h"
#include "hdfspp/hdfspp.h"
#include "fs/bad_datanode_tracker.h"
#include "reader/block_reader.h"
#include "reader/fileinfo.h"

#include "asio.hpp"

#include <thread>
#include "namenode_operations.h"

namespace hdfs {

/*
 * FileSystem: The consumer's main point of interaction with the cluster as
 * a whole.
 *
 * Initially constructed in a disconnected state; call Connect before operating
 * on the FileSystem.
 *
 * All open files must be closed before the FileSystem is destroyed.
 *
 * Threading model: thread-safe for all operations
 * Lifetime: pointer created for consumer who is responsible for deleting it
 */
class FileSystemImpl : public FileSystem {
public:
  MEMCHECKED_CLASS(FileSystemImpl)
  FileSystemImpl(IoService *&io_service, const std::string& user_name, const Options &options);
  ~FileSystemImpl() override;

  /* attempt to connect to namenode, return bad status on failure */
  void Connect(const std::string &server, const std::string &service,
               const std::function<void(const Status &, FileSystem *)> &handler) override;
  /* attempt to connect to namenode, return bad status on failure */
  Status Connect(const std::string &server, const std::string &service) override;

  /* Connect to the NN indicated in options.defaultFs */
  virtual void ConnectToDefaultFs(
      const std::function<void(const Status &, FileSystem *)> &handler) override;
  virtual Status ConnectToDefaultFs() override;

  virtual void Open(const std::string &path,
                    const std::function<void(const Status &, FileHandle *)>
                        &handler) override;
  Status Open(const std::string &path, FileHandle **handle) override;

  void GetFileInfo(
      const std::string &path,
      const std::function<void(const Status &, const StatInfo &)> &handler) override;

  Status GetFileInfo(const std::string &path, StatInfo & stat_info) override;

  /**
   * Retrieves the file system information such as the total raw size of all files in the filesystem
   * and the raw capacity of the filesystem
   *
   *  @param FsInfo      struct to be populated by GetFsStats
   **/
  void GetFsStats(
      const std::function<void(const Status &, const FsInfo &)> &handler) override;

  Status GetFsStats(FsInfo & fs_info) override;

  void GetListing(
        const std::string &path,
        const std::function<bool(const Status &, std::shared_ptr<std::vector<StatInfo>> &, bool)> &handler) override;

  Status GetListing(const std::string &path, std::shared_ptr<std::vector<StatInfo>> &stat_infos) override;

  virtual void GetBlockLocations(const std::string & path,
    const std::function<void(const Status &, std::shared_ptr<FileBlockLocation> locations)> ) override;
  virtual Status GetBlockLocations(const std::string & path,
    std::shared_ptr<FileBlockLocation> * locations) override;


  /*****************************************************************************
   *                    FILE SYSTEM SNAPSHOT FUNCTIONS
   ****************************************************************************/

  /**
   * Creates a snapshot of a snapshottable directory specified by path
   *
   *  @param path    Path to the directory to be snapshotted (must be non-empty)
   *  @param name    Name to be given to the created snapshot (may be empty)
   **/
  void CreateSnapshot(const std::string &path, const std::string &name,
      const std::function<void(const Status &)> &handler) override;
  Status CreateSnapshot(const std::string &path, const std::string &name) override;

  /**
   * Deletes the directory snapshot specified by path and name
   *
   *  @param path    Path to the snapshotted directory (must be non-empty)
   *  @param name    Name of the snapshot to be deleted (must be non-empty)
   **/
  void DeleteSnapshot(const std::string &path, const std::string &name,
        const std::function<void(const Status &)> &handler) override;
  Status DeleteSnapshot(const std::string &path, const std::string &name) override;

  /**
   * Allows snapshots to be made on the specified directory
   *
   *  @param path    Path to the directory to be made snapshottable (must be non-empty)
   **/
  void AllowSnapshot(const std::string &path, const std::function<void(const Status &)> &handler) override;
  Status AllowSnapshot(const std::string &path) override;

  /**
   * Disallows snapshots to be made on the specified directory
   *
   *  @param path    Path to the directory to be made non-snapshottable (must be non-empty)
   **/
  void DisallowSnapshot(const std::string &path, const std::function<void(const Status &)> &handler) override;
  Status DisallowSnapshot(const std::string &path) override;

  void SetFsEventCallback(fs_event_callback callback) override;

  /* add a new thread to handle asio requests, return number of threads in pool
   */
  int AddWorkerThread();

  /* how many worker threads are servicing asio requests */
  int WorkerThreadCount() { return worker_threads_.size(); }

  /* all monitored events will need to lookup handlers */
  std::shared_ptr<LibhdfsEvents> get_event_handlers();

private:
  const Options options_;
  const std::string client_name_;
  std::string cluster_name_;
  /**
   *  The IoService must be the first member variable to ensure that it gets
   *  destroyed last.  This allows other members to dequeue things from the
   *  service in their own destructors.
   **/
  std::unique_ptr<IoServiceImpl> io_service_;
  NameNodeOperations nn_;
  std::shared_ptr<BadDataNodeTracker> bad_node_tracker_;

  struct WorkerDeleter {
    void operator()(std::thread *t);
  };
  typedef std::unique_ptr<std::thread, WorkerDeleter> WorkerPtr;
  std::vector<WorkerPtr> worker_threads_;

  /**
   * Runtime event monitoring handlers.
   * Note:  This is really handy to have for advanced usage but
   * exposes implementation details that may change at any time.
   **/
  std::shared_ptr<LibhdfsEvents> event_handlers_;

  void GetListingShim(const Status &stat, std::shared_ptr<std::vector<StatInfo>> &stat_infos, bool has_more,
                      std::string path,
                      const std::function<bool(const Status &, std::shared_ptr<std::vector<StatInfo>>&, bool)> &handler);

};
}

#endif
