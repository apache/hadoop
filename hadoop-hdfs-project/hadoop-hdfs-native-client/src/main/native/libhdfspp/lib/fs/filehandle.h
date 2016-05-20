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
#ifndef LIBHDFSPP_LIB_FS_FILEHANDLE_H_
#define LIBHDFSPP_LIB_FS_FILEHANDLE_H_

#include "common/hdfs_public_api.h"
#include "common/async_stream.h"
#include "common/cancel_tracker.h"
#include "common/libhdfs_events_impl.h"
#include "common/new_delete.h"
#include "reader/fileinfo.h"
#include "reader/readergroup.h"

#include "asio.hpp"
#include "bad_datanode_tracker.h"
#include "ClientNamenodeProtocol.pb.h"

#include <mutex>
#include <iostream>

namespace hdfs {

class BlockReader;
class BlockReaderOptions;
class DataNodeConnection;

/*
 * FileHandle: coordinates operations on a particular file in HDFS
 *
 * Threading model: not thread-safe; consumers and io_service should not call
 *    concurrently.  PositionRead is the exceptions; they can be
 *    called concurrently and repeatedly.
 * Lifetime: pointer returned to consumer by FileSystem::Open.  Consumer is
 *    resonsible for freeing the object.
 */
class FileHandleImpl : public FileHandle {
public:
  MEMCHECKED_CLASS(FileHandleImpl)
  FileHandleImpl(const std::string & cluster_name,
                 const std::string & path,
                 ::asio::io_service *io_service, const std::string &client_name,
                  const std::shared_ptr<const struct FileInfo> file_info,
                  std::shared_ptr<BadDataNodeTracker> bad_data_nodes,
                  std::shared_ptr<LibhdfsEvents> event_handlers);

  /*
   * [Some day reliably] Reads a particular offset into the data file.
   * On error, bytes_read returns the number of bytes successfully read; on
   * success, bytes_read will equal nbyte
   */
  void PositionRead(
    void *buf,
    size_t nbyte,
    uint64_t offset,
    const std::function<void(const Status &status, size_t bytes_read)> &handler
    ) override;

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
  Status PositionRead(void *buf, size_t *bytes_read, off_t offset) override;
  Status Read(void *buf, size_t *nbyte) override;
  Status Seek(off_t *offset, std::ios_base::seekdir whence) override;


  /*
   * Reads some amount of data into the buffer.  Will attempt to find the best
   * datanode and read data from it.
   *
   * If an error occurs during connection or transfer, the callback will be
   * called with bytes_read equal to the number of bytes successfully transferred.
   * If no data nodes can be found, status will be Status::ResourceUnavailable.
   *
   */
  void AsyncPreadSome(size_t offset, const MutableBuffers &buffers,
                      std::shared_ptr<NodeExclusionRule> excluded_nodes,
                      const std::function<void(const Status &status,
                      const std::string &dn_id, size_t bytes_read)> handler);

  /**
   *  Cancels all operations instantiated from this FileHandle.
   *  Will set a flag to abort continuation pipelines when they try to move to the next step.
   *  Closes TCP connections to Datanode in order to abort pipelines waiting on slow IO.
   **/
  virtual void CancelOperations(void) override;

  virtual void SetFileEventCallback(file_event_callback callback) override;

  /**
   * Ephemeral objects created by the filehandle will need to get the event
   * handler registry owned by the FileSystem.
   **/
  std::shared_ptr<LibhdfsEvents> get_event_handlers();

protected:
  virtual std::shared_ptr<BlockReader> CreateBlockReader(const BlockReaderOptions &options,
                                                 std::shared_ptr<DataNodeConnection> dn);
  virtual std::shared_ptr<DataNodeConnection> CreateDataNodeConnection(
      ::asio::io_service *io_service,
      const ::hadoop::hdfs::DatanodeInfoProto & dn,
      const hadoop::common::TokenProto * token);
private:
  const std::string cluster_name_;
  const std::string path_;
  ::asio::io_service * const io_service_;
  const std::string client_name_;
  const std::shared_ptr<const struct FileInfo> file_info_;
  std::shared_ptr<BadDataNodeTracker> bad_node_tracker_;
  bool CheckSeekBounds(ssize_t desired_position);
  off_t offset_;
  CancelHandle cancel_state_;
  ReaderGroup readers_;
  std::shared_ptr<LibhdfsEvents> event_handlers_;
};

}

#endif
