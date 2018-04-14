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

#include "hdfspp/ioservice.h"
#include "common/async_stream.h"
#include "common/cancel_tracker.h"
#include "common/libhdfs_events_impl.h"
#include "common/new_delete.h"
#include "reader/fileinfo.h"
#include "reader/readergroup.h"

#include "bad_datanode_tracker.h"
#include "ClientNamenodeProtocol.pb.h"

#include <mutex>

namespace hdfs {

class BlockReader;
struct BlockReaderOptions;
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
                 std::shared_ptr<IoService> io_service, const std::string &client_name,
                  const std::shared_ptr<const struct FileInfo> file_info,
                  std::shared_ptr<BadDataNodeTracker> bad_data_nodes,
                  std::shared_ptr<LibhdfsEvents> event_handlers);

  /*
   * Reads the file at the specified offset into the buffer.
   * bytes_read returns the number of bytes successfully read on success
   * and on error. Status::InvalidOffset is returned when trying to begin
   * a read past the EOF.
   */
  void PositionRead(
    void *buf,
    size_t buf_size,
    uint64_t offset,
    const std::function<void(const Status &status, size_t bytes_read)> &handler
    ) override;

  /**
   *  Reads the file at the specified offset into the buffer.
   *  @param buf        output buffer
   *  @param buf_size   size of the output buffer
   *  @param offset     offset at which to start reading
   *  @param bytes_read number of bytes successfully read
   */
  Status PositionRead(void *buf, size_t buf_size, off_t offset, size_t *bytes_read) override;
  Status Read(void *buf, size_t buf_size, size_t *bytes_read) override;
  Status Seek(off_t *offset, std::ios_base::seekdir whence) override;


  /*
   * Reads some amount of data into the buffer.  Will attempt to find the best
   * datanode and read data from it.
   *
   * If an error occurs during connection or transfer, the callback will be
   * called with bytes_read equal to the number of bytes successfully transferred.
   * If no data nodes can be found, status will be Status::ResourceUnavailable.
   * If trying to begin a read past the EOF, status will be Status::InvalidOffset.
   *
   */
  void AsyncPreadSome(size_t offset, const MutableBuffer &buffer,
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

  /* how many bytes have been successfully read */
  virtual uint64_t get_bytes_read() override;

  /* resets the number of bytes read to zero */
  virtual void clear_bytes_read() override;

protected:
  virtual std::shared_ptr<BlockReader> CreateBlockReader(const BlockReaderOptions &options,
                                                         std::shared_ptr<DataNodeConnection> dn,
                                                         std::shared_ptr<hdfs::LibhdfsEvents> event_handlers);
  virtual std::shared_ptr<DataNodeConnection> CreateDataNodeConnection(
      std::shared_ptr<IoService> io_service,
      const ::hadoop::hdfs::DatanodeInfoProto & dn,
      const hadoop::common::TokenProto * token);
private:
  const std::string cluster_name_;
  const std::string path_;
  std::shared_ptr<IoService> io_service_;
  const std::string client_name_;
  const std::shared_ptr<const struct FileInfo> file_info_;
  std::shared_ptr<BadDataNodeTracker> bad_node_tracker_;
  bool CheckSeekBounds(ssize_t desired_position);
  off_t offset_;
  CancelHandle cancel_state_;
  ReaderGroup readers_;
  std::shared_ptr<LibhdfsEvents> event_handlers_;
  std::atomic<uint64_t> bytes_read_;
};

}

#endif
