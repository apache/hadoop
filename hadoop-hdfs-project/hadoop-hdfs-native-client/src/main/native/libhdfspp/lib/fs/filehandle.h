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
#include "reader/fileinfo.h"

#include "asio.hpp"

#include <mutex>

namespace hdfs {

/*
 * FileHandle: coordinates operations on a particular file in HDFS
 *
 * Threading model: not thread-safe; consumers and io_service should not call
 *    concurrently.  PositionRead and cancel() are the exceptions; they can be
 *    called concurrently and repeatedly.
 * Lifetime: pointer returned to consumer by FileSystem::Open.  Consumer is
 *    resonsible for freeing the object.
 */
class FileHandleImpl : public FileHandle {
public:
  FileHandleImpl(::asio::io_service *io_service, const std::string &client_name,
                  const std::shared_ptr<const struct FileInfo> file_info);

  /*
   * [Some day reliably] Reads a particular offset into the data file.
   * On error, bytes_read returns the number of bytes successfully read; on
   * success, bytes_read will equal nbyte
   */
  CancelHandle PositionRead(
		void *buf,
		size_t nbyte,
		uint64_t offset,
        const std::function<void(const Status &status, size_t bytes_read)> &handler
    ) override;
  size_t PositionRead(void *buf, size_t bytes_read, off_t offset) override;

  /*
   * Reads some amount of data into the buffer.  Will attempt to find the best
   * datanode and read data from it.
   *
   * If an error occurs during connection or transfer, the callback will be
   * called with bytes_read equal to the number of bytes successfully transferred.
   * If no data nodes can be found, status will be Status::ResourceUnavailable.
   *
   */
  CancelHandle AsyncPreadSome(size_t offset, const MutableBuffers &buffers,
                      const std::set<std::string> &excluded_datanodes,
                      const std::function<void(const Status &status, const std::string &dn_id, size_t bytes_read)> handler);
private:
  ::asio::io_service * const io_service_;
  const std::string client_name_;
  const std::shared_ptr<const struct FileInfo> file_info_;
  // Shared pointer to the FileSystem's dead nodes object goes here
};

}

#endif
