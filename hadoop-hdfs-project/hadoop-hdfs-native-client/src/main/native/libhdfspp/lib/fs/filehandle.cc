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

#include "filehandle.h"
#include "common/continuation/continuation.h"
#include "connection/datanodeconnection.h"
#include "reader/block_reader.h"

#include "ClientNamenodeProtocol.pb.h"

#include <future>

namespace hdfs {

using ::hadoop::hdfs::LocatedBlocksProto;

FileHandle::~FileHandle() {}

FileHandleImpl::FileHandleImpl(::asio::io_service *io_service, const std::string &client_name,
                                 const std::shared_ptr<const struct FileInfo> file_info)
    : io_service_(io_service), client_name_(client_name), file_info_(file_info) {
}




CancelHandle FileHandleImpl::PositionRead(
    void *buf, size_t nbyte, uint64_t offset,
    const std::function<void(const Status &, size_t)>
        &handler) {

  // This is where retry and dead DN node elision will occur

  return AsyncPreadSome(offset, asio::buffer(buf, nbyte), std::set<std::string>(),
                        [handler](const Status &status, const std::string &dn_id, size_t bytes_read){
                          (void)dn_id;
                          handler(status, bytes_read); }
                        );
}

size_t FileHandleImpl::PositionRead(void *buf, size_t nbyte, off_t offset) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());

  /* wrap async call with promise/future to make it blocking */
  size_t read_count = 0;
  auto callback = [stat, &read_count](const Status &s, size_t bytes) {
    stat->set_value(s);
    read_count = bytes;
  };

  PositionRead(buf, nbyte, offset, callback);

  /* wait for async to finish */
  auto s = future.get();

  if (!s.ok()) {
    return -1;
  }
  return (ssize_t)read_count;
}

/*
 * Note that this method must be thread-safe w.r.t. the unsafe operations occurring
 * on the FileHandle
 */
CancelHandle FileHandleImpl::AsyncPreadSome(
    size_t offset, const MutableBuffers &buffers,
    const std::set<std::string> &excluded_datanodes,
    const std::function<void(const Status &, const std::string &, size_t)> handler) {
  using ::hadoop::hdfs::DatanodeInfoProto;
  using ::hadoop::hdfs::LocatedBlockProto;

  auto it = std::find_if(
      file_info_->blocks_.begin(), file_info_->blocks_.end(), [offset](const LocatedBlockProto &p) {
        return p.offset() <= offset && offset < p.offset() + p.b().numbytes();
      });

  if (it == file_info_->blocks_.end()) {
    handler(Status::InvalidArgument("Cannot find corresponding blocks"), "", 0);
    return CancelHandle(nullptr);
  }

  ::hadoop::hdfs::LocatedBlockProto targetBlock = *it;

  const DatanodeInfoProto *chosen_dn = nullptr;
  for (int i = 0; i < targetBlock.locs_size(); ++i) {
    const auto &di = targetBlock.locs(i);
    if (!excluded_datanodes.count(di.id().datanodeuuid())) {
      chosen_dn = &di;
      break;
    }
  }

  if (!chosen_dn) {
    handler(Status::ResourceUnavailable("No datanodes available"), "", 0);
    return CancelHandle(nullptr);
  }

  uint64_t offset_within_block = offset - targetBlock.offset();
  uint64_t size_within_block = std::min<uint64_t>(
      targetBlock.b().numbytes() - offset_within_block, asio::buffer_size(buffers));

  // This is where we will put the logic for re-using a DN connection; we can
  //    steal the FileHandle's dn and put it back when we're done
  std::shared_ptr<DataNodeConnection> dn = std::make_shared<DataNodeConnectionImpl>(io_service_, *chosen_dn, nullptr /*token*/);
  std::string dn_id = dn->uuid_;
  std::string client_name = client_name_;

  // Wrap the DN in a block reader to handle the state and logic of the
  //    block request protocol
  std::shared_ptr<BlockReader> reader;
  reader.reset(new BlockReaderImpl(BlockReaderOptions(), dn));


  auto read_handler = [reader, dn_id, handler](const Status & status, size_t transferred) {
    handler(status, dn_id, transferred);
  };

  dn->Connect([handler,read_handler,targetBlock,offset_within_block,size_within_block, buffers, reader, dn_id, client_name]
          (Status status, std::shared_ptr<DataNodeConnection> dn) {
    (void)dn;
    if (status.ok()) {
      reader->AsyncReadBlock(
          client_name, targetBlock, offset_within_block,
          asio::buffer(buffers, size_within_block), read_handler);
    } else {
      handler(status, dn_id, 0);
    }
  });

  return CancelHandle(dn);
}


}
