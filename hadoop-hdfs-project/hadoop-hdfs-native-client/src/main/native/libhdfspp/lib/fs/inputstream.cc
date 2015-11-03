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

#include "filesystem.h"
#include "common/continuation/continuation.h"

namespace hdfs {

using ::hadoop::hdfs::LocatedBlocksProto;

InputStream::~InputStream() {}

InputStreamImpl::InputStreamImpl(::asio::io_service *io_service, const std::string &client_name,
                                 const std::shared_ptr<const struct FileInfo> file_info)
    : io_service_(io_service), client_name_(client_name), file_info_(file_info) {
}

void InputStreamImpl::PositionRead(
    void *buf, size_t nbyte, uint64_t offset,
    const std::set<std::string> &excluded_datanodes,
    const std::function<void(const Status &, const std::string &, size_t)>
        &handler) {
  AsyncPreadSome(offset, asio::buffer(buf, nbyte), excluded_datanodes, handler);
}

void InputStreamImpl::AsyncPreadSome(
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
    return;
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
    return;
  }

  uint64_t offset_within_block = offset - targetBlock.offset();
  uint64_t size_within_block = std::min<uint64_t>(
      targetBlock.b().numbytes() - offset_within_block, asio::buffer_size(buffers));

  // This is where we will put the logic for re-using a DN connection
  dn_ = std::make_shared<DataNodeConnectionImpl>(io_service_, *chosen_dn, nullptr);
  std::string dn_id = dn_->uuid_;

  std::shared_ptr<BlockReader> reader;
  reader.reset(new RemoteBlockReader(BlockReaderOptions(), dn_));
  
  auto read_handler = [dn_id, handler](const Status & status, size_t transferred) {
    handler(status, dn_id, transferred);
  };

  dn_->Connect([this,handler,read_handler,targetBlock,offset_within_block,size_within_block, buffers, reader]
          (Status status, std::shared_ptr<DataNodeConnection> dn) {
    (void)dn;
    if (status.ok()) {
      reader->AsyncReadBlock(
          reader.get(), client_name_, targetBlock, offset_within_block,
          asio::buffer(buffers, size_within_block), read_handler);
    } else {
      handler(status, "", 0);
    }
  });
}

}
