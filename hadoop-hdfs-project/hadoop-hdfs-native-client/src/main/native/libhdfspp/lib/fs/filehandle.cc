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

#include <future>
#include <tuple>

namespace hdfs {

using ::hadoop::hdfs::LocatedBlocksProto;

FileHandle::~FileHandle() {}

FileHandleImpl::FileHandleImpl(::asio::io_service *io_service, const std::string &client_name,
                                 const std::shared_ptr<const struct FileInfo> file_info,
                                 std::shared_ptr<BadDataNodeTracker> bad_data_nodes)
    : io_service_(io_service), client_name_(client_name), file_info_(file_info),
      bad_node_tracker_(bad_data_nodes), offset_(0), cancel_state_(CancelTracker::New()) {
}

void FileHandleImpl::PositionRead(
    void *buf, size_t nbyte, uint64_t offset,
    const std::function<void(const Status &, size_t)> &handler) {
  /* prevent usage after cancelation */
  if(cancel_state_->is_canceled()) {
    handler(Status::Canceled(), 0);
    return;
  }

  auto callback = [this, handler](const Status &status,
                                  const std::string &contacted_datanode,
                                  size_t bytes_read) {
    /* determine if DN gets marked bad */
    if (ShouldExclude(status)) {
      bad_node_tracker_->AddBadNode(contacted_datanode);
    }

    handler(status, bytes_read);
  };

  AsyncPreadSome(offset, asio::buffer(buf, nbyte), bad_node_tracker_, callback);
}

Status FileHandleImpl::PositionRead(void *buf, size_t *nbyte, off_t offset) {
  auto callstate = std::make_shared<std::promise<std::tuple<Status, size_t>>>();
  std::future<std::tuple<Status, size_t>> future(callstate->get_future());

  /* wrap async call with promise/future to make it blocking */
  auto callback = [callstate](const Status &s, size_t bytes) {
    callstate->set_value(std::make_tuple(s,bytes));
  };

  PositionRead(buf, *nbyte, offset, callback);

  /* wait for async to finish */
  auto returnstate = future.get();
  auto stat = std::get<0>(returnstate);

  if (!stat.ok()) {
    return stat;
  }

  *nbyte = std::get<1>(returnstate);
  return stat;
}

Status FileHandleImpl::Read(void *buf, size_t *nbyte) {
  Status stat = PositionRead(buf, nbyte, offset_);
  if(!stat.ok()) {
    return stat;
  }

  offset_ += *nbyte;
  return Status::OK();
}

Status FileHandleImpl::Seek(off_t *offset, std::ios_base::seekdir whence) {
  if(cancel_state_->is_canceled()) {
    return Status::Canceled();
  }

  off_t new_offset = -1;

  switch (whence) {
    case std::ios_base::beg:
      new_offset = *offset;
      break;
    case std::ios_base::cur:
      new_offset = offset_ + *offset;
      break;
    case std::ios_base::end:
      new_offset = file_info_->file_length_ + *offset;
      break;
    default:
      /* unsupported */
      return Status::InvalidArgument("Invalid Seek whence argument");
  }

  if(!CheckSeekBounds(new_offset)) {
    return Status::InvalidArgument("Seek offset out of bounds");
  }
  offset_ = new_offset;

  *offset = offset_;
  return Status::OK();
}

/* return false if seek will be out of bounds */
bool FileHandleImpl::CheckSeekBounds(ssize_t desired_position) {
  ssize_t file_length = file_info_->file_length_;

  if (desired_position < 0 || desired_position > file_length) {
    return false;
  }

  return true;
}

/*
 * Note that this method must be thread-safe w.r.t. the unsafe operations occurring
 * on the FileHandle
 */
void FileHandleImpl::AsyncPreadSome(
    size_t offset, const MutableBuffers &buffers,
    std::shared_ptr<NodeExclusionRule> excluded_nodes,
    const std::function<void(const Status &, const std::string &, size_t)> handler) {
  using ::hadoop::hdfs::DatanodeInfoProto;
  using ::hadoop::hdfs::LocatedBlockProto;

  if(cancel_state_->is_canceled()) {
    handler(Status::Canceled(), "", 0);
    return;
  }

  /**
   *  Note: block and chosen_dn will end up pointing to things inside
   *  the blocks_ vector.  They shouldn't be directly deleted.
   **/
  auto block = std::find_if(
      file_info_->blocks_.begin(), file_info_->blocks_.end(), [offset](const LocatedBlockProto &p) {
        return p.offset() <= offset && offset < p.offset() + p.b().numbytes();
      });

  if (block == file_info_->blocks_.end()) {
    handler(Status::InvalidArgument("Cannot find corresponding blocks"), "", 0);
    return;
  }

  /**
   * If user supplies a rule use it, otherwise use the tracker.
   * User is responsible for making sure one of them isn't null.
   **/
  std::shared_ptr<NodeExclusionRule> rule =
      excluded_nodes != nullptr ? excluded_nodes : bad_node_tracker_;

  auto datanodes = block->locs();
  auto it = std::find_if(datanodes.begin(), datanodes.end(),
                         [rule](const DatanodeInfoProto &dn) {
                           return !rule->IsBadNode(dn.id().datanodeuuid());
                         });

  if (it == datanodes.end()) {
    handler(Status::ResourceUnavailable("No datanodes available"), "", 0);
    return;
  }

  DatanodeInfoProto &chosen_dn = *it;

  uint64_t offset_within_block = offset - block->offset();
  uint64_t size_within_block = std::min<uint64_t>(
      block->b().numbytes() - offset_within_block, asio::buffer_size(buffers));

  // This is where we will put the logic for re-using a DN connection; we can
  //    steal the FileHandle's dn and put it back when we're done
  std::shared_ptr<DataNodeConnection> dn = CreateDataNodeConnection(io_service_, chosen_dn, &block->blocktoken());
  std::string dn_id = dn->uuid_;
  std::string client_name = client_name_;

  // Wrap the DN in a block reader to handle the state and logic of the
  //    block request protocol
  std::shared_ptr<BlockReader> reader;
  reader = CreateBlockReader(BlockReaderOptions(), dn);


  auto read_handler = [reader, dn_id, handler](const Status & status, size_t transferred) {
    handler(status, dn_id, transferred);
  };

  dn->Connect([handler,read_handler,block,offset_within_block,size_within_block, buffers, reader, dn_id, client_name]
          (Status status, std::shared_ptr<DataNodeConnection> dn) {
    (void)dn;
    if (status.ok()) {
      reader->AsyncReadBlock(
          client_name, *block, offset_within_block,
          asio::buffer(buffers, size_within_block), read_handler);
    } else {
      handler(status, dn_id, 0);
    }
  });

  return;
}

std::shared_ptr<BlockReader> FileHandleImpl::CreateBlockReader(const BlockReaderOptions &options,
                                               std::shared_ptr<DataNodeConnection> dn)
{
  std::shared_ptr<BlockReader> reader = std::make_shared<BlockReaderImpl>(options, dn, cancel_state_);
  readers_.AddReader(reader);
  return reader;
}

std::shared_ptr<DataNodeConnection> FileHandleImpl::CreateDataNodeConnection(
    ::asio::io_service * io_service,
    const ::hadoop::hdfs::DatanodeInfoProto & dn,
    const hadoop::common::TokenProto * token) {
  return std::make_shared<DataNodeConnectionImpl>(io_service, dn, token);
}

void FileHandleImpl::CancelOperations() {
  cancel_state_->set_canceled();

  /* Push update to BlockReaders that may be hung in an asio call */
  std::vector<std::shared_ptr<BlockReader>> live_readers = readers_.GetLiveReaders();
  for(auto reader : live_readers) {
    reader->CancelOperation();
  }
}


bool FileHandle::ShouldExclude(const Status &s) {
  if (s.ok()) {
    return false;
  }

  switch (s.code()) {
    /* client side resource exhaustion */
    case Status::kResourceUnavailable:
    case Status::kOperationCanceled:
      return false;
    case Status::kInvalidArgument:
    case Status::kUnimplemented:
    case Status::kException:
    default:
      return true;
  }
}

}
