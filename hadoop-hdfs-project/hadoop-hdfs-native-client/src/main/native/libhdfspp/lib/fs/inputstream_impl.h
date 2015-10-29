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
#ifndef FS_INPUTSTREAM_IMPL_H_
#define FS_INPUTSTREAM_IMPL_H_

#include "reader/block_reader.h"

#include "common/continuation/asio.h"
#include "common/continuation/protobuf.h"
#include "filesystem.h"

#include <functional>
#include <future>
#include <type_traits>

namespace hdfs {

struct InputStreamImpl::RemoteBlockReaderTrait {
  typedef RemoteBlockReader<asio::ip::tcp::socket> Reader;
  struct State {
    std::shared_ptr<DataNodeConnectionImpl> dn_;
    std::shared_ptr<Reader> reader_;
    size_t transferred_;
    Reader *reader() { return reader_.get(); }
    size_t *transferred() { return &transferred_; }
    const size_t *transferred() const { return &transferred_; }
  };
  static continuation::Pipeline<State> *
  CreatePipeline(std::shared_ptr<DataNodeConnectionImpl> dn) {
    auto m = continuation::Pipeline<State>::Create();
    auto &s = m->state();
    s.reader_ = std::make_shared<Reader>(BlockReaderOptions(), dn->conn_.get());
    return m;
  }
};

template <class Reader>
struct ReadOperation::HandshakeContinuation : continuation::Continuation {
  HandshakeContinuation(Reader *reader, const std::string &client_name,
                        const hadoop::common::TokenProto *token,
                        const hadoop::hdfs::ExtendedBlockProto *block,
                        uint64_t length, uint64_t offset)
      : reader_(reader), client_name_(client_name), length_(length),
        offset_(offset) {
    if (token) {
      token_.reset(new hadoop::common::TokenProto());
      token_->CheckTypeAndMergeFrom(*token);
    }
    block_.CheckTypeAndMergeFrom(*block);
  }

  virtual void Run(const Next &next) override {
    reader_->async_request_block(client_name_, token_.get(), &block_, length_,
                           offset_, next);
  }

private:
  Reader *reader_;
  const std::string client_name_;
  std::unique_ptr<hadoop::common::TokenProto> token_;
  hadoop::hdfs::ExtendedBlockProto block_;
  uint64_t length_;
  uint64_t offset_;
};

template <class Reader, class MutableBufferSequence>
struct ReadOperation::ReadBlockContinuation : continuation::Continuation {
  ReadBlockContinuation(Reader *reader, MutableBufferSequence buffer,
                        size_t *transferred)
      : reader_(reader), buffer_(buffer),
        buffer_size_(asio::buffer_size(buffer)), transferred_(transferred) {
    static_assert(!std::is_reference<MutableBufferSequence>::value,
                  "Buffer must not be a reference type");
  }

  virtual void Run(const Next &next) override {
    *transferred_ = 0;
    next_ = next;
    OnReadData(Status::OK(), 0);
  }

private:
  Reader *reader_;
  const MutableBufferSequence buffer_;
  const size_t buffer_size_;
  size_t *transferred_;
  std::function<void(const Status &)> next_;

  void OnReadData(const Status &status, size_t transferred) {
    using std::placeholders::_1;
    using std::placeholders::_2;
    *transferred_ += transferred;
    if (!status.ok()) {
      next_(status);
    } else if (*transferred_ >= buffer_size_) {
      next_(status);
    } else {
      reader_->async_read_packet(
          asio::buffer(buffer_ + *transferred_, buffer_size_ - *transferred_),
          std::bind(&ReadBlockContinuation::OnReadData, this, _1, _2));
    }
  }
};

template <class MutableBufferSequence, class Handler>
void InputStreamImpl::AsyncPreadSome(
    size_t offset, const MutableBufferSequence &buffers,
    const std::set<std::string> &excluded_datanodes, const Handler &handler) {
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

  //TODO: re-use DN connection
  dn_ = std::make_shared<DataNodeConnectionImpl>(io_service_, *chosen_dn);
  dn_->Connect([this,handler,targetBlock,offset_within_block,size_within_block, buffers]
          (Status status, std::shared_ptr<DataNodeConnectionImpl> dn) {
    if (status.ok()) {
      ReadOperation::AsyncReadBlock<RemoteBlockReaderTrait, DataNodeConnectionImpl, MutableBufferSequence, Handler>(
          dn, client_name_, targetBlock, offset_within_block,
          asio::buffer(buffers, size_within_block), handler);
    } else {
      handler(status, "", 0);
    }
  });
}

template <class BlockReaderTrait, class DataNodeConnection, class MutableBufferSequence, class Handler>
void ReadOperation::AsyncReadBlock(
    std::shared_ptr<DataNodeConnection> dn,
    const std::string & client_name,
    const hadoop::hdfs::LocatedBlockProto &block,
    size_t offset,
    const MutableBufferSequence &buffers, const Handler &handler) {

  typedef typename BlockReaderTrait::Reader Reader;
  auto m = BlockReaderTrait::CreatePipeline(dn);
  auto &s = m->state();

  size_t size = asio::buffer_size(buffers);
  m->Push(new HandshakeContinuation<Reader>(s.reader(), client_name, nullptr,
                                            &block.b(), size, offset))
    .Push(new ReadBlockContinuation<Reader, MutableBufferSequence>(
        s.reader(), buffers, s.transferred()));
  const std::string &dnid = dn->uuid_;
  m->Run([handler, dnid](const Status &status,
                         const typename BlockReaderTrait::State &state) {
    handler(status, dnid, *state.transferred());
  });
}
}

#endif
