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

#include <functional>
#include <future>
#include <type_traits>
#include <algorithm>

namespace hdfs {

struct InputStreamImpl::RemoteBlockReaderTrait {
  typedef RemoteBlockReader<asio::ip::tcp::socket> Reader;
  struct State {
    std::unique_ptr<asio::ip::tcp::socket> conn_;
    std::shared_ptr<Reader> reader_;
    std::array<asio::ip::tcp::endpoint, 1> endpoints_;
    size_t transferred_;
    Reader *reader() { return reader_.get(); }
    size_t *transferred() { return &transferred_; }
    const size_t *transferred() const { return &transferred_; }
  };
  static continuation::Pipeline<State> *CreatePipeline(
      ::asio::io_service *io_service,
      const ::hadoop::hdfs::DatanodeInfoProto &dn) {
    using namespace ::asio::ip;
    auto m = continuation::Pipeline<State>::Create();
    auto &s = m->state();
    s.conn_.reset(new tcp::socket(*io_service));
    s.reader_ = std::make_shared<Reader>(BlockReaderOptions(), s.conn_.get());
    auto datanode = dn.id();
    s.endpoints_[0] = tcp::endpoint(address::from_string(datanode.ipaddr()),
                                    datanode.xferport());

    m->Push(continuation::Connect(s.conn_.get(), s.endpoints_.begin(),
                                  s.endpoints_.end()));
    return m;
  }
};

template <class Reader>
struct InputStreamImpl::HandshakeContinuation : continuation::Continuation {
  HandshakeContinuation(Reader *reader, const std::string &client_name,
                        const hadoop::common::TokenProto *token,
                        const hadoop::hdfs::ExtendedBlockProto *block,
                        uint64_t length, uint64_t offset)
      : reader_(reader),
        client_name_(client_name),
        length_(length),
        offset_(offset) {
    if (token) {
      token_.reset(new hadoop::common::TokenProto());
      token_->CheckTypeAndMergeFrom(*token);
    }
    block_.CheckTypeAndMergeFrom(*block);
  }

  virtual void Run(const Next &next) override {
    reader_->async_connect(client_name_, token_.get(), &block_, length_,
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
struct InputStreamImpl::ReadBlockContinuation : continuation::Continuation {
  ReadBlockContinuation(Reader *reader, MutableBufferSequence buffer,
                        size_t *transferred)
      : reader_(reader),
        buffer_(buffer),
        buffer_size_(asio::buffer_size(buffer)),
        transferred_(transferred) {
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
      reader_->async_read_some(
          asio::buffer(buffer_ + *transferred_, buffer_size_ - *transferred_),
          std::bind(&ReadBlockContinuation::OnReadData, this, _1, _2));
    }
  }
};

template <class MutableBufferSequence, class Handler>
void InputStreamImpl::AsyncPreadSome(
    size_t offset, const MutableBufferSequence &buffers,
    std::shared_ptr<NodeExclusionRule> excluded_nodes, const Handler &handler) {
  using ::hadoop::hdfs::DatanodeInfoProto;
  using ::hadoop::hdfs::LocatedBlockProto;

  /**
   *  Note: block and chosen_dn will end up pointing to things inside
   *  the blocks_ vector.  They shouldn't be directly deleted.
   **/
  auto block = std::find_if(
      blocks_.begin(), blocks_.end(), [offset](const LocatedBlockProto &p) {
        return p.offset() <= offset && offset < p.offset() + p.b().numbytes();
      });

  if (block == blocks_.end()) {
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

  DatanodeInfoProto *chosen_dn = &*it;

  uint64_t offset_within_block = offset - block->offset();
  uint64_t size_within_block = std::min<uint64_t>(
      block->b().numbytes() - offset_within_block, asio::buffer_size(buffers));

  AsyncReadBlock<RemoteBlockReaderTrait>(
      fs_->rpc_engine().client_name(), *block, *chosen_dn, offset_within_block,
      asio::buffer(buffers, size_within_block), handler);
}

template <class BlockReaderTrait, class MutableBufferSequence, class Handler>
void InputStreamImpl::AsyncReadBlock(
    const std::string &client_name,
    const hadoop::hdfs::LocatedBlockProto &block,
    const hadoop::hdfs::DatanodeInfoProto &dn, size_t offset,
    const MutableBufferSequence &buffers, const Handler &handler) {
  typedef typename BlockReaderTrait::Reader Reader;
  auto m =
      BlockReaderTrait::CreatePipeline(&fs_->rpc_engine().io_service(), dn);
  auto &s = m->state();
  size_t size = asio::buffer_size(buffers);
  m->Push(new HandshakeContinuation<Reader>(s.reader(), client_name, nullptr,
                                            &block.b(), size, offset))
      .Push(new ReadBlockContinuation<Reader, MutableBufferSequence>(
          s.reader(), buffers, s.transferred()));
  const std::string &dnid = dn.id().datanodeuuid();
  m->Run([handler, dnid](const Status &status,
                         const typename BlockReaderTrait::State &state) {
    handler(status, dnid, *state.transferred());
  });
}
}

#endif
