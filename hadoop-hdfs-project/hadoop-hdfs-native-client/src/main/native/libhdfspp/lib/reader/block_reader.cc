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

#include "reader/block_reader.h"
#include "reader/datatransfer.h"

#include "common/continuation/continuation.h"
#include "common/continuation/asio.h"
#include "common/logging.h"
#include "common/util.h"

#include <future>

#include <boost/asio/buffer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/completion_condition.hpp>

namespace hdfs {

#define FMT_CONT_AND_PARENT_ADDR "this=" << (void*)this << ", parent=" << (void*)parent_
#define FMT_CONT_AND_READER_ADDR "this=" << (void*)this << ", reader=" << (void*)reader_
#define FMT_THIS_ADDR "this=" << (void*)this


// Stuff an OpReadBlockProto message with required fields.
hadoop::hdfs::OpReadBlockProto ReadBlockProto(const std::string &client_name,
    bool verify_checksum, const hadoop::common::TokenProto *token,
    const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length, uint64_t offset)
{
  using namespace hadoop::hdfs;
  using namespace hadoop::common;
  BaseHeaderProto *base_h = new BaseHeaderProto();
  base_h->set_allocated_block(new ExtendedBlockProto(*block));
  if (token) {
    base_h->set_allocated_token(new TokenProto(*token));
  }
  ClientOperationHeaderProto *h = new ClientOperationHeaderProto();
  h->set_clientname(client_name);
  h->set_allocated_baseheader(base_h);

  OpReadBlockProto p;
  p.set_allocated_header(h);
  p.set_offset(offset);
  p.set_len(length);
  p.set_sendchecksums(verify_checksum);
  // TODO: p.set_allocated_cachingstrategy();
  return p;
}

//
//  Notes about the BlockReader and associated object lifecycles  (9/29/16)
//  -We have a several stages in the read pipeline.  Each stage represents a logical
//   step in the HDFS block transfer logic.  They are implemented as continuations
//   for now, and in some cases the stage may have a nested continuation as well.
//   It's important to make sure that continuations, nested or otherwise, cannot
//   outlive the objects they depend on.
//
//  -The BlockReader holds a shared_ptr to the DataNodeConnection that's used in each
//   pipeline stage.  The connection object must never be destroyed while operations are
//   pending on the ASIO side (see HDFS-10931).  In order to prevent a state where the
//   BlockReader or one of the corresponding pipelines outlives the connection each
//   pipeline stage must explicitly hold a shared pointer copied from BlockReaderImpl::dn_.
//


static int8_t unsecured_request_block_header[3] = {0, kDataTransferVersion, Operation::kReadBlock};

void BlockReaderImpl::AsyncRequestBlock(const std::string &client_name,
    const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
    uint64_t offset, const std::function<void(Status)> &handler)
{
  LOG_TRACE(kBlockReader, << "BlockReaderImpl::AsyncRequestBlock("
                          << FMT_THIS_ADDR << ", ..., length="
                          << length << ", offset=" << offset << ", ...) called");

  // The total number of bytes that we need to transfer from the DN is
  // the amount that the user wants (bytesToRead), plus the padding at
  // the beginning in order to chunk-align. Note that the DN may elect
  // to send more than this amount if the read starts/ends mid-chunk.
  bytes_to_read_ = length;

  struct State {
    std::string header;
    hadoop::hdfs::OpReadBlockProto request;
    hadoop::hdfs::BlockOpResponseProto response;
  };

  auto m = continuation::Pipeline<State>::Create(cancel_state_);
  State *s = &m->state();

  s->request = ReadBlockProto(client_name, options_.verify_checksum,
                              dn_->token_.get(), block, length, offset);

  s->header = std::string((const char*)unsecured_request_block_header, 3);

  bool serialize_success = true;
  s->header += SerializeDelimitedProtobufMessage(&s->request, &serialize_success);

  if(!serialize_success) {
    handler(Status::Error("Unable to serialize protobuf message"));
    return;
  }

  auto read_pb_message =
      new continuation::ReadDelimitedPBMessageContinuation<AsyncStream, 16384>(dn_, &s->response);

  m->Push(asio_continuation::Write(dn_, boost::asio::buffer(s->header))).Push(read_pb_message);

  m->Run([this, handler, offset](const Status &status, const State &s) {    Status stat = status;
    if (stat.ok()) {
      const auto &resp = s.response;

      if(this->event_handlers_) {
        event_response event_resp = this->event_handlers_->call(FILE_DN_READ_EVENT, "", "", 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
        if (stat.ok() && event_resp.response_type() == event_response::kTest_Error) {
          stat = Status::Error("Test error");
        }
#endif
      }

      if (stat.ok() && resp.status() == ::hadoop::hdfs::Status::SUCCESS) {
        if (resp.has_readopchecksuminfo()) {
          const auto &checksum_info = resp.readopchecksuminfo();
          chunk_padding_bytes_ = offset - checksum_info.chunkoffset();
        }
        state_ = kReadPacketHeader;
      } else {
        stat = Status::Error(s.response.message().c_str());
      }
    }
    handler(stat);
  });
}

Status BlockReaderImpl::RequestBlock(const std::string &client_name,
                                     const hadoop::hdfs::ExtendedBlockProto *block,
                                     uint64_t length, uint64_t offset)
{
  LOG_TRACE(kBlockReader, << "BlockReaderImpl::RequestBlock("
                          << FMT_THIS_ADDR <<"..., length="
                          << length << ", offset=" << offset << ") called");

  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());
  AsyncRequestBlock(client_name, block, length, offset,
                [stat](const Status &status) { stat->set_value(status); });
  return future.get();
}

struct BlockReaderImpl::ReadPacketHeader : continuation::Continuation
{
  ReadPacketHeader(BlockReaderImpl *parent) : parent_(parent), shared_conn_(parent->dn_) {}

  virtual void Run(const Next &next) override {
    LOG_TRACE(kBlockReader, << "BlockReaderImpl::ReadPacketHeader::Run("
                            << FMT_CONT_AND_PARENT_ADDR << ") called");

    parent_->packet_data_read_bytes_ = 0;
    parent_->packet_len_ = 0;
    auto handler = [next, this](const boost::system::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      } else {
        parent_->packet_len_ = packet_length();
        parent_->header_.Clear();
        bool v = parent_->header_.ParseFromArray(&buf_[kHeaderStart],
                                                 header_length());
        assert(v && "Failed to parse the header");
        (void)v; //avoids unused variable warning
        parent_->state_ = kReadChecksum;
      }
      if(parent_->event_handlers_) {
        event_response event_resp = parent_->event_handlers_->call(FILE_DN_READ_EVENT, "", "", 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
        if (status.ok() && event_resp.response_type() == event_response::kTest_Error) {
          status = Status::Error("Test error");
        }
#endif
      }
      next(status);
    };

    boost::asio::async_read(*parent_->dn_, boost::asio::buffer(buf_),
                     std::bind(&ReadPacketHeader::CompletionHandler, this,
                               std::placeholders::_1, std::placeholders::_2), handler);
  }

private:
  static const size_t kMaxHeaderSize = 512;
  static const size_t kPayloadLenOffset = 0;
  static const size_t kPayloadLenSize = sizeof(int32_t);
  static const size_t kHeaderLenOffset = 4;
  static const size_t kHeaderLenSize = sizeof(int16_t);
  static const size_t kHeaderStart = kPayloadLenSize + kHeaderLenSize;

  BlockReaderImpl *parent_;
  std::array<char, kMaxHeaderSize> buf_;

  size_t packet_length() const {
    return ntohl(*reinterpret_cast<const unsigned *>(&buf_[kPayloadLenOffset]));
  }

  size_t header_length() const {
    return ntohs(*reinterpret_cast<const short *>(&buf_[kHeaderLenOffset]));
  }

  size_t CompletionHandler(const boost::system::error_code &ec, size_t transferred) {
    if (ec) {
      return 0;
    } else if (transferred < kHeaderStart) {
      return kHeaderStart - transferred;
    } else {
      return kHeaderStart + header_length() - transferred;
    }
  }

  // Keep the DN connection alive
  std::shared_ptr<DataNodeConnection> shared_conn_;
};

struct BlockReaderImpl::ReadChecksum : continuation::Continuation
{
  ReadChecksum(BlockReaderImpl *parent) : parent_(parent), shared_conn_(parent->dn_) {}

  virtual void Run(const Next &next) override {
    LOG_TRACE(kBlockReader, << "BlockReaderImpl::ReadChecksum::Run("
                            << FMT_CONT_AND_PARENT_ADDR << ") called");

    auto parent = parent_;
    if (parent->state_ != kReadChecksum) {
      next(Status::OK());
      return;
    }

    std::shared_ptr<DataNodeConnection> keep_conn_alive_ = shared_conn_;

    auto handler = [parent, next, this, keep_conn_alive_](const boost::system::error_code &ec, size_t)
    {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      } else {
        parent->state_ = parent->chunk_padding_bytes_ ? kReadPadding : kReadData;
      }
      if(parent->event_handlers_) {
        event_response event_resp = parent->event_handlers_->call(FILE_DN_READ_EVENT, "", "", 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
        if (status.ok() && event_resp.response_type() == event_response::kTest_Error) {
          status = Status::Error("Test error");
        }
#endif
      }
      next(status);
    };

    parent->checksum_.resize(parent->packet_len_ - sizeof(int) - parent->header_.datalen());

    boost::asio::async_read(*parent->dn_, boost::asio::buffer(parent->checksum_), handler);
  }

private:
  BlockReaderImpl *parent_;

  // Keep the DataNodeConnection alive
  std::shared_ptr<DataNodeConnection> shared_conn_;
};

struct BlockReaderImpl::ReadData : continuation::Continuation
{
  ReadData(BlockReaderImpl *parent, std::shared_ptr<size_t> bytes_transferred,
        const boost::asio::mutable_buffers_1 &buf) : parent_(parent),
                                                     bytes_transferred_(bytes_transferred), buf_(buf), shared_conn_(parent->dn_)
  {
    buf_.begin();
  }

  ~ReadData() {
    buf_.end();
  }

  virtual void Run(const Next &next) override {
    LOG_TRACE(kBlockReader, << "BlockReaderImpl::ReadData::Run("
                            << FMT_CONT_AND_PARENT_ADDR << ") called");
    auto handler =
        [next, this](const boost::system::error_code &ec, size_t transferred) {
          Status status;
          if (ec) {
            status = Status(ec.value(), ec.message().c_str());
          }

          *bytes_transferred_ += transferred;
          parent_->bytes_to_read_ -= transferred;
          parent_->packet_data_read_bytes_ += transferred;

          if (parent_->packet_data_read_bytes_ >= parent_->header_.datalen()) {
            parent_->state_ = kReadPacketHeader;
          }

          if(parent_->event_handlers_) {
            event_response event_resp = parent_->event_handlers_->call(FILE_DN_READ_EVENT, "", "", 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
            if (status.ok() && event_resp.response_type() == event_response::kTest_Error) {
                status = Status::Error("Test error");
            }
#endif
          }
          next(status);
        };

    auto data_len = parent_->header_.datalen() - parent_->packet_data_read_bytes_;

    boost::asio::async_read(*parent_->dn_, buf_, boost::asio::transfer_exactly(data_len), handler);
  }

private:
  BlockReaderImpl *parent_;
  std::shared_ptr<size_t> bytes_transferred_;
  const boost::asio::mutable_buffers_1 buf_;

  // Keep DNConnection alive.
  std::shared_ptr<DataNodeConnection> shared_conn_;
};

struct BlockReaderImpl::ReadPadding : continuation::Continuation
{
  ReadPadding(BlockReaderImpl *parent) : parent_(parent),
        padding_(parent->chunk_padding_bytes_),
        bytes_transferred_(std::make_shared<size_t>(0)),
        read_data_(new ReadData(parent, bytes_transferred_, boost::asio::buffer(padding_))),
        shared_conn_(parent->dn_) {}

  virtual void Run(const Next &next) override {
    LOG_TRACE(kBlockReader, << "BlockReaderImpl::ReadPadding::Run("
                            << FMT_CONT_AND_PARENT_ADDR << ") called");

    if (parent_->state_ != kReadPadding || !parent_->chunk_padding_bytes_) {
      next(Status::OK());
      return;
    }

    std::shared_ptr<DataNodeConnection> keep_conn_alive_ = shared_conn_;

    auto h = [next, this, keep_conn_alive_](const Status &stat) {
      Status status = stat;
      if (status.ok()) {
        assert(reinterpret_cast<const int &>(*bytes_transferred_) == parent_->chunk_padding_bytes_);
        parent_->chunk_padding_bytes_ = 0;
        parent_->state_ = kReadData;
      }
      if(parent_->event_handlers_) {
        event_response event_resp = parent_->event_handlers_->call(FILE_DN_READ_EVENT, "", "", 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
        if (status.ok() && event_resp.response_type() == event_response::kTest_Error) {
          status = Status::Error("Test error");
        }
#endif
      }
      next(status);
    };
    read_data_->Run(h);
  }

private:
  BlockReaderImpl *parent_;
  std::vector<char> padding_;
  std::shared_ptr<size_t> bytes_transferred_;
  std::shared_ptr<continuation::Continuation> read_data_;
  ReadPadding(const ReadPadding &) = delete;
  ReadPadding &operator=(const ReadPadding &) = delete;

  // Keep DNConnection alive.
  std::shared_ptr<DataNodeConnection> shared_conn_;
};


struct BlockReaderImpl::AckRead : continuation::Continuation
{
  AckRead(BlockReaderImpl *parent) : parent_(parent), shared_conn_(parent->dn_) {}

  virtual void Run(const Next &next) override {
    LOG_TRACE(kBlockReader, << "BlockReaderImpl::AckRead::Run(" << FMT_CONT_AND_PARENT_ADDR << ") called");

    if (parent_->bytes_to_read_ >= 0) {
      next(Status::OK());
      return;
    }

    auto m = continuation::Pipeline<hadoop::hdfs::ClientReadStatusProto>::Create(parent_->cancel_state_);

    m->state().set_status(parent_->options_.verify_checksum
                              ? hadoop::hdfs::Status::CHECKSUM_OK
                              : hadoop::hdfs::Status::SUCCESS);

    m->Push(continuation::WriteDelimitedPBMessage(parent_->dn_, &m->state()));

    std::shared_ptr<DataNodeConnection> keep_conn_alive_ = shared_conn_;

    m->Run([this, next, keep_conn_alive_](const Status &stat, const hadoop::hdfs::ClientReadStatusProto &)
    {
      Status status = stat;
      if (status.ok()) {
        parent_->state_ = BlockReaderImpl::kFinished;
      }
      if(parent_->event_handlers_) {
        event_response event_resp = parent_->event_handlers_->call(FILE_DN_READ_EVENT, "", "", 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
        if (status.ok() && event_resp.response_type() == event_response::kTest_Error) {
          status = Status::Error("Test error");
        }
#endif
      }
      next(status);
    });
  }

private:
  BlockReaderImpl *parent_;

  // Keep DNConnection alive.
  std::shared_ptr<DataNodeConnection> shared_conn_;
};

void BlockReaderImpl::AsyncReadPacket(const MutableBuffer &buffer,
    const std::function<void(const Status &, size_t bytes_transferred)> &handler)
{
  assert(state_ != kOpen && "Not connected");

  LOG_TRACE(kBlockReader, << "BlockReaderImpl::AsyncReadPacket called");

  struct State {
    std::shared_ptr<size_t> bytes_transferred;
  };

  auto m = continuation::Pipeline<State>::Create(cancel_state_);
  m->state().bytes_transferred = std::make_shared<size_t>(0);

  // Note: some of these continuations have nested pipelines.
  m->Push(new ReadPacketHeader(this))
      .Push(new ReadChecksum(this))
      .Push(new ReadPadding(this))
      .Push(new ReadData(
          this, m->state().bytes_transferred, buffer))
      .Push(new AckRead(this));

  auto self = this->shared_from_this();
  m->Run([self, handler](const Status &status, const State &state) {
    handler(status, *state.bytes_transferred);
  });
}


size_t BlockReaderImpl::ReadPacket(const MutableBuffer &buffer, Status *status)
{
  LOG_TRACE(kBlockReader, << "BlockReaderImpl::ReadPacket called");

  size_t transferred = 0;
  auto done = std::make_shared<std::promise<void>>();
  auto future = done->get_future();
  AsyncReadPacket(buffer,
                  [status, &transferred, done](const Status &stat, size_t t) {
                    *status = stat;
                    transferred = t;
                    done->set_value();
                  });
  future.wait();
  return transferred;
}


struct BlockReaderImpl::RequestBlockContinuation : continuation::Continuation
{
  RequestBlockContinuation(BlockReader *reader, const std::string &client_name,
        const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length, uint64_t offset)
      : reader_(reader), client_name_(client_name), length_(length), offset_(offset)
  {
    block_.CheckTypeAndMergeFrom(*block);
  }

  virtual void Run(const Next &next) override {
    LOG_TRACE(kBlockReader, << "BlockReaderImpl::RequestBlockContinuation::Run("
                            << FMT_CONT_AND_READER_ADDR << ") called");

    reader_->AsyncRequestBlock(client_name_, &block_, length_, offset_, next);
  }

private:
  BlockReader *reader_;
  const std::string client_name_;
  hadoop::hdfs::ExtendedBlockProto block_;
  uint64_t length_;
  uint64_t offset_;
};

struct BlockReaderImpl::ReadBlockContinuation : continuation::Continuation
{
  ReadBlockContinuation(BlockReader *reader, MutableBuffer buffer, size_t *transferred)
      : reader_(reader), buffer_(buffer), buffer_size_(boost::asio::buffer_size(buffer)), transferred_(transferred) {}

  virtual void Run(const Next &next) override {
    LOG_TRACE(kBlockReader, << "BlockReaderImpl::ReadBlockContinuation::Run("
                            << FMT_CONT_AND_READER_ADDR << ") called");
    *transferred_ = 0;
    next_ = next;
    OnReadData(Status::OK(), 0);
  }

private:
  BlockReader *reader_;
  const MutableBuffer buffer_;
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
      reader_->AsyncReadPacket(
          boost::asio::buffer(buffer_ + *transferred_, buffer_size_ - *transferred_),
          std::bind(&ReadBlockContinuation::OnReadData, this, _1, _2));
    }
  }
};

void BlockReaderImpl::AsyncReadBlock(
    const std::string & client_name,
    const hadoop::hdfs::LocatedBlockProto &block,
    size_t offset,
    const MutableBuffer &buffer,
    const std::function<void(const Status &, size_t)> handler)
{
  LOG_TRACE(kBlockReader, << "BlockReaderImpl::AsyncReadBlock("
                          << FMT_THIS_ADDR << ") called");

  auto m = continuation::Pipeline<size_t>::Create(cancel_state_);
  size_t * bytesTransferred = &m->state();

  size_t size = boost::asio::buffer_size(buffer);

  m->Push(new RequestBlockContinuation(this, client_name, &block.b(), size, offset))
    .Push(new ReadBlockContinuation(this, buffer, bytesTransferred));

  m->Run([handler] (const Status &status, const size_t totalBytesTransferred) {
    handler(status, totalBytesTransferred);
  });
}

void BlockReaderImpl::CancelOperation() {
  LOG_TRACE(kBlockReader, << "BlockReaderImpl::CancelOperation("
                          << FMT_THIS_ADDR << ") called");
  /* just forward cancel to DNConnection */
  dn_->Cancel();
}

}
