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
#ifndef LIBHDFSPP_READER_REMOTE_BLOCK_READER_IMPL_H_
#define LIBHDFSPP_READER_REMOTE_BLOCK_READER_IMPL_H_

#include "datatransfer.h"
#include "common/continuation/asio.h"
#include "common/continuation/protobuf.h"

#include <asio/buffers_iterator.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <arpa/inet.h>

#include <future>

namespace hdfs {

hadoop::hdfs::OpReadBlockProto
ReadBlockProto(const std::string &client_name, bool verify_checksum,
               const hadoop::common::TokenProto *token,
               const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
               uint64_t offset);

template <class Stream>
template <class ConnectHandler>
void RemoteBlockReader<Stream>::async_connect(
    const std::string &client_name, const hadoop::common::TokenProto *token,
    const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
    uint64_t offset, const ConnectHandler &handler) {
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

  auto m = continuation::Pipeline<State>::Create();
  State *s = &m->state();

  s->header.insert(s->header.begin(),
                   {0, kDataTransferVersion, Operation::kReadBlock});
  s->request = std::move(ReadBlockProto(client_name, options_.verify_checksum,
                                        token, block, length, offset));

  auto read_pb_message =
      new continuation::ReadDelimitedPBMessageContinuation<Stream, 16384>(
          stream_, &s->response);

  m->Push(continuation::Write(stream_, asio::buffer(s->header)))
      .Push(continuation::WriteDelimitedPBMessage(stream_, &s->request))
      .Push(read_pb_message);

  m->Run([this, handler, offset](const Status &status, const State &s) {
    Status stat = status;
    if (stat.ok()) {
      const auto &resp = s.response;
      if (resp.status() == ::hadoop::hdfs::Status::SUCCESS) {
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

template <class Stream>
struct RemoteBlockReader<Stream>::ReadPacketHeader
    : continuation::Continuation {
  ReadPacketHeader(RemoteBlockReader<Stream> *parent) : parent_(parent) {}

  virtual void Run(const Next &next) override {
    parent_->packet_data_read_bytes_ = 0;
    parent_->packet_len_ = 0;
    auto handler = [next, this](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      } else {
        parent_->packet_len_ = packet_length();
        parent_->header_.Clear();
        bool v = parent_->header_.ParseFromArray(&buf_[kHeaderStart],
                                                 header_length());
        assert(v && "Failed to parse the header");
        parent_->state_ = kReadChecksum;
      }
      next(status);
    };

    asio::async_read(*parent_->stream_, asio::buffer(buf_),
                     std::bind(&ReadPacketHeader::CompletionHandler, this,
                               std::placeholders::_1, std::placeholders::_2),
                     handler);
  }

private:
  static const size_t kMaxHeaderSize = 512;
  static const size_t kPayloadLenOffset = 0;
  static const size_t kPayloadLenSize = sizeof(int);
  static const size_t kHeaderLenOffset = 4;
  static const size_t kHeaderLenSize = sizeof(short);
  static const size_t kHeaderStart = kPayloadLenSize + kHeaderLenSize;

  RemoteBlockReader<Stream> *parent_;
  std::array<char, kMaxHeaderSize> buf_;

  size_t packet_length() const {
    return ntohl(*reinterpret_cast<const unsigned *>(&buf_[kPayloadLenOffset]));
  }

  size_t header_length() const {
    return ntohs(*reinterpret_cast<const short *>(&buf_[kHeaderLenOffset]));
  }

  size_t CompletionHandler(const asio::error_code &ec, size_t transferred) {
    if (ec) {
      return 0;
    } else if (transferred < kHeaderStart) {
      return kHeaderStart - transferred;
    } else {
      return kHeaderStart + header_length() - transferred;
    }
  }
};

template <class Stream>
struct RemoteBlockReader<Stream>::ReadChecksum : continuation::Continuation {
  ReadChecksum(RemoteBlockReader<Stream> *parent) : parent_(parent) {}

  virtual void Run(const Next &next) override {
    auto parent = parent_;
    if (parent->state_ != kReadChecksum) {
      next(Status::OK());
      return;
    }

    auto handler = [parent, next](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      } else {
        parent->state_ =
            parent->chunk_padding_bytes_ ? kReadPadding : kReadData;
      }
      next(status);
    };
    parent->checksum_.resize(parent->packet_len_ - sizeof(int) -
                             parent->header_.datalen());
    asio::async_read(*parent->stream_, asio::buffer(parent->checksum_),
                     handler);
  }

private:
  RemoteBlockReader<Stream> *parent_;
};

template <class Stream>
struct RemoteBlockReader<Stream>::ReadPadding : continuation::Continuation {
  ReadPadding(RemoteBlockReader<Stream> *parent)
      : parent_(parent), padding_(parent->chunk_padding_bytes_),
        bytes_transferred_(std::make_shared<size_t>(0)),
        read_data_(new ReadData<asio::mutable_buffers_1>(
            parent, bytes_transferred_, asio::buffer(padding_))) {}

  virtual void Run(const Next &next) override {
    if (parent_->state_ != kReadPadding || !parent_->chunk_padding_bytes_) {
      next(Status::OK());
      return;
    }

    auto h = [next, this](const Status &status) {
      if (status.ok()) {
        assert(reinterpret_cast<const int &>(*bytes_transferred_) ==
               parent_->chunk_padding_bytes_);
        parent_->chunk_padding_bytes_ = 0;
        parent_->state_ = kReadData;
      }
      next(status);
    };
    read_data_->Run(h);
  }

private:
  RemoteBlockReader<Stream> *parent_;
  std::vector<char> padding_;
  std::shared_ptr<size_t> bytes_transferred_;
  std::shared_ptr<continuation::Continuation> read_data_;
  ReadPadding(const ReadPadding &) = delete;
  ReadPadding &operator=(const ReadPadding &) = delete;
};

template <class Stream>
template <class MutableBufferSequence>
struct RemoteBlockReader<Stream>::ReadData : continuation::Continuation {
  ReadData(RemoteBlockReader<Stream> *parent,
           std::shared_ptr<size_t> bytes_transferred,
           const MutableBufferSequence &buf)
      : parent_(parent), bytes_transferred_(bytes_transferred), buf_(buf) {}

  virtual void Run(const Next &next) override {
    auto handler =
        [next, this](const asio::error_code &ec, size_t transferred) {
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
          next(status);
        };

    auto data_len =
        parent_->header_.datalen() - parent_->packet_data_read_bytes_;
    async_read(*parent_->stream_, buf_, asio::transfer_exactly(data_len),
               handler);
  }

private:
  RemoteBlockReader<Stream> *parent_;
  std::shared_ptr<size_t> bytes_transferred_;
  MutableBufferSequence buf_;
};

template <class Stream>
struct RemoteBlockReader<Stream>::AckRead : continuation::Continuation {
  AckRead(RemoteBlockReader<Stream> *parent) : parent_(parent) {}

  virtual void Run(const Next &next) override {
    if (parent_->bytes_to_read_ > 0) {
      next(Status::OK());
      return;
    }

    auto m =
        continuation::Pipeline<hadoop::hdfs::ClientReadStatusProto>::Create();
    m->state().set_status(parent_->options_.verify_checksum
                              ? hadoop::hdfs::Status::CHECKSUM_OK
                              : hadoop::hdfs::Status::SUCCESS);

    m->Push(
        continuation::WriteDelimitedPBMessage(parent_->stream_, &m->state()));

    m->Run([this, next](const Status &status,
                        const hadoop::hdfs::ClientReadStatusProto &) {
      if (status.ok()) {
        parent_->state_ = RemoteBlockReader<Stream>::kFinished;
      }
      next(status);
    });
  }

private:
  RemoteBlockReader<Stream> *parent_;
};

template <class Stream>
template <class MutableBufferSequence, class ReadHandler>
void RemoteBlockReader<Stream>::async_read_some(
    const MutableBufferSequence &buffers, const ReadHandler &handler) {
  assert(state_ != kOpen && "Not connected");

  struct State {
    std::shared_ptr<size_t> bytes_transferred;
  };
  auto m = continuation::Pipeline<State>::Create();
  m->state().bytes_transferred = std::make_shared<size_t>(0);

  m->Push(new ReadPacketHeader(this))
      .Push(new ReadChecksum(this))
      .Push(new ReadPadding(this))
      .Push(new ReadData<MutableBufferSequence>(
          this, m->state().bytes_transferred, buffers))
      .Push(new AckRead(this));

  auto self = this->shared_from_this();
  m->Run([self, handler](const Status &status, const State &state) {
    handler(status, *state.bytes_transferred);
  });
}

template <class Stream>
template <class MutableBufferSequence>
size_t
RemoteBlockReader<Stream>::read_some(const MutableBufferSequence &buffers,
                                     Status *status) {
  size_t transferred = 0;
  auto done = std::make_shared<std::promise<void>>();
  auto future = done->get_future();
  async_read_some(buffers,
                  [status, &transferred, done](const Status &stat, size_t t) {
                    *status = stat;
                    transferred = t;
                    done->set_value();
                  });
  future.wait();
  return transferred;
}

template <class Stream>
Status RemoteBlockReader<Stream>::connect(
    const std::string &client_name, const hadoop::common::TokenProto *token,
    const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
    uint64_t offset) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());
  async_connect(client_name, token, block, length, offset,
                [stat](const Status &status) { stat->set_value(status); });
  return future.get();
}
}

#endif
