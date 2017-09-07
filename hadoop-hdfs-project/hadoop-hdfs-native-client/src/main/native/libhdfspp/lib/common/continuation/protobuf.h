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
#ifndef LIBHDFSPP_COMMON_CONTINUATION_PROTOBUF_H_
#define LIBHDFSPP_COMMON_CONTINUATION_PROTOBUF_H_

#include "common/util.h"

#include <asio/read.hpp>

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <cassert>

namespace hdfs {
namespace continuation {

template <class Stream, size_t MaxMessageSize = 512>
struct ReadDelimitedPBMessageContinuation : public Continuation {
  ReadDelimitedPBMessageContinuation(std::shared_ptr<Stream> stream,
                                     ::google::protobuf::MessageLite *msg)
      : stream_(stream), msg_(msg) {}

  virtual void Run(const Next &next) override {
    namespace pbio = google::protobuf::io;
    auto handler = [this, next](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = ToStatus(ec);
      } else {
        pbio::ArrayInputStream as(&buf_[0], buf_.size());
        pbio::CodedInputStream is(&as);
        uint32_t size = 0;
        bool v = is.ReadVarint32(&size);
        assert(v);
        (void)v; //avoids unused variable warning
        is.PushLimit(size);
        msg_->Clear();
        v = msg_->MergeFromCodedStream(&is);
        assert(v);
      }
      next(status);
    };
    asio::async_read(*stream_,
        asio::buffer(buf_),
        std::bind(&ReadDelimitedPBMessageContinuation::CompletionHandler, this,
                  std::placeholders::_1, std::placeholders::_2),
        handler);
  }

private:
  size_t CompletionHandler(const asio::error_code &ec, size_t transferred) {
    if (ec) {
      return 0;
    }

    size_t offset = 0, len = 0;
    for (size_t i = 0; i + 1 < transferred && i < sizeof(int32_t); ++i) {
      len = (len << 7) | (buf_[i] & 0x7f);
      if ((uint8_t)buf_.at(i) < 0x80) {
        offset = i + 1;
        break;
      }
    }

    assert(offset + len < buf_.size() && "Message is too big");
    return offset ? len + offset - transferred : 1;
  }

  std::shared_ptr<Stream> stream_;
  ::google::protobuf::MessageLite *msg_;
  std::array<char, MaxMessageSize> buf_;
};

template <class Stream>
struct WriteDelimitedPBMessageContinuation : Continuation {
  WriteDelimitedPBMessageContinuation(std::shared_ptr<Stream> stream,
                                      const google::protobuf::MessageLite *msg)
      : stream_(stream), msg_(msg) {}

  virtual void Run(const Next &next) override {
    bool success = true;
    buf_ = SerializeDelimitedProtobufMessage(msg_, &success);

    if(!success) {
      next(Status::Error("Unable to serialize protobuf message."));
      return;
    }

    asio::async_write(*stream_, asio::buffer(buf_), [next](const asio::error_code &ec, size_t) { next(ToStatus(ec)); } );
  }

private:
  std::shared_ptr<Stream> stream_;
  const google::protobuf::MessageLite *msg_;
  std::string buf_;
};

template <class Stream, size_t MaxMessageSize = 512>
static inline Continuation *
ReadDelimitedPBMessage(std::shared_ptr<Stream> stream, ::google::protobuf::MessageLite *msg) {
  return new ReadDelimitedPBMessageContinuation<Stream, MaxMessageSize>(stream,
                                                                        msg);
}

template <class Stream>
static inline Continuation *
WriteDelimitedPBMessage(std::shared_ptr<Stream> stream, ::google::protobuf::MessageLite *msg) {
  return new WriteDelimitedPBMessageContinuation<Stream>(stream, msg);
}
}
}
#endif
