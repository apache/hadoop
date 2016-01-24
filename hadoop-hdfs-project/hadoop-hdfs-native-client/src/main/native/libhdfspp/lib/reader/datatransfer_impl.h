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
#ifndef LIB_READER_DATATRANFER_IMPL_H_
#define LIB_READER_DATATRANFER_IMPL_H_

#include "datatransfer.pb.h"
#include "common/continuation/continuation.h"
#include "common/continuation/asio.h"
#include "common/continuation/protobuf.h"

#include <asio/read.hpp>
#include <asio/buffer.hpp>

namespace hdfs {

namespace DataTransferSaslStreamUtil {
Status
ConvertToStatus(const ::hadoop::hdfs::DataTransferEncryptorMessageProto *msg,
                std::string *payload);
void PrepareInitialHandshake(
    ::hadoop::hdfs::DataTransferEncryptorMessageProto *msg);
}

template <class Stream>
struct DataTransferSaslStream<Stream>::Authenticator
    : continuation::Continuation {
  Authenticator(DigestMD5Authenticator *authenticator,
                const std::string *request,
                hadoop::hdfs::DataTransferEncryptorMessageProto *msg)
      : authenticator_(authenticator), request_(request), msg_(msg) {}

  virtual void Run(const Next &next) override {
    using namespace ::hadoop::hdfs;
    std::string response;
    Status status = authenticator_->EvaluateResponse(*request_, &response);
    msg_->Clear();
    if (status.ok()) {
      // TODO: Handle encryption scheme
      msg_->set_payload(response);
      msg_->set_status(
          DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS);
    } else {
      msg_->set_status(
          DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_ERROR);
    }
    next(Status::OK());
  }

private:
  DigestMD5Authenticator *authenticator_;
  const std::string *request_;
  hadoop::hdfs::DataTransferEncryptorMessageProto *msg_;
};

template <class Stream>
struct DataTransferSaslStream<Stream>::ReadSaslMessage
    : continuation::Continuation {
  ReadSaslMessage(std::shared_ptr<Stream> stream, std::string *data)
      : stream_(stream), data_(data), read_pb_(stream, &resp_) {}

  virtual void Run(const Next &next) override {
    auto handler = [this, next](const Status &status) {
      if (status.ok()) {
        Status new_stat =
            DataTransferSaslStreamUtil::ConvertToStatus(&resp_, data_);
        next(new_stat);
      } else {
        next(status);
      }
    };
    read_pb_.Run(handler);
  }

private:
  std::shared_ptr<Stream> stream_;
  std::string *data_;
  hadoop::hdfs::DataTransferEncryptorMessageProto resp_;
  continuation::ReadDelimitedPBMessageContinuation<Stream, 1024> read_pb_;
};

template <class Stream>
template <class Handler>
void DataTransferSaslStream<Stream>::Handshake(const Handler &next) {
  using ::hadoop::hdfs::DataTransferEncryptorMessageProto;
  using ::hdfs::asio_continuation::Write;
  using ::hdfs::continuation::WriteDelimitedPBMessage;

  static const int kMagicNumber = htonl(kDataTransferSasl);
  static const asio::const_buffers_1 kMagicNumberBuffer = asio::buffer(
      reinterpret_cast<const char *>(kMagicNumber), sizeof(kMagicNumber));

  struct State {
    DataTransferEncryptorMessageProto req0;
    std::string resp0;
    DataTransferEncryptorMessageProto req1;
    std::string resp1;
    std::shared_ptr<Stream> stream;
  };
  auto m = continuation::Pipeline<State>::Create();
  State *s = &m->state();
  s->stream = stream_;

  DataTransferSaslStreamUtil::PrepareInitialHandshake(&s->req0);

  m->Push(Write(stream_.get(), kMagicNumberBuffer))
      .Push(WriteDelimitedPBMessage(stream_, &s->req0))
      .Push(new ReadSaslMessage(stream_, &s->resp0))
      .Push(new Authenticator(&authenticator_, &s->resp0, &s->req1))
      .Push(WriteDelimitedPBMessage(stream_, &s->req1))
      .Push(new ReadSaslMessage(stream_, &s->resp1));
  m->Run([next](const Status &status, const State &) { next(status); });
}

template <class Stream>
void DataTransferSaslStream<Stream>::Cancel() {
  /* implement with secured reads */
}

}

#endif
