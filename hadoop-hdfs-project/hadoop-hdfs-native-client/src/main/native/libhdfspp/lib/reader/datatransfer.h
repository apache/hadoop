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
#ifndef LIB_READER_DATA_TRANSFER_H_
#define LIB_READER_DATA_TRANSFER_H_

#include "common/sasl_authenticator.h"
#include "common/async_stream.h"
#include "connection/datanodeconnection.h"
#include <memory>


namespace hdfs {

enum {
  kDataTransferVersion = 28,
  kDataTransferSasl = 0xdeadbeef,
};

enum Operation {
  kWriteBlock = 80,
  kReadBlock = 81,
};

template <class Stream> class DataTransferSaslStream : public DataNodeConnection {
public:
  DataTransferSaslStream(std::shared_ptr<Stream> stream, const std::string &username,
                         const std::string &password)
      : stream_(stream), authenticator_(username, password) {}

  template <class Handler> void Handshake(const Handler &next);

  void async_read_some(const MutableBuffers &buf,
          std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > handler) override {
    stream_->async_read_some(buf, handler);
  }

  void async_write_some(const ConstBuffers &buf,
            std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > handler) override {
    stream_->async_write_some(buf, handler);
  }

  void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) override
  {(void)handler;  /*TODO: Handshaking goes here*/};
private:
  DataTransferSaslStream(const DataTransferSaslStream &) = delete;
  DataTransferSaslStream &operator=(const DataTransferSaslStream &) = delete;
  std::shared_ptr<Stream> stream_;
  DigestMD5Authenticator authenticator_;
  struct ReadSaslMessage;
  struct Authenticator;
};
}

#include "datatransfer_impl.h"

#endif
