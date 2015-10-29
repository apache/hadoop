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

template <class Stream> class DataTransferSaslStream : public AsyncStream {
public:
  DataTransferSaslStream(std::shared_ptr<Stream> stream, const std::string &username,
                         const std::string &password)
      : stream_(stream), authenticator_(username, password) {}

  template <class Handler> void Handshake(const Handler &next);

    virtual void async_read(const asio::mutable_buffers_1	& buffers,
               std::function<void (const asio::error_code & error,
                                   std::size_t bytes_transferred) > completed_handler) 
      { stream_->async_read(buffers, completed_handler); }
    
    virtual void async_read(const asio::mutable_buffers_1	& buffers,
               std::function<bool (const asio::error_code & error,
                                   std::size_t bytes_transferred) > completion_handler,
               std::function<void (const asio::error_code & error,
                                   std::size_t bytes_transferred) > completed_handler)
      { stream_->async_read(buffers, completion_handler, completed_handler); }
    virtual void async_write(const asio::const_buffers_1 & buffers, 
               std::function<void (const asio::error_code &ec, size_t)> handler)
      { stream_->async_write(buffers, handler); }

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
