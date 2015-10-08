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

namespace hdfs {

enum {
  kDataTransferVersion = 28,
  kDataTransferSasl = 0xdeadbeef,
};

enum Operation {
  kWriteBlock = 80,
  kReadBlock = 81,
};

template <class Stream> class DataTransferSaslStream {
public:
  DataTransferSaslStream(Stream *stream, const std::string &username,
                         const std::string &password)
      : stream_(stream), authenticator_(username, password) {}

  template <class Handler> void Handshake(const Handler &next);

  template <class MutableBufferSequence, class ReadHandler>
  void async_read_some(const MutableBufferSequence &buffers,
                       ReadHandler &&handler);

  template <class ConstBufferSequence, class WriteHandler>
  void async_write_some(const ConstBufferSequence &buffers,
                        WriteHandler &&handler);

private:
  DataTransferSaslStream(const DataTransferSaslStream &) = delete;
  DataTransferSaslStream &operator=(const DataTransferSaslStream &) = delete;
  Stream *stream_;
  DigestMD5Authenticator authenticator_;
  struct ReadSaslMessage;
  struct Authenticator;
};
}

#include "datatransfer_impl.h"

#endif
