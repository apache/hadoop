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
#ifndef LIBHDFSPP_TEST_MOCK_CONNECTION_H_
#define LIBHDFSPP_TEST_MOCK_CONNECTION_H_

#include <asio/error_code.hpp>
#include <asio/buffer.hpp>
#include <asio/streambuf.hpp>
#include <gmock/gmock.h>

namespace hdfs {

class MockConnectionBase {
public:
  virtual ~MockConnectionBase();
  typedef std::pair<asio::error_code, std::string> ProducerResult;
  template <class MutableBufferSequence, class Handler>
  void async_read_some(const MutableBufferSequence &buf, Handler &&handler) {
    if (produced_.size() == 0) {
      ProducerResult r = Produce();
      if (r.first) {
        handler(r.first, 0);
      }
      asio::mutable_buffers_1 data = produced_.prepare(r.second.size());
      asio::buffer_copy(data, asio::buffer(r.second));
      produced_.commit(r.second.size());
    }

    size_t len = std::min(asio::buffer_size(buf), produced_.size());
    asio::buffer_copy(buf, produced_.data());
    produced_.consume(len);
    handler(asio::error_code(), len);
  }

  template <class ConstBufferSequence, class Handler>
  void async_write_some(const ConstBufferSequence &buf, Handler &&handler) {
    // CompletionResult res = OnWrite(buf);
    handler(asio::error_code(), asio::buffer_size(buf));
  }

protected:
  virtual ProducerResult Produce() = 0;

private:
  asio::streambuf produced_;
};
}

#endif
