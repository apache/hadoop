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

#include "common/async_stream.h"

#include <boost/system/error_code.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/io_service.hpp>

#include <gmock/gmock.h>

namespace hdfs {

typedef std::pair<boost::system::error_code, std::string> ProducerResult;
class AsioProducer {
public:
  /*
   *  Return either:
   *     (::boost::system::error_code(), <some data>) for a good result
   *     (<an ::asio::error instance>, <anything>) to pass an error to the caller
   *     (::asio::error::would_block, <anything>) to block the next call forever
   */

  virtual ProducerResult Produce() = 0;
};


class MockConnectionBase : public AsioProducer, public AsyncStream {
public:
  MockConnectionBase(boost::asio::io_service *io_service);
  virtual ~MockConnectionBase();
  typedef std::pair<boost::system::error_code, std::string> ProducerResult;

  void async_read_some(const MutableBuffer &buf,
          std::function<void (const boost::system::error_code & error,
                                 std::size_t bytes_transferred) > handler) override {
    if (produced_.size() == 0) {
      ProducerResult r = Produce();
      if (r.first == boost::asio::error::would_block) {
        return; // No more reads to do
      }
      if (r.first) {
        io_service_->post(std::bind(handler, r.first, 0));
        return;
      }
      boost::asio::mutable_buffers_1 data = produced_.prepare(r.second.size());
      boost::asio::buffer_copy(data, boost::asio::buffer(r.second));
      produced_.commit(r.second.size());
    }

    size_t len = std::min(boost::asio::buffer_size(buf), produced_.size());
    boost::asio::buffer_copy(buf, produced_.data());
    produced_.consume(len);
    io_service_->post(std::bind(handler, boost::system::error_code(), len));
  }

  void async_write_some(const ConstBuffer &buf,
            std::function<void (const boost::system::error_code & error,
                                 std::size_t bytes_transferred) > handler) override {
    // CompletionResult res = OnWrite(buf);
    io_service_->post(std::bind(handler, boost::system::error_code(), boost::asio::buffer_size(buf)));
  }

  template <class Endpoint, class Callback>
  void async_connect(const Endpoint &, Callback &&handler) {
    io_service_->post([handler]() { handler(::boost::system::error_code()); });
  }

  virtual void cancel() {}
  virtual void close() {}
protected:
  ProducerResult Produce() override = 0;
  boost::asio::io_service *io_service_;

private:
  boost::asio::streambuf produced_;
};




class SharedConnectionData : public AsioProducer {
 public:
  bool checkProducerForConnect = false;

  MOCK_METHOD0(Produce, ProducerResult());
};

class SharedMockConnection : public MockConnectionBase {
public:
  using MockConnectionBase::MockConnectionBase;

  template <class Endpoint, class Callback>
  void async_connect(const Endpoint &, Callback &&handler) {
    auto data = shared_connection_data_.lock();
    assert(data);

    if (!data->checkProducerForConnect) {
      io_service_->post([handler]() { handler(::boost::system::error_code()); });
    } else {
      ProducerResult result = Produce();
      if (result.first == boost::asio::error::would_block) {
        return; // Connect will hang
      } else {
        io_service_->post([handler, result]() { handler( result.first); });
      }
    }
  }

  static void SetSharedConnectionData(std::shared_ptr<SharedConnectionData> new_producer) {
    shared_connection_data_ = new_producer; // get a weak reference to it
  }

protected:
  ProducerResult Produce() override;

  static std::weak_ptr<SharedConnectionData> shared_connection_data_;
};

}

#endif
