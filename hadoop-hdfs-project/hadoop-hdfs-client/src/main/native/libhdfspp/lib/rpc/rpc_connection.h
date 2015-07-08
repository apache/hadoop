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
#ifndef LIB_RPC_RPC_CONNECTION_H_
#define LIB_RPC_RPC_CONNECTION_H_

#include "rpc_engine.h"
#include "common/util.h"

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

namespace hdfs {

template <class NextLayer> class RpcConnectionImpl : public RpcConnection {
public:
  RpcConnectionImpl(RpcEngine *engine);
  virtual void Connect(const std::vector<::asio::ip::tcp::endpoint> &server,
                       Callback &&handler) override;
  virtual void Handshake(Callback &&handler) override;
  virtual void Shutdown() override;
  virtual void OnSendCompleted(const ::asio::error_code &ec,
                               size_t transferred) override;
  virtual void OnRecvCompleted(const ::asio::error_code &ec,
                               size_t transferred) override;

private:
  NextLayer next_layer_;
};

template <class NextLayer>
RpcConnectionImpl<NextLayer>::RpcConnectionImpl(RpcEngine *engine)
    : RpcConnection(engine)
    , next_layer_(engine->io_service())
{}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::Connect(
    const std::vector<::asio::ip::tcp::endpoint> &server, Callback &&handler) {
  ::asio::async_connect(
      next_layer_, server.begin(), server.end(),
      [handler](const ::asio::error_code &ec,
                std::vector<::asio::ip::tcp::endpoint>::const_iterator) {
        handler(ToStatus(ec));
      });
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::Handshake(Callback &&handler) {
  auto handshake_packet = PrepareHandshakePacket();
  ::asio::async_write(
      next_layer_, asio::buffer(*handshake_packet),
      [handshake_packet, handler](const ::asio::error_code &ec, size_t) {
        handler(ToStatus(ec));
      });
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::OnSendCompleted(const ::asio::error_code &ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(engine_state_lock_);

  request_over_the_wire_.reset();
  if (ec) {
    // TODO: Current RPC has failed -- we should abandon the
    // connection and do proper clean up
    assert(false && "Unimplemented");
  }

  if (!pending_requests_.size()) {
    return;
  }

  std::shared_ptr<Request> req = pending_requests_.front();
  pending_requests_.erase(pending_requests_.begin());
  requests_on_fly_[req->call_id()] = req;
  request_over_the_wire_ = req;

  // TODO: set the timeout for the RPC request

  asio::async_write(
      next_layer_, asio::buffer(req->payload()),
      std::bind(&RpcConnectionImpl<NextLayer>::OnSendCompleted, this, _1, _2));
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::OnRecvCompleted(const ::asio::error_code &ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(engine_state_lock_);

  switch (ec.value()) {
  case 0:
    // No errors
    break;
  case asio::error::operation_aborted:
    // The event loop has been shut down. Ignore the error.
    return;
  default:
    assert(false && "Unimplemented");
  }

  if (resp_state_ == kReadLength) {
    resp_state_ = kReadContent;
    auto buf = ::asio::buffer(reinterpret_cast<char *>(&resp_length_),
                              sizeof(resp_length_));
    asio::async_read(next_layer_, buf,
                     std::bind(&RpcConnectionImpl<NextLayer>::OnRecvCompleted,
                               this, _1, _2));

  } else if (resp_state_ == kReadContent) {
    resp_state_ = kParseResponse;
    resp_length_ = ntohl(resp_length_);
    resp_data_.resize(resp_length_);
    asio::async_read(next_layer_, ::asio::buffer(resp_data_),
                     std::bind(&RpcConnectionImpl<NextLayer>::OnRecvCompleted, this, _1, _2));

  } else if (resp_state_ == kParseResponse) {
    resp_state_ = kReadLength;
    HandleRpcResponse(resp_data_);
    resp_data_.clear();
    Start();
  }
}

template <class NextLayer> void RpcConnectionImpl<NextLayer>::Shutdown() {
  next_layer_.close();
}
}

#endif
