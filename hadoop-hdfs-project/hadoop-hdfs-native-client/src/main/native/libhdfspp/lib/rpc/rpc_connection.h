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

#include "common/logging.h"
#include "common/util.h"

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

namespace hdfs {

template <class NextLayer>
class RpcConnectionImpl : public RpcConnection {
public:
  RpcConnectionImpl(RpcEngine *engine);
  virtual void Connect(const ::asio::ip::tcp::endpoint &server,
                       RpcCallback &handler);
  virtual void ConnectAndFlush(
      const ::asio::ip::tcp::endpoint &server) override;
  virtual void Handshake(RpcCallback &handler) override;
  virtual void Disconnect() override;
  virtual void OnSendCompleted(const ::asio::error_code &ec,
                               size_t transferred) override;
  virtual void OnRecvCompleted(const ::asio::error_code &ec,
                               size_t transferred) override;
  virtual void FlushPendingRequests() override;


  NextLayer &next_layer() { return next_layer_; }

  void TEST_set_connected(bool new_value) { connected_ = new_value; }

 private:
  const Options options_;
  NextLayer next_layer_;
};

template <class NextLayer>
RpcConnectionImpl<NextLayer>::RpcConnectionImpl(RpcEngine *engine)
    : RpcConnection(engine),
      options_(engine->options()),
      next_layer_(engine->io_service()) {}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::Connect(
    const ::asio::ip::tcp::endpoint &server, RpcCallback &handler) {
  auto connectionSuccessfulReq = std::make_shared<Request>(
      engine_, [handler](::google::protobuf::io::CodedInputStream *is,
                         const Status &status) {
        (void)is;
        handler(status);
      });
  pending_requests_.push_back(connectionSuccessfulReq);
  this->ConnectAndFlush(server);  // need "this" so compiler can infer type of CAF
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::ConnectAndFlush(
    const ::asio::ip::tcp::endpoint &server) {
  std::shared_ptr<RpcConnection> shared_this = shared_from_this();
  next_layer_.async_connect(server,
                            [shared_this, this](const ::asio::error_code &ec) {
                              std::lock_guard<std::mutex> state_lock(connection_state_lock_);
                              Status status = ToStatus(ec);
                              if (status.ok()) {
                                StartReading();
                                Handshake([shared_this, this](const Status &s) {
                                  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
                                  if (s.ok()) {
                                    FlushPendingRequests();
                                  } else {
                                    CommsError(s);
                                  };
                                });
                              } else {
                                CommsError(status);
                              }
                            });
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::Handshake(RpcCallback &handler) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  auto shared_this = shared_from_this();
  auto handshake_packet = PrepareHandshakePacket();
  ::asio::async_write(next_layer_, asio::buffer(*handshake_packet),
                      [handshake_packet, handler, shared_this, this](
                          const ::asio::error_code &ec, size_t) {
                        Status status = ToStatus(ec);
                        if (status.ok()) {
                          connected_ = true;
                        }
                        handler(status);
                      });
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::OnSendCompleted(const ::asio::error_code &ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  request_over_the_wire_.reset();
  if (ec) {
    LOG_WARN() << "Network error during RPC write: " << ec.message();
    CommsError(ToStatus(ec));
    return;
  }

  FlushPendingRequests();
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::FlushPendingRequests() {
  using namespace ::std::placeholders;

  // Lock should be held
  assert(lock_held(connection_state_lock_));

  if (pending_requests_.empty()) {
    return;
  }

  if (!connected_) {
    return;
  }

  // Don't send if we don't need to
  if (request_over_the_wire_) {
    return;
  }

  std::shared_ptr<Request> req = pending_requests_.front();
  pending_requests_.erase(pending_requests_.begin());

  std::shared_ptr<RpcConnection> shared_this = shared_from_this();
  std::shared_ptr<std::string> payload = std::make_shared<std::string>();
  req->GetPacket(payload.get());
  if (!payload->empty()) {
    requests_on_fly_[req->call_id()] = req;
    request_over_the_wire_ = req;

    req->timer().expires_from_now(
        std::chrono::milliseconds(options_.rpc_timeout));
    req->timer().async_wait(std::bind(
      &RpcConnection::HandleRpcTimeout, this, req, _1));

    asio::async_write(next_layer_, asio::buffer(*payload),
                      [shared_this, this, payload](const ::asio::error_code &ec,
                                                   size_t size) {
                        OnSendCompleted(ec, size);
                      });
  } else {  // Nothing to send for this request, inform the handler immediately
    io_service().post(
        // Never hold locks when calling a callback
        [req]() { req->OnResponseArrived(nullptr, Status::OK()); }
    );

    // Reschedule to flush the next one
    AsyncFlushPendingRequests();
  }
}


template <class NextLayer>
void RpcConnectionImpl<NextLayer>::OnRecvCompleted(const ::asio::error_code &ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  std::shared_ptr<RpcConnection> shared_this = shared_from_this();

  switch (ec.value()) {
    case 0:
      // No errors
      break;
    case asio::error::operation_aborted:
      // The event loop has been shut down. Ignore the error.
      return;
    default:
      LOG_WARN() << "Network error during RPC read: " << ec.message();
      CommsError(ToStatus(ec));
      return;
  }

  if (!response_) { /* start a new one */
    response_ = std::make_shared<Response>();
  }

  if (response_->state_ == Response::kReadLength) {
    response_->state_ = Response::kReadContent;
    auto buf = ::asio::buffer(reinterpret_cast<char *>(&response_->length_),
                              sizeof(response_->length_));
    asio::async_read(
        next_layer_, buf,
        [shared_this, this](const ::asio::error_code &ec, size_t size) {
          OnRecvCompleted(ec, size);
        });
  } else if (response_->state_ == Response::kReadContent) {
    response_->state_ = Response::kParseResponse;
    response_->length_ = ntohl(response_->length_);
    response_->data_.resize(response_->length_);
    asio::async_read(
        next_layer_, ::asio::buffer(response_->data_),
        [shared_this, this](const ::asio::error_code &ec, size_t size) {
          OnRecvCompleted(ec, size);
        });
  } else if (response_->state_ == Response::kParseResponse) {
    HandleRpcResponse(response_);
    response_ = nullptr;
    StartReading();
  }
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::Disconnect() {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  request_over_the_wire_.reset();
  next_layer_.cancel();
  next_layer_.close();
  connected_ = false;
}
}

#endif
