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

#include "common/auth_info.h"
#include "common/logging.h"
#include "common/util.h"
#include "common/libhdfs_events_impl.h"
#include "sasl_protocol.h"

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

namespace hdfs {

template <class NextLayer>
class RpcConnectionImpl : public RpcConnection {
public:
  RpcConnectionImpl(RpcEngine *engine);
  virtual ~RpcConnectionImpl() override;

  virtual void Connect(const std::vector<::asio::ip::tcp::endpoint> &server,
                       const AuthInfo & auth_info,
                       RpcCallback &handler);
  virtual void ConnectAndFlush(
      const std::vector<::asio::ip::tcp::endpoint> &server) override;
  virtual void SendHandshake(RpcCallback &handler) override;
  virtual void SendContext(RpcCallback &handler) override;
  virtual void Disconnect() override;
  virtual void OnSendCompleted(const ::asio::error_code &ec,
                               size_t transferred) override;
  virtual void OnRecvCompleted(const ::asio::error_code &ec,
                               size_t transferred) override;
  virtual void FlushPendingRequests() override;


  NextLayer &next_layer() { return next_layer_; }

  void TEST_set_connected(bool connected) { connected_ = connected ? kConnected : kNotYetConnected; }

 private:
  const Options options_;
  std::vector<::asio::ip::tcp::endpoint> additional_endpoints_;
  NextLayer next_layer_;

  void ConnectComplete(const ::asio::error_code &ec);
};

template <class NextLayer>
RpcConnectionImpl<NextLayer>::RpcConnectionImpl(RpcEngine *engine)
    : RpcConnection(engine),
      options_(engine->options()),
      next_layer_(engine->io_service()) {
    LOG_TRACE(kRPC, << "RpcConnectionImpl::RpcConnectionImpl called");
}

template <class NextLayer>
RpcConnectionImpl<NextLayer>::~RpcConnectionImpl() {
  LOG_DEBUG(kRPC, << "RpcConnectionImpl::~RpcConnectionImpl called &" << (void*)this);

  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  if (pending_requests_.size() > 0)
    LOG_WARN(kRPC, << "RpcConnectionImpl::~RpcConnectionImpl called with items in the pending queue");
  if (requests_on_fly_.size() > 0)
    LOG_WARN(kRPC, << "RpcConnectionImpl::~RpcConnectionImpl called with items in the requests_on_fly queue");
}


template <class NextLayer>
void RpcConnectionImpl<NextLayer>::Connect(
    const std::vector<::asio::ip::tcp::endpoint> &server,
    const AuthInfo & auth_info,
    RpcCallback &handler) {
  LOG_TRACE(kRPC, << "RpcConnectionImpl::Connect called");

  this->auth_info_ = auth_info;

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
    const std::vector<::asio::ip::tcp::endpoint> &server) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  if (server.empty()) {
    Status s = Status::InvalidArgument("No endpoints provided");
    CommsError(s);
    return;
  }

  if (connected_ == kConnected) {
    FlushPendingRequests();
    return;
  }
  if (connected_ != kNotYetConnected) {
    LOG_WARN(kRPC, << "RpcConnectionImpl::ConnectAndFlush called while connected=" << ToString(connected_));
    return;
  }
  connected_ = kConnecting;

  // Take the first endpoint, but remember the alternatives for later
  additional_endpoints_ = server;
  ::asio::ip::tcp::endpoint first_endpoint = additional_endpoints_.front();
  additional_endpoints_.erase(additional_endpoints_.begin());

  auto shared_this = shared_from_this();
  next_layer_.async_connect(first_endpoint, [shared_this, this](const ::asio::error_code &ec) {
    ConnectComplete(ec);
  });
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::ConnectComplete(const ::asio::error_code &ec) {
  auto shared_this = RpcConnectionImpl<NextLayer>::shared_from_this();
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  LOG_TRACE(kRPC, << "RpcConnectionImpl::ConnectComplete called");

  Status status = ToStatus(ec);
  if(event_handlers_) {
    auto event_resp = event_handlers_->call(FS_NN_CONNECT_EVENT, cluster_name_.c_str(), 0);
#ifndef NDEBUG
    if (event_resp.response() == event_response::kTest_Error) {
      status = event_resp.status();
    }
#endif
  }

  if (status.ok()) {
    StartReading();
    SendHandshake([shared_this, this](const Status & s) {
      HandshakeComplete(s);
    });
  } else {
    std::string err = SafeDisconnect(get_asio_socket_ptr(&next_layer_));
    if(!err.empty()) {
      LOG_INFO(kRPC, << "Rpc connection failed to connect to endpoint, error closing connection: " << err);
    }

    if (!additional_endpoints_.empty()) {
      // If we have additional endpoints, keep trying until we either run out or
      //    hit one
      ::asio::ip::tcp::endpoint next_endpoint = additional_endpoints_.front();
      additional_endpoints_.erase(additional_endpoints_.begin());

      next_layer_.async_connect(next_endpoint, [shared_this, this](const ::asio::error_code &ec) {
        ConnectComplete(ec);
      });
    } else {
      CommsError(status);
    }
  }
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::SendHandshake(RpcCallback &handler) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  LOG_TRACE(kRPC, << "RpcConnectionImpl::SendHandshake called");

  auto shared_this = shared_from_this();
  auto handshake_packet = PrepareHandshakePacket();
  ::asio::async_write(next_layer_, asio::buffer(*handshake_packet),
                      [handshake_packet, handler, shared_this, this](
                          const ::asio::error_code &ec, size_t) {
                        Status status = ToStatus(ec);
                        handler(status);
                      });
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::SendContext(RpcCallback &handler) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  LOG_TRACE(kRPC, << "RpcConnectionImpl::SendContext called");

  auto shared_this = shared_from_this();
  auto context_packet = PrepareContextPacket();
  ::asio::async_write(next_layer_, asio::buffer(*context_packet),
                      [context_packet, handler, shared_this, this](
                          const ::asio::error_code &ec, size_t) {
                        Status status = ToStatus(ec);
                        handler(status);
                      });
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::OnSendCompleted(const ::asio::error_code &ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  LOG_TRACE(kRPC, << "RpcConnectionImpl::OnSendCompleted called");

  request_over_the_wire_.reset();
  if (ec) {
    LOG_WARN(kRPC, << "Network error during RPC write: " << ec.message());
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

  LOG_TRACE(kRPC, << "RpcConnectionImpl::FlushPendingRequests called");

  // Don't send if we don't need to
  if (request_over_the_wire_) {
    return;
  }

  std::shared_ptr<Request> req;
  switch (connected_) {
  case kNotYetConnected:
    return;
  case kConnecting:
    return;
  case kAuthenticating:
    if (auth_requests_.empty()) {
      return;
    }
    req = auth_requests_.front();
    auth_requests_.erase(auth_requests_.begin());
    break;
  case kConnected:
    if (pending_requests_.empty()) {
      return;
    }
    req = pending_requests_.front();
    pending_requests_.erase(pending_requests_.begin());
    break;
  case kDisconnected:
    LOG_DEBUG(kRPC, << "RpcConnectionImpl::FlushPendingRequests attempted to flush a " << ToString(connected_) << " connection");
    return;
  default:
    LOG_DEBUG(kRPC, << "RpcConnectionImpl::FlushPendingRequests invalid state: " << ToString(connected_));
    return;
  }

  std::shared_ptr<RpcConnection> shared_this = shared_from_this();
  auto weak_this = std::weak_ptr<RpcConnection>(shared_this);
  auto weak_req = std::weak_ptr<Request>(req);

  std::shared_ptr<std::string> payload = std::make_shared<std::string>();
  req->GetPacket(payload.get());
  if (!payload->empty()) {
    assert(requests_on_fly_.find(req->call_id()) == requests_on_fly_.end());
    requests_on_fly_[req->call_id()] = req;
    request_over_the_wire_ = req;

    req->timer().expires_from_now(
        std::chrono::milliseconds(options_.rpc_timeout));
    req->timer().async_wait([weak_this, weak_req, this](const ::asio::error_code &ec) {
        auto timeout_this = weak_this.lock();
        auto timeout_req = weak_req.lock();
        if (timeout_this && timeout_req)
          this->HandleRpcTimeout(timeout_req, ec);
    });

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
void RpcConnectionImpl<NextLayer>::OnRecvCompleted(const ::asio::error_code &asio_ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  LOG_TRACE(kRPC, << "RpcConnectionImpl::OnRecvCompleted called");

  std::shared_ptr<RpcConnection> shared_this = shared_from_this();

  ::asio::error_code ec = asio_ec;
  if(event_handlers_) {
    auto event_resp = event_handlers_->call(FS_NN_READ_EVENT, cluster_name_.c_str(), 0);
#ifndef NDEBUG
    if (event_resp.response() == event_response::kTest_Error) {
        ec = std::make_error_code(std::errc::network_down);
    }
#endif
  }

  switch (ec.value()) {
    case 0:
      // No errors
      break;
    case asio::error::operation_aborted:
      // The event loop has been shut down. Ignore the error.
      return;
    default:
      LOG_WARN(kRPC, << "Network error during RPC read: " << ec.message());
      CommsError(ToStatus(ec));
      return;
  }

  if (!current_response_state_) { /* start a new one */
    current_response_state_ = std::make_shared<Response>();
  }

  if (current_response_state_->state_ == Response::kReadLength) {
    current_response_state_->state_ = Response::kReadContent;
    auto buf = ::asio::buffer(reinterpret_cast<char *>(&current_response_state_->length_),
                              sizeof(current_response_state_->length_));
    asio::async_read(
        next_layer_, buf,
        [shared_this, this](const ::asio::error_code &ec, size_t size) {
          OnRecvCompleted(ec, size);
        });
  } else if (current_response_state_->state_ == Response::kReadContent) {
    current_response_state_->state_ = Response::kParseResponse;
    current_response_state_->length_ = ntohl(current_response_state_->length_);
    current_response_state_->data_.resize(current_response_state_->length_);
    asio::async_read(
        next_layer_, ::asio::buffer(current_response_state_->data_),
        [shared_this, this](const ::asio::error_code &ec, size_t size) {
          OnRecvCompleted(ec, size);
        });
  } else if (current_response_state_->state_ == Response::kParseResponse) {
    HandleRpcResponse(current_response_state_);
    current_response_state_ = nullptr;
    StartReading();
  }
}

template <class NextLayer>
void RpcConnectionImpl<NextLayer>::Disconnect() {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  LOG_INFO(kRPC, << "RpcConnectionImpl::Disconnect called");

  request_over_the_wire_.reset();
  if (connected_ == kConnecting || connected_ == kAuthenticating || connected_ == kConnected) {
    // Don't print out errors, we were expecting a disconnect here
    SafeDisconnect(get_asio_socket_ptr(&next_layer_));
  }
  connected_ = kDisconnected;
}
}

#endif
