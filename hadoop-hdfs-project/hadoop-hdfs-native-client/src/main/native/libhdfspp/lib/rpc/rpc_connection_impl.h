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
#ifndef LIB_RPC_RPC_CONNECTION_IMPL_H_
#define LIB_RPC_RPC_CONNECTION_IMPL_H_

#include "rpc_connection.h"
#include "rpc_engine.h"
#include "request.h"

#include "common/auth_info.h"
#include "common/logging.h"
#include "common/util.h"
#include "common/libhdfs_events_impl.h"
#include "hdfspp/ioservice.h"

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

#include <system_error>

namespace hdfs {

template <class Socket>
class RpcConnectionImpl : public RpcConnection {
public:
  MEMCHECKED_CLASS(RpcConnectionImpl)

  RpcConnectionImpl(std::shared_ptr<RpcEngine> engine);
  virtual ~RpcConnectionImpl() override;

  virtual void Connect(const std::vector<::asio::ip::tcp::endpoint> &server,
                       const AuthInfo & auth_info,
                       RpcCallback &handler) override;
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


  Socket &TEST_get_mutable_socket() { return socket_; }

  void TEST_set_connected(bool connected) { connected_ = connected ? kConnected : kNotYetConnected; }

 private:
  const Options options_;
  ::asio::ip::tcp::endpoint current_endpoint_;
  std::vector<::asio::ip::tcp::endpoint> additional_endpoints_;
  Socket socket_;
  ::asio::deadline_timer connect_timer_;

  void ConnectComplete(const ::asio::error_code &ec, const ::asio::ip::tcp::endpoint &remote);
};

template <class Socket>
RpcConnectionImpl<Socket>::RpcConnectionImpl(std::shared_ptr<RpcEngine> engine)
    : RpcConnection(engine),
      options_(engine->options()),
      socket_(engine->io_service()->GetRaw()),
      connect_timer_(engine->io_service()->GetRaw())
{
      LOG_TRACE(kRPC, << "RpcConnectionImpl::RpcConnectionImpl called &" << (void*)this);
}

template <class Socket>
RpcConnectionImpl<Socket>::~RpcConnectionImpl() {
  LOG_DEBUG(kRPC, << "RpcConnectionImpl::~RpcConnectionImpl called &" << (void*)this);

  if (pending_requests_.size() > 0)
    LOG_WARN(kRPC, << "RpcConnectionImpl::~RpcConnectionImpl called with items in the pending queue");
  if (sent_requests_.size() > 0)
    LOG_WARN(kRPC, << "RpcConnectionImpl::~RpcConnectionImpl called with items in the sent_requests queue");
}

template <class Socket>
void RpcConnectionImpl<Socket>::Connect(
    const std::vector<::asio::ip::tcp::endpoint> &server,
    const AuthInfo & auth_info,
    RpcCallback &handler) {
  LOG_TRACE(kRPC, << "RpcConnectionImpl::Connect called");

  this->auth_info_ = auth_info;

  std::shared_ptr<Request> connectionRequest;
  { // Scope to minimize how long RpcEngine's lifetime may be extended
    std::shared_ptr<LockFreeRpcEngine> pinned_engine = engine_.lock();
    if(!pinned_engine) {
      LOG_ERROR(kRPC, << "RpcConnectionImpl@" << this << " attempted to access invalid RpcEngine");
      handler(Status::Error("Invalid RpcEngine access."));
      return;
    }

    connectionRequest = std::make_shared<Request>(pinned_engine,
        [handler](::google::protobuf::io::CodedInputStream *is,const Status &status) {
            (void)is;
            handler(status);
        });
  }

  pending_requests_.push_back(connectionRequest);
  this->ConnectAndFlush(server);  // need "this" so compiler can infer type of CAF
}

template <class Socket>
void RpcConnectionImpl<Socket>::ConnectAndFlush(
    const std::vector<::asio::ip::tcp::endpoint> &server) {

  LOG_INFO(kRPC, << "ConnectAndFlush called");
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
  current_endpoint_ = first_endpoint;

  auto shared_this = shared_from_this();
  socket_.async_connect(first_endpoint, [shared_this, this, first_endpoint](const ::asio::error_code &ec) {
    ConnectComplete(ec, first_endpoint);
  });

  // Prompt the timer to timeout
  auto weak_this = std::weak_ptr<RpcConnection>(shared_this);
  connect_timer_.expires_from_now(
        std::chrono::milliseconds(options_.rpc_connect_timeout));
  connect_timer_.async_wait([shared_this, this, first_endpoint](const ::asio::error_code &ec) {
      if (ec)
        ConnectComplete(ec, first_endpoint);
      else
        ConnectComplete(make_error_code(asio::error::host_unreachable), first_endpoint);
  });
}

template <class Socket>
void RpcConnectionImpl<Socket>::ConnectComplete(const ::asio::error_code &ec, const ::asio::ip::tcp::endpoint & remote) {
  auto shared_this = RpcConnectionImpl<Socket>::shared_from_this();
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  connect_timer_.cancel();

  LOG_TRACE(kRPC, << "RpcConnectionImpl::ConnectComplete called");

  // Could be an old async connect returning a result after we've moved on
  if (remote != current_endpoint_) {
      LOG_DEBUG(kRPC, << "Got ConnectComplete for " << remote << " but current_endpoint_ is " << current_endpoint_);
      return;
  }
  if (connected_ != kConnecting) {
      LOG_DEBUG(kRPC, << "Got ConnectComplete but current state is " << connected_);;
      return;
  }

  Status status = ToStatus(ec);
  if(event_handlers_) {
    event_response event_resp = event_handlers_->call(FS_NN_CONNECT_EVENT, cluster_name_.c_str(), 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
    if (event_resp.response_type() == event_response::kTest_Error) {
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
    LOG_DEBUG(kRPC, << "Rpc connection failed; err=" << status.ToString());;
    std::string err = SafeDisconnect(get_asio_socket_ptr(&socket_));
    if(!err.empty()) {
      LOG_INFO(kRPC, << "Rpc connection failed to connect to endpoint, error closing connection: " << err);
    }

    if (!additional_endpoints_.empty()) {
      // If we have additional endpoints, keep trying until we either run out or
      //    hit one
      ::asio::ip::tcp::endpoint next_endpoint = additional_endpoints_.front();
      additional_endpoints_.erase(additional_endpoints_.begin());
      current_endpoint_ = next_endpoint;

      socket_.async_connect(next_endpoint, [shared_this, this, next_endpoint](const ::asio::error_code &ec) {
        ConnectComplete(ec, next_endpoint);
      });
      connect_timer_.expires_from_now(
            std::chrono::milliseconds(options_.rpc_connect_timeout));
      connect_timer_.async_wait([shared_this, this, next_endpoint](const ::asio::error_code &ec) {
          if (ec)
            ConnectComplete(ec, next_endpoint);
          else
            ConnectComplete(make_error_code(asio::error::host_unreachable), next_endpoint);
        });
    } else {
      CommsError(status);
    }
  }
}

template <class Socket>
void RpcConnectionImpl<Socket>::SendHandshake(RpcCallback &handler) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  LOG_TRACE(kRPC, << "RpcConnectionImpl::SendHandshake called");
  connected_ = kHandshaking;

  auto shared_this = shared_from_this();
  auto handshake_packet = PrepareHandshakePacket();
  ::asio::async_write(socket_, asio::buffer(*handshake_packet),
                      [handshake_packet, handler, shared_this, this](
                          const ::asio::error_code &ec, size_t) {
                        Status status = ToStatus(ec);
                        handler(status);
                      });
}

template <class Socket>
void RpcConnectionImpl<Socket>::SendContext(RpcCallback &handler) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  LOG_TRACE(kRPC, << "RpcConnectionImpl::SendContext called");

  auto shared_this = shared_from_this();
  auto context_packet = PrepareContextPacket();
  ::asio::async_write(socket_, asio::buffer(*context_packet),
                      [context_packet, handler, shared_this, this](
                          const ::asio::error_code &ec, size_t) {
                        Status status = ToStatus(ec);
                        handler(status);
                      });
}

template <class Socket>
void RpcConnectionImpl<Socket>::OnSendCompleted(const ::asio::error_code &ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  LOG_TRACE(kRPC, << "RpcConnectionImpl::OnSendCompleted called");

  outgoing_request_.reset();
  if (ec) {
    LOG_WARN(kRPC, << "Network error during RPC write: " << ec.message());
    CommsError(ToStatus(ec));
    return;
  }

  FlushPendingRequests();
}

template <class Socket>
void RpcConnectionImpl<Socket>::FlushPendingRequests() {
  using namespace ::std::placeholders;

  // Lock should be held
  assert(lock_held(connection_state_lock_));

  LOG_TRACE(kRPC, << "RpcConnectionImpl::FlushPendingRequests called");

  // Don't send if we don't need to
  if (outgoing_request_) {
    return;
  }

  std::shared_ptr<Request> req;
  switch (connected_) {
  case kNotYetConnected:
    return;
  case kConnecting:
    return;
  case kHandshaking:
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
    assert(sent_requests_.find(req->call_id()) == sent_requests_.end());
    sent_requests_[req->call_id()] = req;
    outgoing_request_ = req;

    req->timer().expires_from_now(
        std::chrono::milliseconds(options_.rpc_timeout));
    req->timer().async_wait([weak_this, weak_req, this](const ::asio::error_code &ec) {
        auto timeout_this = weak_this.lock();
        auto timeout_req = weak_req.lock();
        if (timeout_this && timeout_req)
          this->HandleRpcTimeout(timeout_req, ec);
    });

    asio::async_write(socket_, asio::buffer(*payload),
                      [shared_this, this, payload](const ::asio::error_code &ec,
                                                   size_t size) {
                        OnSendCompleted(ec, size);
                      });
  } else {  // Nothing to send for this request, inform the handler immediately
    std::shared_ptr<IoService> service = GetIoService();
    if(!service) {
      LOG_ERROR(kRPC, << "RpcConnectionImpl@" << this << " attempted to access null IoService");
      // No easy way to bail out of this context, but the only way to get here is when
      // the FileSystem is being destroyed.
      return;
    }

    service->PostTask(
        // Never hold locks when calling a callback
        [req]() { req->OnResponseArrived(nullptr, Status::OK()); }
    );

    // Reschedule to flush the next one
    AsyncFlushPendingRequests();
  }
}


template <class Socket>
void RpcConnectionImpl<Socket>::OnRecvCompleted(const ::asio::error_code &original_ec,
                                                   size_t) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  ::asio::error_code my_ec(original_ec);

  LOG_TRACE(kRPC, << "RpcConnectionImpl::OnRecvCompleted called");

  std::shared_ptr<RpcConnection> shared_this = shared_from_this();

  if(event_handlers_) {
    event_response event_resp = event_handlers_->call(FS_NN_READ_EVENT, cluster_name_.c_str(), 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
    if (event_resp.response_type() == event_response::kTest_Error) {
      my_ec = std::make_error_code(std::errc::network_down);
    }
#endif
  }

  switch (my_ec.value()) {
    case 0:
      // No errors
      break;
    case asio::error::operation_aborted:
      // The event loop has been shut down. Ignore the error.
      return;
    default:
      LOG_WARN(kRPC, << "Network error during RPC read: " << my_ec.message());
      CommsError(ToStatus(my_ec));
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
        socket_, buf,
        [shared_this, this](const ::asio::error_code &ec, size_t size) {
          OnRecvCompleted(ec, size);
        });
  } else if (current_response_state_->state_ == Response::kReadContent) {
    current_response_state_->state_ = Response::kParseResponse;
    current_response_state_->length_ = ntohl(current_response_state_->length_);
    current_response_state_->data_.resize(current_response_state_->length_);
    asio::async_read(
        socket_, ::asio::buffer(current_response_state_->data_),
        [shared_this, this](const ::asio::error_code &ec, size_t size) {
          OnRecvCompleted(ec, size);
        });
  } else if (current_response_state_->state_ == Response::kParseResponse) {
    // Check return status from the RPC response.  We may have received a msg
    // indicating a server side error.

    Status stat = HandleRpcResponse(current_response_state_);

    if(stat.get_server_exception_type() == Status::kStandbyException) {
      // May need to bail out, connect to new NN, and restart loop
      LOG_INFO(kRPC, << "Communicating with standby NN, attempting to reconnect");
    }

    current_response_state_ = nullptr;
    StartReading();
  }
}

template <class Socket>
void RpcConnectionImpl<Socket>::Disconnect() {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  LOG_INFO(kRPC, << "RpcConnectionImpl::Disconnect called");

  outgoing_request_.reset();
  if (connected_ == kConnecting || connected_ == kHandshaking || connected_ == kAuthenticating || connected_ == kConnected) {
    // Don't print out errors, we were expecting a disconnect here
    SafeDisconnect(get_asio_socket_ptr(&socket_));
  }
  connected_ = kDisconnected;
}
}

#endif
