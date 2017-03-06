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
#include "rpc_engine.h"
#include "rpc_connection_impl.h"
#include "sasl_protocol.h"

#include "RpcHeader.pb.h"
#include "ProtobufRpcEngine.pb.h"
#include "IpcConnectionContext.pb.h"

namespace hdfs {

namespace pb = ::google::protobuf;
namespace pbio = ::google::protobuf::io;

using namespace ::hadoop::common;
using namespace ::std::placeholders;

static const int kNoRetry = -1;

static void AddHeadersToPacket(
    std::string *res, std::initializer_list<const pb::MessageLite *> headers,
    const std::string *payload) {
  int len = 0;
  std::for_each(
      headers.begin(), headers.end(),
      [&len](const pb::MessageLite *v) { len += DelimitedPBMessageSize(v); });

  if (payload) {
    len += payload->size();
  }

  int net_len = htonl(len);
  res->reserve(res->size() + sizeof(net_len) + len);

  pbio::StringOutputStream ss(res);
  pbio::CodedOutputStream os(&ss);
  os.WriteRaw(reinterpret_cast<const char *>(&net_len), sizeof(net_len));

  uint8_t *buf = os.GetDirectBufferForNBytesAndAdvance(len);
  assert(buf);

  std::for_each(
      headers.begin(), headers.end(), [&buf](const pb::MessageLite *v) {
        buf = pbio::CodedOutputStream::WriteVarint32ToArray(v->ByteSize(), buf);
        buf = v->SerializeWithCachedSizesToArray(buf);
      });

  if (payload) {
    buf = os.WriteStringToArray(*payload, buf);
  }
}

RpcConnection::~RpcConnection() {}

RpcConnection::RpcConnection(LockFreeRpcEngine *engine)
    : engine_(engine),
      connected_(kNotYetConnected) {}

::asio::io_service &RpcConnection::io_service() {
  return engine_->io_service();
}

void RpcConnection::StartReading() {
  auto shared_this = shared_from_this();
  io_service().post([shared_this, this] () {
    OnRecvCompleted(::asio::error_code(), 0);
  });
}

void RpcConnection::HandshakeComplete(const Status &s) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  LOG_TRACE(kRPC, << "RpcConnectionImpl::HandshakeComplete called");

  if (s.ok()) {
    if (connected_ == kHandshaking) {
      auto shared_this = shared_from_this();

      connected_ = kAuthenticating;
      if (auth_info_.useSASL()) {
#ifdef USE_SASL
        sasl_protocol_ = std::make_shared<SaslProtocol>(cluster_name_, auth_info_, shared_from_this());
        sasl_protocol_->SetEventHandlers(event_handlers_);
        sasl_protocol_->Authenticate([shared_this, this](
                          const Status & status, const AuthInfo & new_auth_info) {
                        AuthComplete(status, new_auth_info); } );
#else
        AuthComplete_locked(Status::Error("SASL is required, but no SASL library was found"), auth_info_);
#endif
      } else {
        AuthComplete_locked(Status::OK(), auth_info_);
      }
    }
  } else {
    CommsError(s);
  };
}

void RpcConnection::AuthComplete(const Status &s, const AuthInfo & new_auth_info) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  AuthComplete_locked(s, new_auth_info);
}

void RpcConnection::AuthComplete_locked(const Status &s, const AuthInfo & new_auth_info) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling
  LOG_TRACE(kRPC, << "RpcConnectionImpl::AuthComplete called");

  // Free the sasl_protocol object
  sasl_protocol_.reset();

  if (s.ok()) {
    auth_info_ = new_auth_info;

    auto shared_this = shared_from_this();
    SendContext([shared_this, this](const Status & s) {
      ContextComplete(s);
    });
  } else {
    CommsError(s);
  };
}

void RpcConnection::ContextComplete(const Status &s) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  LOG_TRACE(kRPC, << "RpcConnectionImpl::ContextComplete called");

  if (s.ok()) {
    if (connected_ == kAuthenticating) {
      connected_ = kConnected;
    }
    FlushPendingRequests();
  } else {
    CommsError(s);
  };
}

void RpcConnection::AsyncFlushPendingRequests() {
  std::shared_ptr<RpcConnection> shared_this = shared_from_this();
  io_service().post([shared_this, this]() {
    std::lock_guard<std::mutex> state_lock(connection_state_lock_);

    LOG_TRACE(kRPC, << "RpcConnection::AsyncFlushPendingRequests called (connected=" << ToString(connected_) << ")");

    if (!request_over_the_wire_) {
      FlushPendingRequests();
    }
  });
}

Status RpcConnection::HandleRpcResponse(std::shared_ptr<Response> response) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  response->ar.reset(new pbio::ArrayInputStream(&response->data_[0], response->data_.size()));
  response->in.reset(new pbio::CodedInputStream(response->ar.get()));
  response->in->PushLimit(response->data_.size());
  RpcResponseHeaderProto h;
  ReadDelimitedPBMessage(response->in.get(), &h);

  auto req = RemoveFromRunningQueue(h.callid());
  if (!req) {
    LOG_WARN(kRPC, << "RPC response with Unknown call id " << h.callid());
    if((int32_t)h.callid() == RpcEngine::kCallIdSasl) {
      return Status::AuthenticationFailed("You have an unsecured client connecting to a secured server");
    } else {
      return Status::Error("Rpc response with unknown call id");
    }
  }

  Status status;
  if(event_handlers_) {
    event_response event_resp = event_handlers_->call(FS_NN_READ_EVENT, cluster_name_.c_str(), 0);
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
    if (event_resp.response() == event_response::kTest_Error) {
      status = event_resp.status();
    }
#endif
  }

  if (status.ok() && h.has_exceptionclassname()) {
    status =
      Status::Exception(h.exceptionclassname().c_str(), h.errormsg().c_str());
  }

  if(status.get_server_exception_type() == Status::kStandbyException) {
    LOG_WARN(kRPC, << "Tried to connect to standby. status = " << status.ToString());

    // We got the request back, but it needs to be resent to the other NN
    std::vector<std::shared_ptr<Request>> reqs_to_redirect = {req};
    PrependRequests_locked(reqs_to_redirect);

    CommsError(status);
    return status;
  }

  io_service().post([req, response, status]() {
    req->OnResponseArrived(response->in.get(), status);  // Never call back while holding a lock
  });

  return Status::OK();
}

void RpcConnection::HandleRpcTimeout(std::shared_ptr<Request> req,
                                     const ::asio::error_code &ec) {
  if (ec.value() == asio::error::operation_aborted) {
    return;
  }

  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  auto r = RemoveFromRunningQueue(req->call_id());
  if (!r) {
    // The RPC might have been finished and removed from the queue
    return;
  }

  Status stat = ToStatus(ec ? ec : make_error_code(::asio::error::timed_out));

  r->OnResponseArrived(nullptr, stat);
}

std::shared_ptr<std::string> RpcConnection::PrepareHandshakePacket() {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  /**   From Client.java:
   *
   * Write the connection header - this is sent when connection is established
   * +----------------------------------+
   * |  "hrpc" 4 bytes                  |
   * +----------------------------------+
   * |  Version (1 byte)                |
   * +----------------------------------+
   * |  Service Class (1 byte)          |
   * +----------------------------------+
   * |  AuthProtocol (1 byte)           |
   * +----------------------------------+
   *
   * AuthProtocol: 0->none, -33->SASL
   */

  char auth_protocol = auth_info_.useSASL() ? -33 : 0;
  const char handshake_header[] = {'h', 'r', 'p', 'c',
                                    RpcEngine::kRpcVersion, 0, auth_protocol};
  auto res =
      std::make_shared<std::string>(handshake_header, sizeof(handshake_header));

  return res;
}

std::shared_ptr<std::string> RpcConnection::PrepareContextPacket() {
  // This needs to be send after the SASL handshake, and
  // after the SASL handshake (if any)
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  auto res = std::make_shared<std::string>();

  RpcRequestHeaderProto h;
  h.set_rpckind(RPC_PROTOCOL_BUFFER);
  h.set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  h.set_callid(RpcEngine::kCallIdConnectionContext);
  h.set_clientid(engine_->client_name());

  IpcConnectionContextProto handshake;
  handshake.set_protocol(engine_->protocol_name());
  const std::string & user_name = auth_info_.getUser();
  if (!user_name.empty()) {
    *handshake.mutable_userinfo()->mutable_effectiveuser() = user_name;
  }
  AddHeadersToPacket(res.get(), {&h, &handshake}, nullptr);

  return res;
}

void RpcConnection::AsyncRpc(
    const std::string &method_name, const ::google::protobuf::MessageLite *req,
    std::shared_ptr<::google::protobuf::MessageLite> resp,
    const RpcCallback &handler) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  AsyncRpc_locked(method_name, req, resp, handler);
}

void RpcConnection::AsyncRpc_locked(
    const std::string &method_name, const ::google::protobuf::MessageLite *req,
    std::shared_ptr<::google::protobuf::MessageLite> resp,
    const RpcCallback &handler) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  auto wrapped_handler =
      [resp, handler](pbio::CodedInputStream *is, const Status &status) {
        if (status.ok()) {
          if (is) {  // Connect messages will not have an is
            ReadDelimitedPBMessage(is, resp.get());
          }
        }
        handler(status);
      };

  int call_id = (method_name != SASL_METHOD_NAME ? engine_->NextCallId() : RpcEngine::kCallIdSasl);
  auto r = std::make_shared<Request>(engine_, method_name, call_id, req,
                                     std::move(wrapped_handler));
  auto r_vector = std::vector<std::shared_ptr<Request> > (1, r);
  SendRpcRequests(r_vector);
}

void RpcConnection::AsyncRpc(const std::vector<std::shared_ptr<Request> > & requests) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  SendRpcRequests(requests);
}

void RpcConnection::SendRpcRequests(const std::vector<std::shared_ptr<Request> > & requests) {
  LOG_TRACE(kRPC, << "RpcConnection::SendRpcRequests[] called; connected=" << ToString(connected_));
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling

  if (connected_ == kDisconnected) {
    // Oops.  The connection failed _just_ before the engine got a chance
    //    to send it.  Register it as a failure
    Status status = Status::ResourceUnavailable("RpcConnection closed before send.");
    engine_->AsyncRpcCommsError(status, shared_from_this(), requests);
  } else {
    for (auto r: requests) {
      if (r->method_name() != SASL_METHOD_NAME)
        pending_requests_.push_back(r);
      else
        auth_requests_.push_back(r);
    }
    if (connected_ == kConnected || connected_ == kHandshaking || connected_ == kAuthenticating) { // Dont flush if we're waiting or handshaking
      FlushPendingRequests();
    }
  }
}


void RpcConnection::PreEnqueueRequests(
    std::vector<std::shared_ptr<Request>> requests) {
  // Public method - acquire lock
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);

  LOG_DEBUG(kRPC, << "RpcConnection::PreEnqueueRequests called");

  assert(connected_ == kNotYetConnected);

  pending_requests_.insert(pending_requests_.end(), requests.begin(),
                           requests.end());
  // Don't start sending yet; will flush when connected
}

// Only call when already holding conn state lock
void RpcConnection::PrependRequests_locked( std::vector<std::shared_ptr<Request>> requests) {
  LOG_DEBUG(kRPC, << "RpcConnection::PrependRequests called");

  pending_requests_.insert(pending_requests_.begin(), requests.begin(),
                           requests.end());
  // Don't start sending yet; will flush when connected
}

void RpcConnection::SetEventHandlers(std::shared_ptr<LibhdfsEvents> event_handlers) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  event_handlers_ = event_handlers;
  if (sasl_protocol_) {
    sasl_protocol_->SetEventHandlers(event_handlers);
  }
}

void RpcConnection::SetClusterName(std::string cluster_name) {
  std::lock_guard<std::mutex> state_lock(connection_state_lock_);
  cluster_name_ = cluster_name;
}

void RpcConnection::CommsError(const Status &status) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling
  LOG_DEBUG(kRPC, << "RpcConnection::CommsError called");

  Disconnect();

  // Anything that has been queued to the connection (on the fly or pending)
  //    will get dinged for a retry
  std::vector<std::shared_ptr<Request>> requestsToReturn;
  std::transform(requests_on_fly_.begin(), requests_on_fly_.end(),
                 std::back_inserter(requestsToReturn),
                 std::bind(&RequestOnFlyMap::value_type::second, _1));
  requests_on_fly_.clear();

  requestsToReturn.insert(requestsToReturn.end(),
                         std::make_move_iterator(pending_requests_.begin()),
                         std::make_move_iterator(pending_requests_.end()));
  pending_requests_.clear();

  engine_->AsyncRpcCommsError(status, shared_from_this(), requestsToReturn);
}

void RpcConnection::ClearAndDisconnect(const ::asio::error_code &ec) {
  Disconnect();
  std::vector<std::shared_ptr<Request>> requests;
  std::transform(requests_on_fly_.begin(), requests_on_fly_.end(),
                 std::back_inserter(requests),
                 std::bind(&RequestOnFlyMap::value_type::second, _1));
  requests_on_fly_.clear();
  requests.insert(requests.end(),
                  std::make_move_iterator(pending_requests_.begin()),
                  std::make_move_iterator(pending_requests_.end()));
  pending_requests_.clear();
  for (const auto &req : requests) {
    req->OnResponseArrived(nullptr, ToStatus(ec));
  }
}

std::shared_ptr<Request> RpcConnection::RemoveFromRunningQueue(int call_id) {
  assert(lock_held(connection_state_lock_));  // Must be holding lock before calling
  auto it = requests_on_fly_.find(call_id);
  if (it == requests_on_fly_.end()) {
    return std::shared_ptr<Request>();
  }

  auto req = it->second;
  requests_on_fly_.erase(it);
  return req;
}

std::string RpcConnection::ToString(ConnectedState connected) {
  switch(connected) {
    case kNotYetConnected: return "NotYetConnected";
    case kConnecting:      return "Connecting";
    case kHandshaking:     return "Handshaking";
    case kAuthenticating:  return "Authenticating";
    case kConnected:       return "Connected";
    case kDisconnected:    return "Disconnected";
    default:               return "Invalid ConnectedState";
  }
}

}// end namespace hdfs
