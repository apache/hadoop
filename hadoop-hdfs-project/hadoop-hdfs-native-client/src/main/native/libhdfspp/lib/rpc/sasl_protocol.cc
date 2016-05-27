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

#include "sasl_protocol.h"
#include "sasl_engine.h"
#include "rpc_engine.h"
#include "common/logging.h"

#include <optional.hpp>

namespace hdfs {

using namespace hadoop::common;
using namespace google::protobuf;
template <class T>
using optional = std::experimental::optional<T>;

/*****
 * Threading model: all entry points need to acquire the sasl_lock before accessing
 * members of the class
 *
 * Lifecycle model: asio may have outstanding callbacks into this class for arbitrary
 * amounts of time, so any references to the class must be shared_ptr's.  The
 * SASLProtocol keeps a weak_ptr to the owning RpcConnection, which might go away,
 * so the weak_ptr should be locked only long enough to make callbacks into the
 * RpcConnection.
 */

SaslProtocol::SaslProtocol(const std::string & cluster_name,
                           const AuthInfo & auth_info,
                           std::shared_ptr<RpcConnection> connection) :
        state_(kUnstarted),
        cluster_name_(cluster_name),
        auth_info_(auth_info),
        connection_(connection)
{
}

SaslProtocol::~SaslProtocol()
{
  std::lock_guard<std::mutex> state_lock(sasl_state_lock_);
  event_handlers_->call("SASL End", cluster_name_.c_str(), 0);
}

void SaslProtocol::SetEventHandlers(std::shared_ptr<LibhdfsEvents> event_handlers) {
  std::lock_guard<std::mutex> state_lock(sasl_state_lock_);
  event_handlers_ = event_handlers;
}

void SaslProtocol::authenticate(std::function<void(const Status & status, const AuthInfo new_auth_info)> callback)
{
  std::lock_guard<std::mutex> state_lock(sasl_state_lock_);

  LOG_TRACE(kRPC, << "Authenticating as " << auth_info_.getUser());

  assert(state_ == kUnstarted);
  event_handlers_->call("SASL Start", cluster_name_.c_str(), 0);

  callback_ = callback;
  state_ = kNegotiate;

  std::shared_ptr<RpcSaslProto> req_msg = std::make_shared<RpcSaslProto>();
  req_msg->set_state(RpcSaslProto_SaslState_NEGOTIATE);

  // We cheat here since this is always called while holding the RpcConnection's lock
  std::shared_ptr<RpcConnection> connection = connection_.lock();
  if (!connection) {
    return;
  }

  std::shared_ptr<RpcSaslProto> resp_msg = std::make_shared<RpcSaslProto>();
  auto self(shared_from_this());
  connection->AsyncRpc_locked(SASL_METHOD_NAME, req_msg.get(), resp_msg,
                       [self, req_msg, resp_msg] (const Status & status) { self->OnServerResponse(status, resp_msg.get()); } );
}

AuthInfo::AuthMethod ParseMethod(const std::string & method)
  {
    if (0 == strcasecmp(method.c_str(), "SIMPLE")) {
      return AuthInfo::kSimple;
    }
    else if (0 == strcasecmp(method.c_str(), "KERBEROS")) {
      return AuthInfo::kKerberos;
    }
    else if (0 == strcasecmp(method.c_str(), "TOKEN")) {
      return AuthInfo::kToken;
    }
    else {
      return AuthInfo::kUnknownAuth;
    }
}

void SaslProtocol::Negotiate(const hadoop::common::RpcSaslProto * response)
{
  std::vector<SaslMethod> protocols;

  bool simple_available = false;

#if defined USE_SASL
  #if defined USE_CYRUS_SASL
    sasl_engine_.reset(new CyrusSaslEngine());
  #elif defined USE_GSASL
    sasl_engine_.reset(new GSaslEngine());
  #else
    #error USE_SASL defined but no engine (USE_GSASL) defined
  #endif
#endif
  if (auth_info_.getToken()) {
    sasl_engine_->setPasswordInfo(auth_info_.getToken().value().identifier,
                                  auth_info_.getToken().value().password);
  }
  sasl_engine_->setKerberosInfo(auth_info_.getUser()); // HDFS-10451 will look up principal by username


  auto auths = response->auths();
  for (int i = 0; i < auths.size(); ++i) {
      auto auth = auths.Get(i);
      AuthInfo::AuthMethod method = ParseMethod(auth.method());

      switch(method) {
      case AuthInfo::kToken:
      case AuthInfo::kKerberos: {
          SaslMethod new_method;
          new_method.mechanism = auth.mechanism();
          new_method.protocol = auth.protocol();
          new_method.serverid = auth.serverid();
          new_method.data = const_cast<RpcSaslProto_SaslAuth *>(&response->auths().Get(i));
          protocols.push_back(new_method);
        }
        break;
      case AuthInfo::kSimple:
        simple_available = true;
        break;
      case AuthInfo::kUnknownAuth:
        LOG_WARN(kRPC, << "Unknown auth method " << auth.method() << "; ignoring");
        break;
      default:
        LOG_WARN(kRPC, << "Invalid auth type:  " << method << "; ignoring");
        break;
      }
  }

  if (!protocols.empty()) {
    auto init = sasl_engine_->start(protocols);
    if (init.first.ok()) {
      auto chosen_auth = reinterpret_cast<RpcSaslProto_SaslAuth *>(init.second.data);

      // Prepare initiate message
      RpcSaslProto initiate;
      initiate.set_state(RpcSaslProto_SaslState_INITIATE);
      RpcSaslProto_SaslAuth * respAuth = initiate.add_auths();
      respAuth->CopyFrom(*chosen_auth);

      LOG_TRACE(kRPC, << "Using auth: " << chosen_auth->protocol() << "/" <<
              chosen_auth->mechanism() << "/" << chosen_auth->serverid());

      std::string challenge = chosen_auth->has_challenge() ? chosen_auth->challenge() : "";
      auto sasl_challenge = sasl_engine_->step(challenge);

      if (sasl_challenge.first.ok()) {
        if (!sasl_challenge.second.empty()) {
          initiate.set_token(sasl_challenge.second);
        }

        std::shared_ptr<RpcSaslProto> return_msg = std::make_shared<RpcSaslProto>();
        SendSaslMessage(initiate);
        return;
      } else {
        AuthComplete(sasl_challenge.first, auth_info_);
        return;
      }
    } else if (!simple_available) {
      // If simple IS available, fall through to below
      AuthComplete(init.first, auth_info_);
      return;
    }
  }

  // There were no protocols, or the SaslEngine couldn't make one work
  if (simple_available) {
    // Simple was the only one we could use.  That's OK.
    AuthComplete(Status::OK(), auth_info_);
    return;
  } else {
    // We didn't understand any of the protocols; give back some information
    std::stringstream ss;
    ss << "Client cannot authenticate via: ";

    for (int i = 0; i < auths.size(); ++i) {
      auto auth = auths.Get(i);
      ss << auth.mechanism() << ", ";
    }

    AuthComplete(Status::Error(ss.str().c_str()), auth_info_);
    return;
  }
}

void SaslProtocol::Challenge(const hadoop::common::RpcSaslProto * challenge)
{
  if (!sasl_engine_) {
    AuthComplete(Status::Error("Received challenge before negotiate"), auth_info_);
    return;
  }

  RpcSaslProto response;
  response.CopyFrom(*challenge);
  response.set_state(RpcSaslProto_SaslState_RESPONSE);

  std::string challenge_token = challenge->has_token() ? challenge->token() : "";
  auto sasl_response = sasl_engine_->step(challenge_token);

  if (sasl_response.first.ok()) {
    response.set_token(sasl_response.second);

    std::shared_ptr<RpcSaslProto> return_msg = std::make_shared<RpcSaslProto>();
    SendSaslMessage(response);
  } else {
    AuthComplete(sasl_response.first, auth_info_);
    return;
  }
}

bool SaslProtocol::SendSaslMessage(RpcSaslProto & message)
{
  assert(lock_held(sasl_state_lock_));  // Must be holding lock before calling

  // RpcConnection might have been freed when we weren't looking.  Lock it
  //   to make sure it's there long enough for us
  std::shared_ptr<RpcConnection> connection = connection_.lock();
  if (!connection) {
    LOG_DEBUG(kRPC, << "Tried sending a SASL Message but the RPC connection was gone");
    return false;
  }

  std::shared_ptr<RpcSaslProto> resp_msg = std::make_shared<RpcSaslProto>();
  auto self(shared_from_this());
  connection->AsyncRpc(SASL_METHOD_NAME, &message, resp_msg,
                       [self, resp_msg] (const Status & status) {
                         self->OnServerResponse(status, resp_msg.get());
                       } );

  return true;
}

bool SaslProtocol::AuthComplete(const Status & status, const AuthInfo & auth_info)
{
  assert(lock_held(sasl_state_lock_));  // Must be holding lock before calling

  // RpcConnection might have been freed when we weren't looking.  Lock it
  //   to make sure it's there long enough for us
  std::shared_ptr<RpcConnection> connection = connection_.lock();
  if (!connection) {
    LOG_DEBUG(kRPC, << "Tried sending an AuthComplete but the RPC connection was gone: " << status.ToString());
    return false;
  }

  if (!status.ok()) {
    auth_info_.setMethod(AuthInfo::kAuthFailed);
  }

  LOG_TRACE(kRPC, << "AuthComplete: " << status.ToString());
  connection->AuthComplete(status, auth_info);

  return true;
}

void SaslProtocol::OnServerResponse(const Status & status, const hadoop::common::RpcSaslProto * response)
{
  std::lock_guard<std::mutex> state_lock(sasl_state_lock_);
  LOG_TRACE(kRPC, << "Received SASL response: " << status.ToString());

  if (status.ok()) {
    switch(response->state()) {
    case RpcSaslProto_SaslState_NEGOTIATE:
      Negotiate(response);
      break;
    case RpcSaslProto_SaslState_CHALLENGE:
      Challenge(response);
      break;
    case RpcSaslProto_SaslState_SUCCESS:
      if (sasl_engine_) {
        sasl_engine_->finish();
      }
      AuthComplete(Status::OK(), auth_info_);
      break;

    case RpcSaslProto_SaslState_INITIATE: // Server side only
    case RpcSaslProto_SaslState_RESPONSE: // Server side only
    case RpcSaslProto_SaslState_WRAP:
      LOG_ERROR(kRPC, << "Invalid client-side SASL state: " << response->state());
      AuthComplete(Status::Error("Invalid client-side state"), auth_info_);
      break;
    default:
      LOG_ERROR(kRPC, << "Unknown client-side SASL state: " << response->state());
      AuthComplete(Status::Error("Unknown client-side state"), auth_info_);
      break;
    }
  } else {
    AuthComplete(status, auth_info_);
  }
}


}
