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
#include "rpc_connection.h"
#include "common/logging.h"
#include "common/optional_wrapper.h"

#include "sasl_engine.h"
#include "sasl_protocol.h"

#if defined USE_SASL
  #if defined USE_CYRUS_SASL
    #include "cyrus_sasl_engine.h"  // CySaslEngine()
  #elif defined USE_GSASL
    #include      "gsasl_engine.h"  //  GSaslEngine()
  #else
    #error USE_SASL defined but no engine (USE_GSASL) defined
  #endif
#endif

namespace hdfs {

using namespace hadoop::common;
using namespace google::protobuf;

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
  assert(state_ != kNegotiate);
}

void SaslProtocol::SetEventHandlers(std::shared_ptr<LibhdfsEvents> event_handlers) {
  std::lock_guard<std::mutex> state_lock(sasl_state_lock_);
  event_handlers_ = event_handlers;
} // SetEventHandlers() method

void SaslProtocol::Authenticate(std::function<void(const Status & status, const AuthInfo new_auth_info)> callback)
{
  std::lock_guard<std::mutex> state_lock(sasl_state_lock_);

  callback_ = callback;
  state_ = kNegotiate;
  event_handlers_->call("SASL Start", cluster_name_.c_str(), 0);

  std::shared_ptr<RpcSaslProto> req_msg = std::make_shared<RpcSaslProto>();
  req_msg->set_state(RpcSaslProto_SaslState_NEGOTIATE);

  // We cheat here since this is always called while holding the RpcConnection's lock
  std::shared_ptr<RpcConnection> connection = connection_.lock();
  if (!connection) {
    AuthComplete(Status::AuthenticationFailed("Lost RPC Connection"), AuthInfo());
    return;
  }

  std::shared_ptr<RpcSaslProto> resp_msg = std::make_shared<RpcSaslProto>();
  auto self(shared_from_this());
  connection->AsyncRpc_locked(SASL_METHOD_NAME, req_msg.get(), resp_msg,
                       [self, req_msg, resp_msg] (const Status & status) {
            self->OnServerResponse(status, resp_msg.get()); } );
} // authenticate() method

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
} // ParseMethod()

// build_init_msg():
// Helper function for Start(), to keep
// these ProtoBuf-RPC calls out of the sasl_engine code.

std::pair<Status, RpcSaslProto>
SaslProtocol::BuildInitMessage(std::string & token, const hadoop::common::RpcSaslProto * negotiate_msg)
{
  // The init message needs one of the RpcSaslProto_SaslAuth structures that
  //    was sent in the negotiate message as our chosen mechanism
  // Map the chosen_mech name back to the RpcSaslProto_SaslAuth from the negotiate
  //    message that it corresponds to
  SaslMethod & chosenMech = sasl_engine_->chosen_mech_;
  auto auths = negotiate_msg->auths();
  auto pb_auth_it = std::find_if(auths.begin(), auths.end(),
                                 [chosenMech](const RpcSaslProto_SaslAuth & data)
                                 {
                                   return data.mechanism() == chosenMech.mechanism;
                                 });
  if (pb_auth_it == auths.end())
    return std::make_pair(Status::Error("Couldn't find mechanism in negotiate msg"), RpcSaslProto());
  auto & pb_auth = *pb_auth_it;

  // Prepare INITIATE message
  RpcSaslProto initiate = RpcSaslProto();

  initiate.set_state(RpcSaslProto_SaslState_INITIATE);
  // initiate message will contain:
  //   token_ (binary data), and
  //   auths_[ ], an array of objects just like pb_auth.
  // In our case, we want the auths array
  // to hold just the single element from pb_auth:

  RpcSaslProto_SaslAuth * respAuth = initiate.add_auths();
  respAuth->CopyFrom(pb_auth);

  // Mostly, an INITIATE message contains a "Token".
  // For GSSAPI, the token is a Kerberos AP_REQ, aka
  // "Authenticated application request," comprising
  // the client's application ticket & and an encrypted
  // message that Kerberos calls an "authenticator".

  if (token.empty()) {
    const char * errmsg = "SaslProtocol::build_init_msg():  No token available.";
    LOG_ERROR(kRPC, << errmsg);
    return std::make_pair(Status::Error(errmsg), RpcSaslProto());
  }

  // add challenge token to the INITIATE message:
  initiate.set_token(token);

  // the initiate message is ready to send:
  return std::make_pair(Status::OK(), initiate);
} // build_init_msg()

// Converts the RpcSaslProto.auths ararray from RpcSaslProto_SaslAuth PB
//    structures to SaslMethod structures
static bool
extract_auths(std::vector<SaslMethod>   & resp_auths,
     const hadoop::common::RpcSaslProto * response) {

  bool simple_avail = false;
  auto pb_auths = response->auths();

  // For our GSSAPI case, an element of pb_auths contains:
  //    method_      = "KERBEROS"
  //    mechanism_   = "GSSAPI"
  //    protocol_    = "nn"      /* "name node", AKA "hdfs"
  //    serverid_    = "foobar1.acmecorp.com"
  //    challenge_   = ""
  //   _cached_size_ = 0
  //   _has_bits_    = 15

  for (int i = 0; i < pb_auths.size(); ++i) {
      auto  pb_auth = pb_auths.Get(i);
      AuthInfo::AuthMethod method = ParseMethod(pb_auth.method());

      switch(method) {
      case AuthInfo::kToken:
      case AuthInfo::kKerberos: {
          SaslMethod new_method;
          new_method.mechanism = pb_auth.mechanism();
          new_method.protocol  = pb_auth.protocol();
          new_method.serverid  = pb_auth.serverid();
          new_method.challenge = pb_auth.has_challenge() ?
                                 pb_auth.challenge()     : "";
          resp_auths.push_back(new_method);
        }
        break;
      case AuthInfo::kSimple:
        simple_avail = true;
        break;
      case AuthInfo::kUnknownAuth:
        LOG_WARN(kRPC, << "Unknown auth method " << pb_auth.method() << "; ignoring");
        break;
      default:
        LOG_WARN(kRPC, << "Invalid auth type:  " << method << "; ignoring");
        break;
      }
  } // for
  return simple_avail;
} // extract_auths()

void SaslProtocol::ResetEngine() {
#if defined USE_SASL
  #if defined USE_CYRUS_SASL
    sasl_engine_.reset(new CySaslEngine());
  #elif defined USE_GSASL
    sasl_engine_.reset(new GSaslEngine());
  #else
    #error USE_SASL defined but no engine (USE_GSASL) defined
  #endif
#endif
    return;
} // Reset_Engine() method

void SaslProtocol::Negotiate(const hadoop::common::RpcSaslProto * response)
{
  this->ResetEngine(); // get a new SaslEngine

  if (auth_info_.getToken()) {
    sasl_engine_->SetPasswordInfo(auth_info_.getToken().value().identifier,
                                  auth_info_.getToken().value().password);
  }
  sasl_engine_->SetKerberosInfo(auth_info_.getUser()); // TODO: map to principal?

  // Copy the response's auths list to an array of SaslMethod objects.
  // SaslEngine shouldn't need to know about the protobuf classes.
  std::vector<SaslMethod> resp_auths;
  bool simple_available = extract_auths(resp_auths,   response);
  bool mech_chosen = sasl_engine_->ChooseMech(resp_auths);

  if (mech_chosen) {

    // Prepare an INITIATE message,
    // later on we'll send it to the hdfs server:
    auto     start_result  = sasl_engine_->Start();
    Status   status        = start_result.first;
    if (! status.ok()) {
      // start() failed, simple isn't avail,
      // so give up & stop authentication:
      AuthComplete(status, auth_info_);
      return;
    }
    // token.second is a binary buffer, containing
    // client credentials that will prove the
    // client's identity to the application server.
    // Put the token into an INITIATE message:
    auto init = BuildInitMessage(start_result.second, response);

    // If all is OK, send the INITIATE msg to the hdfs server;
    // Otherwise, if possible, fail over to simple authentication:
    status = init.first;
    if (status.ok()) {
      SendSaslMessage(init.second);
      return;
    }
    if (!simple_available) {
      // build_init_msg() failed, simple isn't avail,
      // so give up & stop authentication:
      AuthComplete(status, auth_info_);
      return;
    }
    // If simple IS available, fall through to below,
    // but without build_init_msg()'s failure-status.
  }

  // There were no resp_auths, or the SaslEngine couldn't make one work
  if (simple_available) {
    // Simple was the only one we could use.  That's OK.
    AuthComplete(Status::OK(), auth_info_);
    return;
  } else {
    // We didn't understand any of the resp_auths;
    // Give back some information
    std::stringstream ss;
    ss << "Client cannot authenticate via: ";

    auto pb_auths = response->auths();
    for (int i = 0; i < pb_auths.size(); ++i) {
      const RpcSaslProto_SaslAuth & pb_auth = pb_auths.Get(i);
      ss << pb_auth.mechanism() << ", ";
    }

    AuthComplete(Status::Error(ss.str().c_str()), auth_info_);
    return;
  }
} // Negotiate() method

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
  auto sasl_response = sasl_engine_->Step(challenge_token);

  if (sasl_response.first.ok()) {
    response.set_token(sasl_response.second);

    std::shared_ptr<RpcSaslProto> return_msg = std::make_shared<RpcSaslProto>();
    SendSaslMessage(response);
  } else {
    AuthComplete(sasl_response.first, auth_info_);
    return;
  }
} // Challenge() method

bool SaslProtocol::SendSaslMessage(RpcSaslProto & message)
{
  assert(lock_held(sasl_state_lock_));  // Must be holding lock before calling

  // RpcConnection might have been freed when we weren't looking.  Lock it
  //   to make sure it's there long enough for us
  std::shared_ptr<RpcConnection> connection = connection_.lock();
  if (!connection) {
    LOG_DEBUG(kRPC, << "Tried sending a SASL Message but the RPC connection was gone");
    AuthComplete(Status::AuthenticationFailed("Lost RPC Connection"), AuthInfo());
    return false;
  }

  std::shared_ptr<RpcSaslProto> resp_msg = std::make_shared<RpcSaslProto>();
  auto self(shared_from_this());
  connection->AsyncRpc(SASL_METHOD_NAME, &message, resp_msg,
                       [self, resp_msg] (const Status & status) {
                         self->OnServerResponse(status, resp_msg.get());
                       } );

  return true;
} // SendSaslMessage() method

// AuthComplete():  stop the auth effort, successful ot not:
bool SaslProtocol::AuthComplete(const Status & status, const AuthInfo & auth_info)
{
  assert(lock_held(sasl_state_lock_));  // Must be holding lock before calling
  state_ = kComplete;
  event_handlers_->call("SASL End", cluster_name_.c_str(), 0);

  // RpcConnection might have been freed when we weren't looking.  Lock it
  //   to make sure it's there long enough for us
  std::shared_ptr<RpcConnection> connection = connection_.lock();
  if (!connection) {
    LOG_DEBUG(kRPC, << "Tried sending an AuthComplete but the RPC connection was gone: " << status.ToString());
    return false;
  }

  LOG_TRACE(kRPC, << "Received SASL response" << status.ToString());
  connection->AuthComplete(status, auth_info);

  return true;
} // AuthComplete() method

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
        sasl_engine_->Finish();
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
} // OnServerResponse() method

} // namespace hdfs
