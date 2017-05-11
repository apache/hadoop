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

#ifndef LIB_RPC_SASLPROTOCOL_H
#define LIB_RPC_SASLPROTOCOL_H

#include <memory>
#include <mutex>
#include <functional>

#include <RpcHeader.pb.h>

#include "hdfspp/status.h"
#include "common/auth_info.h"
#include "common/libhdfs_events_impl.h"

namespace hdfs {

static constexpr const char * SASL_METHOD_NAME = "sasl message";

class RpcConnection;
class SaslEngine;
class SaslMethod;

class SaslProtocol : public std::enable_shared_from_this<SaslProtocol>
{
public:
  SaslProtocol(const std::string &cluster_name,
               const AuthInfo & auth_info,
               std::shared_ptr<RpcConnection> connection);
  virtual ~SaslProtocol();

  void SetEventHandlers(std::shared_ptr<LibhdfsEvents> event_handlers);

  // Start the async authentication process.  Must be called while holding the
  //   connection lock, but all callbacks will occur outside of the connection lock
  void Authenticate(std::function<void(const Status & status, const AuthInfo new_auth_info)> callback);
  void OnServerResponse(const Status & status, const hadoop::common::RpcSaslProto * response);
  std::pair<Status, hadoop::common::RpcSaslProto> BuildInitMessage( std::string & token, const hadoop::common::RpcSaslProto * negotiate_msg);
private:
  enum State {
    kUnstarted,
    kNegotiate,
    kComplete
  };

  // Lock for access to members of the class
  std::mutex sasl_state_lock_;

  State state_;
  const std::string cluster_name_;
  AuthInfo auth_info_;
  std::weak_ptr<RpcConnection> connection_;
  std::function<void(const Status & status, const AuthInfo new_auth_info)> callback_;
  std::unique_ptr<SaslEngine> sasl_engine_;
  std::shared_ptr<LibhdfsEvents> event_handlers_;

  bool SendSaslMessage(hadoop::common::RpcSaslProto & message);
  bool AuthComplete(const Status & status, const AuthInfo & auth_info);

  void ResetEngine();
  void Negotiate(const hadoop::common::RpcSaslProto * response);
  void Challenge(const hadoop::common::RpcSaslProto * response);

}; // class SaslProtocol

} // namespace hdfs

#endif /* LIB_RPC_SASLPROTOCOL_H */
