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
#ifndef LIB_RPC_RPC_ENGINE_H_
#define LIB_RPC_RPC_ENGINE_H_

#include "hdfspp/options.h"
#include "hdfspp/status.h"

#include "common/auth_info.h"
#include "common/retry_policy.h"
#include "common/libhdfs_events_impl.h"
#include "common/util.h"
#include "common/new_delete.h"
#include "common/namenode_info.h"
#include "namenode_tracker.h"

#include <google/protobuf/message_lite.h>

#include <asio/ip/tcp.hpp>
#include <asio/deadline_timer.hpp>

#include <atomic>
#include <memory>
#include <vector>
#include <mutex>

namespace hdfs {

  /*
   *        NOTE ABOUT LOCKING MODELS
   *
   * To prevent deadlocks, anything that might acquire multiple locks must
   * acquire the lock on the RpcEngine first, then the RpcConnection.  Callbacks
   * will never be called while holding any locks, so the components are free
   * to take locks when servicing a callback.
   *
   * An RpcRequest or RpcConnection should never call any methods on the RpcEngine
   * except for those that are exposed through the LockFreeRpcEngine interface.
   */

typedef const std::function<void(const Status &)> RpcCallback;

class LockFreeRpcEngine;
class RpcConnection;
class SaslProtocol;
class RpcConnection;
class Request;
class IoService;

/*
 * These methods of the RpcEngine will never acquire locks, and are safe for
 * RpcConnections to call while holding a ConnectionLock.
 */
class LockFreeRpcEngine {
public:
  MEMCHECKED_CLASS(LockFreeRpcEngine)

  /* Enqueues a CommsError without acquiring a lock*/
  virtual void AsyncRpcCommsError(const Status &status,
                      std::shared_ptr<RpcConnection> failedConnection,
                      std::vector<std::shared_ptr<Request>> pendingRequests) = 0;


  virtual const RetryPolicy *retry_policy() = 0;
  virtual int NextCallId() = 0;

  virtual const std::string &client_name() = 0;
  virtual const std::string &client_id() = 0;
  virtual const std::string &user_name() = 0;
  virtual const std::string &protocol_name() = 0;
  virtual int protocol_version() = 0;
  virtual std::shared_ptr<IoService> io_service() const = 0;
  virtual const Options &options() = 0;
};


/*
 * An engine for reliable communication with a NameNode.  Handles connection,
 * retry, and (someday) failover of the requested messages.
 *
 * Threading model: thread-safe.  All callbacks will be called back from
 *   an asio pool and will not hold any internal locks
 */
class RpcEngine : public LockFreeRpcEngine, public std::enable_shared_from_this<RpcEngine> {
 public:
  MEMCHECKED_CLASS(RpcEngine)
  enum { kRpcVersion = 9 };
  enum {
    kCallIdAuthorizationFailed = -1,
    kCallIdInvalid = -2,
    kCallIdConnectionContext = -3,
    kCallIdPing = -4,
    kCallIdSasl = -33
  };

  RpcEngine(std::shared_ptr<IoService> service, const Options &options,
            const std::string &client_name, const std::string &user_name,
            const char *protocol_name, int protocol_version);

  void Connect(const std::string & cluster_name,
               const std::vector<ResolvedNamenodeInfo> servers,
               RpcCallback &handler);

  bool CancelPendingConnect();

  void AsyncRpc(const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                const std::shared_ptr<::google::protobuf::MessageLite> &resp,
                const std::function<void(const Status &)> &handler);

  void Shutdown();

  /* Enqueues a CommsError without acquiring a lock*/
  void AsyncRpcCommsError(const Status &status,
                     std::shared_ptr<RpcConnection> failedConnection,
                     std::vector<std::shared_ptr<Request>> pendingRequests) override;
  void RpcCommsError(const Status &status,
                     std::shared_ptr<RpcConnection> failedConnection,
                     std::vector<std::shared_ptr<Request>> pendingRequests);


  const RetryPolicy * retry_policy() override { return retry_policy_.get(); }
  int NextCallId() override { return ++call_id_; }

  void TEST_SetRpcConnection(std::shared_ptr<RpcConnection> conn);
  void TEST_SetRetryPolicy(std::unique_ptr<const RetryPolicy> policy);
  std::unique_ptr<const RetryPolicy> TEST_GenerateRetryPolicyUsingOptions();

  const std::string &client_name() override { return client_name_; }
  const std::string &client_id() override { return client_id_; }
  const std::string &user_name() override { return auth_info_.getUser(); }
  const std::string &protocol_name() override { return protocol_name_; }
  int protocol_version() override { return protocol_version_; }
  std::shared_ptr<IoService> io_service() const override { return io_service_; }
  const Options &options() override { return options_; }
  static std::string GetRandomClientName();

  void SetFsEventCallback(fs_event_callback callback);
protected:
  std::shared_ptr<RpcConnection> conn_;
  std::shared_ptr<RpcConnection> InitializeConnection();
  virtual std::shared_ptr<RpcConnection> NewConnection();
  virtual std::unique_ptr<const RetryPolicy> MakeRetryPolicy(const Options &options);

  static std::string getRandomClientId();

  // Remember all of the last endpoints in case we need to reconnect and retry
  std::vector<::asio::ip::tcp::endpoint> last_endpoints_;

private:
  mutable std::shared_ptr<IoService> io_service_;
  const Options options_;
  const std::string client_name_;
  const std::string client_id_;
  const std::string protocol_name_;
  const int protocol_version_;
  std::unique_ptr<const RetryPolicy> retry_policy_; //null --> no retry
  AuthInfo auth_info_;
  std::string cluster_name_;
  std::atomic_int call_id_;
  ::asio::deadline_timer retry_timer;

  std::shared_ptr<LibhdfsEvents> event_handlers_;

  std::mutex engine_state_lock_;

  // Once Connect has been canceled there is no going back
  bool connect_canceled_;

  // Keep endpoint info for all HA connections, a non-null ptr indicates
  // that HA info was found in the configuation.
  std::unique_ptr<HANamenodeTracker> ha_persisted_info_;
};
}

#endif
