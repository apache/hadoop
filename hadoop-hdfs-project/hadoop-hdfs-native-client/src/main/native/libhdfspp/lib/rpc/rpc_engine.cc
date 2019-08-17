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
#include "common/util.h"
#include "common/logging.h"
#include "common/namenode_info.h"
#include "common/optional_wrapper.h"

#include <algorithm>

namespace hdfs {

template <class T>
using optional = std::experimental::optional<T>;


RpcEngine::RpcEngine(std::shared_ptr<IoService> io_service, const Options &options,
                     const std::string &client_name, const std::string &user_name,
                     const char *protocol_name, int protocol_version)
    : io_service_(io_service),
      options_(options),
      client_name_(client_name),
      client_id_(getRandomClientId()),
      protocol_name_(protocol_name),
      protocol_version_(protocol_version),
      call_id_(0),
      retry_timer(io_service->GetRaw()),
      event_handlers_(std::make_shared<LibhdfsEvents>()),
      connect_canceled_(false)
{
  LOG_DEBUG(kRPC, << "RpcEngine::RpcEngine called");

  auth_info_.setUser(user_name);
  if (options.authentication == Options::kKerberos) {
    auth_info_.setMethod(AuthInfo::kKerberos);
  }
}

void RpcEngine::Connect(const std::string &cluster_name,
                        const std::vector<ResolvedNamenodeInfo> servers,
                        RpcCallback &handler) {
  std::lock_guard<std::mutex> state_lock(engine_state_lock_);
  LOG_DEBUG(kRPC, << "RpcEngine::Connect called");

  last_endpoints_ = servers[0].endpoints;
  cluster_name_ = cluster_name;
  LOG_TRACE(kRPC, << "Got cluster name \"" << cluster_name << "\" in RpcEngine::Connect")

  ha_persisted_info_.reset(new HANamenodeTracker(servers, io_service_, event_handlers_));
  if(!ha_persisted_info_->is_enabled()) {
    ha_persisted_info_.reset();
  }

  // Construct retry policy after we determine if config is HA
  retry_policy_ = MakeRetryPolicy(options_);

  conn_ = InitializeConnection();
  conn_->Connect(last_endpoints_, auth_info_, handler);
}

bool RpcEngine::CancelPendingConnect() {
  if(connect_canceled_) {
    LOG_DEBUG(kRPC, << "RpcEngine@" << this << "::CancelPendingConnect called more than once");
    return false;
  }

  connect_canceled_ = true;
  return true;
}

void RpcEngine::Shutdown() {
  LOG_DEBUG(kRPC, << "RpcEngine::Shutdown called");
  io_service_->PostLambda([this]() {
    std::lock_guard<std::mutex> state_lock(engine_state_lock_);
    conn_.reset();
  });
}

std::unique_ptr<const RetryPolicy> RpcEngine::MakeRetryPolicy(const Options &options) {
  LOG_DEBUG(kRPC, << "RpcEngine::MakeRetryPolicy called");

  if(ha_persisted_info_) {
    LOG_INFO(kRPC, << "Cluster is HA configued so policy will default to HA until a knob is implemented");
    return std::unique_ptr<RetryPolicy>(new FixedDelayWithFailover(options.rpc_retry_delay_ms,
                                                                   options.max_rpc_retries,
                                                                   options.failover_max_retries,
                                                                   options.failover_connection_max_retries));
  } else if (options.max_rpc_retries > 0) {
    return std::unique_ptr<RetryPolicy>(new FixedDelayRetryPolicy(options.rpc_retry_delay_ms,
                                                                  options.max_rpc_retries));
  } else {
    return nullptr;
  }
}

std::string RpcEngine::getRandomClientId()
{
  /**
   *  The server is requesting a 16-byte UUID:
   *  https://github.com/c9n/hadoop/blob/master/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/ClientId.java
   *
   *  This function generates a 16-byte UUID (version 4):
   *  https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29
   **/
  std::vector<unsigned char>buf(16);
  RAND_pseudo_bytes(&buf[0], buf.size());

  //clear the first four bits of byte 6 then set the second bit
  buf[6] = (buf[6] & 0x0f) | 0x40;

  //clear the second bit of byte 8 and set the first bit
  buf[8] = (buf[8] & 0xbf) | 0x80;
  return std::string(reinterpret_cast<const char*>(&buf[0]), buf.size());
}



void RpcEngine::TEST_SetRpcConnection(std::shared_ptr<RpcConnection> conn) {
  conn_ = conn;
  retry_policy_ = MakeRetryPolicy(options_);
}

void RpcEngine::TEST_SetRetryPolicy(std::unique_ptr<const RetryPolicy> policy) {
  retry_policy_ = std::move(policy);
}

std::unique_ptr<const RetryPolicy> RpcEngine::TEST_GenerateRetryPolicyUsingOptions() {
  return MakeRetryPolicy(options_);
}

void RpcEngine::AsyncRpc(
    const std::string &method_name, const ::google::protobuf::MessageLite *req,
    const std::shared_ptr<::google::protobuf::MessageLite> &resp,
    const std::function<void(const Status &)> &handler) {
  std::lock_guard<std::mutex> state_lock(engine_state_lock_);

  LOG_TRACE(kRPC, << "RpcEngine::AsyncRpc called");

  // In case user-side code isn't checking the status of Connect before doing RPC
  if(connect_canceled_) {
    io_service_->PostLambda(
        [handler](){ handler(Status::Canceled()); }
    );
    return;
  }

  if (!conn_) {
    conn_ = InitializeConnection();
    conn_->ConnectAndFlush(last_endpoints_);
  }
  conn_->AsyncRpc(method_name, req, resp, handler);
}

std::shared_ptr<RpcConnection> RpcEngine::NewConnection()
{
  LOG_DEBUG(kRPC, << "RpcEngine::NewConnection called");

  return std::make_shared<RpcConnectionImpl<::asio::ip::tcp::socket>>(shared_from_this());
}

std::shared_ptr<RpcConnection> RpcEngine::InitializeConnection()
{
  std::shared_ptr<RpcConnection> newConn = NewConnection();
  newConn->SetEventHandlers(event_handlers_);
  newConn->SetClusterName(cluster_name_);
  newConn->SetAuthInfo(auth_info_);

  return newConn;
}

void RpcEngine::AsyncRpcCommsError(
    const Status &status,
    std::shared_ptr<RpcConnection> failedConnection,
    std::vector<std::shared_ptr<Request>> pendingRequests) {
  LOG_ERROR(kRPC, << "RpcEngine::AsyncRpcCommsError called; status=\"" << status.ToString() << "\" conn=" << failedConnection.get() << " reqs=" << std::to_string(pendingRequests.size()));

  io_service_->PostLambda([this, status, failedConnection, pendingRequests]() {
    RpcCommsError(status, failedConnection, pendingRequests);
  });
}

void RpcEngine::RpcCommsError(
    const Status &status,
    std::shared_ptr<RpcConnection> failedConnection,
    std::vector<std::shared_ptr<Request>> pendingRequests) {
  LOG_WARN(kRPC, << "RpcEngine::RpcCommsError called; status=\"" << status.ToString() << "\" conn=" << failedConnection.get() << " reqs=" << std::to_string(pendingRequests.size()));

  std::lock_guard<std::mutex> state_lock(engine_state_lock_);

  // If the failed connection is the current one, shut it down
  //    It will be reconnected when there is work to do
  if (failedConnection == conn_) {
    LOG_INFO(kRPC, << "Disconnecting from failed RpcConnection");
    conn_.reset();
  }

  optional<RetryAction> head_action = optional<RetryAction>();

  // Filter out anything with too many retries already
  if(event_handlers_) {
    event_handlers_->call(FS_NN_PRE_RPC_RETRY_EVENT, "RpcCommsError",
                          reinterpret_cast<int64_t>(this));
  }

  for (auto it = pendingRequests.begin(); it < pendingRequests.end();) {
    auto req = *it;

    LOG_DEBUG(kRPC, << req->GetDebugString());

    RetryAction retry = RetryAction::fail(""); // Default to fail

    if(connect_canceled_) {
      retry = RetryAction::fail("Operation canceled");
    } else if (status.notWorthRetry()) {
      retry = RetryAction::fail(status.ToString().c_str());
    } else if (retry_policy()) {
      retry = retry_policy()->ShouldRetry(status, req->IncrementRetryCount(), req->get_failover_count(), true);
    }

    if (retry.action == RetryAction::FAIL) {
      // If we've exceeded the maximum retry, take the latest error and pass it
      //    on.  There might be a good argument for caching the first error
      //    rather than the last one, that gets messy

      io_service()->PostLambda([req, status]() {
        req->OnResponseArrived(nullptr, status);  // Never call back while holding a lock
      });
      it = pendingRequests.erase(it);
    } else {
      if (!head_action) {
        head_action = retry;
      }

      ++it;
    }
  }

  // If we have reqests that need to be re-sent, ensure that we have a connection
  //   and send the requests to it
  bool haveRequests = !pendingRequests.empty() &&
          head_action && head_action->action != RetryAction::FAIL;

  if (haveRequests) {
    LOG_TRACE(kRPC, << "Have " << std::to_string(pendingRequests.size()) << " requests to resend");
    bool needNewConnection = !conn_;
    if (needNewConnection) {
      LOG_DEBUG(kRPC, << "Creating a new NN conection");


      // If HA is enabled and we have valid HA info then fail over to the standby (hopefully now active)
      if(head_action->action == RetryAction::FAILOVER_AND_RETRY && ha_persisted_info_) {

        for(unsigned int i=0; i<pendingRequests.size();i++) {
          pendingRequests[i]->IncrementFailoverCount();
        }

        ResolvedNamenodeInfo new_active_nn_info;
        bool failoverInfoFound = ha_persisted_info_->GetFailoverAndUpdate(last_endpoints_, new_active_nn_info);
        if(!failoverInfoFound) {
          // This shouldn't be a common case, the set of endpoints was empty, likely due to DNS issues.
          // Another possibility is a network device has been added or removed due to a VM starting or stopping.

          LOG_ERROR(kRPC, << "Failed to find endpoints for the alternate namenode."
                          << "Make sure Namenode hostnames can be found with a DNS lookup.");
          // Kill all pending RPC requests since there's nowhere for this to go
          Status badEndpointStatus = Status::Error("No endpoints found for namenode");

          for(unsigned int i=0; i<pendingRequests.size(); i++) {
            std::shared_ptr<Request> sharedCurrentRequest = pendingRequests[i];
            io_service()->PostLambda([sharedCurrentRequest, badEndpointStatus]() {
              sharedCurrentRequest->OnResponseArrived(nullptr, badEndpointStatus);  // Never call back while holding a lock
            });
          }

          // Clear request vector. This isn't a recoverable error.
          pendingRequests.clear();
        }

        if(ha_persisted_info_->is_resolved()) {
          LOG_INFO(kRPC, << "Going to try connecting to alternate Namenode: " << new_active_nn_info.uri.str());
          last_endpoints_ = new_active_nn_info.endpoints;
        } else {
          LOG_WARN(kRPC, << "It looks HA is turned on, but unable to fail over. has info="
                         << ha_persisted_info_->is_enabled() << " resolved=" << ha_persisted_info_->is_resolved());
        }
      }

      conn_ = InitializeConnection();
      conn_->PreEnqueueRequests(pendingRequests);

      if (head_action->delayMillis > 0) {
        auto weak_conn = std::weak_ptr<RpcConnection>(conn_);
        retry_timer.expires_from_now(
            std::chrono::milliseconds(head_action->delayMillis));
        retry_timer.async_wait([this, weak_conn](asio::error_code ec) {
          auto strong_conn = weak_conn.lock();
          if ( (!ec) && (strong_conn) ) {
            strong_conn->ConnectAndFlush(last_endpoints_);
          }
        });
      } else {
        conn_->ConnectAndFlush(last_endpoints_);
      }
    } else {
      // We have an existing connection (which might be closed; we don't know
      //    until we hold the connection local) and should just add the new requests
      conn_->AsyncRpc(pendingRequests);
    }
  }
}


void RpcEngine::SetFsEventCallback(fs_event_callback callback) {
  event_handlers_->set_fs_callback(callback);
}


}
