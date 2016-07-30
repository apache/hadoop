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
#include "common/util.h"
#include "common/logging.h"
#include "common/namenode_info.h"
#include "optional.hpp"

#include <future>
#include <algorithm>

namespace hdfs {

template <class T>
using optional = std::experimental::optional<T>;

HANamenodeTracker::HANamenodeTracker(const std::vector<ResolvedNamenodeInfo> &servers,
                                     ::asio::io_service *ioservice,
                                     std::shared_ptr<LibhdfsEvents> event_handlers)
                  : enabled_(false), resolved_(false),
                    ioservice_(ioservice), event_handlers_(event_handlers)
{
  LOG_TRACE(kRPC, << "HANamenodeTracker got the following nodes");
  for(unsigned int i=0;i<servers.size();i++)
    LOG_TRACE(kRPC, << servers[i].str());

  if(servers.size() >= 2) {
    LOG_TRACE(kRPC, << "Creating HA namenode tracker");
    if(servers.size() > 2) {
      LOG_WARN(kRPC, << "Nameservice declares more than two nodes.  Some won't be used.");
    }

    active_info_ = servers[0];
    standby_info_ = servers[1];
    LOG_INFO(kRPC, << "Active namenode url  = " << active_info_.uri.str());
    LOG_INFO(kRPC, << "Standby namenode url = " << standby_info_.uri.str());

    enabled_ = true;
    if(!active_info_.endpoints.empty() || !standby_info_.endpoints.empty()) {
      resolved_ = true;
    }
  }
}


HANamenodeTracker::~HANamenodeTracker() { }


static std::string format_endpoints(const std::vector<::asio::ip::tcp::endpoint> &pts) {
  std::stringstream ss;
  for(unsigned int i=0; i<pts.size(); i++)
    if(i == pts.size() - 1)
      ss << pts[i];
    else
      ss << pts[i] << ", ";
  return ss.str();
}

//  Pass in endpoint from current connection, this will do a reverse lookup
//  and return the info for the standby node. It will also swap its state internally.
ResolvedNamenodeInfo HANamenodeTracker::GetFailoverAndUpdate(::asio::ip::tcp::endpoint current_endpoint) {
  LOG_TRACE(kRPC, << "Swapping from endpoint " << current_endpoint);
  mutex_guard swap_lock(swap_lock_);

  ResolvedNamenodeInfo failover_node;

  // Connected to standby, switch standby to active
  if(IsCurrentActive_locked(current_endpoint)) {
    std::swap(active_info_, standby_info_);
    if(event_handlers_)
      event_handlers_->call(FS_NN_FAILOVER_EVENT, active_info_.nameservice.c_str(),
                            reinterpret_cast<int64_t>(active_info_.uri.str().c_str()));
    failover_node = active_info_;
  } else if(IsCurrentStandby_locked(current_endpoint)) {
    // Connected to standby
    if(event_handlers_)
      event_handlers_->call(FS_NN_FAILOVER_EVENT, active_info_.nameservice.c_str(),
                            reinterpret_cast<int64_t>(active_info_.uri.str().c_str()));
    failover_node = active_info_;
  } else {
    // Invalid state, throw for testing
    std::string ep1 = format_endpoints(active_info_.endpoints);
    std::string ep2 = format_endpoints(standby_info_.endpoints);

    std::stringstream msg;
    msg << "Looked for " << current_endpoint << " in\n";
    msg << ep1 << " and\n";
    msg << ep2 << std::endl;

    LOG_ERROR(kRPC, << "Unable to find RPC connection in config " << msg.str() << ". Bailing out.");
    throw std::runtime_error(msg.str());
  }

  if(failover_node.endpoints.empty()) {
    LOG_WARN(kRPC, << "No endpoints for node " << failover_node.uri.str() << " attempting to resolve again");
    if(!ResolveInPlace(ioservice_, failover_node)) {
      LOG_ERROR(kRPC, << "Fallback endpoint resolution for node " << failover_node.uri.str()
                      << "failed.  Please make sure your configuration is up to date.");
    }
  }
  return failover_node;
}

bool HANamenodeTracker::IsCurrentActive_locked(const ::asio::ip::tcp::endpoint &ep) const {
  for(unsigned int i=0;i<active_info_.endpoints.size();i++) {
    if(ep.address() == active_info_.endpoints[i].address()) {
      if(ep.port() != active_info_.endpoints[i].port())
        LOG_WARN(kRPC, << "Port mismatch: " << ep << " vs " << active_info_.endpoints[i] << " trying anyway..");
      return true;
    }
  }
  return false;
}

bool HANamenodeTracker::IsCurrentStandby_locked(const ::asio::ip::tcp::endpoint &ep) const {
  for(unsigned int i=0;i<standby_info_.endpoints.size();i++) {
    if(ep.address() == standby_info_.endpoints[i].address()) {
      if(ep.port() != standby_info_.endpoints[i].port())
        LOG_WARN(kRPC, << "Port mismatch: " << ep << " vs " << standby_info_.endpoints[i] << " trying anyway..");
      return true;
    }
  }
  return false;
}

RpcEngine::RpcEngine(::asio::io_service *io_service, const Options &options,
                     const std::string &client_name, const std::string &user_name,
                     const char *protocol_name, int protocol_version)
    : io_service_(io_service),
      options_(options),
      client_name_(client_name),
      protocol_name_(protocol_name),
      protocol_version_(protocol_version),
      call_id_(0),
      retry_timer(*io_service),
      event_handlers_(std::make_shared<LibhdfsEvents>())
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
  retry_policy_ = std::move(MakeRetryPolicy(options_));

  conn_ = InitializeConnection();
  conn_->Connect(last_endpoints_, auth_info_, handler);
}

void RpcEngine::Shutdown() {
  LOG_DEBUG(kRPC, << "RpcEngine::Shutdown called");
  io_service_->post([this]() {
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

void RpcEngine::TEST_SetRpcConnection(std::shared_ptr<RpcConnection> conn) {
  conn_ = conn;
  retry_policy_ = std::move(MakeRetryPolicy(options_));
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

  if (!conn_) {
    conn_ = InitializeConnection();
    conn_->ConnectAndFlush(last_endpoints_);
  }
  conn_->AsyncRpc(method_name, req, resp, handler);
}

Status RpcEngine::Rpc(
    const std::string &method_name, const ::google::protobuf::MessageLite *req,
    const std::shared_ptr<::google::protobuf::MessageLite> &resp) {

  LOG_TRACE(kRPC, << "RpcEngine::Rpc called");

  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());
  AsyncRpc(method_name, req, resp,
           [stat](const Status &status) { stat->set_value(status); });
  return future.get();
}

std::shared_ptr<RpcConnection> RpcEngine::NewConnection()
{
  LOG_DEBUG(kRPC, << "RpcEngine::NewConnection called");

  return std::make_shared<RpcConnectionImpl<::asio::ip::tcp::socket>>(this);
}

std::shared_ptr<RpcConnection> RpcEngine::InitializeConnection()
{
  std::shared_ptr<RpcConnection> result = NewConnection();
  result->SetEventHandlers(event_handlers_);
  result->SetClusterName(cluster_name_);
  return result;
}


void RpcEngine::AsyncRpcCommsError(
    const Status &status,
    std::shared_ptr<RpcConnection> failedConnection,
    std::vector<std::shared_ptr<Request>> pendingRequests) {
  LOG_ERROR(kRPC, << "RpcEngine::AsyncRpcCommsError called; status=\"" << status.ToString() << "\" conn=" << failedConnection.get() << " reqs=" << pendingRequests.size());

  io_service().post([this, status, failedConnection, pendingRequests]() {
    RpcCommsError(status, failedConnection, pendingRequests);
  });
}

void RpcEngine::RpcCommsError(
    const Status &status,
    std::shared_ptr<RpcConnection> failedConnection,
    std::vector<std::shared_ptr<Request>> pendingRequests) {
  LOG_WARN(kRPC, << "RpcEngine::RpcCommsError called; status=\"" << status.ToString() << "\" conn=" << failedConnection.get() << " reqs=" << pendingRequests.size());

  std::lock_guard<std::mutex> state_lock(engine_state_lock_);

  // If the failed connection is the current one, shut it down
  //    It will be reconnected when there is work to do
  if (failedConnection == conn_) {
    LOG_INFO(kRPC, << "Disconnecting from failed RpcConnection");
    conn_.reset();
  }

  optional<RetryAction> head_action = optional<RetryAction>();

  //We are talking to the Standby NN, let's talk to the active one instead.
  if(ha_persisted_info_ && status.get_server_exception_type() == Status::kStandbyException) {
    LOG_INFO(kRPC, << "Received StandbyException.  Failing over.");
    head_action = RetryAction::failover(std::max(0,options_.rpc_retry_delay_ms));
  } else {
    // Filter out anything with too many retries already
    for (auto it = pendingRequests.begin(); it < pendingRequests.end();) {
      auto req = *it;

      LOG_DEBUG(kRPC, << req->GetDebugString());

      RetryAction retry = RetryAction::fail(""); // Default to fail

      if (retry_policy()) {
        retry = retry_policy()->ShouldRetry(status, req->IncrementRetryCount(), req->get_failover_count(), true);
      }

      if (retry.action == RetryAction::FAIL) {
        // If we've exceeded the maximum retry, take the latest error and pass it
        //    on.  There might be a good argument for caching the first error
        //    rather than the last one, that gets messy

        io_service().post([req, status]() {
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
  }

  // If we have reqests that need to be re-sent, ensure that we have a connection
  //   and send the requests to it
  bool haveRequests = !pendingRequests.empty() &&
          head_action && head_action->action != RetryAction::FAIL;

  if (haveRequests) {
    LOG_TRACE(kRPC, << "Have " << pendingRequests.size() << " requests to resend");
    bool needNewConnection = !conn_;
    if (needNewConnection) {
      LOG_DEBUG(kRPC, << "Creating a new NN conection");


      // If HA is enabled and we have valid HA info then fail over to the standby (hopefully now active)
      if(head_action->action == RetryAction::FAILOVER_AND_RETRY && ha_persisted_info_) {

        for(unsigned int i=0; i<pendingRequests.size();i++)
          pendingRequests[i]->IncrementFailoverCount();

        ResolvedNamenodeInfo new_active_nn_info =
            ha_persisted_info_->GetFailoverAndUpdate(last_endpoints_[0]/*reverse lookup*/);

        LOG_INFO(kRPC, << "Going to try connecting to alternate Datanode: " << new_active_nn_info.uri.str());

        if(ha_persisted_info_->is_resolved()) {
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
