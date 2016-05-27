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
#include "optional.hpp"

#include <future>

namespace hdfs {

template <class T>
using optional = std::experimental::optional<T>;

RpcEngine::RpcEngine(::asio::io_service *io_service, const Options &options,
                     const std::string &client_name, const std::string &user_name,
                     const char *protocol_name, int protocol_version)
    : io_service_(io_service),
      options_(options),
      client_name_(client_name),
      protocol_name_(protocol_name),
      protocol_version_(protocol_version),
      retry_policy_(std::move(MakeRetryPolicy(options))),
      call_id_(0),
      retry_timer(*io_service),
      event_handlers_(std::make_shared<LibhdfsEvents>()) {

    auth_info_.setUser(user_name);
    if (options.authentication == Options::kKerberos) {
        auth_info_.setMethod(AuthInfo::kKerberos);
    }

    LOG_DEBUG(kRPC, << "RpcEngine::RpcEngine called");
}

void RpcEngine::Connect(const std::string &cluster_name,
                        const std::vector<::asio::ip::tcp::endpoint> &server,
                        RpcCallback &handler) {
  std::lock_guard<std::mutex> state_lock(engine_state_lock_);
  LOG_DEBUG(kRPC, << "RpcEngine::Connect called");

  last_endpoints_ = server;
  cluster_name_ = cluster_name;

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
  if (options.max_rpc_retries > 0) {
    return std::unique_ptr<RetryPolicy>(new FixedDelayRetryPolicy(options.rpc_retry_delay_ms, options.max_rpc_retries));
  } else {
    return nullptr;
  }
}

void RpcEngine::TEST_SetRpcConnection(std::shared_ptr<RpcConnection> conn) {
  conn_ = conn;
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
  LOG_ERROR(kRPC, << "RpcEngine::AsyncRpcCommsError called; conn=" << failedConnection.get() << " reqs=" << pendingRequests.size());

  io_service().post([this, status, failedConnection, pendingRequests]() {
    RpcCommsError(status, failedConnection, pendingRequests);
  });
}

void RpcEngine::RpcCommsError(
    const Status &status,
    std::shared_ptr<RpcConnection> failedConnection,
    std::vector<std::shared_ptr<Request>> pendingRequests) {
  (void)status;

  LOG_ERROR(kRPC, << "RpcEngine::RpcCommsError called; conn=" << failedConnection.get() << " reqs=" << pendingRequests.size());

  std::lock_guard<std::mutex> state_lock(engine_state_lock_);

  // If the failed connection is the current one, shut it down
  //    It will be reconnected when there is work to do
  if (failedConnection == conn_) {
        conn_.reset();
  }

  auto head_action = optional<RetryAction>();

  // Filter out anything with too many retries already
  for (auto it = pendingRequests.begin(); it < pendingRequests.end();) {
    auto req = *it;

    RetryAction retry = RetryAction::fail(""); // Default to fail
    if (retry_policy()) {
      retry = retry_policy()->ShouldRetry(status, req->IncrementRetryCount(), 0, true);
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

  // If we have reqests that need to be re-sent, ensure that we have a connection
  //   and send the requests to it
  bool haveRequests = !pendingRequests.empty() &&
          head_action && head_action->action != RetryAction::FAIL;

  if (haveRequests) {
    bool needNewConnection = !conn_;
    if (needNewConnection) {
      conn_ = InitializeConnection();
      conn_->PreEnqueueRequests(pendingRequests);

      if (head_action->delayMillis > 0) {
        auto weak_conn = std::weak_ptr<RpcConnection>(conn_);
        retry_timer.expires_from_now(
            std::chrono::milliseconds(options_.rpc_retry_delay_ms));
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
