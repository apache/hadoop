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

#include <future>

namespace hdfs {

RpcEngine::RpcEngine(::asio::io_service *io_service, const Options &options,
                     const std::string &client_name, const char *protocol_name,
                     int protocol_version)
    : io_service_(io_service), options_(options), client_name_(client_name),
      protocol_name_(protocol_name), protocol_version_(protocol_version),
      call_id_(0) {
}

void RpcEngine::Connect(const ::asio::ip::tcp::endpoint &server,
                        const std::function<void(const Status &)> &handler) {
  conn_.reset(new RpcConnectionImpl<::asio::ip::tcp::socket>(this));
  conn_->Connect(server, [this, handler](const Status &stat) {
    if (!stat.ok()) {
      handler(stat);
    } else {
      conn_->Handshake([handler](const Status &s) { handler(s); });
    }
  });
}

void RpcEngine::Start() { conn_->Start(); }

void RpcEngine::Shutdown() {
  io_service_->post([this]() { conn_->Shutdown(); });
}

void RpcEngine::TEST_SetRpcConnection(std::unique_ptr<RpcConnection> *conn) {
  conn_.reset(conn->release());
}

void RpcEngine::AsyncRpc(
    const std::string &method_name, const ::google::protobuf::MessageLite *req,
    const std::shared_ptr<::google::protobuf::MessageLite> &resp,
    const std::function<void(const Status &)> &handler) {
  conn_->AsyncRpc(method_name, req, resp, handler);
}

Status
RpcEngine::Rpc(const std::string &method_name,
               const ::google::protobuf::MessageLite *req,
               const std::shared_ptr<::google::protobuf::MessageLite> &resp) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());
  AsyncRpc(method_name, req, resp,
           [stat](const Status &status) { stat->set_value(status); });
  return future.get();
}

Status RpcEngine::RawRpc(const std::string &method_name, const std::string &req,
                         std::shared_ptr<std::string> resp) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());
  conn_->AsyncRawRpc(method_name, req, resp,
                     [stat](const Status &status) { stat->set_value(status); });
  return future.get();
}

}
