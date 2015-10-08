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

#include "libhdfspp/options.h"
#include "libhdfspp/status.h"

#include <google/protobuf/message_lite.h>

#include <asio/ip/tcp.hpp>
#include <asio/deadline_timer.hpp>

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace hdfs {

class RpcEngine;
class RpcConnection {
public:
  typedef std::function<void(const Status &)> Callback;
  virtual ~RpcConnection();
  RpcConnection(RpcEngine *engine);
  virtual void Connect(const ::asio::ip::tcp::endpoint &server,
                       Callback &&handler) = 0;
  virtual void Handshake(Callback &&handler) = 0;
  virtual void Shutdown() = 0;

  void Start();
  void AsyncRpc(const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                std::shared_ptr<::google::protobuf::MessageLite> resp,
                const Callback &handler);

  void AsyncRawRpc(const std::string &method_name, const std::string &request,
                   std::shared_ptr<std::string> resp, Callback &&handler);

protected:
  class Request;
  RpcEngine *const engine_;
  virtual void OnSendCompleted(const ::asio::error_code &ec,
                               size_t transferred) = 0;
  virtual void OnRecvCompleted(const ::asio::error_code &ec,
                               size_t transferred) = 0;

  ::asio::io_service &io_service();
  std::shared_ptr<std::string> PrepareHandshakePacket();
  static std::string
  SerializeRpcRequest(const std::string &method_name,
                      const ::google::protobuf::MessageLite *req);
  void HandleRpcResponse(const std::vector<char> &data);
  void HandleRpcTimeout(std::shared_ptr<Request> req,
                        const ::asio::error_code &ec);
  void FlushPendingRequests();
  void ClearAndDisconnect(const ::asio::error_code &ec);
  std::shared_ptr<Request> RemoveFromRunningQueue(int call_id);

  enum ResponseState {
    kReadLength,
    kReadContent,
    kParseResponse,
  } resp_state_;
  unsigned resp_length_;
  std::vector<char> resp_data_;

  class Request {
  public:
    typedef std::function<void(::google::protobuf::io::CodedInputStream *is,
                               const Status &status)> Handler;
    Request(RpcConnection *parent, const std::string &method_name,
            const std::string &request, Handler &&callback);
    Request(RpcConnection *parent, const std::string &method_name,
            const ::google::protobuf::MessageLite *request, Handler &&callback);

    int call_id() const { return call_id_; }
    ::asio::deadline_timer &timer() { return timer_; }
    const std::string &payload() const { return payload_; }
    void OnResponseArrived(::google::protobuf::io::CodedInputStream *is,
                           const Status &status);

  private:
    const int call_id_;
    ::asio::deadline_timer timer_;
    std::string payload_;
    Handler handler_;
  };

  // The request being sent over the wire
  std::shared_ptr<Request> request_over_the_wire_;
  // Requests to be sent over the wire
  std::vector<std::shared_ptr<Request>> pending_requests_;
  // Requests that are waiting for responses
  typedef std::unordered_map<int, std::shared_ptr<Request>> RequestOnFlyMap;
  RequestOnFlyMap requests_on_fly_;
  // Lock for mutable parts of this class that need to be thread safe
  std::mutex engine_state_lock_;
};

class RpcEngine {
public:
  enum { kRpcVersion = 9 };
  enum {
    kCallIdAuthorizationFailed = -1,
    kCallIdInvalid = -2,
    kCallIdConnectionContext = -3,
    kCallIdPing = -4
  };

  RpcEngine(::asio::io_service *io_service, const Options &options,
            const std::string &client_name, const char *protocol_name,
            int protocol_version);

  void AsyncRpc(const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                const std::shared_ptr<::google::protobuf::MessageLite> &resp,
                const std::function<void(const Status &)> &handler);

  Status Rpc(const std::string &method_name,
             const ::google::protobuf::MessageLite *req,
             const std::shared_ptr<::google::protobuf::MessageLite> &resp);
  /**
   * Send raw bytes as RPC payload. This is intended to be used in JNI
   * bindings only.
   **/
  Status RawRpc(const std::string &method_name, const std::string &req,
                std::shared_ptr<std::string> resp);
  void Connect(const ::asio::ip::tcp::endpoint &server,
               const std::function<void(const Status &)> &handler);
  void Start();
  void Shutdown();
  void TEST_SetRpcConnection(std::unique_ptr<RpcConnection> *conn);

  int NextCallId() { return ++call_id_; }

  const std::string &client_name() const { return client_name_; }
  const std::string &protocol_name() const { return protocol_name_; }
  int protocol_version() const { return protocol_version_; }
  ::asio::io_service &io_service() { return *io_service_; }
  const Options &options() { return options_; }
  static std::string GetRandomClientName();

private:
  ::asio::io_service *io_service_;
  Options options_;
  const std::string client_name_;
  const std::string protocol_name_;
  const int protocol_version_;
  std::atomic_int call_id_;
  std::unique_ptr<RpcConnection> conn_;
};
}

#endif
