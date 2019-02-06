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
#ifndef LIB_RPC_RPC_CONNECTION_H
#define LIB_RPC_RPC_CONNECTION_H

/*
 * Encapsulates a persistent connection to the NameNode, and the sending of
 * RPC requests and evaluating their responses.
 *
 * Can have multiple RPC requests in-flight simultaneously, but they are
 * evaluated in-order on the server side in a blocking manner.
 *
 * Threading model: public interface is thread-safe
 * All handlers passed in to method calls will be called from an asio thread,
 *   and will not be holding any internal RpcConnection locks.
 */

#include "request.h"
#include "common/auth_info.h"
#include "common/libhdfs_events_impl.h"
#include "common/new_delete.h"
#include "hdfspp/status.h"

#include <functional>
#include <memory>
#include <vector>
#include <deque>
#include <unordered_map>

namespace hdfs {

typedef const std::function<void(const Status &)> RpcCallback;

class LockFreeRpcEngine;
class SaslProtocol;

class RpcConnection : public std::enable_shared_from_this<RpcConnection> {
 public:
  MEMCHECKED_CLASS(RpcConnection)
  RpcConnection(std::shared_ptr<LockFreeRpcEngine> engine);
  virtual ~RpcConnection();

  // Note that a single server can have multiple endpoints - especially both
  //   an ipv4 and ipv6 endpoint
  virtual void Connect(const std::vector<::asio::ip::tcp::endpoint> &server,
                       const AuthInfo & auth_info,
                       RpcCallback &handler) = 0;
  virtual void ConnectAndFlush(const std::vector<::asio::ip::tcp::endpoint> &server) = 0;
  virtual void Disconnect() = 0;

  void StartReading();
  void AsyncRpc(const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                std::shared_ptr<::google::protobuf::MessageLite> resp,
                const RpcCallback &handler);

  void AsyncRpc(const std::vector<std::shared_ptr<Request> > & requests);

  // Enqueue requests before the connection is connected.  Will be flushed
  //   on connect
  void PreEnqueueRequests(std::vector<std::shared_ptr<Request>> requests);

  // Put requests at the front of the current request queue
  void PrependRequests_locked(std::vector<std::shared_ptr<Request>> requests);

  void SetEventHandlers(std::shared_ptr<LibhdfsEvents> event_handlers);
  void SetClusterName(std::string cluster_name);
  void SetAuthInfo(const AuthInfo& auth_info);

  std::weak_ptr<LockFreeRpcEngine> engine() { return engine_; }
  std::shared_ptr<IoService> GetIoService();

 protected:
  struct Response {
    enum ResponseState {
      kReadLength,
      kReadContent,
      kParseResponse,
    } state_;
    unsigned length_;
    std::vector<char> data_;

    std::unique_ptr<::google::protobuf::io::ArrayInputStream> ar;
    std::unique_ptr<::google::protobuf::io::CodedInputStream> in;

    Response() : state_(kReadLength), length_(0) {}
  };


  // Initial handshaking protocol: connect->handshake-->(auth)?-->context->connected
  virtual void SendHandshake(RpcCallback &handler) = 0;
  void HandshakeComplete(const Status &s);
  void AuthComplete(const Status &s, const AuthInfo & new_auth_info);
  void AuthComplete_locked(const Status &s, const AuthInfo & new_auth_info);
  virtual void SendContext(RpcCallback &handler) = 0;
  void ContextComplete(const Status &s);

  virtual void OnSendCompleted(const ::asio::error_code &ec,
                               size_t transferred) = 0;
  virtual void OnRecvCompleted(const ::asio::error_code &ec,
                               size_t transferred) = 0;
  virtual void FlushPendingRequests()=0;      // Synchronously write the next request

  void AsyncRpc_locked(
                const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                std::shared_ptr<::google::protobuf::MessageLite> resp,
                const RpcCallback &handler);
  void SendRpcRequests(const std::vector<std::shared_ptr<Request> > & requests);
  void AsyncFlushPendingRequests(); // Queue requests to be flushed at a later time



  std::shared_ptr<std::string> PrepareHandshakePacket();
  std::shared_ptr<std::string> PrepareContextPacket();
  static std::string SerializeRpcRequest(const std::string &method_name,
                                         const ::google::protobuf::MessageLite *req);

  Status HandleRpcResponse(std::shared_ptr<Response> response);
  void HandleRpcTimeout(std::shared_ptr<Request> req,
                        const ::asio::error_code &ec);
  void CommsError(const Status &status);

  void ClearAndDisconnect(const ::asio::error_code &ec);
  std::shared_ptr<Request> RemoveFromRunningQueue(int call_id);

  std::weak_ptr<LockFreeRpcEngine> engine_;
  std::shared_ptr<Response> current_response_state_;
  AuthInfo auth_info_;

  // Connection can have deferred connection, especially when we're pausing
  //   during retry
  enum ConnectedState {
      kNotYetConnected,
      kConnecting,
      kHandshaking,
      kAuthenticating,
      kConnected,
      kDisconnected
  };
  static std::string ToString(ConnectedState connected);
  ConnectedState connected_;

  // State machine for performing a SASL handshake
  std::shared_ptr<SaslProtocol> sasl_protocol_;
  // The request being sent over the wire; will also be in sent_requests_
  std::shared_ptr<Request> outgoing_request_;
  // Requests to be sent over the wire
  std::deque<std::shared_ptr<Request>> pending_requests_;
  // Requests to be sent over the wire during authentication; not retried if
  //   there is a connection error
  std::deque<std::shared_ptr<Request>> auth_requests_;
  // Requests that are waiting for responses
  typedef std::unordered_map<int, std::shared_ptr<Request>> SentRequestMap;
  SentRequestMap sent_requests_;

  std::shared_ptr<LibhdfsEvents> event_handlers_;
  std::string cluster_name_;

  // Lock for mutable parts of this class that need to be thread safe
  std::mutex connection_state_lock_;

  friend class SaslProtocol;
};

} // end namespace hdfs
#endif // end include Guard
