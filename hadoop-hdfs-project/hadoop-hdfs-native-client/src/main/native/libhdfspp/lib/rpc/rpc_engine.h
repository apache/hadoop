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
#include "common/new_delete.h"

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <asio/ip/tcp.hpp>
#include <asio/deadline_timer.hpp>

#include <atomic>
#include <memory>
#include <unordered_map>
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

/*
 * Internal bookkeeping for an outstanding request from the consumer.
 *
 * Threading model: not thread-safe; should only be accessed from a single
 *   thread at a time
 */
class Request {
 public:
  MEMCHECKED_CLASS(Request)
  typedef std::function<void(::google::protobuf::io::CodedInputStream *is,
                             const Status &status)> Handler;

  Request(LockFreeRpcEngine *engine, const std::string &method_name, int call_id,
          const std::string &request, Handler &&callback);
  Request(LockFreeRpcEngine *engine, const std::string &method_name, int call_id,
          const ::google::protobuf::MessageLite *request, Handler &&callback);

  // Null request (with no actual message) used to track the state of an
  //    initial Connect call
  Request(LockFreeRpcEngine *engine, Handler &&handler);

  int call_id() const { return call_id_; }
  std::string  method_name() const { return method_name_; }
  ::asio::deadline_timer &timer() { return timer_; }
  int IncrementRetryCount() { return retry_count_++; }
  void GetPacket(std::string *res) const;
  void OnResponseArrived(::google::protobuf::io::CodedInputStream *is,
                         const Status &status);

 private:
  LockFreeRpcEngine *const engine_;
  const std::string method_name_;
  const int call_id_;

  ::asio::deadline_timer timer_;
  std::string payload_;
  const Handler handler_;

  int retry_count_;
};

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
class RpcConnection : public std::enable_shared_from_this<RpcConnection> {
 public:
  MEMCHECKED_CLASS(RpcConnection)
  RpcConnection(LockFreeRpcEngine *engine);
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

  void SetEventHandlers(std::shared_ptr<LibhdfsEvents> event_handlers);
  void SetClusterName(std::string cluster_name);

  LockFreeRpcEngine *engine() { return engine_; }
  ::asio::io_service &io_service();

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
  static std::string SerializeRpcRequest(
      const std::string &method_name,
      const ::google::protobuf::MessageLite *req);
  void HandleRpcResponse(std::shared_ptr<Response> response);
  void HandleRpcTimeout(std::shared_ptr<Request> req,
                        const ::asio::error_code &ec);
  void CommsError(const Status &status);

  void ClearAndDisconnect(const ::asio::error_code &ec);
  std::shared_ptr<Request> RemoveFromRunningQueue(int call_id);

  LockFreeRpcEngine *const engine_;
  std::shared_ptr<Response> current_response_state_;
  AuthInfo auth_info_;

  // Connection can have deferred connection, especially when we're pausing
  //   during retry
  enum ConnectedState {
      kNotYetConnected,
      kConnecting,
      kAuthenticating,
      kConnected,
      kDisconnected
  };
  static std::string ToString(ConnectedState connected);
  ConnectedState connected_;

  // State machine for performing a SASL handshake
  std::shared_ptr<SaslProtocol> sasl_protocol_;
  // The request being sent over the wire; will also be in requests_on_fly_
  std::shared_ptr<Request> request_over_the_wire_;
  // Requests to be sent over the wire
  std::vector<std::shared_ptr<Request>> pending_requests_;
  // Requests to be sent over the wire during authentication; not retried if
  //   there is a connection error
  std::vector<std::shared_ptr<Request>> auth_requests_;
  // Requests that are waiting for responses
  typedef std::unordered_map<int, std::shared_ptr<Request>> RequestOnFlyMap;
  RequestOnFlyMap requests_on_fly_;
  std::shared_ptr<LibhdfsEvents> event_handlers_;
  std::string cluster_name_;


  // Lock for mutable parts of this class that need to be thread safe
  std::mutex connection_state_lock_;

  friend class SaslProtocol;
};


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


  virtual const RetryPolicy * retry_policy() const = 0;
  virtual int NextCallId() = 0;

  virtual const std::string &client_name() const = 0;
  virtual const std::string &user_name() const = 0;
  virtual const std::string &protocol_name() const = 0;
  virtual int protocol_version() const = 0;
  virtual ::asio::io_service &io_service() = 0;
  virtual const Options &options() const = 0;
};

/*
 * An engine for reliable communication with a NameNode.  Handles connection,
 * retry, and (someday) failover of the requested messages.
 *
 * Threading model: thread-safe.  All callbacks will be called back from
 *   an asio pool and will not hold any internal locks
 */
class RpcEngine : public LockFreeRpcEngine {
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

  RpcEngine(::asio::io_service *io_service, const Options &options,
            const std::string &client_name, const std::string &user_name,
            const char *protocol_name, int protocol_version);

  void Connect(const std::string & cluster_name,
               const std::vector<::asio::ip::tcp::endpoint> &server,
               RpcCallback &handler);

  void AsyncRpc(const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                const std::shared_ptr<::google::protobuf::MessageLite> &resp,
                const std::function<void(const Status &)> &handler);

  Status Rpc(const std::string &method_name,
             const ::google::protobuf::MessageLite *req,
             const std::shared_ptr<::google::protobuf::MessageLite> &resp);

  void Start();
  void Shutdown();

  /* Enqueues a CommsError without acquiring a lock*/
  void AsyncRpcCommsError(const Status &status,
                     std::shared_ptr<RpcConnection> failedConnection,
                     std::vector<std::shared_ptr<Request>> pendingRequests) override;
  void RpcCommsError(const Status &status,
                     std::shared_ptr<RpcConnection> failedConnection,
                     std::vector<std::shared_ptr<Request>> pendingRequests);


  const RetryPolicy * retry_policy() const override { return retry_policy_.get(); }
  int NextCallId() override { return ++call_id_; }

  void TEST_SetRpcConnection(std::shared_ptr<RpcConnection> conn);

  const std::string &client_name() const override { return client_name_; }
  const std::string &user_name() const override { return auth_info_.getUser(); }
  const std::string &protocol_name() const override { return protocol_name_; }
  int protocol_version() const override { return protocol_version_; }
  ::asio::io_service &io_service() override { return *io_service_; }
  const Options &options() const override { return options_; }
  static std::string GetRandomClientName();

  void SetFsEventCallback(fs_event_callback callback);
protected:
  std::shared_ptr<RpcConnection> conn_;
  std::shared_ptr<RpcConnection> InitializeConnection();
  virtual std::shared_ptr<RpcConnection> NewConnection();
  virtual std::unique_ptr<const RetryPolicy> MakeRetryPolicy(const Options &options);

  // Remember all of the last endpoints in case we need to reconnect and retry
  std::vector<::asio::ip::tcp::endpoint> last_endpoints_;

private:
  ::asio::io_service * const io_service_;
  const Options options_;
  const std::string client_name_;
  const std::string protocol_name_;
  const int protocol_version_;
  const std::unique_ptr<const RetryPolicy> retry_policy_; //null --> no retry
  AuthInfo auth_info_;
  std::string cluster_name_;
  std::atomic_int call_id_;
  ::asio::deadline_timer retry_timer;

  std::shared_ptr<LibhdfsEvents> event_handlers_;

  std::mutex engine_state_lock_;

};
}

#endif
