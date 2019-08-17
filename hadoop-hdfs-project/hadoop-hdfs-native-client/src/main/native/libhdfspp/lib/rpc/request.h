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
#ifndef LIB_RPC_RPC_REQUEST_H
#define LIB_RPC_RPC_REQUEST_H

#include "hdfspp/status.h"
#include "common/util.h"
#include "common/new_delete.h"

#include <string>
#include <memory>

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <asio/deadline_timer.hpp>


namespace hdfs {

class LockFreeRpcEngine;
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

  // Constructors will not make any blocking calls while holding the shared_ptr<RpcEngine>
  Request(std::shared_ptr<LockFreeRpcEngine> engine, const std::string &method_name, int call_id,
          const ::google::protobuf::MessageLite *request, Handler &&callback);

  // Null request (with no actual message) used to track the state of an
  //    initial Connect call
  Request(std::shared_ptr<LockFreeRpcEngine> engine, Handler &&handler);

  int call_id() const { return call_id_; }
  std::string  method_name() const { return method_name_; }
  ::asio::deadline_timer &timer() { return timer_; }
  int IncrementRetryCount() { return retry_count_++; }
  int IncrementFailoverCount();
  void GetPacket(std::string *res) const;
  void OnResponseArrived(::google::protobuf::io::CodedInputStream *is,
                         const Status &status);

  int get_failover_count() {return failover_count_;}

  std::string GetDebugString() const;

 private:
  std::weak_ptr<LockFreeRpcEngine> engine_;
  const std::string method_name_;
  const int call_id_;

  ::asio::deadline_timer timer_;
  std::string payload_;
  const Handler handler_;

  int retry_count_;
  int failover_count_;
};

} // end namespace hdfs
#endif // end include guard
