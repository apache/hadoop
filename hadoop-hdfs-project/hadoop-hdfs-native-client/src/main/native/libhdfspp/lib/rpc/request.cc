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


#include "request.h"
#include "rpc_engine.h"
#include "sasl_protocol.h"
#include "hdfspp/ioservice.h"

#include "RpcHeader.pb.h"
#include "ProtobufRpcEngine.pb.h"
#include "IpcConnectionContext.pb.h"

#include <sstream>

namespace hdfs {

namespace pb = ::google::protobuf;
namespace pbio = ::google::protobuf::io;

using namespace ::hadoop::common;
using namespace ::std::placeholders;

static const int kNoRetry = -1;

// Protobuf helper functions.
// Note/todo: Using the zero-copy protobuf API here makes the simple procedures
//   below tricky to read and debug while providing minimal benefit.  Reducing
//   allocations in BlockReader (HDFS-11266) and smarter use of std::stringstream
//   will have a much larger impact according to cachegrind profiles on common
//   workloads.
static void AddHeadersToPacket(std::string *res,
                               std::initializer_list<const pb::MessageLite *> headers,
                               const std::string *payload) {
  int len = 0;
  std::for_each(
      headers.begin(), headers.end(),
      [&len](const pb::MessageLite *v) { len += DelimitedPBMessageSize(v); });

  if (payload) {
    len += payload->size();
  }

  int net_len = htonl(len);
  res->reserve(res->size() + sizeof(net_len) + len);

  pbio::StringOutputStream ss(res);
  pbio::CodedOutputStream os(&ss);
  os.WriteRaw(reinterpret_cast<const char *>(&net_len), sizeof(net_len));

  uint8_t *buf = os.GetDirectBufferForNBytesAndAdvance(len);
  assert(buf);

  std::for_each(
      headers.begin(), headers.end(), [&buf](const pb::MessageLite *v) {
        buf = pbio::CodedOutputStream::WriteVarint32ToArray(v->ByteSize(), buf);
        buf = v->SerializeWithCachedSizesToArray(buf);
      });

  if (payload) {
    buf = os.WriteStringToArray(*payload, buf);
  }
}

static void ConstructPayload(std::string *res, const pb::MessageLite *header) {
  int len = DelimitedPBMessageSize(header);
  res->reserve(len);
  pbio::StringOutputStream ss(res);
  pbio::CodedOutputStream os(&ss);
  uint8_t *buf = os.GetDirectBufferForNBytesAndAdvance(len);
  assert(buf);
  buf = pbio::CodedOutputStream::WriteVarint32ToArray(header->ByteSize(), buf);
  buf = header->SerializeWithCachedSizesToArray(buf);
}

static void SetRequestHeader(std::weak_ptr<LockFreeRpcEngine> weak_engine, int call_id,
                             const std::string &method_name, int retry_count,
                             RpcRequestHeaderProto *rpc_header,
                             RequestHeaderProto *req_header)
{
  // Ensure the RpcEngine is live.  If it's not then the FileSystem is being destructed.
  std::shared_ptr<LockFreeRpcEngine> counted_engine = weak_engine.lock();
  if(!counted_engine) {
    LOG_ERROR(kRPC, << "SetRequestHeader attempted to access an invalid RpcEngine");
    return;
  }

  rpc_header->set_rpckind(RPC_PROTOCOL_BUFFER);
  rpc_header->set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  rpc_header->set_callid(call_id);
  if (retry_count != kNoRetry) {
    rpc_header->set_retrycount(retry_count);
  }
  rpc_header->set_clientid(counted_engine->client_id());
  req_header->set_methodname(method_name);
  req_header->set_declaringclassprotocolname(counted_engine->protocol_name());
  req_header->set_clientprotocolversion(counted_engine->protocol_version());
}

// Request implementation

Request::Request(std::shared_ptr<LockFreeRpcEngine> engine, const std::string &method_name, int call_id,
                 const pb::MessageLite *request, Handler &&handler)
    : engine_(engine),
      method_name_(method_name),
      call_id_(call_id),
      timer_(engine->io_service()->GetRaw()),
      handler_(std::move(handler)),
      retry_count_(engine->retry_policy() ? 0 : kNoRetry),
      failover_count_(0)
{
  ConstructPayload(&payload_, request);
}

Request::Request(std::shared_ptr<LockFreeRpcEngine> engine, Handler &&handler)
    : engine_(engine),
      call_id_(-1/*Handshake ID*/),
      timer_(engine->io_service()->GetRaw()),
      handler_(std::move(handler)),
      retry_count_(engine->retry_policy() ? 0 : kNoRetry),
      failover_count_(0) {
}

void Request::GetPacket(std::string *res) const {
  LOG_TRACE(kRPC, << "Request::GetPacket called");

  if (payload_.empty())
    return;

  RpcRequestHeaderProto rpc_header;
  RequestHeaderProto req_header;
  SetRequestHeader(engine_, call_id_, method_name_, retry_count_, &rpc_header,
                   &req_header);

  // SASL messages don't have a request header
  if (method_name_ != SASL_METHOD_NAME)
    AddHeadersToPacket(res, {&rpc_header, &req_header}, &payload_);
  else
    AddHeadersToPacket(res, {&rpc_header}, &payload_);
}

void Request::OnResponseArrived(pbio::CodedInputStream *is,
                                const Status &status) {
  LOG_TRACE(kRPC, << "Request::OnResponseArrived called");
  handler_(is, status);
}

std::string Request::GetDebugString() const {
  // Basic description of this object, aimed at debugging
  std::stringstream ss;
  ss << "\nRequest Object:\n";
  ss << "\tMethod name    = \"" << method_name_ << "\"\n";
  ss << "\tCall id        = " << call_id_ << "\n";
  ss << "\tRetry Count    = " << retry_count_ << "\n";
  ss << "\tFailover count = " << failover_count_ << "\n";
  return ss.str();
}

int Request::IncrementFailoverCount() {
  // reset retry count when failing over
  retry_count_ = 0;
  return failover_count_++;
}

} // end namespace hdfs
