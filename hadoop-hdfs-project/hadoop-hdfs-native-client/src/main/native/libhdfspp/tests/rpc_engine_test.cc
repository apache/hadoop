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

#include "mock_connection.h"
#include "test.pb.h"
#include "RpcHeader.pb.h"
#include "rpc/rpc_connection.h"

#include <google/protobuf/io/coded_stream.h>

#include <gmock/gmock.h>

using ::hadoop::common::RpcResponseHeaderProto;
using ::hadoop::common::EmptyRequestProto;
using ::hadoop::common::EmptyResponseProto;
using ::hadoop::common::EchoRequestProto;
using ::hadoop::common::EchoResponseProto;

using ::asio::error_code;

using ::testing::Return;

using ::std::make_pair;
using ::std::string;

namespace pb = ::google::protobuf;
namespace pbio = ::google::protobuf::io;

namespace hdfs {

std::vector<asio::ip::basic_endpoint<asio::ip::tcp>> make_endpoint() {
  std::vector<asio::ip::basic_endpoint<asio::ip::tcp>> result;
  result.push_back(asio::ip::basic_endpoint<asio::ip::tcp>());
  return result;
}

class MockRPCConnection : public MockConnectionBase {
 public:
  MockRPCConnection(::asio::io_service &io_service)
      : MockConnectionBase(&io_service) {}
  MOCK_METHOD0(Produce, ProducerResult());
};

class SharedMockRPCConnection : public SharedMockConnection {
 public:
  SharedMockRPCConnection(::asio::io_service &io_service)
      : SharedMockConnection(&io_service) {}
};

class SharedConnectionEngine : public RpcEngine {
  using RpcEngine::RpcEngine;

protected:
  std::shared_ptr<RpcConnection> NewConnection() override {
    // Stuff in some dummy endpoints so we don't error out
    last_endpoints_ = make_endpoint();

    return std::make_shared<RpcConnectionImpl<SharedMockRPCConnection>>(this);
  }

};

}

static inline std::pair<error_code, string> RpcResponse(
    const RpcResponseHeaderProto &h, const std::string &data,
    const ::asio::error_code &ec = error_code()) {
  uint32_t payload_length =
      pbio::CodedOutputStream::VarintSize32(h.ByteSize()) +
      pbio::CodedOutputStream::VarintSize32(data.size()) + h.ByteSize() +
      data.size();

  std::string res;
  res.resize(sizeof(uint32_t) + payload_length);
  uint8_t *buf = reinterpret_cast<uint8_t *>(const_cast<char *>(res.c_str()));

  buf = pbio::CodedOutputStream::WriteLittleEndian32ToArray(
      htonl(payload_length), buf);
  buf = pbio::CodedOutputStream::WriteVarint32ToArray(h.ByteSize(), buf);
  buf = h.SerializeWithCachedSizesToArray(buf);
  buf = pbio::CodedOutputStream::WriteVarint32ToArray(data.size(), buf);
  buf = pbio::CodedOutputStream::WriteStringToArray(data, buf);

  return std::make_pair(ec, std::move(res));
}


using namespace hdfs;

TEST(RpcEngineTest, TestRoundTrip) {
  ::asio::io_service io_service;
  Options options;
  RpcEngine engine(&io_service, options, "foo", "protocol", 1);
  RpcConnectionImpl<MockRPCConnection> *conn =
      new RpcConnectionImpl<MockRPCConnection>(&engine);
  conn->TEST_set_connected(true);
  conn->StartReading();

  EchoResponseProto server_resp;
  server_resp.set_message("foo");

  RpcResponseHeaderProto h;
  h.set_callid(1);
  h.set_status(RpcResponseHeaderProto::SUCCESS);
  EXPECT_CALL(conn->next_layer(), Produce())
      .WillOnce(Return(RpcResponse(h, server_resp.SerializeAsString())));

  std::shared_ptr<RpcConnection> conn_ptr(conn);
  engine.TEST_SetRpcConnection(conn_ptr);

  bool complete = false;

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());
  engine.AsyncRpc("test", &req, resp, [resp, &complete,&io_service](const Status &stat) {
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ("foo", resp->message());
    complete = true;
    io_service.stop();
  });
  io_service.run();
  ASSERT_TRUE(complete);
}

TEST(RpcEngineTest, TestConnectionResetAndFail) {
  ::asio::io_service io_service;
  Options options;
  RpcEngine engine(&io_service, options, "foo", "protocol", 1);
  RpcConnectionImpl<MockRPCConnection> *conn =
      new RpcConnectionImpl<MockRPCConnection>(&engine);
  conn->TEST_set_connected(true);
  conn->StartReading();

  bool complete = false;

  RpcResponseHeaderProto h;
  h.set_callid(1);
  h.set_status(RpcResponseHeaderProto::SUCCESS);
  EXPECT_CALL(conn->next_layer(), Produce())
      .WillOnce(Return(RpcResponse(
          h, "", make_error_code(::asio::error::connection_reset))));

  std::shared_ptr<RpcConnection> conn_ptr(conn);
  engine.TEST_SetRpcConnection(conn_ptr);

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());

  engine.AsyncRpc("test", &req, resp, [&complete, &io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_FALSE(stat.ok());
  });
  io_service.run();
  ASSERT_TRUE(complete);
}


TEST(RpcEngineTest, TestConnectionResetAndRecover) {
  ::asio::io_service io_service;
  Options options;
  options.max_rpc_retries = 1;
  options.rpc_retry_delay_ms = 0;
  SharedConnectionEngine engine(&io_service, options, "foo", "protocol", 1);

  EchoResponseProto server_resp;
  server_resp.set_message("foo");

  bool complete = false;

  auto producer = std::make_shared<SharedConnectionData>();
  RpcResponseHeaderProto h;
  h.set_callid(1);
  h.set_status(RpcResponseHeaderProto::SUCCESS);
  EXPECT_CALL(*producer, Produce())
      .WillOnce(Return(RpcResponse(
          h, "", make_error_code(::asio::error::connection_reset))))
      .WillOnce(Return(RpcResponse(h, server_resp.SerializeAsString())));
  SharedMockConnection::SetSharedConnectionData(producer);

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());

  engine.AsyncRpc("test", &req, resp, [&complete, &io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_TRUE(stat.ok());
  });
  io_service.run();
  ASSERT_TRUE(complete);
}

TEST(RpcEngineTest, TestConnectionResetAndRecoverWithDelay) {
  ::asio::io_service io_service;
  Options options;
  options.max_rpc_retries = 1;
  options.rpc_retry_delay_ms = 1;
  SharedConnectionEngine engine(&io_service, options, "foo", "protocol", 1);

  EchoResponseProto server_resp;
  server_resp.set_message("foo");

  bool complete = false;

  auto producer = std::make_shared<SharedConnectionData>();
  RpcResponseHeaderProto h;
  h.set_callid(1);
  h.set_status(RpcResponseHeaderProto::SUCCESS);
  EXPECT_CALL(*producer, Produce())
      .WillOnce(Return(RpcResponse(
          h, "", make_error_code(::asio::error::connection_reset))))
      .WillOnce(Return(RpcResponse(h, server_resp.SerializeAsString())));
  SharedMockConnection::SetSharedConnectionData(producer);

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());

  engine.AsyncRpc("test", &req, resp, [&complete, &io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_TRUE(stat.ok());
  });

  ::asio::deadline_timer timer(io_service);
  timer.expires_from_now(std::chrono::hours(100));
  timer.async_wait([](const asio::error_code & err){(void)err; ASSERT_FALSE("Timed out"); });

  io_service.run();
  ASSERT_TRUE(complete);
}

TEST(RpcEngineTest, TestConnectionFailure)
{
  auto producer = std::make_shared<SharedConnectionData>();
  producer->checkProducerForConnect = true;
  SharedMockConnection::SetSharedConnectionData(producer);

  // Error and no retry
  ::asio::io_service io_service;

  bool complete = false;

  Options options;
  options.max_rpc_retries = 0;
  options.rpc_retry_delay_ms = 0;
  SharedConnectionEngine engine(&io_service, options, "foo", "protocol", 1);
  EXPECT_CALL(*producer, Produce())
      .WillOnce(Return(std::make_pair(make_error_code(::asio::error::connection_reset), "")));

  engine.Connect(make_endpoint(), [&complete, &io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_FALSE(stat.ok());
  });
  io_service.run();
  ASSERT_TRUE(complete);
}

TEST(RpcEngineTest, TestConnectionFailureRetryAndFailure)
{
  auto producer = std::make_shared<SharedConnectionData>();
  producer->checkProducerForConnect = true;
  SharedMockConnection::SetSharedConnectionData(producer);

  ::asio::io_service io_service;

  bool complete = false;

  Options options;
  options.max_rpc_retries = 2;
  options.rpc_retry_delay_ms = 0;
  SharedConnectionEngine engine(&io_service, options, "foo", "protocol", 1);
  EXPECT_CALL(*producer, Produce())
      .WillOnce(Return(std::make_pair(make_error_code(::asio::error::connection_reset), "")))
      .WillOnce(Return(std::make_pair(make_error_code(::asio::error::connection_reset), "")))
      .WillOnce(Return(std::make_pair(make_error_code(::asio::error::connection_reset), "")));

  engine.Connect(make_endpoint(), [&complete, &io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_FALSE(stat.ok());
  });
  io_service.run();
  ASSERT_TRUE(complete);
}

TEST(RpcEngineTest, TestConnectionFailureAndRecover)
{
  auto producer = std::make_shared<SharedConnectionData>();
  producer->checkProducerForConnect = true;
  SharedMockConnection::SetSharedConnectionData(producer);

  ::asio::io_service io_service;

  bool complete = false;

  Options options;
  options.max_rpc_retries = 1;
  options.rpc_retry_delay_ms = 0;
  SharedConnectionEngine engine(&io_service, options, "foo", "protocol", 1);
  EXPECT_CALL(*producer, Produce())
      .WillOnce(Return(std::make_pair(make_error_code(::asio::error::connection_reset), "")))
      .WillOnce(Return(std::make_pair(::asio::error_code(), "")))
      .WillOnce(Return(std::make_pair(::asio::error::would_block, "")));

  engine.Connect(make_endpoint(), [&complete, &io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_TRUE(stat.ok());
  });
  io_service.run();
  ASSERT_TRUE(complete);
}

TEST(RpcEngineTest, TestConnectionFailureAndAsyncRecover)
{
  // Error and async recover
  auto producer = std::make_shared<SharedConnectionData>();
  producer->checkProducerForConnect = true;
  SharedMockConnection::SetSharedConnectionData(producer);

  ::asio::io_service io_service;

  bool complete = false;

  Options options;
  options.max_rpc_retries = 1;
  options.rpc_retry_delay_ms = 1;
  SharedConnectionEngine engine(&io_service, options, "foo", "protocol", 1);
  EXPECT_CALL(*producer, Produce())
      .WillOnce(Return(std::make_pair(make_error_code(::asio::error::connection_reset), "")))
      .WillOnce(Return(std::make_pair(::asio::error_code(), "")))
      .WillOnce(Return(std::make_pair(::asio::error::would_block, "")));

  engine.Connect(make_endpoint(), [&complete, &io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_TRUE(stat.ok());
  });

  ::asio::deadline_timer timer(io_service);
  timer.expires_from_now(std::chrono::hours(100));
  timer.async_wait([](const asio::error_code & err){(void)err; ASSERT_FALSE("Timed out"); });

  io_service.run();
  ASSERT_TRUE(complete);
}

TEST(RpcEngineTest, TestTimeout) {
  ::asio::io_service io_service;
  Options options;
  options.rpc_timeout = 1;
  RpcEngine engine(&io_service, options, "foo", "protocol", 1);
  RpcConnectionImpl<MockRPCConnection> *conn =
      new RpcConnectionImpl<MockRPCConnection>(&engine);
  conn->TEST_set_connected(true);
  conn->StartReading();

    EXPECT_CALL(conn->next_layer(), Produce())
        .WillOnce(Return(std::make_pair(::asio::error::would_block, "")));

  std::shared_ptr<RpcConnection> conn_ptr(conn);
  engine.TEST_SetRpcConnection(conn_ptr);

  bool complete = false;

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());
  engine.AsyncRpc("test", &req, resp, [resp, &complete,&io_service](const Status &stat) {
    complete = true;
    io_service.stop();
    ASSERT_FALSE(stat.ok());
  });

  ::asio::deadline_timer timer(io_service);
  timer.expires_from_now(std::chrono::hours(100));
  timer.async_wait([](const asio::error_code & err){(void)err; ASSERT_FALSE("Timed out"); });

  io_service.run();
  ASSERT_TRUE(complete);
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code = RUN_ALL_TESTS();

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return exit_code;
}
