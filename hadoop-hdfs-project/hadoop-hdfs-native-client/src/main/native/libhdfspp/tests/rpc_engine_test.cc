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

class MockRPCConnection : public MockConnectionBase {
public:
  MockRPCConnection(::asio::io_service &io_service)
      : MockConnectionBase(&io_service) {}
  MOCK_METHOD0(Produce, ProducerResult());
  template <class Endpoint, class Callback>
  void async_connect(const Endpoint &, Callback &&handler) {
    handler(::asio::error_code());
  }
  void cancel() {}
  void close() {}
};

static inline std::pair<error_code, string>
RpcResponse(const RpcResponseHeaderProto &h, const std::string &data,
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
}

using namespace hdfs;

TEST(RpcEngineTest, TestRoundTrip) {
  ::asio::io_service io_service;
  Options options;
  RpcEngine engine(&io_service, options, "foo", "protocol", 1);
  RpcConnectionImpl<MockRPCConnection> *conn =
      new RpcConnectionImpl<MockRPCConnection>(&engine);
  EchoResponseProto server_resp;
  server_resp.set_message("foo");

  RpcResponseHeaderProto h;
  h.set_callid(1);
  h.set_status(RpcResponseHeaderProto::SUCCESS);
  EXPECT_CALL(conn->next_layer(), Produce())
      .WillOnce(Return(RpcResponse(h, server_resp.SerializeAsString())));

  std::unique_ptr<RpcConnection> conn_ptr(conn);
  engine.TEST_SetRpcConnection(&conn_ptr);

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());
  engine.AsyncRpc("test", &req, resp, [resp, &io_service](const Status &stat) {
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ("foo", resp->message());
    io_service.stop();
  });
  conn->Start();
  io_service.run();
}

TEST(RpcEngineTest, TestConnectionReset) {
  ::asio::io_service io_service;
  Options options;
  RpcEngine engine(&io_service, options, "foo", "protocol", 1);
  RpcConnectionImpl<MockRPCConnection> *conn =
      new RpcConnectionImpl<MockRPCConnection>(&engine);

  RpcResponseHeaderProto h;
  h.set_callid(1);
  h.set_status(RpcResponseHeaderProto::SUCCESS);
  EXPECT_CALL(conn->next_layer(), Produce())
      .WillOnce(Return(RpcResponse(
          h, "", make_error_code(::asio::error::connection_reset))));

  std::unique_ptr<RpcConnection> conn_ptr(conn);
  engine.TEST_SetRpcConnection(&conn_ptr);

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());

  engine.AsyncRpc("test", &req, resp, [&io_service](const Status &stat) {
    ASSERT_FALSE(stat.ok());
  });

  engine.AsyncRpc("test", &req, resp, [&io_service](const Status &stat) {
    io_service.stop();
    ASSERT_FALSE(stat.ok());
  });
  conn->Start();
  io_service.run();
}

TEST(RpcEngineTest, TestTimeout) {
  ::asio::io_service io_service;
  Options options;
  options.rpc_timeout = 1;
  RpcEngine engine(&io_service, options, "foo", "protocol", 1);
  RpcConnectionImpl<MockRPCConnection> *conn =
      new RpcConnectionImpl<MockRPCConnection>(&engine);

  EXPECT_CALL(conn->next_layer(), Produce()).Times(0);

  std::unique_ptr<RpcConnection> conn_ptr(conn);
  engine.TEST_SetRpcConnection(&conn_ptr);

  EchoRequestProto req;
  req.set_message("foo");
  std::shared_ptr<EchoResponseProto> resp(new EchoResponseProto());
  engine.AsyncRpc("test", &req, resp, [resp, &io_service](const Status &stat) {
    io_service.stop();
    ASSERT_FALSE(stat.ok());
  });

  ::asio::deadline_timer timer(io_service);
  timer.expires_from_now(std::chrono::milliseconds(options.rpc_timeout * 2));
  timer.async_wait(std::bind(&RpcConnection::Start, conn));
  io_service.run();
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
