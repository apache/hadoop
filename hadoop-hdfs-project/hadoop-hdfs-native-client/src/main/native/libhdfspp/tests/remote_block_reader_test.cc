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

#include "datatransfer.pb.h"
#include "common/util.h"
#include "reader/block_reader.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace hdfs;

using ::hadoop::common::TokenProto;
using ::hadoop::hdfs::BlockOpResponseProto;
using ::hadoop::hdfs::ChecksumProto;
using ::hadoop::hdfs::DataTransferEncryptorMessageProto;
using ::hadoop::hdfs::ExtendedBlockProto;
using ::hadoop::hdfs::PacketHeaderProto;
using ::hadoop::hdfs::ReadOpChecksumInfoProto;

using ::asio::buffer;
using ::asio::error_code;
using ::asio::mutable_buffers_1;
using ::testing::Return;
using std::make_pair;
using std::string;

namespace pb = ::google::protobuf;
namespace pbio = pb::io;

namespace hdfs {

class MockDNConnection : public MockConnectionBase {
 public:
  MockDNConnection(::asio::io_service &io_service)
      : MockConnectionBase(&io_service) {}
  MOCK_METHOD0(Produce, ProducerResult());
};
}

static inline string ToDelimitedString(const pb::MessageLite *msg) {
  string res;
  res.reserve(hdfs::DelimitedPBMessageSize(msg));
  pbio::StringOutputStream os(&res);
  pbio::CodedOutputStream out(&os);
  out.WriteVarint32(msg->ByteSize());
  msg->SerializeToCodedStream(&out);
  return res;
}

static inline std::pair<error_code, string> Produce(const std::string &s) {
  return make_pair(error_code(), s);
}

static inline std::pair<error_code, string> ProducePacket(
    const std::string &data, const std::string &checksum, int offset_in_block,
    int seqno, bool last_packet) {
  PacketHeaderProto proto;
  proto.set_datalen(data.size());
  proto.set_offsetinblock(offset_in_block);
  proto.set_seqno(seqno);
  proto.set_lastpacketinblock(last_packet);

  char prefix[6];
  *reinterpret_cast<unsigned *>(prefix) =
      htonl(data.size() + checksum.size() + sizeof(int32_t));
  *reinterpret_cast<short *>(prefix + sizeof(int32_t)) =
      htons(proto.ByteSize());
  std::string payload(prefix, sizeof(prefix));
  payload.reserve(payload.size() + proto.ByteSize() + checksum.size() +
                  data.size());
  proto.AppendToString(&payload);
  payload += checksum;
  payload += data;
  return std::make_pair(error_code(), std::move(payload));
}

template <class Stream = MockDNConnection, class Handler>
static std::shared_ptr<RemoteBlockReader<Stream>> ReadContent(
    Stream *conn, TokenProto *token, const ExtendedBlockProto &block,
    uint64_t length, uint64_t offset, const mutable_buffers_1 &buf,
    const Handler &handler) {
  BlockReaderOptions options;
  auto reader = std::make_shared<RemoteBlockReader<Stream>>(options, conn);
  Status result;
  reader->async_connect("libhdfs++", token, &block, length, offset,
                        [buf, reader, handler](const Status &stat) {
                          if (!stat.ok()) {
                            handler(stat, 0);
                          } else {
                            reader->async_read_some(buf, handler);
                          }
                        });
  return reader;
}

TEST(RemoteBlockReaderTest, TestReadWholeBlock) {
  static const size_t kChunkSize = 512;
  static const string kChunkData(kChunkSize, 'a');
  ::asio::io_service io_service;
  MockDNConnection conn(io_service);
  BlockOpResponseProto block_op_resp;

  block_op_resp.set_status(::hadoop::hdfs::Status::SUCCESS);
  EXPECT_CALL(conn, Produce())
      .WillOnce(Return(Produce(ToDelimitedString(&block_op_resp))))
      .WillOnce(Return(ProducePacket(kChunkData, "", 0, 1, true)));

  ExtendedBlockProto block;
  block.set_poolid("foo");
  block.set_blockid(0);
  block.set_generationstamp(0);

  std::string data(kChunkSize, 0);
  ReadContent(&conn, nullptr, block, kChunkSize, 0,
              buffer(const_cast<char *>(data.c_str()), data.size()),
              [&data, &io_service](const Status &stat, size_t transferred) {
                ASSERT_TRUE(stat.ok());
                ASSERT_EQ(kChunkSize, transferred);
                ASSERT_EQ(kChunkData, data);
                io_service.stop();
              });
  io_service.run();
}

TEST(RemoteBlockReaderTest, TestReadWithinChunk) {
  static const size_t kChunkSize = 1024;
  static const size_t kLength = kChunkSize / 4 * 3;
  static const size_t kOffset = kChunkSize / 4;
  static const string kChunkData = string(kOffset, 'a') + string(kLength, 'b');

  ::asio::io_service io_service;
  MockDNConnection conn(io_service);
  BlockOpResponseProto block_op_resp;
  ReadOpChecksumInfoProto *checksum_info =
      block_op_resp.mutable_readopchecksuminfo();
  checksum_info->set_chunkoffset(0);
  ChecksumProto *checksum = checksum_info->mutable_checksum();
  checksum->set_type(::hadoop::hdfs::ChecksumTypeProto::CHECKSUM_NULL);
  checksum->set_bytesperchecksum(512);
  block_op_resp.set_status(::hadoop::hdfs::Status::SUCCESS);

  EXPECT_CALL(conn, Produce())
      .WillOnce(Return(Produce(ToDelimitedString(&block_op_resp))))
      .WillOnce(Return(ProducePacket(kChunkData, "", kOffset, 1, true)));

  ExtendedBlockProto block;
  block.set_poolid("foo");
  block.set_blockid(0);
  block.set_generationstamp(0);

  string data(kLength, 0);
  ReadContent(&conn, nullptr, block, data.size(), kOffset,
              buffer(const_cast<char *>(data.c_str()), data.size()),
              [&data, &io_service](const Status &stat, size_t transferred) {
                ASSERT_TRUE(stat.ok());
                ASSERT_EQ(kLength, transferred);
                ASSERT_EQ(kChunkData.substr(kOffset, kLength), data);
                io_service.stop();
              });
  io_service.run();
}

TEST(RemoteBlockReaderTest, TestReadMultiplePacket) {
  static const size_t kChunkSize = 1024;
  static const string kChunkData(kChunkSize, 'a');

  ::asio::io_service io_service;
  MockDNConnection conn(io_service);
  BlockOpResponseProto block_op_resp;
  block_op_resp.set_status(::hadoop::hdfs::Status::SUCCESS);

  EXPECT_CALL(conn, Produce())
      .WillOnce(Return(Produce(ToDelimitedString(&block_op_resp))))
      .WillOnce(Return(ProducePacket(kChunkData, "", 0, 1, false)))
      .WillOnce(Return(ProducePacket(kChunkData, "", kChunkSize, 2, true)));

  ExtendedBlockProto block;
  block.set_poolid("foo");
  block.set_blockid(0);
  block.set_generationstamp(0);

  string data(kChunkSize, 0);
  mutable_buffers_1 buf = buffer(const_cast<char *>(data.c_str()), data.size());
  BlockReaderOptions options;
  auto reader =
      std::make_shared<RemoteBlockReader<MockDNConnection>>(options, &conn);
  Status result;
  reader->async_connect(
      "libhdfs++", nullptr, &block, data.size(), 0,
      [buf, reader, &data, &io_service](const Status &stat) {
        ASSERT_TRUE(stat.ok());
        reader->async_read_some(
            buf, [buf, reader, &data, &io_service](const Status &stat,
                                                   size_t transferred) {
              ASSERT_TRUE(stat.ok());
              ASSERT_EQ(kChunkSize, transferred);
              ASSERT_EQ(kChunkData, data);
              data.clear();
              data.resize(kChunkSize);
              transferred = 0;
              reader->async_read_some(
                  buf,
                  [&data, &io_service](const Status &stat, size_t transferred) {
                    ASSERT_TRUE(stat.ok());
                    ASSERT_EQ(kChunkSize, transferred);
                    ASSERT_EQ(kChunkData, data);
                    io_service.stop();
                  });
            });
      });
  io_service.run();
}

TEST(RemoteBlockReaderTest, TestSaslConnection) {
  static const size_t kChunkSize = 512;
  static const string kChunkData(kChunkSize, 'a');
  static const string kAuthPayload =
      "realm=\"0\",nonce=\"+GAWc+O6yEAWpew/"
      "qKah8qh4QZLoOLCDcTtEKhlS\",qop=\"auth\","
      "charset=utf-8,algorithm=md5-sess";
  ::asio::io_service io_service;
  MockDNConnection conn(io_service);
  BlockOpResponseProto block_op_resp;
  block_op_resp.set_status(::hadoop::hdfs::Status::SUCCESS);

  DataTransferEncryptorMessageProto sasl_resp0, sasl_resp1;
  sasl_resp0.set_status(
      ::hadoop::hdfs::
          DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS);
  sasl_resp0.set_payload(kAuthPayload);
  sasl_resp1.set_status(
      ::hadoop::hdfs::
          DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS);

  EXPECT_CALL(conn, Produce())
      .WillOnce(Return(Produce(ToDelimitedString(&sasl_resp0))))
      .WillOnce(Return(Produce(ToDelimitedString(&sasl_resp1))))
      .WillOnce(Return(Produce(ToDelimitedString(&block_op_resp))))
      .WillOnce(Return(ProducePacket(kChunkData, "", 0, 1, true)));

  DataTransferSaslStream<MockDNConnection> sasl_conn(&conn, "foo", "bar");
  ExtendedBlockProto block;
  block.set_poolid("foo");
  block.set_blockid(0);
  block.set_generationstamp(0);

  std::string data(kChunkSize, 0);
  sasl_conn.Handshake([&sasl_conn, &block, &data, &io_service](
      const Status &s) {
    ASSERT_TRUE(s.ok());
    ReadContent(&sasl_conn, nullptr, block, kChunkSize, 0,
                buffer(const_cast<char *>(data.c_str()), data.size()),
                [&data, &io_service](const Status &stat, size_t transferred) {
                  ASSERT_TRUE(stat.ok());
                  ASSERT_EQ(kChunkSize, transferred);
                  ASSERT_EQ(kChunkData, data);
                  io_service.stop();
                });
  });
  io_service.run();
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
