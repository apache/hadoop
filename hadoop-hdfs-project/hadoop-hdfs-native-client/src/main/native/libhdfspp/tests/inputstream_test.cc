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

#include "fs/filesystem.h"
#include <gmock/gmock.h>
#include <openssl/rand.h>

using hadoop::common::TokenProto;
using hadoop::hdfs::DatanodeInfoProto;
using hadoop::hdfs::DatanodeIDProto;
using hadoop::hdfs::ExtendedBlockProto;
using hadoop::hdfs::LocatedBlockProto;
using hadoop::hdfs::LocatedBlocksProto;

using ::testing::_;
using ::testing::InvokeArgument;
using ::testing::Return;

using namespace hdfs;

namespace hdfs {

class MockReader {
public:
  virtual ~MockReader() {}
  MOCK_METHOD2(
      async_read_packet,
      void(const asio::mutable_buffers_1 &,
           const std::function<void(const Status &, size_t transferred)> &));

  MOCK_METHOD6(async_request_block,
               void(const std::string &, TokenProto *, ExtendedBlockProto *,
                    uint64_t, uint64_t,
                    const std::function<void(const Status &)> &));
};

class MockDataNodeConnection : public DataNodeConnection, public std::enable_shared_from_this<MockDataNodeConnection> {
public:
  MockDataNodeConnection() {
    int id;
    RAND_pseudo_bytes((unsigned char *)&id, sizeof(id));

    std::stringstream ss;
    ss << "dn_" << id;
    uuid_ = ss.str();
  }

  MOCK_METHOD1(Connect, void(std::function<void(Status status, std::shared_ptr<DataNodeConnection>)>));
  MOCK_METHOD2(async_read, 
               void(const asio::mutable_buffers_1 & buffers,
                    std::function<void (const asio::error_code &,
                                        std::size_t) >));
  MOCK_METHOD3(async_read, 
               void(const asio::mutable_buffers_1 & buffers,
                    std::function<size_t(const asio::error_code &,
                                        std::size_t) >,
                    std::function<void (const asio::error_code &,
                                        std::size_t) >));
  MOCK_METHOD2(async_write, 
               void(const asio::const_buffers_1 & buffers,
                    std::function<void (const asio::error_code &,
                                        std::size_t) >));
  
  std::string uuid_;
};

template <class Trait> struct MockBlockReaderTrait {
  typedef MockReader Reader;
  struct State {
    MockReader reader_;
    size_t transferred_;
    Reader *reader() { return &reader_; }
    size_t *transferred() { return &transferred_; }
    const size_t *transferred() const { return &transferred_; }
  };

  static continuation::Pipeline<State> *
  CreatePipeline(std::shared_ptr<DataNodeConnection> dn) {
    (void) dn;
    auto m = continuation::Pipeline<State>::Create();
    *m->state().transferred() = 0;
    Trait::InitializeMockReader(m->state().reader());
    return m;
  }
};
}

TEST(InputStreamTest, TestReadSingleTrunk) {
  auto file_info = std::make_shared<struct FileInfo>();
  LocatedBlocksProto blocks;
  LocatedBlockProto block;
  char buf[4096] = {
      0,
  };

  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_request_block(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_packet(_, _))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf)));
    }
  };

  auto conn = std::make_shared<MockDataNodeConnection>();
  ReadOperation::AsyncReadBlock<MockBlockReaderTrait<Trait>>(
       conn, RpcEngine::GetRandomClientName(), block, 0, asio::buffer(buf, sizeof(buf)),
      [&stat, &read](const Status &status, const std::string &, size_t transferred) {
        stat = status;
        read = transferred;
      });
  ASSERT_TRUE(stat.ok());
  ASSERT_EQ(sizeof(buf), read);
  read = 0;
}

TEST(InputStreamTest, TestReadMultipleTrunk) {
  LocatedBlockProto block;
  char buf[4096] = {
      0,
  };
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_request_block(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_packet(_, _))
          .Times(4)
          .WillRepeatedly(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4));
    }
  };

  auto conn = std::make_shared<MockDataNodeConnection>();
  ReadOperation::AsyncReadBlock<MockBlockReaderTrait<Trait>>(
       conn, RpcEngine::GetRandomClientName(), block, 0, asio::buffer(buf, sizeof(buf)),
      [&stat, &read](const Status &status, const std::string &,
                     size_t transferred) {
        stat = status;
        read = transferred;
      });
  ASSERT_TRUE(stat.ok());
  ASSERT_EQ(sizeof(buf), read);
  read = 0;
}

TEST(InputStreamTest, TestReadError) {
  LocatedBlockProto block;
  char buf[4096] = {
      0,
  };
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_request_block(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_packet(_, _))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4))
          .WillOnce(InvokeArgument<1>(Status::Error("error"), 0));
    }
  };

  auto conn = std::make_shared<MockDataNodeConnection>();
  ReadOperation::AsyncReadBlock<MockBlockReaderTrait<Trait>>(
       conn, RpcEngine::GetRandomClientName(), block, 0, asio::buffer(buf, sizeof(buf)),
      [&stat, &read](const Status &status, const std::string &,
                     size_t transferred) {
        stat = status;
        read = transferred;
      });
  ASSERT_FALSE(stat.ok());
  ASSERT_EQ(sizeof(buf) / 4 * 3, read);
  read = 0;
}

TEST(InputStreamTest, TestExcludeDataNode) {
  auto file_info = std::make_shared<struct FileInfo>();
  file_info->blocks_.push_back(LocatedBlockProto());
  LocatedBlockProto & block = file_info->blocks_[0];
  ExtendedBlockProto *b = block.mutable_b();
  b->set_poolid("");
  b->set_blockid(1);
  b->set_generationstamp(1);
  b->set_numbytes(4096);

  // Set up the one block to have one datanode holding it
  DatanodeInfoProto *di = block.add_locs();
  DatanodeIDProto *dnid = di->mutable_id();
  dnid->set_datanodeuuid("foo");

  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  InputStreamImpl is(&io_service.io_service(), RpcEngine::GetRandomClientName(),  file_info);
  Status stat;
  size_t read = 0;

  // Exclude the one datanode with the data
  std::set<std::string> excluded_dn({"foo"});
  is.AsyncPreadSome(0, asio::buffer(buf, sizeof(buf)), excluded_dn,
      [&stat, &read](const Status &status, const std::string &, size_t transferred) {
        stat = status;
        read = transferred;
      });
      
  // Should fail with no resource available
  ASSERT_EQ(static_cast<int>(std::errc::resource_unavailable_try_again), stat.code());
  ASSERT_EQ(0UL, read);
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
