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
      async_read_some,
      void(const asio::mutable_buffers_1 &,
           const std::function<void(const Status &, size_t transferred)> &));

  MOCK_METHOD6(async_connect,
               void(const std::string &, TokenProto *, ExtendedBlockProto *,
                    uint64_t, uint64_t,
                    const std::function<void(const Status &)> &));
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
  CreatePipeline(::asio::io_service *, const DatanodeInfoProto &) {
    auto m = continuation::Pipeline<State>::Create();
    *m->state().transferred() = 0;
    Trait::InitializeMockReader(m->state().reader());
    return m;
  }
};
}

TEST(InputStreamTest, TestReadSingleTrunk) {
  LocatedBlocksProto blocks;
  LocatedBlockProto block;
  DatanodeInfoProto dn;
  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  Options options;
  FileSystemImpl fs(&io_service, options);
  InputStreamImpl is(&fs, &blocks);
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_connect(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_some(_, _))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf)));
    }
  };

  is.AsyncReadBlock<MockBlockReaderTrait<Trait>>(
      "client", block, dn, 0, asio::buffer(buf, sizeof(buf)),
      [&stat, &read](const Status &status, const std::string &, size_t transferred) {
        stat = status;
        read = transferred;
      });
  ASSERT_TRUE(stat.ok());
  ASSERT_EQ(sizeof(buf), read);
  read = 0;
}

TEST(InputStreamTest, TestReadMultipleTrunk) {
  LocatedBlocksProto blocks;
  LocatedBlockProto block;
  DatanodeInfoProto dn;
  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  Options options;
  FileSystemImpl fs(&io_service, options);
  InputStreamImpl is(&fs, &blocks);
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_connect(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_some(_, _))
          .Times(4)
          .WillRepeatedly(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4));
    }
  };

  is.AsyncReadBlock<MockBlockReaderTrait<Trait>>(
      "client", block, dn, 0, asio::buffer(buf, sizeof(buf)),
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
  LocatedBlocksProto blocks;
  LocatedBlockProto block;
  DatanodeInfoProto dn;
  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  Options options;
  FileSystemImpl fs(&io_service, options);
  InputStreamImpl is(&fs, &blocks);
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_connect(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_some(_, _))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf) / 4))
          .WillOnce(InvokeArgument<1>(Status::Error("error"), 0));
    }
  };

  is.AsyncReadBlock<MockBlockReaderTrait<Trait>>(
      "client", block, dn, 0, asio::buffer(buf, sizeof(buf)),
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
  LocatedBlocksProto blocks;
  LocatedBlockProto *block = blocks.add_blocks();
  ExtendedBlockProto *b = block->mutable_b();
  b->set_poolid("");
  b->set_blockid(1);
  b->set_generationstamp(1);
  b->set_numbytes(4096);

  DatanodeInfoProto *di = block->add_locs();
  DatanodeIDProto *dnid = di->mutable_id();
  dnid->set_datanodeuuid("foo");

  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  Options options;
  FileSystemImpl fs(&io_service, options);
  InputStreamImpl is(&fs, &blocks);
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_connect(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_some(_, _))
          .WillOnce(InvokeArgument<1>(Status::OK(), sizeof(buf)));
    }
  };


  std::set<std::string> excluded_dn({"foo"});
  is.AsyncPreadSome(0, asio::buffer(buf, sizeof(buf)), excluded_dn,
      [&stat, &read](const Status &status, const std::string &, size_t transferred) {
        stat = status;
        read = transferred;
      });
  ASSERT_EQ(static_cast<int>(std::errc::resource_unavailable_try_again), stat.code());
  ASSERT_EQ(0UL, read);
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
