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
#include "fs/bad_datanode_tracker.h"

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

template <class Trait>
struct MockBlockReaderTrait {
  typedef MockReader Reader;
  struct State {
    MockReader reader_;
    size_t transferred_;
    Reader *reader() { return &reader_; }
    size_t *transferred() { return &transferred_; }
    const size_t *transferred() const { return &transferred_; }
  };

  static continuation::Pipeline<State> *CreatePipeline(
      ::asio::io_service *, const DatanodeInfoProto &) {
    auto m = continuation::Pipeline<State>::Create();
    *m->state().transferred() = 0;
    Trait::InitializeMockReader(m->state().reader());
    return m;
  }
};

TEST(BadDataNodeTest, RecoverableError) {
  LocatedBlocksProto blocks;
  LocatedBlockProto block;
  DatanodeInfoProto dn;
  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  Options default_options;
  FileSystemImpl fs(&io_service, default_options);
  auto tracker = std::make_shared<BadDataNodeTracker>();
  InputStreamImpl is(&fs, &blocks, tracker);
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_connect(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_some(_, _))
          // resource unavailable error
          .WillOnce(InvokeArgument<1>(
              Status::ResourceUnavailable(
                  "Unable to get some resource, try again later"),
              sizeof(buf)));
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

  std::string failing_dn = "id_of_bad_datanode";
  if (!stat.ok()) {
    if (InputStream::ShouldExclude(stat)) {
      tracker->AddBadNode(failing_dn);
    }
  }

  ASSERT_FALSE(tracker->IsBadNode(failing_dn));
}

TEST(BadDataNodeTest, InternalError) {
  LocatedBlocksProto blocks;
  LocatedBlockProto block;
  DatanodeInfoProto dn;
  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  Options default_options;
  auto tracker = std::make_shared<BadDataNodeTracker>();
  FileSystemImpl fs(&io_service, default_options);
  InputStreamImpl is(&fs, &blocks, tracker);
  Status stat;
  size_t read = 0;
  struct Trait {
    static void InitializeMockReader(MockReader *reader) {
      EXPECT_CALL(*reader, async_connect(_, _, _, _, _, _))
          .WillOnce(InvokeArgument<5>(Status::OK()));

      EXPECT_CALL(*reader, async_read_some(_, _))
          // something bad happened on the DN, calling again isn't going to help
          .WillOnce(
              InvokeArgument<1>(Status::Exception("server_explosion_exception",
                                                  "the server exploded"),
                                sizeof(buf)));
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

  std::string failing_dn = "id_of_bad_datanode";
  if (!stat.ok()) {
    if (InputStream::ShouldExclude(stat)) {
      tracker->AddBadNode(failing_dn);
    }
  }

  ASSERT_TRUE(tracker->IsBadNode(failing_dn));
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
