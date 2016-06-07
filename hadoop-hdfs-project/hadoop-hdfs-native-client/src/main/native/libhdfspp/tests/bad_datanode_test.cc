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
#include "common/libhdfs_events_impl.h"

#include "common/util.h"

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

class MockReader : public BlockReader {
public:
  MOCK_METHOD2(
      AsyncReadPacket,
      void(const asio::mutable_buffers_1 &,
           const std::function<void(const Status &, size_t transferred)> &));

  MOCK_METHOD5(AsyncRequestBlock,
               void(const std::string &client_name,
                     const hadoop::hdfs::ExtendedBlockProto *block,
                     uint64_t length, uint64_t offset,
                     const std::function<void(Status)> &handler));

  MOCK_METHOD5(AsyncReadBlock, void(
    const std::string & client_name,
    const hadoop::hdfs::LocatedBlockProto &block,
    size_t offset,
    const MutableBuffers &buffers,
    const std::function<void(const Status &, size_t)> handler));

  virtual void CancelOperation() override {
    /* no-op, declared pure virtual */
  }
};

class MockDNConnection : public DataNodeConnection, public std::enable_shared_from_this<MockDNConnection> {
    void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) override {
      handler(Status::OK(), shared_from_this());
    }

  void async_read_some(const MutableBuffers &buf,
        std::function<void (const asio::error_code & error,
                               std::size_t bytes_transferred) > handler) override {
      (void)buf;
      handler(asio::error::fault, 0);
  }

  void async_write_some(const ConstBuffers &buf,
            std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > handler) override {
      (void)buf;
      handler(asio::error::fault, 0);
  }

  virtual void Cancel() override {
    /* no-op, declared pure virtual */
  }
};


class PartialMockFileHandle : public FileHandleImpl {
  using FileHandleImpl::FileHandleImpl;
public:
  std::shared_ptr<MockReader> mock_reader_ = std::make_shared<MockReader>();
protected:
  std::shared_ptr<BlockReader> CreateBlockReader(const BlockReaderOptions &options,
                                                 std::shared_ptr<DataNodeConnection> dn) override
  {
    (void) options; (void) dn;
    assert(mock_reader_);
    return mock_reader_;
  }
  std::shared_ptr<DataNodeConnection> CreateDataNodeConnection(
      ::asio::io_service *io_service,
      const ::hadoop::hdfs::DatanodeInfoProto & dn,
      const hadoop::common::TokenProto * token) override {
    (void) io_service; (void) dn; (void) token;
    return std::make_shared<MockDNConnection>();
  }


};

TEST(BadDataNodeTest, TestNoNodes) {
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
  auto bad_node_tracker = std::make_shared<BadDataNodeTracker>();
  auto monitors = std::make_shared<LibhdfsEvents>();
  bad_node_tracker->AddBadNode("foo");

  PartialMockFileHandle is("cluster", "file", &io_service.io_service(), GetRandomClientName(), file_info, bad_node_tracker, monitors);
  Status stat;
  size_t read = 0;

  // Exclude the one datanode with the data
  is.AsyncPreadSome(0, asio::buffer(buf, sizeof(buf)), nullptr,
      [&stat, &read](const Status &status, const std::string &, size_t transferred) {
        stat = status;
        read = transferred;
      });

  // Should fail with no resource available
  ASSERT_EQ(static_cast<int>(std::errc::resource_unavailable_try_again), stat.code());
  ASSERT_EQ(0UL, read);
}

TEST(BadDataNodeTest, NNEventCallback) {
  auto file_info = std::make_shared<struct FileInfo>();
  file_info->blocks_.push_back(LocatedBlockProto());
  LocatedBlockProto & block = file_info->blocks_[0];
  ExtendedBlockProto *b = block.mutable_b();
  b->set_poolid("");
  b->set_blockid(1);
  b->set_generationstamp(1);
  b->set_numbytes(4096);

  // Set up the one block to have one datanodes holding it
  DatanodeInfoProto *di = block.add_locs();
  DatanodeIDProto *dnid = di->mutable_id();
  dnid->set_datanodeuuid("dn1");

  char buf[4096] = {
      0,
  };
  IoServiceImpl io_service;
  auto tracker = std::make_shared<BadDataNodeTracker>();


  // Set up event callbacks
  int calls = 0;
  std::vector<std::string> callbacks;
  auto monitors = std::make_shared<LibhdfsEvents>();
  monitors->set_file_callback([&calls, &callbacks] (const char * event,
                    const char * cluster,
                    const char * file,
                    int64_t value) {
    (void)cluster; (void) file; (void)value;
    callbacks.push_back(event);

    // Allow connect call to succeed by fail on read
    if (calls++ == 1)
      return event_response::test_err(Status::Error("Test"));

    return event_response::ok();
  });
  PartialMockFileHandle is("cluster", "file", &io_service.io_service(), GetRandomClientName(),  file_info, tracker, monitors);
  Status stat;
  size_t read = 0;

  EXPECT_CALL(*is.mock_reader_, AsyncReadBlock(_,_,_,_,_))
      // Will return OK, but our callback will subvert it
      .WillOnce(InvokeArgument<4>(
          Status::OK(), 0));

  is.AsyncPreadSome(
      0, asio::buffer(buf, sizeof(buf)), nullptr,
      [&stat, &read](const Status &status, const std::string &,
                     size_t transferred) {
        stat = status;
        read = transferred;
      });

  ASSERT_FALSE(stat.ok());
  ASSERT_EQ(2, callbacks.size());
  ASSERT_EQ(FILE_DN_CONNECT_EVENT, callbacks[0]);
  ASSERT_EQ(FILE_DN_READ_EVENT, callbacks[1]);
}


TEST(BadDataNodeTest, RecoverableError) {
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
  auto tracker = std::make_shared<BadDataNodeTracker>();
  auto monitors = std::make_shared<LibhdfsEvents>();
  PartialMockFileHandle is("cluster", "file", &io_service.io_service(), GetRandomClientName(),  file_info, tracker, monitors);
  Status stat;
  size_t read = 0;
  EXPECT_CALL(*is.mock_reader_, AsyncReadBlock(_,_,_,_,_))
      // resource unavailable error
      .WillOnce(InvokeArgument<4>(
          Status::ResourceUnavailable("Unable to get some resource, try again later"), 0));


  is.AsyncPreadSome(
      0, asio::buffer(buf, sizeof(buf)), nullptr,
      [&stat, &read](const Status &status, const std::string &,
                     size_t transferred) {
        stat = status;
        read = transferred;
      });

  ASSERT_FALSE(stat.ok());

  std::string failing_dn = "id_of_bad_datanode";
  if (!stat.ok()) {
    if (FileHandle::ShouldExclude(stat)) {
      tracker->AddBadNode(failing_dn);
    }
  }

  ASSERT_FALSE(tracker->IsBadNode(failing_dn));
}

TEST(BadDataNodeTest, InternalError) {
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
  auto tracker = std::make_shared<BadDataNodeTracker>();
  auto monitors = std::make_shared<LibhdfsEvents>();
  PartialMockFileHandle is("cluster", "file", &io_service.io_service(), GetRandomClientName(),  file_info, tracker, monitors);
  Status stat;
  size_t read = 0;
  EXPECT_CALL(*is.mock_reader_, AsyncReadBlock(_,_,_,_,_))
      // resource unavailable error
      .WillOnce(InvokeArgument<4>(
              Status::Exception("server_explosion_exception",
                                "the server exploded"),
                                sizeof(buf)));

  is.AsyncPreadSome(
      0, asio::buffer(buf, sizeof(buf)), nullptr,
      [&stat, &read](const Status &status, const std::string &,
                     size_t transferred) {
        stat = status;
        read = transferred;
      });

  ASSERT_FALSE(stat.ok());

  std::string failing_dn = "id_of_bad_datanode";
  if (!stat.ok()) {
    if (FileHandle::ShouldExclude(stat)) {
      tracker->AddBadNode(failing_dn);
    }
  }

  ASSERT_TRUE(tracker->IsBadNode(failing_dn));
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
