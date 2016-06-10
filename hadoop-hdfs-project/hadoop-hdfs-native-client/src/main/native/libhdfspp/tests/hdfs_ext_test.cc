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

//#include "expect.h"

#include "hdfspp_mini_dfs.h"
#include "hdfspp/hdfs_ext.h"
#include <chrono>

namespace hdfs {

class HdfsExtTest: public ::testing::Test {
public:
  MiniCluster cluster;
};

// Make sure we can set up a mini-cluster and connect to it
TEST_F(HdfsExtTest, TestGetBlockLocations) {
  HdfsHandle connection = cluster.connect_c();
  EXPECT_NE(nullptr, connection.handle());

  hdfsBlockLocations * blocks = nullptr;

  // Free a null pointer
  int result = hdfsFreeBlockLocations(blocks);
  EXPECT_EQ(0, result);

  // Test non-extant files
  result = hdfsGetBlockLocations(connection, "non_extant_file", &blocks);
  EXPECT_NE(0, result);  // Should be an error

  // Test an extant file
  std::string filename = connection.newFile(1024);
  result = hdfsGetBlockLocations(connection, filename.c_str(), &blocks);
  EXPECT_EQ(0, result);

  EXPECT_EQ(1024, blocks->fileLength);
  EXPECT_EQ(1, blocks->num_blocks);
  EXPECT_EQ(0, blocks->isUnderConstruction);
  EXPECT_NE(0, blocks->isLastBlockComplete);
  EXPECT_EQ(1024, blocks->blocks->num_bytes);
  EXPECT_EQ(0, blocks->blocks->start_offset);
  EXPECT_EQ(1, blocks->blocks->num_locations);
  EXPECT_NE(nullptr, blocks->blocks->locations->hostname);
  EXPECT_NE(nullptr, blocks->blocks->locations->ip_address);
  EXPECT_NE(0, blocks->blocks->locations->xfer_port);

  result = hdfsFreeBlockLocations(blocks);
  EXPECT_EQ(0, result);

}


// Writing a file to the filesystem and checking the used space
TEST_F(HdfsExtTest, TestGetUsed) {
  using namespace std::chrono;

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  // File system's used space before writing
  tOffset used_before_write;
  EXPECT_GE(used_before_write = hdfsGetUsed(fs), 0);

  // Write to a file
  tOffset fileSize = 1024;
  std::string filename = connection.newFile(fileSize);

  //Need to run hdfsGetUsed() in a loop until the refreshInterval
  //is passed on the filesystem and the used space is updated
  //Time-out is 3 minutes
  tOffset used_after_write;
  tOffset difference;
  minutes beginTime = duration_cast<minutes>(
      system_clock::now().time_since_epoch());
  minutes currentTime;
  do{
    EXPECT_GE(used_after_write = hdfsGetUsed(fs), 0);
    difference = used_after_write - used_before_write;
    currentTime = duration_cast<minutes>(
          system_clock::now().time_since_epoch());
  } while (difference == 0 && currentTime.count() - beginTime.count() < 3);

  //There should be at least fileSize bytes added to the used space
  EXPECT_GT(difference, fileSize);
  //There could be additional metadata added to the used space,
  //but no more than double the fileSize
  EXPECT_LT(difference, fileSize * 2);

}


//Testing allow, disallow, create, and delete snapshot
TEST_F(HdfsExtTest, TestSnapshotOperations) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //argument 'path' is NULL
  EXPECT_EQ(-1, hdfsAllowSnapshot(fs, nullptr));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, nullptr, "Bad"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, nullptr, "Bad"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsDisallowSnapshot(fs, nullptr));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //argument 'name' is NULL for deletion
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, "/dir/", nullptr));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //Path not found
  std::string path = "/wrong/dir/";
  EXPECT_EQ(-1, hdfsAllowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsDisallowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);

  //Not a directory
  path = connection.newFile(1024); //1024 byte file
  EXPECT_EQ(-1, hdfsAllowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);
  EXPECT_EQ(-1, hdfsDisallowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);

  //Not snapshottable directory
  std::string dirName = connection.newDir();
  EXPECT_EQ(0, hdfsDisallowSnapshot(fs, dirName.c_str()));
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, dirName.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //Verify snapshot created
  EXPECT_EQ(0, hdfsAllowSnapshot(fs, dirName.c_str()));
  EXPECT_EQ(0, hdfsCreateSnapshot(fs, dirName.c_str(), "Good"));
  std::string snapDir = dirName + ".snapshot/";
  int size;
  hdfsFileInfo *file_infos;
  EXPECT_NE(nullptr, file_infos = hdfsListDirectory(fs, snapDir.c_str(), &size));
  EXPECT_EQ(1, size);
  EXPECT_STREQ("Good", file_infos[0].mName);
  hdfsFreeFileInfo(file_infos, 1);

  //Verify snapshot deleted
  EXPECT_EQ(0, hdfsDeleteSnapshot(fs, dirName.c_str(), "Good"));
  EXPECT_EQ(nullptr, file_infos = hdfsListDirectory(fs, snapDir.c_str(), &size));
  EXPECT_EQ(0, size);
  hdfsFreeFileInfo(file_infos, 0);
}


}


int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();

  return exit_code;
}
