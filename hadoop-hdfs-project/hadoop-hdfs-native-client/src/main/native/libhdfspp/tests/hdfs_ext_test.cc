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


}


int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();

  return exit_code;
}
