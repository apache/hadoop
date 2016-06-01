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

#include "hdfspp_mini_dfs.h"

namespace hdfs {

class HdfsMiniDfsSmokeTest: public ::testing::Test {
public:
  MiniCluster cluster;
};

// Make sure we can set up a mini-cluster and connect to it
TEST_F(HdfsMiniDfsSmokeTest, SmokeTest) {
  FSHandle handle = cluster.connect();
  EXPECT_NE(nullptr, handle.handle());

  HdfsHandle connection = cluster.connect_c();
  EXPECT_NE(nullptr, connection.handle());
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
