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


#include <hdfs/hdfs.h>
#include <hdfspp/hdfs_ext.h>

#include <google/protobuf/io/coded_stream.h>
#include <gmock/gmock.h>

#include <string.h>
#include <string>

using ::testing::_;
using ::testing::InvokeArgument;
using ::testing::Return;

/* Don't need a real minidfs cluster since this just passes invalid params. */

TEST(HdfsppErrors, NullFileSystem) {

  char buf[4096];

  hdfsFS fs = nullptr;
  hdfsFile fd = reinterpret_cast<hdfsFile>(1);

  tSize res = hdfsRead(fs, fd, buf, 4096);
  ASSERT_EQ(res, -1);

  hdfsGetLastError(buf, 4096);

  ASSERT_EQ(std::string(buf), "Cannot perform FS operations with null FS handle.");
}

TEST(HdfsppErrors, NullFileHandle) {
  char buf[4096];

  hdfsFS fs = reinterpret_cast<hdfsFS>(1);
  hdfsFile fd = nullptr;

  tSize res = hdfsRead(fs, fd, buf, 4096);
  ASSERT_EQ(res, -1);

  hdfsGetLastError(buf, 4096);

  ASSERT_EQ(std::string(buf), "Cannot perform FS operations with null File handle.");
}

TEST(HdfsppErrors, ZeroLength) {
  char buf[1];
  buf[0] = 0;

  hdfsFS fs = reinterpret_cast<hdfsFS>(1);
  hdfsFile fd = nullptr;

  tSize res = hdfsRead(fs, fd, buf, 1);
  ASSERT_EQ(res, -1);

  hdfsGetLastError(buf, 0);

  ASSERT_EQ(std::string(buf), "");
}

TEST(HdfsppErrors, NegativeLength) {
  char buf[1];
  buf[0] = 0;

  hdfsFS fs = reinterpret_cast<hdfsFS>(1);
  hdfsFile fd = nullptr;

  tSize res = hdfsRead(fs, fd, buf, 1);
  ASSERT_EQ(res, -1);

  hdfsGetLastError(buf, -1);

  ASSERT_EQ(std::string(buf), "");
}

TEST(HdfsppErrors, MessageTruncation) {
  char buf[4096];

  hdfsFS fs = reinterpret_cast<hdfsFS>(1);
  hdfsFile fd = nullptr;

  tSize res = hdfsRead(fs, fd, buf, 4096);
  ASSERT_EQ(res, -1);

  hdfsGetLastError(buf, 10);

  ASSERT_EQ(std::string(buf), "Cannot pe");
}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();

  return exit_code;
}
