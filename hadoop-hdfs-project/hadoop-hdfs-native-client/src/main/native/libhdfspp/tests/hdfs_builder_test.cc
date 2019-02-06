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

#include "hdfspp/hdfs_ext.h"
#include "configuration_test.h"
#include <gmock/gmock.h>
#include <google/protobuf/stubs/common.h>

using ::testing::_;

using namespace hdfs;

TEST(HdfsBuilderTest, TestStubBuilder) {
  {
    TempDir tempDir1;

    hdfsBuilder * builder = hdfsNewBuilderFromDirectory(tempDir1.path.c_str());
    hdfsFreeBuilder(builder);
  }

  {
    hdfsBuilder * builder = hdfsNewBuilderFromDirectory("/this/path/does/not/exist");
    hdfsFreeBuilder(builder);
  }
}

TEST(HdfsBuilderTest, TestRead)
{
  // Reading string values
  {
    TempDir tempDir1;
    TempFile tempFile1(tempDir1.path + "/core-site.xml");
    writeSimpleConfig(tempFile1.filename, "key1", "value1");

    hdfsBuilder * builder = hdfsNewBuilderFromDirectory(tempDir1.path.c_str());

    char * readVal = nullptr;
    int result = hdfsBuilderConfGetStr(builder, "key1", &readVal);
    ASSERT_EQ(0, result);
    ASSERT_NE(nullptr, readVal);
    EXPECT_EQ("value1", std::string(readVal));
    hdfsConfStrFree(readVal);

    readVal = nullptr;
    result = hdfsBuilderConfGetStr(builder, "key2", &readVal);
    ASSERT_EQ(0, result);
    EXPECT_EQ(nullptr, readVal);

    hdfsFreeBuilder(builder);
  }

  // Reading int values
  {
    TempDir tempDir1;
    TempFile tempFile1(tempDir1.path + "/core-site.xml");
    writeSimpleConfig(tempFile1.filename, "key1", "100");

    hdfsBuilder * builder = hdfsNewBuilderFromDirectory(tempDir1.path.c_str());

    int readVal = -1;
    int result = hdfsBuilderConfGetInt(builder, "key1", &readVal);
    EXPECT_EQ(0, result);
    EXPECT_EQ(100, readVal);

    readVal = -1;
    result = hdfsBuilderConfGetInt(builder, "key2", &readVal);
    EXPECT_EQ(0, result);
    EXPECT_EQ(-1, readVal);

    hdfsFreeBuilder(builder);
  }
}

TEST(HdfsBuilderTest, TestSet)
{
  {
    // Setting values in an empty builder
    // Don't use default, or it will load any data in /etc/hadoop
    hdfsBuilder * builder = hdfsNewBuilderFromDirectory("/this/path/does/not/exist");

    int result = hdfsBuilderConfSetStr(builder, "key1", "100");
    EXPECT_EQ(0, result);

    int readVal = -1;
    result = hdfsBuilderConfGetInt(builder, "key1", &readVal);
    EXPECT_EQ(0, result);
    EXPECT_EQ(100, readVal);

    // Set value in non-empty builder
    result = hdfsBuilderConfSetStr(builder, "key2", "200");
    EXPECT_EQ(0, result);

    readVal = -1;
    result = hdfsBuilderConfGetInt(builder, "key2", &readVal);
    EXPECT_EQ(0, result);
    EXPECT_EQ(200, readVal);

    // Overwrite value
    result = hdfsBuilderConfSetStr(builder, "key2", "300");
    EXPECT_EQ(0, result);

    readVal = -1;
    result = hdfsBuilderConfGetInt(builder, "key2", &readVal);
    EXPECT_EQ(0, result);
    EXPECT_EQ(300, readVal);

    hdfsFreeBuilder(builder);
  }
}

int main(int argc, char *argv[]) {
  /*
   *  The following line must be executed to initialize Google Mock
   * (and Google Test) before running the tests.
   */
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code = RUN_ALL_TESTS();

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return exit_code;
}
