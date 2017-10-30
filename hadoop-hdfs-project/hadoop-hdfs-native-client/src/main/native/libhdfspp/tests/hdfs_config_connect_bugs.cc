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

#include <google/protobuf/stubs/common.h>

#include <cstring>
#include <chrono>
#include <exception>


static const char *hdfs_11294_core_site_txt =
"<configuration>\n"
"  <property name=\"fs.defaultFS\" value=\"hdfs://NAMESERVICE1\"/>\n"
"  <property name=\"hadoop.security.authentication\" value=\"simple\"/>\n"
"  <property name=\"ipc.client.connect.retry.interval\" value=\"1\">\n"
"</configuration>\n";

static const char *hdfs_11294_hdfs_site_txt =
"<configuration>\n"
"  <property>\n"
"    <name>dfs.nameservices</name>\n"
"    <value>NAMESERVICE1</value>\n"
"  </property>\n"
"  <property>\n"
"    <name>dfs.ha.namenodes.NAMESERVICE1</name>\n"
"    <value>nn1, nn2</value>\n"
"  </property>\n"
"  <property>\n"
"    <name>dfs.namenode.rpc-address.NAMESERVICE1.nn1</name>\n"
"    <value>nonesuch1.apache.org:8020</value>\n"
"  </property>\n"
"  <property>\n"
"    <name>dfs.namenode.servicerpc-address.NAMESERVICE1.nn1</name>\n"
"    <value>nonesuch1.apache.org:8040</value>\n"
"  </property>\n"
"  <property>\n"
"    <name>dfs.namenode.http-address.NAMESERVICE1.nn1</name>\n"
"    <value>nonesuch1.apache.org:50070</value>\n"
"  </property>\n"
"  <property>\n"
"    <name>dfs.namenode.rpc-address.NAMESERVICE1.nn2</name>\n"
"    <value>nonesuch2.apache.org:8020</value>\n"
"  </property>\n"
"  <property>\n"
"    <name>dfs.namenode.servicerpc-address.NAMESERVICE1.nn2</name>\n"
"    <value>nonesuch2.apache.org:8040</value>\n"
"  </property>\n"
"  <property>\n"
"    <name>dfs.namenode.http-address.NAMESERVICE1.nn2</name>\n"
"    <value>nonesuch2.apache.org:50070</value>\n"
"  </property>\n"
"</configuration>\n";




namespace hdfs {

// Make sure we can set up a mini-cluster and connect to it
TEST(ConfigConnectBugs, Test_HDFS_11294) {
  // Directory for hdfs config
  TempDir td;

  const std::string& tempDirPath = td.path;
  const std::string coreSitePath = tempDirPath + "/core-site.xml";
  const std::string hdfsSitePath = tempDirPath + "/hdfs-site.xml";

  // Write configs
  FILE *coreSite = fopen(coreSitePath.c_str(), "w");
  EXPECT_NE(coreSite, nullptr);
  int coreSiteLength = strlen(hdfs_11294_core_site_txt);
  size_t res = fwrite(hdfs_11294_core_site_txt, 1, coreSiteLength, coreSite);
  EXPECT_EQ(res, coreSiteLength);
  EXPECT_EQ(fclose(coreSite), 0);

  FILE *hdfsSite = fopen(hdfsSitePath.c_str(), "w");
  EXPECT_NE(hdfsSite, nullptr);
  int hdfsSiteLength = strlen(hdfs_11294_hdfs_site_txt);
  res = fwrite(hdfs_11294_hdfs_site_txt, 1, hdfsSiteLength, hdfsSite);
  EXPECT_EQ(res, hdfsSiteLength);
  EXPECT_EQ(fclose(hdfsSite), 0);

  // Load configs with new FS
  hdfsBuilder *bld = hdfsNewBuilderFromDirectory(tempDirPath.c_str());
  hdfsBuilderSetNameNode(bld, "NAMESERVICE1");

  // In HDFS-11294 connecting would crash because DNS couldn't resolve
  // endpoints but the RpcEngine would attempt to dereference a non existant
  // element in a std::vector and crash.  Test passes if connect doesn't crash.
  hdfsFS fileSystem = hdfsBuilderConnect(bld);

  // FS shouldn't be created if it can't connect.
  EXPECT_EQ(fileSystem, nullptr);

  // Verify it got to endpoint check
  char errMsgBuf[100];
  memset(errMsgBuf, 0, 100);
  EXPECT_EQ( hdfsGetLastError(errMsgBuf, 100), 0);
  EXPECT_STREQ(errMsgBuf, "Exception:No endpoints found for namenode");


  // remove config files
  EXPECT_EQ(remove(coreSitePath.c_str()), 0);
  EXPECT_EQ(remove(hdfsSitePath.c_str()), 0);
}

} // end namespace hdfs

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();

  return exit_code;
}
