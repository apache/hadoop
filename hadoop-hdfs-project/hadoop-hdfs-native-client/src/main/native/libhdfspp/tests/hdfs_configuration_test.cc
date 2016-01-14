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

#include "common/hdfs_configuration.h"
#include "configuration_test.h"
#include <gmock/gmock.h>

using ::testing::_;

using namespace hdfs;

namespace hdfs
{
TEST(HdfsConfigurationTest, TestDefaultOptions)
{
  // Completely empty stream
  {
    HdfsConfiguration empty_config = ConfigurationLoader().New<HdfsConfiguration>();
    Options options = empty_config.GetOptions();
    EXPECT_EQ(Options::kDefaultRpcTimeout, options.rpc_timeout);
  }
}

TEST(HdfsConfigurationTest, TestSetOptions)
{
  // Completely empty stream
  {
    std::stringstream stream;
    simpleConfigStream(stream, HdfsConfiguration::kDfsClientSocketTimeoutKey, 100,
                               HdfsConfiguration::kIpcClientConnectMaxRetriesKey, 101,
                               HdfsConfiguration::kIpcClientConnectRetryIntervalKey, 102);

    optional<HdfsConfiguration> config = ConfigurationLoader().Load<HdfsConfiguration>(stream.str());
    EXPECT_TRUE(config && "Read stream");
    Options options = config->GetOptions();

    EXPECT_EQ(100, options.rpc_timeout);
    EXPECT_EQ(101, options.max_rpc_retries);
    EXPECT_EQ(102, options.rpc_retry_delay_ms);
  }
}

TEST(HdfsConfigurationTest, TestDefaultConfigs) {
  // Search path
  {
    TempDir tempDir;
    TempFile coreSite(tempDir.path + "/core-site.xml");
    writeSimpleConfig(coreSite.filename, "key1", "value1");
    TempFile hdfsSite(tempDir.path + "/hdfs-site.xml");
    writeSimpleConfig(hdfsSite.filename, "key2", "value2");

    ConfigurationLoader loader;
    loader.SetSearchPath(tempDir.path);

    optional<HdfsConfiguration> config = loader.LoadDefaultResources<HdfsConfiguration>();
    EXPECT_TRUE(config && "Parse streams");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config->GetWithDefault("key2", ""));
  }

  // Only core-site.xml available
  {
    TempDir tempDir;
    TempFile coreSite(tempDir.path + "/core-site.xml");
    writeSimpleConfig(coreSite.filename, "key1", "value1");

    ConfigurationLoader loader;
    loader.SetSearchPath(tempDir.path);

    optional<HdfsConfiguration> config = loader.LoadDefaultResources<HdfsConfiguration>();
    EXPECT_TRUE(config && "Parse streams");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }

  // Only hdfs-site available
  {
    TempDir tempDir;
    TempFile hdfsSite(tempDir.path + "/hdfs-site.xml");
    writeSimpleConfig(hdfsSite.filename, "key2", "value2");

    ConfigurationLoader loader;
    loader.SetSearchPath(tempDir.path);

    optional<HdfsConfiguration> config = loader.LoadDefaultResources<HdfsConfiguration>();
    EXPECT_TRUE(config && "Parse streams");
    EXPECT_EQ("value2", config->GetWithDefault("key2", ""));
  }
}

int main(int argc, char *argv[])
{
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}

}
