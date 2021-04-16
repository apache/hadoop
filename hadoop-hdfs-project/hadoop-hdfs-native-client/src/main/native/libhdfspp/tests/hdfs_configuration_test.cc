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
#include <iostream>

using ::testing::_;

using namespace hdfs;

namespace hdfs
{
TEST(HdfsConfigurationTest, TestDefaultOptions)
{
  // Completely empty stream
  {
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    HdfsConfiguration empty_config = config_loader.NewConfig<HdfsConfiguration>();
    Options options = empty_config.GetOptions();
    EXPECT_EQ(Options::kDefaultRpcTimeout, options.rpc_timeout);
  }
}

TEST(HdfsConfigurationTest, TestSetOptions)
{
  // Completely empty stream
  {
    std::stringstream stream;
    simpleConfigStream(stream,
                       HdfsConfiguration::kFsDefaultFsKey, "/FDFK",
                       HdfsConfiguration::kDfsClientSocketTimeoutKey, 100,
                       HdfsConfiguration::kIpcClientConnectMaxRetriesKey, 101,
                       HdfsConfiguration::kIpcClientConnectRetryIntervalKey, 102,
                       HdfsConfiguration::kIpcClientConnectTimeoutKey, 103,
                       HdfsConfiguration::kHadoopSecurityAuthenticationKey, HdfsConfiguration::kHadoopSecurityAuthentication_kerberos
            );
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<HdfsConfiguration> config = config_loader.Load<HdfsConfiguration>(stream.str());
    EXPECT_TRUE(config && "Read stream");
    Options options = config->GetOptions();

    EXPECT_EQ("/FDFK", options.defaultFS.str());
    EXPECT_EQ(100, options.rpc_timeout);
    EXPECT_EQ(101, options.max_rpc_retries);
    EXPECT_EQ(102, options.rpc_retry_delay_ms);
    EXPECT_EQ(103, options.rpc_connect_timeout);
    EXPECT_EQ(Options::kKerberos, options.authentication);
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

TEST(HdfsConfigurationTest, TestConfigParserAPI) {
  // Config parser API
  {
    TempDir tempDir;
    TempFile coreSite(tempDir.path + "/core-site.xml");
    writeSimpleConfig(coreSite.filename, "key1", "value1");
    TempFile hdfsSite(tempDir.path + "/hdfs-site.xml");
    writeSimpleConfig(hdfsSite.filename, "key2", "value2");

    ConfigParser parser(tempDir.path);

    EXPECT_EQ("value1", parser.get_string_or("key1", ""));
    EXPECT_EQ("value2", parser.get_string_or("key2", ""));

    auto stats = parser.ValidateResources();

    EXPECT_EQ("core-site.xml", stats[0].first);
    EXPECT_EQ("OK", stats[0].second.ToString());

    EXPECT_EQ("hdfs-site.xml", stats[1].first);
    EXPECT_EQ("OK", stats[1].second.ToString());
  }

  {
    TempDir tempDir;
    TempFile coreSite(tempDir.path + "/core-site.xml");
    writeSimpleConfig(coreSite.filename, "key1", "value1");
    TempFile hdfsSite(tempDir.path + "/hdfs-site.xml");
    writeDamagedConfig(hdfsSite.filename, "key2", "value2");

    ConfigParser parser(tempDir.path);

    EXPECT_EQ("value1", parser.get_string_or("key1", ""));
    EXPECT_EQ("", parser.get_string_or("key2", ""));

    auto stats = parser.ValidateResources();

    EXPECT_EQ("core-site.xml", stats[0].first);
    EXPECT_EQ("OK", stats[0].second.ToString());

    EXPECT_EQ("hdfs-site.xml", stats[1].first);
    EXPECT_EQ("Exception:The configuration file has invalid xml around character 74", stats[1].second.ToString());
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
