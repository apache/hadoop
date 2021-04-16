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

#include "configuration_test.h"
#include "common/configuration.h"
#include "common/configuration_loader.h"
#include <gmock/gmock.h>
#include <cstdio>
#include <fstream>

using ::testing::_;

using namespace hdfs;

namespace hdfs {

TEST(ConfigurationTest, TestDegenerateInputs) {
  /* Completely empty stream */
  {
    std::stringstream stream;
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>("");
    EXPECT_FALSE(config && "Empty stream");
  }

  /* No values */
  {
    std::string data = "<configuration></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(data);
    EXPECT_TRUE(config && "Blank config");
  }

  /* Extraneous values */
  {
    std::string data = "<configuration><spam></spam></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(data);
    EXPECT_TRUE(config && "Extraneous values");
  }
}

TEST(ConfigurationTest, TestBasicOperations) {
  /* Single value */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }

  /* Multiple values */
  {
    optional<Configuration> config =
        simpleConfig("key1", "value1", "key2", "value2");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config->GetWithDefault("key2", ""));
  }

  /* Case-insensitive */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    EXPECT_EQ("value1", config->GetWithDefault("KEY1", ""));
  }

  /* No defaults */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    optional<std::string> value = config->Get("key1");
    EXPECT_TRUE((bool)value);
    EXPECT_EQ("value1", *value);
    EXPECT_FALSE(config->Get("key2"));
  }
}

TEST(ConfigurationTest, TestCompactValues) {
  {
    std::stringstream stream;
    stream << "<configuration><property name=\"key1\" "
              "value=\"value1\"/></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Compact value parse");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }
}

TEST(ConfigurationTest, TestMultipleResources) {
  /* Single value */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    std::stringstream stream2;
    simpleConfigStream(stream2, "key2", "value2");
    optional<Configuration> config2 =
        ConfigurationLoader().OverlayResourceString(config.value(), stream2.str());
    EXPECT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value1", config2->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config2->GetWithDefault("key2", ""));
  }
}

TEST(ConfigurationTest, TestStringResource) {
  /* Single value */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    std::string str = stream.str();

    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }
}

TEST(ConfigurationTest, TestValueOverlay) {
  /* Incremental updates */
  {
    ConfigurationLoader loader;
    std::stringstream stream;
    stream << "<configuration>"
            "<property><name>key1</name><value>value1</value><final>false</final></property>"
            "<property><name>final2</name><value>value2</value><final>true</final></property>"
            "</configuration>";
    optional<Configuration> config = loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config->GetWithDefault("final2", ""));
    config = loader.OverlayValue(config.value(), "key3", "value3");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config->GetWithDefault("final2", ""));
    EXPECT_EQ("value3", config->GetWithDefault("key3", ""));
    config = loader.OverlayValue(config.value(), "final2", "value4");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config->GetWithDefault("final2", ""));
    EXPECT_EQ("value3", config->GetWithDefault("key3", ""));

    // Case insensitive overlay
    config = loader.OverlayValue(config.value(), "KEY3", "value3a");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config->GetWithDefault("final2", ""));
    EXPECT_EQ("value3a", config->GetWithDefault("key3", ""));
  }
}

TEST(ConfigurationTest, TestFinal) {
  {
    /* Explicitly non-final non-compact value */
    std::stringstream stream;
    stream << "<configuration><property><name>key1</name><value>value1</"
              "value><final>false</final></property></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    std::stringstream stream2;
    simpleConfigStream(stream2, "key1", "value2");
    optional<Configuration> config2 =
        ConfigurationLoader().OverlayResourceString(config.value(), stream2.str());
    EXPECT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value2", config2->GetWithDefault("key1", ""));
  }
  {
    /* Explicitly final non-compact value */
    std::stringstream stream;
    stream << "<configuration><property><name>key1</name><value>value1</"
              "value><final>true</final></property></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    std::stringstream stream2;
    simpleConfigStream(stream2, "key1", "value2");
    optional<Configuration> config2 =
        ConfigurationLoader().OverlayResourceString(config.value(), stream2.str());
    EXPECT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value1", config2->GetWithDefault("key1", ""));
  }
  {
    /* Explicitly non-final compact value */
    std::stringstream stream;
    stream << "<configuration><property name=\"key1\" value=\"value1\" "
              "final=\"false\"/></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    std::stringstream stream2;
    simpleConfigStream(stream2, "key1", "value2");
    optional<Configuration> config2 =
        ConfigurationLoader().OverlayResourceString(config.value(), stream2.str());
    EXPECT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value2", config2->GetWithDefault("key1", ""));
  }
  {
    /* Explicitly final compact value */
    std::stringstream stream;
    stream << "<configuration><property name=\"key1\" value=\"value1\" "
              "final=\"true\"/></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    std::stringstream stream2;
    simpleConfigStream(stream2, "key1", "value2");
    optional<Configuration> config2 =
        ConfigurationLoader().OverlayResourceString(config.value(), stream2.str());
    EXPECT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value1", config2->GetWithDefault("key1", ""));
  }
  {
    /* Bogus final value */
    std::stringstream stream;
    stream << "<configuration><property><name>key1</name><value>value1</"
              "value><final>spam</final></property></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    std::stringstream stream2;
    simpleConfigStream(stream2, "key1", "value2");
    optional<Configuration> config2 =
        ConfigurationLoader().OverlayResourceString(config.value(), stream2.str());
    EXPECT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value2", config2->GetWithDefault("key1", ""));
  }
  {
    /* Blank final value */
    std::stringstream stream;
    stream << "<configuration><property><name>key1</name><value>value1</"
              "value><final></final></property></configuration>";
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    std::stringstream stream2;
    simpleConfigStream(stream2, "key1", "value2");
    optional<Configuration> config2 =
        ConfigurationLoader().OverlayResourceString(config.value(), stream2.str());
    EXPECT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value2", config2->GetWithDefault("key1", ""));
  }
}

TEST(ConfigurationTest, TestFileReads)
{
  // Single stream
  {
    TempFile tempFile;
    writeSimpleConfig(tempFile.filename, "key1", "value1");

    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.LoadFromFile<Configuration>(tempFile.filename);
    EXPECT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }

  // Multiple files
  {
    TempFile tempFile;
    writeSimpleConfig(tempFile.filename, "key1", "value1");

    ConfigurationLoader loader;
    optional<Configuration> config = loader.LoadFromFile<Configuration>(tempFile.filename);
    ASSERT_TRUE(config && "Parse first stream");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));

    TempFile tempFile2;
    writeSimpleConfig(tempFile2.filename, "key2", "value2");
    optional<Configuration> config2 = loader.OverlayResourceFile(*config, tempFile2.filename);
    ASSERT_TRUE(config2 && "Parse second stream");
    EXPECT_EQ("value1", config2->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config2->GetWithDefault("key2", ""));
  }

  // Try to add a directory
  {
    TempDir tempDir;

    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.LoadFromFile<Configuration>(tempDir.path);
    EXPECT_FALSE(config && "Add directory as file resource");
  }


  // Search path splitting
  {
    ConfigurationLoader loader;
    loader.SetSearchPath("foo:/bar:baz/:/fioux/:/bar/bar/bong");

    // Paths will have / appended to them if not already present
    EXPECT_EQ("foo/:/bar/:baz/:/fioux/:/bar/bar/bong/", loader.GetSearchPath());
  }

  // Search path
  {
    TempDir tempDir1;
    TempFile tempFile1(tempDir1.path + "/file1.xml");
    writeSimpleConfig(tempFile1.filename, "key1", "value1");
    TempDir tempDir2;
    TempFile tempFile2(tempDir2.path + "/file2.xml");
    writeSimpleConfig(tempFile2.filename, "key2", "value2");
    TempDir tempDir3;
    TempFile tempFile3(tempDir3.path + "/file3.xml");
    writeSimpleConfig(tempFile3.filename, "key3", "value3");

    ConfigurationLoader loader;
    loader.SetSearchPath(tempDir1.path + ":" + tempDir2.path + ":" + tempDir3.path);
    optional<Configuration> config1 = loader.LoadFromFile<Configuration>("file1.xml");
    EXPECT_TRUE(config1 && "Parse first stream");
    optional<Configuration> config2 = loader.OverlayResourceFile(*config1, "file2.xml");
    EXPECT_TRUE(config2 && "Parse second stream");
    optional<Configuration> config3 = loader.OverlayResourceFile(*config2, "file3.xml");
    EXPECT_TRUE(config3 && "Parse third stream");
    EXPECT_EQ("value1", config3->GetWithDefault("key1", ""));
    EXPECT_EQ("value2", config3->GetWithDefault("key2", ""));
    EXPECT_EQ("value3", config3->GetWithDefault("key3", ""));
  }
}

TEST(ConfigurationTest, TestDefaultConfigs) {
  // Search path
  {
    TempDir tempDir;
    TempFile coreSite(tempDir.path + "/core-site.xml");
    writeSimpleConfig(coreSite.filename, "key1", "value1");

    ConfigurationLoader loader;
    loader.SetSearchPath(tempDir.path);

    optional<Configuration> config = loader.LoadDefaultResources<Configuration>();
    EXPECT_TRUE(config && "Parse streams");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }
}

TEST(ConfigurationTest, TestIntConversions) {
  /* No defaults */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "1");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    optional<int64_t> value = config->GetInt("key1");
    EXPECT_TRUE((bool)value);
    EXPECT_EQ(1, *value);
    EXPECT_FALSE(config->GetInt("key2"));
  }

  {
    optional<Configuration> config = simpleConfig("key1", "1");
    EXPECT_EQ(1, config->GetIntWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "-100");
    EXPECT_EQ(-100, config->GetIntWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", " 1 ");
    EXPECT_EQ(1, config->GetIntWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "");
    EXPECT_EQ(-1, config->GetIntWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "spam");
    EXPECT_EQ(-1, config->GetIntWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key2", "");
    EXPECT_EQ(-1, config->GetIntWithDefault("key1", -1));
  }
}

TEST(ConfigurationTest, TestDoubleConversions) {
  /* No defaults */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "1");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    optional<double> value = config->GetDouble("key1");
    EXPECT_TRUE((bool)value);
    EXPECT_EQ(1, *value);
    EXPECT_FALSE(config->GetDouble("key2"));
  }

  {
    optional<Configuration> config = simpleConfig("key1", "1");
    EXPECT_EQ(1, config->GetDoubleWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "-100");
    EXPECT_EQ(-100, config->GetDoubleWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", " 1 ");
    EXPECT_EQ(1, config->GetDoubleWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "");
    EXPECT_EQ(-1, config->GetDoubleWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "spam");
    EXPECT_EQ(-1, config->GetDoubleWithDefault("key1", -1));
  }
  {
    optional<Configuration> config = simpleConfig("key2", "");
    EXPECT_EQ(-1, config->GetDoubleWithDefault("key1", -1));
  }
  { /* Out of range */
    optional<Configuration> config = simpleConfig("key2", "1e9999");
    EXPECT_EQ(-1, config->GetDoubleWithDefault("key1", -1));
  }
}

TEST(ConfigurationTest, TestBoolConversions) {
  /* No defaults */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "true");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    optional<bool> value = config->GetBool("key1");
    EXPECT_TRUE((bool)value);
    EXPECT_EQ(true, *value);
    EXPECT_FALSE(config->GetBool("key2"));
  }

  {
    optional<Configuration> config = simpleConfig("key1", "true");
    EXPECT_EQ(true, config->GetBoolWithDefault("key1", false));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "tRuE");
    EXPECT_EQ(true, config->GetBoolWithDefault("key1", false));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "false");
    EXPECT_FALSE(config->GetBoolWithDefault("key1", true));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "FaLsE");
    EXPECT_FALSE(config->GetBoolWithDefault("key1", true));
  }
  {
    optional<Configuration> config = simpleConfig("key1", " FaLsE ");
    EXPECT_FALSE(config->GetBoolWithDefault("key1", true));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "");
    EXPECT_EQ(true, config->GetBoolWithDefault("key1", true));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "spam");
    EXPECT_EQ(true, config->GetBoolWithDefault("key1", true));
  }
  {
    optional<Configuration> config = simpleConfig("key1", "");
    EXPECT_EQ(true, config->GetBoolWithDefault("key2", true));
  }
}

TEST(ConfigurationTest, TestUriConversions) {
  /* No defaults */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "hdfs:///");
    ConfigurationLoader config_loader;
    config_loader.ClearSearchPath();
    optional<Configuration> config = config_loader.Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    optional<URI> value = config->GetUri("key1");
    EXPECT_TRUE((bool)value);
    EXPECT_EQ("hdfs:///", value->str());
    EXPECT_FALSE(config->GetUri("key2"));
  }

  {
    optional<Configuration> config = simpleConfig("key1", "hdfs:///");
    EXPECT_EQ("hdfs:///", config->GetUriWithDefault("key1", "http:///").str());
  }
  {
    optional<Configuration> config = simpleConfig("key1", " hdfs:/// ");
    EXPECT_EQ("hdfs:///", config->GetUriWithDefault("key1", "http:///").str());
  }
  {
    optional<Configuration> config = simpleConfig("key1", "");
    EXPECT_EQ("", config->GetUriWithDefault("key1", "http:///").str());
  }
  {
    optional<Configuration> config = simpleConfig("key1", "%%");  // invalid URI
    EXPECT_EQ("http:///", config->GetUriWithDefault("key1", "http:///").str());
  }
  {
    optional<Configuration> config = simpleConfig("key2", "hdfs:///");
    EXPECT_EQ("http:///", config->GetUriWithDefault("key1", "http:///").str());
  }
}



int main(int argc, char *argv[]) {
  /*
   *  The following line must be executed to initialize Google Mock
   * (and Google Test) before running the tests.
   */
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
}
