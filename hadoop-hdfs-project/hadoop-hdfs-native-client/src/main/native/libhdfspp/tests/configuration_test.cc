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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>("");
    EXPECT_FALSE(config && "Empty stream");
  }

  /* No values */
  {
    std::string data = "<configuration></configuration>";
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(data);
    EXPECT_TRUE(config && "Blank config");
  }

  /* Extraneous values */
  {
    std::string data = "<configuration><spam></spam></configuration>";
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(data);
    EXPECT_TRUE(config && "Extraneous values");
  }
}

TEST(ConfigurationTest, TestBasicOperations) {
  /* Single value */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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

  /* No defaults */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Compact value parse");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }
}

TEST(ConfigurationTest, TestMultipleResources) {
  /* Single value */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "value1");
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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

    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
    EXPECT_TRUE(config && "Parse single value");
    EXPECT_EQ("value1", config->GetWithDefault("key1", ""));
  }
}

TEST(ConfigurationTest, TestFinal) {
  {
    /* Explicitly non-final non-compact value */
    std::stringstream stream;
    stream << "<configuration><property><name>key1</name><value>value1</"
              "value><final>false</final></property></configuration>";
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
TEST(ConfigurationTest, TestIntConversions) {
  /* No defaults */
  {
    std::stringstream stream;
    simpleConfigStream(stream, "key1", "1");
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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
    optional<Configuration> config = ConfigurationLoader().Load<Configuration>(stream.str());
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

int main(int argc, char *argv[]) {
  /*
   *  The following line must be executed to initialize Google Mock
   * (and Google Test) before running the tests.
   */
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
}
