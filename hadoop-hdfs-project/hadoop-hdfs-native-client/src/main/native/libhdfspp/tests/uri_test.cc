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

#include "common/uri.h"
#include <gmock/gmock.h>

using ::testing::_;

using namespace hdfs;

TEST(UriTest, TestDegenerateInputs) {
  /* Empty input */
  {
    optional<URI> uri = URI::parse_from_string("");
    EXPECT_TRUE(uri && "Empty input");
  }

  /* Invalid encoding */
  {
    optional<URI> uri = URI::parse_from_string("%%");
    EXPECT_FALSE(uri && "Bad input");
  }

  /* Invalid port */
  {
    optional<URI> uri = URI::parse_from_string("hdfs://nn:foo/");
    EXPECT_FALSE(uri && "Bad port");
  }

  /* Negative port */
  {
    optional<URI> uri = URI::parse_from_string("hdfs://nn:-100/");
    EXPECT_FALSE(uri && "Negative port");
  }

  /* Empty paths */
  {
    optional<URI> uri = URI::parse_from_string("hdfs://////");
    EXPECT_TRUE(uri && "Empty paths");
  }

}


TEST(UriTest, TestNominalInputs) {
  /* Simple input */
  {
    optional<URI> uri = URI::parse_from_string("hdfs:///foo");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("hdfs", uri->get_scheme());
    EXPECT_EQ("", uri->get_host());
    EXPECT_EQ(0, uri->get_port().value_or(0));
    EXPECT_EQ("/foo", uri->get_path());
    EXPECT_EQ("", uri->get_fragment());
    EXPECT_EQ("", uri->get_query());
  }

  /* With authority */
  {
    optional<URI> uri = URI::parse_from_string("hdfs://host:100/foo");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("hdfs", uri->get_scheme());
    EXPECT_EQ("host", uri->get_host());
    EXPECT_EQ(100, uri->get_port().value_or(0));
    EXPECT_EQ("/foo", uri->get_path());
    EXPECT_EQ("", uri->get_fragment());
    EXPECT_EQ("", uri->get_query());
  }

  /* No scheme */
  {
    optional<URI> uri = URI::parse_from_string("/foo");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("", uri->get_scheme());
    EXPECT_EQ("", uri->get_host());
    EXPECT_EQ(0, uri->get_port().value_or(0));
    EXPECT_EQ("/foo", uri->get_path());
    EXPECT_EQ("", uri->get_fragment());
    EXPECT_EQ("", uri->get_query());
  }

  /* All fields */
  {
    optional<URI> uri = URI::parse_from_string("hdfs://nn:8020/path/to/data?a=b&c=d#fragment");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("hdfs", uri->get_scheme());
    EXPECT_EQ("nn", uri->get_host());
    EXPECT_EQ(8020, uri->get_port().value_or(0));
    EXPECT_EQ("/path/to/data", uri->get_path());
    EXPECT_EQ("a=b&c=d", uri->get_query());
    EXPECT_EQ(3, uri->get_path_elements().size());
    EXPECT_EQ("path", uri->get_path_elements()[0]);
    EXPECT_EQ("to", uri->get_path_elements()[1]);
    EXPECT_EQ("data", uri->get_path_elements()[2]);
    EXPECT_EQ(2, uri->get_query_elements().size());
    EXPECT_EQ("a", uri->get_query_elements()[0].first);
    EXPECT_EQ("b", uri->get_query_elements()[0].second);
    EXPECT_EQ("c", uri->get_query_elements()[1].first);
    EXPECT_EQ("d", uri->get_query_elements()[1].second);
    EXPECT_EQ("fragment", uri->get_fragment());
  }
}

TEST(UriTest, TestEncodedInputs) {
  // Note that scheme and port cannot be uri-encoded

  /* Encoded input */
  {
    optional<URI> uri = URI::parse_from_string("S://%5E:1/+%5E%20?%5E=%5E#%5E");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("S", uri->get_scheme());
    EXPECT_EQ("^", uri->get_host());
    EXPECT_EQ(1, uri->get_port().value_or(0));
    EXPECT_EQ("/ ^ ", uri->get_path());
    EXPECT_EQ("^", uri->get_fragment());
    EXPECT_EQ("^=^", uri->get_query());
  }

  /* Lowercase */
  {
    optional<URI> uri = URI::parse_from_string("S://%5e:1/+%5e%20?%5e=%5e#%5e");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("S", uri->get_scheme());
    EXPECT_EQ("^", uri->get_host());
    EXPECT_EQ(1, uri->get_port().value_or(0));
    EXPECT_EQ("/ ^ ", uri->get_path());
    EXPECT_EQ("^", uri->get_fragment());
    EXPECT_EQ("^=^", uri->get_query());
  }
}

TEST(UriTest, TestDecodedInputsAndOutputs) {
  /* All fields non-encoded and shouldn't be interpreted */
  {
    optional<URI> uri = URI::parse_from_string("S://%25/%25+?%25=%25#%25");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("S", uri->get_scheme());
    EXPECT_EQ("%", uri->get_host());
    EXPECT_EQ(0, uri->get_port().value_or(0));
    EXPECT_EQ("/% ", uri->get_path());
    EXPECT_EQ("%", uri->get_fragment());
    EXPECT_EQ("%=%", uri->get_query());
  }

  /* All fields encode fields on their way out */
  {
    optional<URI> uri = URI::parse_from_string("S://%25/%25+?%25=%25#%25");
    ASSERT_TRUE(uri && "Parsed");
    EXPECT_EQ("S", uri->get_scheme(true));
    EXPECT_EQ("%25", uri->get_host(true));
    EXPECT_EQ(0, uri->get_port().value_or(0));
    EXPECT_EQ("/%25+", uri->get_path(true));
    EXPECT_EQ("%25", uri->get_fragment(true));
    EXPECT_EQ("%25=%25", uri->get_query(true));
  }

}

TEST(UriTest, TestSetters) {

  /* Non-encoded inputs */
  {
    URI uri;
    uri.set_scheme("S");
    uri.set_host("%");
    uri.set_port(100);
    uri.set_path("%/%/%");
    uri.set_fragment("%");
    uri.set_query("%25=%25");  //set_query must always be encoded
    EXPECT_EQ("S://%25:100/%25/%25/%25?%25=%25#%25", uri.str());
  }

  /* Incremental adders, non-encoded */
  {
    URI uri;
    uri.set_scheme("S");
    uri.set_host("%");
    uri.set_port(100);
    uri.set_fragment("%");
    EXPECT_EQ("S://%25:100#%25", uri.str());

    uri.add_path("%");
    uri.add_query("%", "%");
    EXPECT_EQ("S://%25:100/%25?%25=%25#%25", uri.str());

    uri.add_path("%");
    uri.add_query("%", "%");
    EXPECT_EQ("S://%25:100/%25/%25?%25=%25&%25=%25#%25", uri.str());
  }

  /* Encoded inputs */
  {
    URI uri;
    uri.set_scheme("S", true);
    uri.set_host("%25", true);
    uri.set_port(100);
    uri.set_path("%25/%25/%25", true);
    uri.set_fragment("%25", true);
    uri.set_query("%25=%25");  //set_query must always be encoded
    EXPECT_EQ("S://%25:100/%25/%25/%25?%25=%25#%25", uri.str());
  }

  /* Incremental adders, encoded */
  {
    URI uri;
    uri.set_scheme("S", true);
    uri.set_host("%25", true);
    uri.set_port(100);
    uri.set_fragment("%25", true);
    EXPECT_EQ("S://%25:100#%25", uri.str());

    uri.add_path("%25", true);
    uri.add_query("%25", "%25", true);
    EXPECT_EQ("S://%25:100/%25?%25=%25#%25", uri.str());

    uri.add_path("%25", true);
    uri.add_query("%25", "%25", true);
    EXPECT_EQ("S://%25:100/%25/%25?%25=%25&%25=%25#%25", uri.str());
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
