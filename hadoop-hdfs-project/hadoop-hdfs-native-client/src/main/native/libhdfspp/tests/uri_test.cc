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

#include "hdfspp/uri.h"
#include <gmock/gmock.h>

using ::testing::_;

using namespace hdfs;

URI expect_uri_throw(const char *uri) {
  bool threw = false;
  std::string what_msg;
  URI val;
  try {
    val = URI::parse_from_string(uri);
  } catch (const uri_parse_error& e) {
    threw = true;
    what_msg = e.what();
  } catch (...) {
    threw = true;
  }

  EXPECT_TRUE(threw);
  EXPECT_EQ(what_msg, uri);
  return val;
}

URI expect_uri_nothrow(const char *uri) {
  bool threw = false;
  std::string what_msg;
  URI val;
  try {
    val = URI::parse_from_string(uri);
  } catch (const uri_parse_error& e) {
    threw = true;
    what_msg = e.what();
  } catch (...) {
    threw = true;
  }

  EXPECT_FALSE(threw);
  EXPECT_EQ(what_msg, "");
  return val;
}


TEST(UriTest, TestDegenerateInputs) {
  /* Empty input */
  expect_uri_nothrow("");

  /* Invalid encoding */
  expect_uri_throw("%%");

  /* Invalid port */
  expect_uri_throw("hdfs://nn:foo/");

  /* Negative port */
  expect_uri_throw("hdfs://nn:-100/");

  /* Empty paths */
  expect_uri_nothrow("hdfs://////");
}


TEST(UriTest, TestNominalInputs) {
  /* Simple input */
  {
    URI uri = expect_uri_nothrow("hdfs:///foo");
    EXPECT_EQ("hdfs", uri.get_scheme());
    EXPECT_EQ("", uri.get_host());
    EXPECT_FALSE(uri.has_port());
    EXPECT_EQ(0, uri.get_port_or_default(0));
    EXPECT_EQ("/foo", uri.get_path());
    EXPECT_EQ("", uri.get_fragment());
    EXPECT_EQ("", uri.get_query());
  }

  /* With authority */
  {
    URI uri = expect_uri_nothrow("hdfs://host:100/foo");
    EXPECT_EQ("hdfs", uri.get_scheme());
    EXPECT_EQ("host", uri.get_host());
    EXPECT_TRUE(uri.has_port());
    EXPECT_EQ(100, uri.get_port());
    EXPECT_EQ(100, uri.get_port_or_default(0));
    EXPECT_EQ("/foo", uri.get_path());
    EXPECT_EQ("", uri.get_fragment());
    EXPECT_EQ("", uri.get_query());
  }

  /* No scheme */
  {
    URI uri = expect_uri_nothrow("/foo");
    EXPECT_EQ("", uri.get_scheme());
    EXPECT_EQ("", uri.get_host());
    EXPECT_FALSE(uri.has_port());
    EXPECT_EQ(0, uri.get_port_or_default(0));
    EXPECT_EQ("/foo", uri.get_path());
    EXPECT_EQ("", uri.get_fragment());
    EXPECT_EQ("", uri.get_query());
  }

  /* All fields */
  {
    URI uri = expect_uri_nothrow("hdfs://nn:8020/path/to/data?a=b&c=d#fragment");
    EXPECT_EQ("hdfs", uri.get_scheme());
    EXPECT_EQ("nn", uri.get_host());
    EXPECT_TRUE(uri.has_port());
    EXPECT_EQ(8020, uri.get_port());
    EXPECT_EQ(8020, uri.get_port_or_default(0));
    EXPECT_EQ("/path/to/data", uri.get_path());
    EXPECT_EQ("a=b&c=d", uri.get_query());
    EXPECT_EQ(3, uri.get_path_elements().size());
    EXPECT_EQ("path", uri.get_path_elements()[0]);
    EXPECT_EQ("to", uri.get_path_elements()[1]);
    EXPECT_EQ("data", uri.get_path_elements()[2]);
    EXPECT_EQ(2, uri.get_query_elements().size());
    EXPECT_EQ("a", uri.get_query_elements()[0].key);
    EXPECT_EQ("b", uri.get_query_elements()[0].value);
    EXPECT_EQ("c", uri.get_query_elements()[1].key);
    EXPECT_EQ("d", uri.get_query_elements()[1].value);
    EXPECT_EQ("fragment", uri.get_fragment());
  }
}

TEST(UriTest, TestEncodedInputs) {
  // Note that scheme and port cannot be uri-encoded

  /* Encoded input */
  {
    URI uri = expect_uri_nothrow("S://%5E:1/+%5E%20?%5E=%5E#%5E");
    EXPECT_EQ("S", uri.get_scheme());
    EXPECT_EQ("^", uri.get_host());
    EXPECT_EQ(1, uri.get_port_or_default(0));
    EXPECT_EQ("/ ^ ", uri.get_path());
    EXPECT_EQ("^", uri.get_fragment());
    EXPECT_EQ("^=^", uri.get_query());
  }

  /* Lowercase */
  {
    URI uri = expect_uri_nothrow("S://%5e:1/+%5e%20?%5e=%5e#%5e");
    EXPECT_EQ("S", uri.get_scheme());
    EXPECT_EQ("^", uri.get_host());
    EXPECT_EQ(1, uri.get_port_or_default(0));
    EXPECT_EQ("/ ^ ", uri.get_path());
    EXPECT_EQ("^", uri.get_fragment());
    EXPECT_EQ("^=^", uri.get_query());
  }
}

TEST(UriTest, TestDecodedInputsAndOutputs) {
  /* All fields non-encoded and shouldn't be interpreted */
  {
    URI uri = expect_uri_nothrow("S://%25/%25+?%25=%25#%25");
    EXPECT_EQ("S", uri.get_scheme());
    EXPECT_EQ("%", uri.get_host());
    EXPECT_EQ(0, uri.get_port_or_default(0));
    EXPECT_EQ("/% ", uri.get_path());
    EXPECT_EQ("%", uri.get_fragment());
    EXPECT_EQ("%=%", uri.get_query());
  }

  /* All fields encode fields on their way out */
  {
    URI uri = expect_uri_nothrow("S://%25/%25+?%25=%25#%25");
    EXPECT_EQ("S", uri.get_scheme(true));
    EXPECT_EQ("%25", uri.get_host(true));
    EXPECT_EQ(0, uri.get_port_or_default(0));
    EXPECT_EQ("/%25+", uri.get_path(true));
    EXPECT_EQ("%25", uri.get_fragment(true));
    EXPECT_EQ("%25=%25", uri.get_query(true));
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

TEST(UriTest, QueryManip) {
  // Not encoded, just basic adding and removing query parts
  {
    URI uri = URI::parse_from_string("hdfs://nn:8020/path?thedude=lebowski&donny=outofhiselement");
    EXPECT_TRUE(uri.has_port());
    EXPECT_EQ(uri.get_query(), "thedude=lebowski&donny=outofhiselement");

    std::vector<URI::Query> queries = uri.get_query_elements();
    EXPECT_EQ(queries.size(), 2);
    EXPECT_EQ(queries[0].key, "thedude");
    EXPECT_EQ(queries[0].value, "lebowski");
    EXPECT_EQ(queries[1].key, "donny");
    EXPECT_EQ(queries[1].value, "outofhiselement");

    uri.remove_query("donny"); // that's a bummer, man
    EXPECT_EQ(uri.get_query(), "thedude=lebowski");
    queries = uri.get_query_elements();
    EXPECT_EQ(queries.size(), 1);
    EXPECT_EQ(queries[0].key, "thedude");
    EXPECT_EQ(queries[0].value, "lebowski");

    uri.add_query("HeyPeter", "CheckItOut");
    EXPECT_EQ(uri.get_query(), "thedude=lebowski&HeyPeter=CheckItOut");
    queries = uri.get_query_elements();
    EXPECT_EQ(queries.size(), 2);
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
