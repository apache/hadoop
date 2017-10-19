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

#include <gtest/gtest.h>
#include <vector>

extern "C" {
#include "util.h"
}

namespace ContainerExecutor {

  class TestUtil : public ::testing::Test {
  protected:
    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };

  TEST_F(TestUtil, test_split_delimiter) {
    std::string str = "1,2,3,4,5,6,7,8,9,10,11";
    char *split_string = (char *) calloc(str.length() + 1, sizeof(char));
    strncpy(split_string, str.c_str(), str.length());
    char **splits = split_delimiter(split_string, ",");
    ASSERT_TRUE(splits != NULL);
    int count = 0;
    while(splits[count] != NULL) {
      ++count;
    }
    ASSERT_EQ(11, count);
    for(int i = 1; i < count; ++i) {
      std::ostringstream oss;
      oss << i;
      ASSERT_STREQ(oss.str().c_str(), splits[i-1]);
    }
    ASSERT_EQ(NULL, splits[count]);
    free_values(splits);

    split_string = (char *) calloc(str.length() + 1, sizeof(char));
    strncpy(split_string, str.c_str(), str.length());
    splits = split_delimiter(split_string, "%");
    ASSERT_TRUE(splits != NULL);
    ASSERT_TRUE(splits[1] == NULL);
    ASSERT_STREQ(str.c_str(), splits[0]);
    free_values(splits);

    splits = split_delimiter(NULL, ",");
    ASSERT_EQ(NULL, splits);
    return;
  }

  TEST_F(TestUtil, test_split) {
    std::string str = "1%2%3%4%5%6%7%8%9%10%11";
    char *split_string = (char *) calloc(str.length() + 1, sizeof(char));
    strncpy(split_string, str.c_str(), str.length());
    char **splits = split(split_string);
    int count = 0;
    while(splits[count] != NULL) {
      ++count;
    }
    ASSERT_EQ(11, count);
    for(int i = 1; i < count; ++i) {
      std::ostringstream oss;
      oss << i;
      ASSERT_STREQ(oss.str().c_str(), splits[i-1]);
    }
    ASSERT_EQ(NULL, splits[count]);
    free_values(splits);

    str = "1,2,3,4,5,6,7,8,9,10,11";
    split_string = (char *) calloc(str.length() + 1, sizeof(char));
    strncpy(split_string, str.c_str(), str.length());
    splits = split(split_string);
    ASSERT_TRUE(splits != NULL);
    ASSERT_TRUE(splits[1] == NULL);
    ASSERT_STREQ(str.c_str(), splits[0]);
    return;
  }

  TEST_F(TestUtil, test_trim) {
    char* trimmed = NULL;

    // Check NULL input
    ASSERT_EQ(NULL, trim(NULL));

    // Check empty input
    trimmed = trim("");
    ASSERT_STREQ("", trimmed);
    free(trimmed);

    // Check single space input
    trimmed = trim(" ");
    ASSERT_STREQ("", trimmed);
    free(trimmed);

    // Check multi space input
    trimmed = trim("   ");
    ASSERT_STREQ("", trimmed);
    free(trimmed);

    // Check both side trim input
    trimmed = trim(" foo ");
    ASSERT_STREQ("foo", trimmed);
    free(trimmed);

    // Check left side trim input
    trimmed = trim("foo   ");
    ASSERT_STREQ("foo", trimmed);
    free(trimmed);

    // Check right side trim input
    trimmed = trim("   foo");
    ASSERT_STREQ("foo", trimmed);
    free(trimmed);

    // Check no trim input
    trimmed = trim("foo");
    ASSERT_STREQ("foo", trimmed);
    free(trimmed);
  }

  TEST_F(TestUtil, test_escape_single_quote) {
    std::vector<std::pair<std::string, std::string> > input_output_vec;
    input_output_vec.push_back(std::make_pair<std::string, std::string>("'abcd'", "'\"'\"'abcd'\"'\"'"));
    input_output_vec.push_back(std::make_pair<std::string, std::string>("'", "'\"'\"'"));

    std::vector<std::pair<std::string, std::string> >::const_iterator itr;
    for (itr = input_output_vec.begin(); itr != input_output_vec.end(); ++itr) {
      char *ret = escape_single_quote(itr->first.c_str());
      ASSERT_STREQ(itr->second.c_str(), ret);
      free(ret);
    }
  }

  TEST_F(TestUtil, test_quote_and_append_arg) {

    char *tmp = static_cast<char *>(malloc(4096));
    size_t tmp_size = 4096;

    memset(tmp, 0, tmp_size);
    quote_and_append_arg(&tmp, &tmp_size, "param=", "argument1");
    ASSERT_STREQ("param='argument1' ", tmp);

    memset(tmp, 0, tmp_size);
    quote_and_append_arg(&tmp, &tmp_size, "param=", "ab'cd");
    ASSERT_STREQ("param='ab'\"'\"'cd' ", tmp);
    free(tmp);

    tmp = static_cast<char *>(malloc(4));
    tmp_size = 4;
    memset(tmp, 0, tmp_size);
    quote_and_append_arg(&tmp, &tmp_size, "param=", "argument1");
    ASSERT_STREQ("param='argument1' ", tmp);
    ASSERT_EQ(1040, tmp_size);
  }
}
