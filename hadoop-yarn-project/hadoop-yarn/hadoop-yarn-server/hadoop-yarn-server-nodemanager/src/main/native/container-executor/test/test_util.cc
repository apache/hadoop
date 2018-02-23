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

  /**
   * Internal function for testing quote_and_append_arg()
   */
  void test_quote_and_append_arg_function_internal(char **str, size_t *size, const char* param, const char *arg, const char *expected_result) {
    const size_t alloc_block_size = QUOTE_AND_APPEND_ARG_GROWTH;
    size_t orig_size = *size;
    size_t expected_size = strlen(expected_result) + 1;
    if (expected_size > orig_size) {
      expected_size += alloc_block_size;
    } else {
      expected_size = orig_size; // fits in original string
    }
    quote_and_append_arg(str, size, param, arg);
    ASSERT_STREQ(*str, expected_result);
    ASSERT_EQ(*size, expected_size);
    return;
  }

  TEST_F(TestUtil, test_quote_and_append_arg) {
    size_t str_real_size = 32;

    // Simple test - size = 32, result = 16
    size_t str_size = str_real_size;
    char *str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "ssss");
    const char *param = "pppp";
    const char *arg = "aaaa";
    const char *expected_result = "sssspppp'aaaa' ";
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // Original test - size = 32, result = 19
    str_size = str_real_size;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    param = "param=";
    arg = "argument1";
    expected_result = "param='argument1' ";
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // Original test - size = 32 and result = 21
    str_size = str_real_size;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    param = "param=";
    arg = "ab'cd";
    expected_result = "param='ab'\"'\"'cd' "; // 21 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // Lie about size of buffer so we don't crash from an actual buffer overflow
    // Original Test - Size = 4 and result = 19
    str_size = 4;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    param = "param=";
    arg = "argument1";
    expected_result = "param='argument1' "; // 19 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // Size = 8 and result = 7
    str_size = 8;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "s");
    param = "p";
    arg = "a";
    expected_result = "sp'a' "; // 7 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // Size = 8 and result = 7
    str_size = 8;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "s");
    param = "p";
    arg = "a";
    expected_result = "sp'a' "; // 7 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // Size = 8 and result = 8
    str_size = 8;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "ss");
    param = "p";
    arg = "a";
    expected_result = "ssp'a' "; // 8 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // size = 8, result = 9 (should grow buffer)
    str_size = 8;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "ss");
    param = "pp";
    arg = "a";
    expected_result = "sspp'a' "; // 9 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // size = 8, result = 10 (should grow buffer)
    str_size = 8;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "ss");
    param = "pp";
    arg = "aa";
    expected_result = "sspp'aa' "; // 10 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // size = 8, result = 11 (should grow buffer)
    str_size = 8;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "sss");
    param = "pp";
    arg = "aa";
    expected_result = "ssspp'aa' "; // 11 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // One with quotes - size = 32, result = 17
    str_size = 32;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "s");
    param = "p";
    arg = "'a'";
    expected_result = "sp''\"'\"'a'\"'\"'' "; // 17 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);

    // One with quotes - size = 16, result = 17
    str_size = 16;
    str = (char *) malloc(str_real_size);
    memset(str, 0, str_real_size);
    strcpy(str, "s");
    param = "p";
    arg = "'a'";
    expected_result = "sp''\"'\"'a'\"'\"'' "; // 17 characters
    test_quote_and_append_arg_function_internal(&str, &str_size, param, arg, expected_result);
    free(str);
  }
}
