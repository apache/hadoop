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

 #include <errno.h>
 #include <fcntl.h>
 #include <inttypes.h>
 #include <signal.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <sys/stat.h>
 #include <sys/wait.h>
 #include <unistd.h>

 #include <gtest/gtest.h>
 #include <sstream>
 #include <openssl/evp.h>

 extern "C" {
 #include "utils/string-utils.h"
 }

 namespace ContainerExecutor {

    class TestStringUtils : public ::testing::Test {
    protected:
      virtual void SetUp() {

      }

      virtual void TearDown() {

      }
    };

    TEST_F(TestStringUtils, test_get_numbers_split_by_comma) {
      const char* input = ",1,2,3,-1,,1,,0,";
      int* numbers;
      size_t n_numbers;
      int rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);

      std::cout << "Testing input=" << input << "\n";
      ASSERT_EQ(0, rc) << "Should succeeded\n";
      ASSERT_EQ(6, n_numbers);
      ASSERT_EQ(1, numbers[0]);
      ASSERT_EQ(-1, numbers[3]);
      ASSERT_EQ(0, numbers[5]);
      free(numbers);

      input = "3";
      rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
      std::cout << "Testing input=" << input << "\n";
      ASSERT_EQ(0, rc) << "Should succeeded\n";
      ASSERT_EQ(1, n_numbers);
      ASSERT_EQ(3, numbers[0]);
      free(numbers);

      input = "";
      rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
      std::cout << "Testing input=" << input << "\n";
      ASSERT_EQ(0, rc) << "Should succeeded\n";
      ASSERT_EQ(0, n_numbers);
      free(numbers);

      input = ",,";
      rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
      std::cout << "Testing input=" << input << "\n";
      ASSERT_EQ(0, rc) << "Should succeeded\n";
      ASSERT_EQ(0, n_numbers);
      free(numbers);

      input = "1,2,aa,bb";
      rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
      std::cout << "Testing input=" << input << "\n";
      ASSERT_TRUE(0 != rc) << "Should failed\n";
      free(numbers);

      input = "1,2,3,-12312312312312312312321311231231231";
      rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
      std::cout << "Testing input=" << input << "\n";
      ASSERT_TRUE(0 != rc) << "Should failed\n";
      free(numbers);
    }

    TEST_F(TestStringUtils, test_validate_container_id) {

      const char *good_input[] = {
          "container_e134_1499953498516_50875_01_000007",
          "container_1499953498516_50875_01_000007",
          "container_e1_12312_11111_02_000001"
      };

      const char *bad_input[] = {
          "CONTAINER",
          "container_e1_12312_11111_02_000001 | /tmp/file"
              "container_e1_12312_11111_02_000001 || # /tmp/file",
          "container_e1_12312_11111_02_000001 # /tmp/file",
          "container_e1_12312_11111_02_000001' || touch /tmp/file #",
          "ubuntu || touch /tmp/file #",
          "''''''''"
      };

      int good_input_size = sizeof(good_input) / sizeof(char *);
      int i = 0;
      for (i = 0; i < good_input_size; i++) {
        int op = validate_container_id(good_input[i]);
        ASSERT_EQ(1, op);
      }

      int bad_input_size = sizeof(bad_input) / sizeof(char *);
      int j = 0;
      for (j = 0; j < bad_input_size; j++) {
        int op = validate_container_id(bad_input[j]);
        ASSERT_EQ(0, op);
      }
    }

    TEST_F(TestStringUtils, test_to_hexstring) {
      const char* input = "hello";
      char* digest = NULL;
      unsigned char raw_digest[EVP_MAX_MD_SIZE];
      unsigned int raw_digest_len = 0;
      int rc = 0;

      EVP_MD_CTX* mdctx = EVP_MD_CTX_create();
      ASSERT_NE(nullptr, mdctx) << "Unable to create EVP MD context\n";

      rc = EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);
      ASSERT_EQ(1, rc) << "Unable to initialize SHA256 digester\n";

      rc = EVP_DigestFinal_ex(mdctx, raw_digest, &raw_digest_len);
      ASSERT_EQ(1, rc) << "Unable to compute digest\n";

      rc = EVP_DigestUpdate(mdctx, input, strlen(input));
      ASSERT_EQ(1, rc) << "Unable to compute digest\n";

      digest = to_hexstring(raw_digest, raw_digest_len);

      ASSERT_STREQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                   digest) << "Digest is not equal to expected hash\n";

      EVP_MD_CTX_destroy(mdctx);
      free(digest);
    }

    TEST_F(TestStringUtils, test_strbuf_on_stack) {
      const int sb_incr = 16;
      strbuf sb;
      bool rc;

      rc = strbuf_init(&sb, sb_incr);
      ASSERT_EQ(true, rc) << "Unable to init strbuf\n";

      rc = strbuf_append_fmt(&sb, sb_incr, "%s%s%s", "hello", "foo", "bar");
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf\n";

      ASSERT_STREQ("hellofoobar", sb.buffer);

      rc = strbuf_append_fmt(&sb, sb_incr, "%s%s%s", "some longer strings",
          " that will cause the strbuf", " to have to realloc");
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf\n";

      ASSERT_STREQ("hellofoobarsome longer strings that will cause the strbuf to have to realloc", sb.buffer);

      strbuf_destroy(&sb);
    }

    TEST_F(TestStringUtils, test_strbuf_in_heap) {
      const int sb_incr = 16;
      strbuf *sb = NULL;
      bool rc;

      sb = strbuf_alloc(sb_incr);
      ASSERT_NE(nullptr, sb) << "Unable to init strbuf\n";

      rc = strbuf_append_fmt(sb, sb_incr, "%s%s%s", "hello", "foo", "bar");
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf";

      ASSERT_STREQ("hellofoobar", sb->buffer);

      rc = strbuf_append_fmt(sb, sb_incr, "%s%s%s", "some longer strings",
          " that will cause the strbuf", " to have to realloc");
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf\n";

      ASSERT_STREQ("hellofoobarsome longer strings that will cause the strbuf to have to realloc", sb->buffer);

      strbuf_free(sb);
    }

    TEST_F(TestStringUtils, test_strbuf_detach) {
      const int sb_incr = 16;
      strbuf sb;
      char *buf;
      bool rc;

      rc = strbuf_init(&sb, sb_incr);
      ASSERT_EQ(true, rc) << "Unable to init strbuf\n";

      rc = strbuf_append_fmt(&sb, sb_incr, "%s%s%s", "hello", "foo", "bar");
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf\n";

      ASSERT_STREQ("hellofoobar", sb.buffer);

      rc = strbuf_append_fmt(&sb, sb_incr, "%s%s%s", "some longer strings",
          " that will cause the strbuf", " to have to realloc");
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf\n";

      ASSERT_STREQ("hellofoobarsome longer strings that will cause the strbuf to have to realloc", sb.buffer);

      buf = strbuf_detach_buffer(&sb);
      ASSERT_NE(nullptr, buf) << "Unable to detach char buf from strbuf\n";


      rc = strbuf_append_fmt(&sb, sb_incr, "%s%s%s", "Buffer detached",
          " so this should allocate", " a new buffer in strbuf");
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf\n";

      ASSERT_STREQ("Buffer detached so this should allocate a new buffer in strbuf", sb.buffer);
      ASSERT_STREQ("hellofoobarsome longer strings that will cause the strbuf to have to realloc", buf);

      free(buf);
      strbuf_destroy(&sb);
    }

    TEST_F(TestStringUtils, test_strbuf_realloc) {
      const int sb_incr = 5;
      strbuf sb;
      char buf[] = "1234567890";
      bool rc;

      int len = strlen(buf);

      rc = strbuf_init(&sb, sb_incr);
      ASSERT_EQ(true, rc) << "Unable to init strbuf\n";
      ASSERT_NE(nullptr, sb.buffer) << "Unable to init strbuf buffer\n";
      ASSERT_EQ(5, sb.capacity) << "Unable to init strbuf capacity\n";
      ASSERT_EQ(0, sb.length) << "Unable to init strbuf length\n";

      rc = strbuf_append_fmt(&sb, sb_incr, "%s", buf);
      ASSERT_EQ(true, rc) << "Unable to append format to strbuf\n";
      ASSERT_NE(nullptr, sb.buffer) << "Unable to append strbuf buffer\n";
      ASSERT_EQ(len + sb_incr + 1, sb.capacity) << "Unable to update strbuf capacity\n";
      ASSERT_EQ(len, sb.length) << "Unable to update strbuf length\n";

      rc = strbuf_realloc(&sb, 10);
      ASSERT_EQ(false, rc) << "realloc to smaller capacity succeeded and has truncated existing string\n";

      strbuf_destroy(&sb);
    }

} // namespace ContainerExecutor
