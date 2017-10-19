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

   input = "3";
   rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
   std::cout << "Testing input=" << input << "\n";
   ASSERT_EQ(0, rc) << "Should succeeded\n";
   ASSERT_EQ(1, n_numbers);
   ASSERT_EQ(3, numbers[0]);

   input = "";
   rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
   std::cout << "Testing input=" << input << "\n";
   ASSERT_EQ(0, rc) << "Should succeeded\n";
   ASSERT_EQ(0, n_numbers);

   input = ",,";
   rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
   std::cout << "Testing input=" << input << "\n";
   ASSERT_EQ(0, rc) << "Should succeeded\n";
   ASSERT_EQ(0, n_numbers);

   input = "1,2,aa,bb";
   rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
   std::cout << "Testing input=" << input << "\n";
   ASSERT_TRUE(0 != rc) << "Should failed\n";

   input = "1,2,3,-12312312312312312312321311231231231";
   rc = get_numbers_split_by_comma(input, &numbers, &n_numbers);
   std::cout << "Testing input=" << input << "\n";
   ASSERT_TRUE(0 != rc) << "Should failed\n";
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

} // namespace ContainerExecutor
