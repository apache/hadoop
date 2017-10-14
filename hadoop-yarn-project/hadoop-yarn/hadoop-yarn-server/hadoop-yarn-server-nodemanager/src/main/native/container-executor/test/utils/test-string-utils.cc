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

} // namespace ContainerExecutor