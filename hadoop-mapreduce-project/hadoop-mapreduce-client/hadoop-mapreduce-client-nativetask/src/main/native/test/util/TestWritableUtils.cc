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

#include "util/WritableUtils.h"
#include "test_commons.h"

void TestVLong(int64_t v) {
  char buff[1024];
  char buff2[1024];
  uint32_t dsize = WritableUtils::GetVLongSize(v);
  uint32_t wsize = (uint32_t)-1;
  WritableUtils::WriteVLong(v, buff, wsize);
  ASSERT_EQ(dsize, wsize);
  memcpy(buff2, buff, wsize);
  uint32_t rsize;
  int64_t rv = WritableUtils::ReadVLong(buff2, rsize);
  ASSERT_EQ(v, rv);
  ASSERT_EQ(rsize, dsize);
}


TEST(WritableUtils, VLong) {
  int num = TestConfig.getInt("test.size", 3000);
  int seed = TestConfig.getInt("test.seed", -1);
  Random r(seed);
  for (int i = 0; i < num; i++) {
    uint64_t v = r.nextLog2(((uint64_t)-1) / 2 - 3);
    TestVLong(v);
    TestVLong(-v);
  }
}



