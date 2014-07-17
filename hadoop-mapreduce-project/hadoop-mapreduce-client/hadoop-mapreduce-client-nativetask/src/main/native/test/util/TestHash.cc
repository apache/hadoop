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

#include "util/Hash.h"
#include "test_commons.h"

static uint64_t test_length(int64_t len, size_t size, size_t loopTime) {
  vector<string> data;
  TestConfig.setInt(GenerateLen, len);
  Generate(data, size, "bytes");
  Timer t;
  uint64_t ret;
  for (size_t m = 0; m < loopTime; m++) {
    for (size_t i = 0; i < data.size(); i++) {
      ret += Hash::BytesHash(data[i].c_str(), data[i].length());
    }
  }
  LOG("%s", t.getInterval(StringUtil::Format("Bytes%3lld", len).c_str()).c_str());
  t.reset();
  for (size_t m = 0; m < loopTime; m++) {
    for (size_t i = 0; i < data.size(); i++) {
      ret += Hash::CityHash(data[i].c_str(), data[i].length());
    }
  }
  LOG("%s", t.getInterval(StringUtil::Format(" City%3lld", len).c_str()).c_str());
  return ret;
}

TEST(Perf, Hash) {
  uint64_t ret = 0;
  ret += test_length(1, 100, 4000);
  ret += test_length(17, 100, 4000);
  ret += test_length(64, 100, 4000);
  ret += test_length(128, 100, 4000);
  ret += test_length(513, 100, 4000);
  fprintf(stderr, "%llu\n", (long long unsigned int)ret);
}
