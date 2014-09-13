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

#include "util/Checksum.h"
#include "test_commons.h"

void TestChecksum(ChecksumType type, void * buff, uint32_t len) {
  uint32_t chm = Checksum::init(type);
  Checksum::update(type, chm, buff, len);
}

TEST(Perf, CRC) {
  uint32_t len = TestConfig.getInt("checksum.perf.size", 1024 * 1024 * 50);
  int testTime = TestConfig.getInt("checksum.perf.time", 2);
  char * buff = new char[len];
  memset(buff, 1, len);
  Timer timer;
  for (int i = 0; i < testTime; i++) {
    TestChecksum(CHECKSUM_CRC32, buff, len);
  }
  LOG("%s", timer.getSpeedM("CRC", len * testTime).c_str());
  timer.reset();
  for (int i = 0; i < testTime; i++) {
    TestChecksum(CHECKSUM_CRC32C, buff, len);
  }
  LOG("%s", timer.getSpeedM("CRC32C", len * testTime).c_str());
  delete[] buff;
}
