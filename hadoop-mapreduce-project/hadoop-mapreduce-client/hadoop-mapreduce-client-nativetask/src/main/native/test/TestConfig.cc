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

#include "lib/commons.h"
#include "lib/BufferStream.h"
#include "lib/Buffers.h"
#include "test_commons.h"

float absoute(float v) {
  if (v > 0) {
    return v;
  } else {
    return -v;
  }
}

TEST(Config, readAndWrite) {
  Config config;
  std::string STR = "CONFIG";
  std::string STRS = "CONFIG,LOG";
  int INT = 3;
  bool BOOL = true;

  config.set("STR", STR.c_str());
  config.set("STRS", STRS.c_str());
  config.setInt("INT", INT);
  config.setBool("BOOL", BOOL);
  config.set("INTS", "3,4");
  config.set("FLOAT", "3.5");
  config.set("FLOATS", "3.5,4.6");

  ASSERT_EQ(0, STR.compare(config.get("STR")));
  ASSERT_EQ(0, STRS.compare(config.get("STRS")));

  ASSERT_EQ(INT, config.getInt("INT"));
  ASSERT_EQ(BOOL, config.getBool("BOOL", false));

  vector<int64_t> ints;
  config.getInts("INTS", ints);
  ASSERT_EQ(2, ints.size());
  ASSERT_EQ(3, ints[0]);
  ASSERT_EQ(4, ints[1]);

  float floatValue = config.getFloat("FLOAT");
  ASSERT_TRUE(absoute(floatValue - 3.5) < 0.01);

  vector<float> floats;
  config.getFloats("FLOATS", floats);
  ASSERT_EQ(2, floats.size());
  ASSERT_TRUE(absoute(floats[0] - 3.5) < 0.01);
  ASSERT_TRUE(absoute(floats[1] - 4.6) < 0.01);
}
