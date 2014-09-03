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
#include "config.h"
#include "lib/BufferStream.h"
#include "lib/Buffers.h"
#include "test_commons.h"

TEST(Buffers, AppendRead) {
  string codec = "";
  vector<string> data;
  Generate(data, 100000, "word");
  string dest;
  dest.reserve(64 * 1024 * 1024);
  OutputStringStream outputStream = OutputStringStream(dest);
  AppendBuffer appendBuffer;
  appendBuffer.init(64 * 1024, &outputStream, codec);
  for (size_t i = 0; i < data.size(); i++) {
    appendBuffer.write(data[i].c_str(), data[i].length());
  }
  appendBuffer.flush();
  InputBuffer inputBuffer = InputBuffer(dest.c_str(), dest.length());
  ReadBuffer readBuffer = ReadBuffer();
  readBuffer.init(64 * 1024, &inputBuffer, codec);
  for (size_t i = 0; i < data.size(); i++) {
    const char * rd = readBuffer.get(data[i].length());
    ASSERT_EQ(data[i], string(rd, data[i].length()));
  }
}

#if defined HADOOP_SNAPPY_LIBRARY
TEST(Buffers, AppendReadSnappy) {
  string codec = "org.apache.hadoop.io.compress.SnappyCodec";
  vector<string> data;
  Generate(data, 100000, "word");
  string dest;
  dest.reserve(64 * 1024 * 1024);
  OutputStringStream outputStream = OutputStringStream(dest);
  AppendBuffer appendBuffer;
  appendBuffer.init(64 * 1024, &outputStream, codec);
  for (size_t i = 0; i < data.size(); i++) {
    appendBuffer.write(data[i].c_str(), data[i].length());
  }
  appendBuffer.flush();
  InputBuffer inputBuffer = InputBuffer(dest.c_str(), dest.length());
  ReadBuffer readBuffer = ReadBuffer();
  readBuffer.init(64 * 1024, &inputBuffer, codec);
  for (size_t i = 0; i < data.size(); i++) {
    const char * rd = readBuffer.get(data[i].length());
    ASSERT_EQ(data[i], string(rd, data[i].length()));
  }
}
#endif
