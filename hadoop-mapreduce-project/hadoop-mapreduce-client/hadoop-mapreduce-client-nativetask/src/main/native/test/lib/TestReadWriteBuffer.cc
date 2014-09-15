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
#include "lib/Combiner.h"
#include "test_commons.h"
#include <iostream>

namespace NativeTask {

TEST(ReadWriteBuffer, readAndWrite) {
  ReadWriteBuffer buff(16);

  int INT = 100;
  int LONG = 200;
  std::string STR = "hello, readWriteBuffer";
  void * POINTER = this;

  int REPEAT = 10;

  for (int i = 0; i < REPEAT; i++) {
    buff.writeInt(INT);
    buff.writeLong(LONG);
    buff.writeString(&STR);
    buff.writePointer(POINTER);
    buff.writeString(STR.c_str(), STR.length());
  }

  uint32_t writePoint = buff.getWritePoint();

  for (int i = 0; i < REPEAT; i++) {
    ASSERT_EQ(INT, buff.readInt());
    ASSERT_EQ(LONG, buff.readLong());
    string * read = buff.readString();
    ASSERT_EQ(0, STR.compare(read->c_str()));
    delete read;

    ASSERT_EQ(POINTER, buff.readPointer());

    read = buff.readString();
    ASSERT_EQ(0, STR.compare(read->c_str()));
    delete read;
  }

  uint32_t readPoint = buff.getReadPoint();
  ASSERT_EQ(writePoint, readPoint);

  buff.setWritePoint(0);
  buff.setReadPoint(0);

  ASSERT_EQ(0, buff.getReadPoint());
  ASSERT_EQ(0, buff.getWritePoint());
}

} /* namespace NativeTask */
