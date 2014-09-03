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

TEST(ByteBuffer, read) {
  char * buff = new char[100];
  ByteBuffer byteBuffer;
  byteBuffer.reset(buff, 100);

  ASSERT_EQ(0, byteBuffer.position());
  ASSERT_EQ(100, byteBuffer.capacity());
  ASSERT_EQ(0, byteBuffer.limit());

  ASSERT_EQ(buff, byteBuffer.current());
  ASSERT_EQ(0, byteBuffer.remain());

  byteBuffer.advance(3);
  ASSERT_EQ(3, byteBuffer.current() - byteBuffer.base());

  byteBuffer.rewind(10, 20);
  ASSERT_EQ(20, byteBuffer.limit());

  ASSERT_EQ(10, byteBuffer.position());
  delete [] buff;
}
} /* namespace NativeTask */
