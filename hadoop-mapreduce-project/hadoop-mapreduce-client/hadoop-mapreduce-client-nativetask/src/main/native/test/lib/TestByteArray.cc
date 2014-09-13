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

TEST(ByteArray, read) {
  ByteArray * buffer = new ByteArray();
  buffer->resize(10);
  ASSERT_EQ(10, buffer->size());
  char * buff1 = buffer->buff();

  buffer->resize(15);
  ASSERT_EQ(15, buffer->size());
  ASSERT_EQ(buffer->buff(), buff1);

  buffer->resize(30);
  ASSERT_EQ(30, buffer->size());
  ASSERT_NE(buffer->buff(), buff1);

  delete buffer;
}

} /* namespace NativeTask */
