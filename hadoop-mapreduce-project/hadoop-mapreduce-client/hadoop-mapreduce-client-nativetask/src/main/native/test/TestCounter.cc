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

#include "commons.h"
#include "BufferStream.h"
#include "Buffers.h"
#include "test_commons.h"

TEST(Counter, test) {
  Counter counter1("group", "key");
  const string & group = counter1.group();
  const string & name = counter1.name();
  ASSERT_EQ(0, name.compare("key"));
  ASSERT_EQ(0, group.compare("group"));

  ASSERT_EQ(0, counter1.get());

  counter1.increase(100);
  ASSERT_EQ(100, counter1.get());
}
