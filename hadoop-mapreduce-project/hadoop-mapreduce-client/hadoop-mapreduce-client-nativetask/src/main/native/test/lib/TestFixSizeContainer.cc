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

TEST(FixSizeContainer, test) {
  uint32_t length = 100;
  FixSizeContainer * container = new FixSizeContainer();
  char * bytes = new char[length];
  container->wrap(bytes, length);

  ASSERT_EQ(0, container->position());
  int pos1 = 3;
  container->position(pos1);
  ASSERT_EQ(pos1, container->position());
  ASSERT_EQ(length - pos1, container->remain());

  container->rewind();
  ASSERT_EQ(0, container->position());
  ASSERT_EQ(length, container->size());

  std::string toBeFilled = "Hello, FixContainer";

  container->fill(toBeFilled.c_str(), toBeFilled.length());

  for (uint32_t i = 0; i < container->position(); i++) {
    char * c = container->base() + i;
    ASSERT_EQ(toBeFilled[i], *c);
  }

  delete [] bytes;
  delete container;
}

} /* namespace NativeTask */
