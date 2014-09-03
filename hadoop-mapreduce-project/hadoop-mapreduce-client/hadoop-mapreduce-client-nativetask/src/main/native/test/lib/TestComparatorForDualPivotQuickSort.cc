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
#include "lib/MemoryBlock.h"
#include "test_commons.h"
#include <iostream>

namespace NativeTask {

static const char * expectedSrc = NULL;
static int expectedSrcLength = 0;

static const char * expectedDest = NULL;
static int expectedDestLength = 0;

static int compareResult = 0;

void checkInputArguments(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  ASSERT_EQ(expectedSrc, src);
  ASSERT_EQ(expectedSrcLength, srcLength);

  ASSERT_EQ(expectedDest, dest);
  ASSERT_EQ(expectedDestLength, destLength);
}

int MockComparatorForDualPivot(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  checkInputArguments(src, srcLength, dest, destLength);
  return compareResult;
}

TEST(ComparatorForDualPivotQuickSort, compare) {
  char * buff = new char[100];
  KVBuffer * kv1 = (KVBuffer *)buff;

  const char * KEY = "KEY";
  const char * VALUE = "VALUE";

  kv1->keyLength = strlen(KEY);
  char * key = kv1->getKey();
  ::memcpy(key, KEY, strlen(KEY));
  kv1->valueLength = strlen(VALUE);
  char * value = kv1->getValue();
  ::memcpy(value, VALUE, strlen(VALUE));

  const char * KEY2 = "KEY2";
  const char * VALUE2 = "VALUE2";

  KVBuffer * kv2 = kv1->next();
  kv2->keyLength = strlen(KEY2);
  char * key2 = kv2->getKey();
  ::memcpy(key2, KEY2, strlen(KEY2));
  kv2->valueLength = strlen(VALUE2);
  char * value2 = kv2->getValue();
  ::memcpy(value2, VALUE2, strlen(VALUE2));

  ComparatorForDualPivotSort comparator(buff, &MockComparatorForDualPivot);

  expectedSrc = kv1->getKey();
  expectedSrcLength = strlen(KEY);

  expectedDest = kv2->getKey();
  expectedDestLength = strlen(KEY2);

  compareResult = -1;

  ASSERT_EQ(-1, comparator((char * )kv1 - buff, (char * )kv2 - buff));
  delete [] buff;
}

} /* namespace NativeTask */
