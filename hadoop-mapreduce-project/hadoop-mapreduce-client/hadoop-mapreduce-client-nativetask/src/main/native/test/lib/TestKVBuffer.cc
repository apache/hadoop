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

TEST(KVBuffer, test) {

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

  ASSERT_EQ(strlen(KEY) + strlen(VALUE) + 8, kv1->length());

  ASSERT_EQ(8, kv1->getKey() - buff);
  ASSERT_EQ(strlen(KEY) + 8, kv1->getValue() - buff);

  kv1->keyLength = bswap(kv1->keyLength);
  kv1->valueLength = bswap(kv1->valueLength);

  ASSERT_EQ(8, kv1->headerLength());
  ASSERT_EQ(strlen(KEY) + strlen(VALUE) + 8, kv1->lengthConvertEndium());
  delete [] buff;
}

} /* namespace NativeTask */
