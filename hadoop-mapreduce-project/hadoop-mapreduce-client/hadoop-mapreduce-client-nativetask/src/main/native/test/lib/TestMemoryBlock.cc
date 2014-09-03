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
#include "test_commons.h"
#include "lib/MapOutputSpec.h"
#include "lib/MemoryBlock.h"

namespace NativeTaskTest {

TEST(MemoryBlock, test) {
  const uint32_t BUFFER_LENGTH = 1000;
  char * bytes = new char[BUFFER_LENGTH];
  MemoryBlock block(bytes, BUFFER_LENGTH);

  uint32_t NON_EXIST = 3;
  ASSERT_EQ(NULL, block.getKVBuffer(NON_EXIST));
  ASSERT_EQ(0, block.getKVCount());
  ASSERT_EQ(BUFFER_LENGTH, block.remainSpace());

  ComparatorPtr bytesComparator = NativeTask::get_comparator(BytesType, NULL);
  block.sort(CPPSORT, bytesComparator);
  ASSERT_EQ(true, block.sorted());

  const uint32_t KV_SIZE = 16;
  KVBuffer * kv1 = block.allocateKVBuffer(KV_SIZE);
  KVBuffer * kv2 = block.allocateKVBuffer(KV_SIZE);

  ASSERT_EQ(2, block.getKVCount());
  ASSERT_EQ(kv1, block.getKVBuffer(0));
  ASSERT_EQ(kv2, block.getKVBuffer(1));

  ASSERT_EQ(BUFFER_LENGTH - 2 * KV_SIZE, block.remainSpace());
  ASSERT_EQ(false, block.sorted());
  delete [] bytes;
}

TEST(MemoryBlock, overflow) {
  const uint32_t BUFFER_LENGTH = 100;
  char * bytes = new char[BUFFER_LENGTH];
  MemoryBlock block(bytes, BUFFER_LENGTH);

  const uint32_t KV_SIZE = 60;
  KVBuffer * kv1 = block.allocateKVBuffer(KV_SIZE);
  KVBuffer * kv2 = block.allocateKVBuffer(KV_SIZE);

  ASSERT_EQ(kv1, block.getKVBuffer(0));
  ASSERT_EQ(kv2, block.getKVBuffer(1));

  ASSERT_EQ(1, block.getKVCount());

  ASSERT_EQ(BUFFER_LENGTH - KV_SIZE, block.remainSpace());
  delete [] bytes;
}

TEST(MemoryBlock, sort) {
  const uint32_t BUFFER_LENGTH = 1000;
  char * bytes = new char[BUFFER_LENGTH];
  MemoryBlock block(bytes, BUFFER_LENGTH);

  const uint32_t KV_SIZE = 16;
  KVBuffer * big = block.allocateKVBuffer(KV_SIZE);
  KVBuffer * small = block.allocateKVBuffer(KV_SIZE);
  KVBuffer * medium = block.allocateKVBuffer(KV_SIZE);

  const uint32_t SMALL = 100;
  const uint32_t MEDIUM = 1000;
  const uint32_t BIG = 10000;

  medium->keyLength = 4;
  medium->valueLength = 4;
  uint32_t * mediumKey = (uint32_t *)medium->getKey();
  *mediumKey = bswap(MEDIUM);

  small->keyLength = 4;
  small->valueLength = 4;
  uint32_t * smallKey = (uint32_t *)small->getKey();
  *smallKey = bswap(SMALL);

  big->keyLength = 4;
  big->valueLength = 4;
  uint32_t * bigKey = (uint32_t *)big->getKey();
  *bigKey = bswap(BIG);

  ComparatorPtr bytesComparator = NativeTask::get_comparator(BytesType, NULL);
  block.sort(CPPSORT, bytesComparator);

  ASSERT_EQ(small, block.getKVBuffer(0));
  ASSERT_EQ(medium, block.getKVBuffer(1));
  ASSERT_EQ(big, block.getKVBuffer(2));
  delete [] bytes;
}

} // namespace NativeTask
