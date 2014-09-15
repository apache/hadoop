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

namespace NativeTask {

TEST(MemoryBlockIterator, test) {
  const uint32_t BUFFER_LENGTH = 100;
  char * bytes = new char[BUFFER_LENGTH];
  MemoryBlock block(bytes, BUFFER_LENGTH);

  const uint32_t KV_SIZE = 60;
  block.allocateKVBuffer(KV_SIZE);
  block.allocateKVBuffer(KV_SIZE);

  MemBlockIterator iter(&block);

  uint32_t keyCount = 0;
  while (iter.next()) {
    KVBuffer * kv = iter.getKVBuffer();
    ASSERT_EQ(block.getKVBuffer(keyCount), kv);
    keyCount++;
  }
  delete [] bytes;
}

class MemoryBlockFactory {
 public:
  static MemoryBlock * create(std::vector<int> & keys) {
    const uint32_t BUFFER_LENGTH = 1000;
    char * bytes = new char[BUFFER_LENGTH];
    MemoryBlock * block1 = new MemoryBlock(bytes, BUFFER_LENGTH);

    const uint32_t KV_SIZE = 16;

    for (uint32_t i = 0; i < keys.size(); i++) {
      uint32_t index = keys[i];
      KVBuffer * kv = block1->allocateKVBuffer(KV_SIZE);

      kv->keyLength = 4;
      kv->valueLength = 4;
      uint32_t * key = (uint32_t *)kv->getKey();
      *key = bswap(index);
    }
    return block1;
  }
};

TEST(MemoryBlockIterator, compare) {
  std::vector<int> vector1;

  vector1.push_back(2);
  vector1.push_back(4);
  vector1.push_back(6);

  std::vector<int> vector2;

  vector2.push_back(1);
  vector2.push_back(3);
  vector2.push_back(5);

  ComparatorPtr bytesComparator = NativeTask::get_comparator(BytesType, NULL);

  MemoryBlock * block1 = MemoryBlockFactory::create(vector1);
  MemoryBlock * block2 = MemoryBlockFactory::create(vector2);

  block1->sort(CPPSORT, bytesComparator);
  block2->sort(CPPSORT, bytesComparator);

  MemBlockIterator * iter1 = new MemBlockIterator(block1);
  MemBlockIterator * iter2 = new MemBlockIterator(block2);

  MemBlockComparator comparator(bytesComparator);

  ASSERT_EQ(false, comparator(iter1, iter2));

  iter1->next();
  ASSERT_EQ(true, comparator(iter1, iter2));

  iter2->next();
  ASSERT_EQ(false, comparator(iter1, iter2));

  delete iter2;
  delete iter1;
  delete [] block2->base();
  delete [] block1->base();
  delete block2;
  delete block1;
}
} // namespace NativeTask

