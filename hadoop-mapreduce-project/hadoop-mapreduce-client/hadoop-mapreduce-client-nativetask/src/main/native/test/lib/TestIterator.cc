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

class MockIterator : public KVIterator {
  std::vector<std::pair<int, int> > kvs;
  uint32_t index;
  uint32_t expectedKeyGroupNum;
  std::map<int, int> expectkeyCountMap;
  char buffer[8];

 public:
  MockIterator()
      : index(0) {
    kvs.push_back(std::pair<int, int>(10, 100));

    kvs.push_back(std::pair<int, int>(10, 100));
    kvs.push_back(std::pair<int, int>(10, 101));
    kvs.push_back(std::pair<int, int>(10, 102));

    kvs.push_back(std::pair<int, int>(20, 200));
    kvs.push_back(std::pair<int, int>(20, 201));
    kvs.push_back(std::pair<int, int>(20, 202));
    kvs.push_back(std::pair<int, int>(30, 302));
    kvs.push_back(std::pair<int, int>(40, 302));
    this->expectedKeyGroupNum = 4;

    expectkeyCountMap[10] = 4;
    expectkeyCountMap[20] = 3;
    expectkeyCountMap[30] = 1;
    expectkeyCountMap[40] = 1;
  }

  bool next(Buffer & key, Buffer & outValue) {
    if (index < kvs.size()) {
      std::pair<int, int> value = kvs.at(index);
      *((int *)buffer) = value.first;
      *(((int *)buffer) + 1) = value.second;
      key.reset(buffer, 4);
      outValue.reset(buffer + 4, 4);
      index++;
      return true;
    }
    return false;
  }

  uint32_t getExpectedKeyGroupCount() {
    return expectedKeyGroupNum;
  }

  std::map<int, int>& getExpectedKeyCountMap() {
    return expectkeyCountMap;
  }
};

void TestKeyGroupIterator() {
  MockIterator * iter = new MockIterator();
  KeyGroupIteratorImpl * groupIterator = new KeyGroupIteratorImpl(iter);
  const char * key = NULL;

  uint32_t keyGroupCount = 0;
  std::map<int, int> actualKeyCount;
  while (groupIterator->nextKey()) {
    keyGroupCount++;
    uint32_t length = 0;
    key = groupIterator->getKey(length);
    int * keyPtr = (int *)key;
    const char * value = NULL;
    while (NULL != (value = groupIterator->nextValue(length))) {
      if (actualKeyCount.find(*keyPtr) == actualKeyCount.end()) {
        actualKeyCount[*keyPtr] = 0;
      }
      actualKeyCount[*keyPtr]++;
    }
  }
  ASSERT_EQ(iter->getExpectedKeyGroupCount(), keyGroupCount);
  std::map<int, int> & expectedKeyCountMap = iter->getExpectedKeyCountMap();
  for (std::map<int, int>::iterator keyCountIter = actualKeyCount.begin();
      keyCountIter != actualKeyCount.end(); ++keyCountIter) {
    uint32_t key = keyCountIter->first;
    uint32_t expectedCount = expectedKeyCountMap[key];
    ASSERT_EQ(expectedCount, keyCountIter->second);
  }
  delete groupIterator;
  delete iter;
}

TEST(Iterator, keyGroupIterator) {
  TestKeyGroupIterator();
}

} /* namespace NativeTask */

