/*
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

#include <algorithm>

#include "lib/commons.h"
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "lib/NativeObjectFactory.h"
#include "lib/PartitionBucketIterator.h"
#include "lib/Merge.h"
#include "NativeTask.h"
#include "util/WritableUtils.h"
#include "util/DualPivotQuickSort.h"
#include "lib/Combiner.h"
#include "lib/TaskCounters.h"
#include "lib/MinHeap.h"

namespace NativeTask {

/////////////////////////////////////////////////////////////////
// PartitionBucket
/////////////////////////////////////////////////////////////////

PartitionBucketIterator::PartitionBucketIterator(PartitionBucket * pb, ComparatorPtr comparator)
    : _pb(pb), _comparator(comparator), _first(true) {
  uint32_t blockCount = _pb->getMemoryBlockCount();
  for (uint32_t i = 0; i < blockCount; i++) {
    MemoryBlock * block = _pb->getMemoryBlock(i);
    MemBlockIteratorPtr blockIterator = new MemBlockIterator(block);
    if (blockIterator->next()) {
      _heap.push_back(blockIterator);
    } else {
      delete blockIterator;
    }
  }
  if (_heap.size() > 1) {
    makeHeap(&(_heap[0]), &(_heap[0]) + _heap.size(), _comparator);
  }
}

PartitionBucketIterator::~PartitionBucketIterator() {
  for (uint32_t i = 0; i < _heap.size(); i++) {
    MemBlockIteratorPtr ptr = _heap[i];
    if (NULL != ptr) {
      delete ptr;
      _heap[i] = NULL;
    }
  }
}

bool PartitionBucketIterator::next() {
  size_t cur_heap_size = _heap.size();
  if (cur_heap_size > 0) {
    if (!_first) {
      if (_heap[0]->next()) { // have more, adjust heap
        if (cur_heap_size == 1) {
          return true;
        } else if (cur_heap_size == 2) {
          MemBlockIteratorPtr * base = &(_heap[0]);

          if (_comparator(base[1], base[0])) {
            std::swap(base[0], base[1]);
          }
        } else {
          MemBlockIteratorPtr * base = &(_heap[0]);
          heapify(base, 1, cur_heap_size, _comparator);
        }
      } else { // no more, pop heap
        // after popHeap, the first element of heap will be removed
        // and replaced by other element, so it needs to be deleted
        delete _heap[0];
        MemBlockIteratorPtr * base = &(_heap[0]);
        popHeap(base, base + cur_heap_size, _comparator);
        _heap.pop_back();
      }
    } else {
      _first = false;
    }
    return _heap.size() > 0;
  }
  return false;
}

bool PartitionBucketIterator::next(Buffer & key, Buffer & value) {
  bool result = next();
  if (result) {
    MemBlockIteratorPtr * base = &(_heap[0]);
    KVBuffer * kvBuffer = base[0]->getKVBuffer();

    key.reset(kvBuffer->getKey(), kvBuffer->keyLength);
    value.reset(kvBuffer->getValue(), kvBuffer->valueLength);

    return true;
  }
  return false;
}
} // namespace NativeTask

