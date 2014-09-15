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

#include "lib/commons.h"
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "lib/NativeObjectFactory.h"
#include "lib/PartitionBucket.h"
#include "lib/Merge.h"
#include "NativeTask.h"
#include "util/WritableUtils.h"
#include "util/DualPivotQuickSort.h"
#include "lib/Combiner.h"
#include "lib/TaskCounters.h"
#include "lib/MinHeap.h"
#include "lib/PartitionBucketIterator.h"

namespace NativeTask {

KVIterator * PartitionBucket::getIterator() {
  if (_memBlocks.size() == 0) {
    return NULL;
  }
  return new PartitionBucketIterator(this, _keyComparator);
}

void PartitionBucket::spill(IFileWriter * writer)
  throw(IOException, UnsupportException) {
  KVIterator * iterator = getIterator();
  if (NULL == iterator || NULL == writer) {
    return;
  }

  if (_combineRunner == NULL) {
    Buffer key;
    Buffer value;

    while (iterator->next(key, value)) {
      writer->write(key.data(), key.length(), value.data(), value.length());
    }
  } else {
    _combineRunner->combine(CombineContext(UNKNOWN), iterator, writer);
  }
  delete iterator;
}

void PartitionBucket::sort(SortAlgorithm type) {
  if (_memBlocks.size() == 0) {
    return;
  }
  if ((!_sorted)) {
    for (uint32_t i = 0; i < _memBlocks.size(); i++) {
      MemoryBlock * block = _memBlocks[i];
      block->sort(type, _keyComparator);
    }
  }
  _sorted = true;
}

} // namespace NativeTask
