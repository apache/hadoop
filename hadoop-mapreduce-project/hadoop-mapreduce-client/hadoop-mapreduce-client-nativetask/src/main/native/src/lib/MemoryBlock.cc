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

#include <algorithm>

#include "NativeTask.h"
#include "lib/commons.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"

#include "lib/MemoryBlock.h"
#include "lib/MemoryPool.h"
#include "util/DualPivotQuickSort.h"

namespace NativeTask {

class MemoryPool;

MemoryBlock::MemoryBlock(char * pos, uint32_t size)
    : _base(pos), _size(size), _position(0), _sorted(false) {
}

KVBuffer * MemoryBlock::getKVBuffer(uint32_t index) {
  if (index >= _kvOffsets.size()) {
    return NULL;
  }
  uint32_t offset = _kvOffsets.at(index);
  KVBuffer * kvbuffer = (KVBuffer*)(_base + offset);
  return kvbuffer;
}

void MemoryBlock::sort(SortAlgorithm type, ComparatorPtr comparator) {
  if ((!_sorted) && (_kvOffsets.size() > 1)) {
    switch (type) {
    case CPPSORT:
      std::sort(_kvOffsets.begin(), _kvOffsets.end(), ComparatorForStdSort(_base, comparator));
      break;
    case DUALPIVOTSORT: {
      DualPivotQuicksort(_kvOffsets, ComparatorForDualPivotSort(_base, comparator));
    }
      break;
    default:
      THROW_EXCEPTION(UnsupportException, "Sort Algorithm not support");
    }
  }
  _sorted = true;
}
} // namespace NativeTask
