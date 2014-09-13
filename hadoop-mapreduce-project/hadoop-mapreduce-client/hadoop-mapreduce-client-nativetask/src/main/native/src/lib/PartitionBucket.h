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

#ifndef PARTITION_BUCKET_H_
#define PARTITION_BUCKET_H_

#include "NativeTask.h"
#include "lib/MemoryPool.h"
#include "lib/MemoryBlock.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"

namespace NativeTask {

/**
 * Buffer for a single partition
 */
class PartitionBucket {
  friend class PartitionBucketIterator;
  friend class TestPartitionBucket;

private:
  std::vector<MemoryBlock *> _memBlocks;
  MemoryPool * _pool;
  uint32_t _partition;
  uint32_t _blockSize;
  ComparatorPtr _keyComparator;
  ICombineRunner * _combineRunner;
  bool _sorted;

public:
  PartitionBucket(MemoryPool * pool, uint32_t partition, ComparatorPtr comparator,
      ICombineRunner * combineRunner, uint32_t blockSize)
      : _pool(pool), _partition(partition), _blockSize(blockSize),
          _keyComparator(comparator), _combineRunner(combineRunner),  _sorted(false) {
    if (NULL == _pool || NULL == comparator) {
      THROW_EXCEPTION_EX(IOException, "pool is NULL, or comparator is not set");
    }

    if (NULL != combineRunner) {
      LOG("[PartitionBucket] combine runner has been set");
    }
  }

  ~PartitionBucket() {
    reset();
  }

  uint32_t getPartitionId() {
    return _partition;
  }

  void reset() {
    for (uint32_t i = 0; i < _memBlocks.size(); i++) {
      if (NULL != _memBlocks[i]) {
        delete _memBlocks[i];
        _memBlocks[i] = NULL;
      }
    }
    _memBlocks.clear();
  }

  KVIterator * getIterator();

  uint32_t getKVCount() const {
    uint32_t size = 0;
    for (uint32_t i = 0; i < _memBlocks.size(); i++) {
      MemoryBlock * block = _memBlocks[i];
      if (NULL != block) {
        size += block->getKVCount();
      }
    }
    return size;
  }

  /**
   * @throws OutOfMemoryException if total_length > io.sort.mb
   */
  KVBuffer * allocateKVBuffer(uint32_t kvLength) {
    if (kvLength == 0) {
      LOG("KV Length is empty, no need to allocate buffer for it");
      return NULL;
    }
    _sorted = false;
    MemoryBlock * memBlock = NULL;
    uint32_t memBlockSize = _memBlocks.size();
    if (memBlockSize > 0) {
      memBlock = _memBlocks[memBlockSize - 1];
    }
    if (NULL != memBlock && memBlock->remainSpace() >= kvLength) {
      return memBlock->allocateKVBuffer(kvLength);
    } else {
      uint32_t min = kvLength;
      uint32_t expect = std::max(_blockSize, min);
      uint32_t allocated = 0;
      char * buff = _pool->allocate(min, expect, allocated);
      if (NULL != buff) {
        memBlock = new MemoryBlock(buff, allocated);
        _memBlocks.push_back(memBlock);
        return memBlock->allocateKVBuffer(kvLength);
      }
    }
    return NULL;
  }

  void sort(SortAlgorithm type);

  void spill(IFileWriter * writer) throw (IOException, UnsupportException);

  uint32_t getMemoryBlockCount() const {
    return _memBlocks.size();
  }

  MemoryBlock * getMemoryBlock(uint32_t index) const {
    return _memBlocks[index];
  }
};

}
;
//namespace NativeTask

#endif /* PARTITION_BUCKET_H_ */
