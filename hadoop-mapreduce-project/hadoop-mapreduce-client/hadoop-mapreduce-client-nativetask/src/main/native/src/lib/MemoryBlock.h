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
#include "commons.h"

#ifndef MEMORYBLOCK_H_
#define MEMORYBLOCK_H_

namespace NativeTask {

class MemoryPool;

class ComparatorForDualPivotSort {
private:
  const char * _base;
  ComparatorPtr _keyComparator;
public:
  ComparatorForDualPivotSort(const char * base, ComparatorPtr comparator)
      : _base(base), _keyComparator(comparator) {
  }

  inline int operator()(uint32_t lhs, uint32_t rhs) {
    KVBuffer * left = (KVBuffer *)(_base + lhs);
    KVBuffer * right = (KVBuffer *)(_base + rhs);
    return (*_keyComparator)(left->content, left->keyLength, right->content, right->keyLength);
  }
};

class ComparatorForStdSort {
private:
  const char * _base;
  ComparatorPtr _keyComparator;
public:
  ComparatorForStdSort(const char * base, ComparatorPtr comparator)
      : _base(base), _keyComparator(comparator) {
  }

public:
  inline bool operator()(uint32_t lhs, uint32_t rhs) {
    KVBuffer * left = (KVBuffer *)(_base + lhs);
    KVBuffer * right = (KVBuffer *)(_base + rhs);
    int ret = (*_keyComparator)(left->getKey(), left->keyLength, right->getKey(), right->keyLength);
    return ret < 0;
  }
};

class MemoryBlock {
private:
  char * _base;
  uint32_t _size;
  uint32_t _position;
  std::vector<uint32_t> _kvOffsets;
  bool _sorted;

public:
  MemoryBlock(char * pos, uint32_t size);

  char * base() {
    return _base;
  }

  bool sorted() {
    return _sorted;
  }

  KVBuffer * allocateKVBuffer(uint32_t length) {
    if (length > remainSpace()) {
      LOG("Unable to allocate kv from memory buffer, length: %d, remain: %d", length, remainSpace());
      return NULL;
    }
    _sorted = false;
    _kvOffsets.push_back(_position);
    char * space = _base + _position;
    _position += length;
    return (KVBuffer *)space;
  }

  uint32_t remainSpace() const {
    return _size - _position;
  }

  uint32_t getKVCount() {
    return _kvOffsets.size();
  }

  KVBuffer * getKVBuffer(uint32_t index);

  void sort(SortAlgorithm type, ComparatorPtr comparator);
};
//class MemoryBlock

class MemBlockIterator {
private:
  MemoryBlock * _memBlock;
  uint32_t _end;
  uint32_t _current;
  KVBuffer * _kvBuffer;

public:

  MemBlockIterator(MemoryBlock * memBlock)
      : _memBlock(memBlock), _end(0), _current(0), _kvBuffer(NULL) {
    _end = memBlock->getKVCount();
  }

  KVBuffer * getKVBuffer() {
    return _kvBuffer;
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  bool next() {
    if (_current >= _end) {
      return false;
    }
    this->_kvBuffer = _memBlock->getKVBuffer(_current);
    ++_current;
    return true;
  }
};
//class MemoryBlockIterator

typedef MemBlockIterator * MemBlockIteratorPtr;

class MemBlockComparator {
private:
  ComparatorPtr _keyComparator;

public:
  MemBlockComparator(ComparatorPtr comparator)
      : _keyComparator(comparator) {
  }

public:
  bool operator()(const MemBlockIteratorPtr lhs, const MemBlockIteratorPtr rhs) {

    KVBuffer * left = lhs->getKVBuffer();
    KVBuffer * right = rhs->getKVBuffer();

    //Treat NULL as infinite MAX, so that we can pop out next value
    if (NULL == left) {
      return false;
    }

    if (NULL == right) {
      return true;
    }

    return (*_keyComparator)(left->content, left->keyLength, right->content, right->keyLength) < 0;
  }
};

} //namespace NativeTask

#endif /* MEMORYBLOCK_H_ */
