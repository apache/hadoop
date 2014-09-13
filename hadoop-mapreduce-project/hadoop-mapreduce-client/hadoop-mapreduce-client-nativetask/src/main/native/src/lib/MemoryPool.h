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

#ifndef MEMORYPOOL_H_
#define MEMORYPOOL_H_

#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "NativeTask.h"
#include "util/StringUtil.h"

namespace NativeTask {

/**
 * Class for allocating memory buffer
 */

class MemoryPool {
private:
  char * _base;
  uint32_t _capacity;
  uint32_t _used;

public:

  MemoryPool()
      : _base(NULL), _capacity(0), _used(0) {
  }

  ~MemoryPool() {
    if (NULL != _base) {
      free(_base);
      _base = NULL;
    }
  }

  void init(uint32_t capacity) throw (OutOfMemoryException) {
    if (capacity > _capacity) {
      if (NULL != _base) {
        free(_base);
        _base = NULL;
      }
      _base = (char*)malloc(capacity);
      if (NULL == _base) {
        THROW_EXCEPTION(OutOfMemoryException, "Not enough memory to init MemoryBlockPool");
      }
      _capacity = capacity;
    }
    reset();
  }

  void reset() {
    _used = 0;
  }

  char * allocate(uint32_t min, uint32_t expect, uint32_t & allocated) {
    if (_used + min > _capacity) {
      return NULL;
    } else if (_used + expect > _capacity) {
      char * buff = _base + _used;
      allocated = min;
      _used += min;
      return buff;
    } else {
      char * buff = _base + _used;
      allocated = expect;
      _used += expect;
      return buff;
    }
  }
};

} // namespace NativeTask

#endif /* MEMORYPOOL_H_ */
