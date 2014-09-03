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

#ifndef MIN_HEAP_H_
#define MIN_HEAP_H_

#include "NativeTask.h"
#include "lib/Buffers.h"

template<typename T, typename Compare>
void heapify(T* first, int rt, int heap_len, Compare & Comp) {
  while (rt * 2 <= heap_len) // not leaf
  {
    int left = (rt << 1); // left child
    int right = (rt << 1) + 1; // right child
    int smallest = rt;
    if (Comp(*(first + left - 1), *(first + smallest - 1))) {
      smallest = left;
    }
    if (right <= heap_len && Comp(*(first + right - 1), *(first + smallest - 1))) {
      smallest = right;
    }
    if (smallest != rt) {
      std::swap(*(first + smallest - 1), *(first + rt - 1));
      rt = smallest;
    } else {
      break;
    }
  }
}

template<typename T, typename Compare>
void makeHeap(T* begin, T* end, Compare & Comp) {
  int heap_len = end - begin;
  if (heap_len >= 0) {
    for (uint32_t i = heap_len / 2; i >= 1; i--) {
      heapify(begin, i, heap_len, Comp);
    }
  }
}

template<typename T, typename Compare>
void popHeap(T* begin, T* end, Compare & Comp) {
  *begin = *(end - 1);
  // adjust [begin, end - 1) to heap
  heapify(begin, 1, end - begin - 1, Comp);
}

#endif /* HEAP_H_ */
