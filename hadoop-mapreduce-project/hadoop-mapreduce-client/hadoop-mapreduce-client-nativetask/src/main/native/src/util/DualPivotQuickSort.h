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

#ifndef DUALPIVOTQUICKSORT_H_
#define DUALPIVOTQUICKSORT_H_

#include <stdint.h>
#include <algorithm>

namespace NativeTask {

// TODO: definitely needs refactoring..
template<typename _Compare>
void DualPivotQuicksort(std::vector<uint32_t> & elements, int left, int right, int div,
    _Compare compare) {

  if (left >= right) {
    return;
  }

  uint32_t * e = &(elements[0]);
  int len = right - left;

  if (len < 27) { // insertion sort for tiny array
    for (int i = left + 1; i <= right; i++) {
      for (int j = i; j > left && compare(e[j - 1], e[j]) > 0; --j) {
        std::swap(e[j], e[j - 1]);
      }
    }
    return;
  }

  int third = len / div;

  // "medians"
  int m1 = left + third;
  int m2 = right - third;

  if (m1 <= left) {
    m1 = left + 1;
  }
  if (m2 >= right) {
    m2 = right - 1;
  }
  if (compare(e[m1], e[m2]) < 0) {
    std::swap(e[m1], e[left]);
    std::swap(e[m2], e[right]);
  } else {
    std::swap(e[m1], e[right]);
    std::swap(e[m2], e[left]);
  }

  // pivot idx
  int pivot1 = left;
  int pivot2 = right;

  // pointers
  int less = left + 1;
  int great = right - 1;

  // sorting
  for (int k = less; k <= great; k++) {
    if (compare(e[k], e[pivot1]) < 0) {
      std::swap(e[k], e[less]);
      less++;
    } else if (compare(e[k], e[pivot2]) > 0) {
      while (k < great && compare(e[great], e[pivot2]) > 0) {
        --great;
      }
      std::swap(e[k], e[great]);
      great--;

      if (compare(e[k], e[pivot1]) < 0) {
        std::swap(e[k], e[less]);
        less++;
      }
    }
  }
  // swaps
  int dist = great - less;

  if (dist < 13) {
    ++div;
  }
  std::swap(e[less - 1], e[left]);
  std::swap(e[great + 1], e[right]);

  // subarrays
  DualPivotQuicksort(elements, left, less - 2, div, compare);
  DualPivotQuicksort(elements, great + 2, right, div, compare);

  // equal elements
  if (dist > len - 13 && pivot1 != pivot2) {
    for (int k = less; k <= great; ++k) {
      if (0 == compare(e[k], e[pivot1])) {
        std::swap(e[k], e[less]);
        less++;
      } else if (0 == compare(e[k], e[pivot2])) {
        std::swap(e[k], e[great]);
        great--;
        if (0 == compare(e[k], e[pivot1])) {
          std::swap(e[k], e[less]);
          less++;
        }
      }
    }
  }

  // subarray
  if (compare(e[pivot1], e[pivot2]) < 0) {
    DualPivotQuicksort(elements, less, great, div, compare);
  }
}

template<typename _Compare>
void DualPivotQuicksort(std::vector<uint32_t> & elements, _Compare compare) {
  DualPivotQuicksort(elements, 0, elements.size() - 1, 3, compare);
}

} // namespace NativeTask

#endif /* DUALPIVOTQUICKSORT_H_ */
