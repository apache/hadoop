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
#include "lib/Streams.h"
#include "lib/Buffers.h"
#include "util/DualPivotQuickSort.h"
#include "test_commons.h"

string gBuffer;

inline const char * get_position(uint32_t offset) {
  return gBuffer.data() + offset;
}

/**
 * fast memcmp
 */
inline int fmemcmporig(const char * src, const char * dest, uint32_t len) {
  const uint64_t * src8 = (const uint64_t*)src;
  const uint64_t * dest8 = (const uint64_t*)dest;
  while (len >= 8) {
    uint64_t l = *src8;
    uint64_t r = *dest8;
    if (l != r) {
      l = bswap64(l);
      r = bswap64(r);
      return l > r ? 1 : -1;
    }
    ++src8;
    ++dest8;
    len -= 8;
  }
  if (len == 0)
    return 0;
  if (len == 1) {
    int l = (int)(*(uint8_t*)src8);
    int r = (int)(*(uint8_t*)dest8);
    return l - r;
  }
  uint64_t mask = (1ULL << (len * 8)) - 1;
  uint64_t l = (*src8) & mask;
  uint64_t r = (*dest8) & mask;
  if (l == r) {
    return 0;
  }
  l = bswap64(l);
  r = bswap64(r);
  return l > r ? 1 : -1;
}

/**
 * c qsort compare function
 */
static int compare_offset(const void * plh, const void * prh) {
  KVBuffer * lhb = (KVBuffer*)get_position(*(uint32_t*)plh);
  KVBuffer * rhb = (KVBuffer*)get_position(*(uint32_t*)prh);
  uint32_t minlen = std::min(lhb->keyLength, rhb->keyLength);
  int ret = memcmp(lhb->getKey(), rhb->getKey(), minlen);
  if (ret) {
    return ret;
  }
  return lhb->keyLength - rhb->keyLength;
}

/**
 * dualpivot sort compare function
 */
class CompareOffset {
 public:
  int64_t operator()(uint32_t lhs, uint32_t rhs) {

    KVBuffer * lhb = (KVBuffer*)get_position(lhs);
    KVBuffer * rhb = (KVBuffer*)get_position(rhs);

    uint32_t minlen = std::min(lhb->keyLength, rhb->keyLength);
    int64_t ret = memcmp(lhb->getKey(), rhb->getKey(), minlen);
    if (ret) {
      return ret;
    }
    return lhb->keyLength - rhb->keyLength;
  }
};

/**
 * quicksort compare function
 */
class OffsetLessThan {
 public:
  bool operator()(uint32_t lhs, uint32_t rhs) {
    KVBuffer * lhb = (KVBuffer*)get_position(lhs);
    KVBuffer * rhb = (KVBuffer*)get_position(rhs);

    uint32_t minlen = std::min(lhb->keyLength, rhb->keyLength);
    int64_t ret = memcmp(lhb->content, rhb->content, minlen);
    return ret < 0 || (ret == 0 && (lhb->keyLength < rhb->keyLength));
  }
};

/**
 * c qsort compare function
 */
static int compare_offset2(const void * plh, const void * prh) {

  KVBuffer * lhb = (KVBuffer*)get_position(*(uint32_t*)plh);
  KVBuffer * rhb = (KVBuffer*)get_position(*(uint32_t*)prh);

  uint32_t minlen = std::min(lhb->keyLength, rhb->keyLength);
  int64_t ret = fmemcmp(lhb->content, rhb->content, minlen);
  if (ret) {
    return ret;
  }
  return lhb->keyLength - rhb->keyLength;
}

/**
 * dualpivot sort compare function
 */
class CompareOffset2 {
 public:
  int64_t operator()(uint32_t lhs, uint32_t rhs) {

    KVBuffer * lhb = (KVBuffer*)get_position(lhs);
    KVBuffer * rhb = (KVBuffer*)get_position(rhs);

    uint32_t minlen = std::min(lhb->keyLength, rhb->keyLength);
    int64_t ret = fmemcmp(lhb->content, rhb->content, minlen);
    if (ret) {
      return ret;
    }
    return lhb->keyLength - rhb->keyLength;
  }
};

/**
 * quicksort compare function
 */
class OffsetLessThan2 {
 public:
  bool operator()(uint32_t lhs, uint32_t rhs) {

    KVBuffer * lhb = (KVBuffer*)get_position(lhs);
    KVBuffer * rhb = (KVBuffer*)get_position(rhs);

    uint32_t minlen = std::min(lhb->keyLength, rhb->keyLength);
    int64_t ret = fmemcmp(lhb->content, rhb->content, minlen);
    return ret < 0 || (ret == 0 && (lhb->keyLength < rhb->keyLength));
  }
};

void makeInputWord(string & dest, vector<uint32_t> & offsets, uint64_t length) {
  Random r;
  dest.reserve(length + 1024);
  string k, v;
  while (true) {
    k = r.nextWord();
    v = r.nextWord();
    offsets.push_back(dest.length());
    uint32_t tempLen = k.length();
    dest.append((const char *)&tempLen, 4);
    dest.append(k.data(), k.length());
    tempLen = v.length();
    dest.append((const char *)&tempLen, 4);
    dest.append(v.data(), v.length());
    if (dest.length() > length) {
      return;
    }
  }
}

TEST(Perf, sort) {
  vector<uint32_t> offsets;
  makeInputWord(gBuffer, offsets, 80000000);
  Timer timer;
  vector<uint32_t> offsetstemp1_0 = offsets;
  vector<uint32_t> offsetstemp1_1 = offsets;
  vector<uint32_t> offsetstemp1_2 = offsets;
  vector<uint32_t> offsetstemp1_3 = offsets;
  timer.reset();
  qsort(&offsetstemp1_0[0], offsetstemp1_0.size(), sizeof(uint32_t), compare_offset);
  qsort(&offsetstemp1_1[0], offsetstemp1_1.size(), sizeof(uint32_t), compare_offset);
  qsort(&offsetstemp1_2[0], offsetstemp1_2.size(), sizeof(uint32_t), compare_offset);
  qsort(&offsetstemp1_3[0], offsetstemp1_3.size(), sizeof(uint32_t), compare_offset);
  LOG("%s", timer.getInterval("qsort").c_str());
  offsetstemp1_0 = offsets;
  offsetstemp1_1 = offsets;
  offsetstemp1_2 = offsets;
  offsetstemp1_3 = offsets;
  timer.reset();
  qsort(&offsetstemp1_0[0], offsetstemp1_0.size(), sizeof(uint32_t), compare_offset2);
  qsort(&offsetstemp1_1[0], offsetstemp1_1.size(), sizeof(uint32_t), compare_offset2);
  qsort(&offsetstemp1_2[0], offsetstemp1_2.size(), sizeof(uint32_t), compare_offset2);
  qsort(&offsetstemp1_3[0], offsetstemp1_3.size(), sizeof(uint32_t), compare_offset2);
  LOG("%s", timer.getInterval("qsort 2").c_str());
  offsetstemp1_0 = offsets;
  offsetstemp1_1 = offsets;
  offsetstemp1_2 = offsets;
  offsetstemp1_3 = offsets;
  timer.reset();
  std::sort(offsetstemp1_0.begin(), offsetstemp1_0.end(), OffsetLessThan());
  std::sort(offsetstemp1_1.begin(), offsetstemp1_1.end(), OffsetLessThan());
  std::sort(offsetstemp1_2.begin(), offsetstemp1_2.end(), OffsetLessThan());
  std::sort(offsetstemp1_3.begin(), offsetstemp1_3.end(), OffsetLessThan());
  LOG("%s", timer.getInterval("std::sort").c_str());
  offsetstemp1_0 = offsets;
  offsetstemp1_1 = offsets;
  offsetstemp1_2 = offsets;
  offsetstemp1_3 = offsets;
  timer.reset();
  std::sort(offsetstemp1_0.begin(), offsetstemp1_0.end(), OffsetLessThan2());
  std::sort(offsetstemp1_1.begin(), offsetstemp1_1.end(), OffsetLessThan2());
  std::sort(offsetstemp1_2.begin(), offsetstemp1_2.end(), OffsetLessThan2());
  std::sort(offsetstemp1_3.begin(), offsetstemp1_3.end(), OffsetLessThan2());
  LOG("%s", timer.getInterval("std::sort 2").c_str());
  offsetstemp1_0 = offsets;
  offsetstemp1_1 = offsets;
  offsetstemp1_2 = offsets;
  offsetstemp1_3 = offsets;
  timer.reset();
  DualPivotQuicksort(offsetstemp1_0, CompareOffset());
  DualPivotQuicksort(offsetstemp1_1, CompareOffset());
  DualPivotQuicksort(offsetstemp1_2, CompareOffset());
  DualPivotQuicksort(offsetstemp1_3, CompareOffset());
  LOG("%s", timer.getInterval("DualPivotQuicksort").c_str());
  offsetstemp1_0 = offsets;
  offsetstemp1_1 = offsets;
  offsetstemp1_2 = offsets;
  offsetstemp1_3 = offsets;
  timer.reset();
  DualPivotQuicksort(offsetstemp1_0, CompareOffset2());
  DualPivotQuicksort(offsetstemp1_1, CompareOffset2());
  DualPivotQuicksort(offsetstemp1_2, CompareOffset2());
  DualPivotQuicksort(offsetstemp1_3, CompareOffset2());
  LOG("%s", timer.getInterval("DualPivotQuicksort 2").c_str());
}

TEST(Perf, sortCacheMiss) {

  LOG("Testing partition based sort, sort 4MB every time");

  vector<uint32_t> offsets;
  makeInputWord(gBuffer, offsets, 80000000);
  Timer timer;
  vector<uint32_t> offsetstemp1_0 = offsets;
  vector<uint32_t> offsetstemp1_1 = offsets;
  vector<uint32_t> offsetstemp1_2 = offsets;
  vector<uint32_t> offsetstemp1_3 = offsets;

  timer.reset();
  DualPivotQuicksort(offsetstemp1_0, CompareOffset2());
  DualPivotQuicksort(offsetstemp1_1, CompareOffset2());
  DualPivotQuicksort(offsetstemp1_2, CompareOffset2());
  DualPivotQuicksort(offsetstemp1_3, CompareOffset2());
  LOG("%s", timer.getInterval("DualPivotQuicksort 2 full sort").c_str());

  uint32_t MOD = 128000;
  uint32_t END = offsets.size();

  for (MOD = 1024; MOD < END; MOD <<= 1) {
    offsetstemp1_0 = offsets;
    offsetstemp1_1 = offsets;
    offsetstemp1_2 = offsets;
    offsetstemp1_3 = offsets;
    timer.reset();

    for (uint32_t i = 0; i <= END / MOD; i++) {
      int base = i * MOD;
      int max = (base + MOD) > END ? END : (base + MOD);
      DualPivotQuicksort(offsetstemp1_0, base, max - 1, 3, CompareOffset2());
    }

    for (uint32_t i = 0; i <= END / MOD; i++) {
      int base = i * MOD;
      int max = (base + MOD) > END ? END : (base + MOD);
      DualPivotQuicksort(offsetstemp1_1, base, max - 1, 3, CompareOffset2());
    }

    for (uint32_t i = 0; i <= END / MOD; i++) {
      int base = i * MOD;
      int max = (base + MOD) > END ? END : (base + MOD);
      DualPivotQuicksort(offsetstemp1_2, base, max - 1, 3, CompareOffset2());
    }

    for (uint32_t i = 0; i <= END / MOD; i++) {
      int base = i * MOD;
      int max = (base + MOD) > END ? END : (base + MOD);
      DualPivotQuicksort(offsetstemp1_3, base, max - 1, 3, CompareOffset2());
    }
    LOG("%s, MOD: %d", timer.getInterval("DualPivotQuicksort 2 partition sort").c_str(), MOD);
  }
}
