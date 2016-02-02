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

#include "test_commons.h"

TEST(Primitives, fmemcmp) {
  std::vector<std::string> vs;
  char buff[14];
  vs.push_back("");
  for (uint32_t i = 0; i < 5000; i += 7) {
    snprintf(buff, 14, "%d", i * 31);
    vs.push_back(buff);
    snprintf(buff, 10, "%010d", i);
    vs.push_back(buff);
  }
  for (size_t i = 0; i < vs.size(); i++) {
    for (size_t j = 0; j < vs.size(); j++) {
      std::string & ls = vs[i];
      std::string & rs = vs[j];
      size_t m = std::min(ls.length(), rs.length());
      int c = memcmp(ls.c_str(), rs.c_str(), m);
      int t = fmemcmp(ls.c_str(), rs.c_str(), m);
      if (!((c == 0 && t == 0) || (c > 0 && t > 0) || (c < 0 && t < 0))) {
        ASSERT_TRUE(false);
      }
    }
  }
}

static int test_memcmp() {
  uint8_t buff[2048];
  for (uint32_t i = 0; i < 2048; i++) {
    buff[i] = i & 0xff;
  }
  std::random_shuffle(buff, buff + 2048);
  int r = 0;
  for (uint32_t i = 0; i < 100000000; i++) {
    int offset = i % 1000;
    r += memcmp(buff, buff + 1024, 5);
    r += memcmp(buff + offset, buff + 1124, 9);
    r += memcmp(buff + offset, buff + 1224, 10);
    r += memcmp(buff + offset, buff + 1324, 15);
    r += memcmp(buff + offset, buff + 1424, 16);
    r += memcmp(buff + offset, buff + 1524, 17);
    r += memcmp(buff + offset, buff + 1624, 18);
    r += memcmp(buff + offset, buff + 1724, 19);
  }
  return r;
}

static int test_fmemcmp() {
  char buff[2048];
  for (uint32_t i = 0; i < 2048; i++) {
    buff[i] = i & 0xff;
  }
  std::random_shuffle(buff, buff + 2048);
  int r = 0;
  for (uint32_t i = 0; i < 100000000; i++) {
    int offset = i % 1000;
    r += fmemcmp(buff, buff + 1024, 5);
    r += fmemcmp(buff + offset, buff + 1124, 9);
    r += fmemcmp(buff + offset, buff + 1224, 10);
    r += fmemcmp(buff + offset, buff + 1324, 15);
    r += fmemcmp(buff + offset, buff + 1424, 16);
    r += fmemcmp(buff + offset, buff + 1524, 17);
    r += fmemcmp(buff + offset, buff + 1624, 18);
    r += fmemcmp(buff + offset, buff + 1724, 19);
  }
  return r;
}

TEST(Perf, fmemcmp) {
  Timer t;
  int a = test_memcmp();
  LOG("%s", t.getInterval(" memcmp ").c_str());
  t.reset();
  int b = test_fmemcmp();
  LOG("%s", t.getInterval(" fmemcmp ").c_str());
  // prevent compiler optimization
  TestConfig.setInt("tempvalue", a + b);
}

static void test_memcpy_perf_len(char * src, char * dest, size_t len, size_t time) {
  for (size_t i = 0; i < time; i++) {
    memcpy(src, dest, len);
    memcpy(dest, src, len);
  }
}

static void test_simple_memcpy_perf_len(char * src, char * dest, size_t len, size_t time) {
  for (size_t i = 0; i < time; i++) {
    simple_memcpy(src, dest, len);
    simple_memcpy(dest, src, len);
  }
}

TEST(Perf, simple_memcpy_small) {
  char * src = new char[10240];
  char * dest = new char[10240];
  char buff[32];
  for (size_t len = 1; len < 256; len = len + 2) {
    LOG("------------------------------");
    snprintf(buff, 32, "       memcpy %luB\t", len);
    Timer t;
    test_memcpy_perf_len(src, dest, len, 1000000);
    LOG("%s", t.getInterval(buff).c_str());
    snprintf(buff, 32, "simple_memcpy %luB\t", len);
    t.reset();
    test_simple_memcpy_perf_len(src, dest, len, 1000000);
    LOG("%s", t.getInterval(buff).c_str());
  }
  delete[] src;
  delete[] dest;
}

inline char * memchrbrf4(char * p, char ch, size_t len) {
  ssize_t i = 0;
  for (; i < ((ssize_t)len) - 3; i += 3) {
    if (p[i] == ch) {
      return p + i;
    }
    if (p[i + 1] == ch) {
      return p + i + 1;
    }
    if (p[i + 2] == ch) {
      return p + i + 2;
    }
  }
  for (; i < (ssize_t)len; i++) {
    if (p[i] == ch) {
      return p + i;
    }
  }
  return NULL;
}

inline char * memchrbrf2(char * p, char ch, size_t len) {
  for (size_t i = 0; i < len / 2; i += 2) {
    if (p[i] == ch) {
      return p + i;
    }
    if (p[i + 1] == ch) {
      return p + i + 1;
    }
  }
  if (len % 2 && p[len - 1] == ch) {
    return p + len - 1;
  }
  return NULL;
}

// not safe in MACOSX, segment fault, should be safe on Linux with out mmap
inline int memchr_sse(const char *s, int c, int len) {
  // len : edx; c: esi; s:rdi
  int index = 0;

#ifdef __X64

  __asm__ __volatile__(
      //"and $0xff, %%esi;" //clear upper bytes
      "movd %%esi, %%xmm1;"

      "mov $1, %%eax;"
      "add $16, %%edx;"
      "mov %%rdi ,%%r8;"

      "1:"
      "movdqu (%%rdi), %%xmm2;"
      "sub $16, %%edx;"
      "addq $16, %%rdi;"
      //"pcmpestri $0x0, %%xmm2,%%xmm1;"
      ".byte 0x66 ,0x0f ,0x3a ,0x61 ,0xca ,0x00;"
      //"lea 16(%%rdi), %%rdi;"
      "ja 1b;"//Res2==0:no match and zflag==0: s is not end
      "jc 3f;"//Res2==1: match and s is not end

      "mov $0xffffffff, %%eax;"//no match
      "jmp 0f;"

      "3:"
      "sub %%r8, %%rdi;"
      "lea -16(%%edi,%%ecx),%%eax;"

      "0:"
      //        "mov %%eax, %0;"
      :"=a"(index),"=D"(s),"=S"(c),"=d"(len)
      :"D"(s),"S"(c),"d"(len)
      :"rcx","r8","memory"
  );

#endif

  return index;
}

TEST(Perf, memchr) {
  Random r;
  int32_t size = 100 * 1024 * 1024;
  int32_t lineLength = TestConfig.getInt("memchr.line.length", 100);
  char * buff = new char[size + 16];
  memset(buff, 'a', size);
  for (int i = 0; i < size / lineLength; i++) {
    buff[r.next_int32(size)] = '\n';
  }
  Timer timer;
  char * pos = buff;
  int count = 0;
  while (true) {
    if (pos == buff + size) {
      break;
    }
    pos = (char*)memchr(pos, '\n', buff + size - pos);
    if (pos == NULL) {
      break;
    }
    pos++;
    count++;
  }
  LOG("%s", timer.getSpeedM2("memchr bytes/lines", size, count).c_str());
  timer.reset();
  pos = buff;
  count = 0;
  while (true) {
    if (pos == buff + size) {
      break;
    }
    pos = (char*)memchrbrf2(pos, '\n', buff + size - pos);
    if (pos == NULL) {
      break;
    }
    pos++;
    count++;
  }
  LOG("%s", timer.getSpeedM2("memchrbrf2 bytes/lines", size, count).c_str());
  timer.reset();
  pos = buff;
  count = 0;
  while (true) {
    if (pos == buff + size) {
      break;
    }
    pos = (char*)memchrbrf4(pos, '\n', buff + size - pos);
    if (pos == NULL) {
      break;
    }
    pos++;
    count++;
  }
  LOG("%s", timer.getSpeedM2("memchrbrf4 bytes/lines", size, count).c_str());
  timer.reset();
  pos = buff;
  count = 0;
  while (true) {
    if (pos == buff + size) {
      break;
    }
    int ret = memchr_sse(pos, '\n', buff + size - pos);
    if (ret == -1) {
      break;
    }
    pos = pos + ret;
    pos++;
    count++;
  }
  LOG("%s", timer.getSpeedM2("memchr_sse bytes/lines", size, count).c_str());
  delete[] buff;
}

TEST(Perf, memcpy_batch) {
  int32_t size = TestConfig.getInt("input.size", 64 * 1024);
  size_t mb = TestConfig.getInt("input.mb", 320) * 1024 * 1024UL;
  char * src = new char[size];
  char * dest = new char[size];
  memset(src, 0, size);
  memset(dest, 0, size);
  Timer t;
  for (size_t i = 0; i < mb; i += size) {
    memcpy(dest, src, size);
  }
  LOG("%s", t.getSpeedM("memcpy", mb).c_str());
  t.reset();
  for (size_t i = 0; i < mb; i += size) {
    simple_memcpy(dest, src, size);
  }
  LOG("%s", t.getSpeedM("simple_memcpy", mb).c_str());
  delete[] src;
  delete[] dest;
}

