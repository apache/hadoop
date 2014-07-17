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

#include "util/SyncUtils.h"
#include "test_commons.h"

class TestThread : public Thread {
  virtual void run() {
    for (uint32_t i = 0; i < 5; i++) {
      usleep(100);
      LOG("sleep %d", i * 100);
    }
  }
};

TEST(SyncUtil, Thread) {
  TestThread a, b, c;
  a.start();
  b.start();
  c.start();
  a.join();
  b.join();
  c.join();
}

class TestBind {
public:

  int get() {
    return 100;
  }

  void foo() {
    for (uint32_t i = 0; i < 2; i++) {
      usleep(100);
      LOG("usleep %d", i * 100);
    }
  }
  void bar(const char * msg) {
    for (uint32_t i = 0; i < 2; i++) {
      usleep(100);
      LOG("usleep %d %s", i * 100, msg);
    }
  }

};

TEST(SyncUtil, ThreadBind) {
  TestBind a = TestBind();
  Runnable * bind1 = BindNew(a, &TestBind::get);
  Thread t = Thread(bind1);
  Runnable * bind2 = BindNew(a, &TestBind::bar, "testmsg");
  Thread t2 = Thread(bind2);
  t.start();
  t2.start();
  t.join();
  t2.join();

  delete bind1;
  delete bind2;
}

//class TestParallelFor {
//protected:
//  SpinLock lock;
//  uint64_t aggr;
//public:
//  TestParallelFor() : aggr(0) {
//  }
//  void add(uint64_t i) {
//    lock.lock();
//    aggr += i;
//    lock.unlock();
//  }
//  void test(uint64_t n, size_t threadnum) {
//    aggr = 0;
//    ParallelFor(*this, &TestParallelFor::add, 0ULL, n, threadnum);
//    ASSERT_EQ(n*(n-1)/2, aggr);
//  }
//};
//
//TEST(SyncUtil, ParallelFor) {
//  TestParallelFor tpf;
//  tpf.test(100000, 2);
//  tpf.test(100000, 3);
//  tpf.test(100000, 4);
//}

TEST(Perf, ThreadOverhead) {
  int64_t threadnum = TestConfig.getInt("thread.num", 1000);
  Thread * t = new Thread[threadnum];
  Timer timer;
  for (uint32_t i = 0; i < threadnum; i++) {
    t[i].start();
  }
  for (uint32_t i = 0; i < threadnum; i++) {
    t[i].join();
  }
  LOG("%lld thread %s", (long long int )threadnum, timer.getInterval("start&join").c_str());
  delete[] t;
}
