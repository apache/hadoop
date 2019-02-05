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

#include <hdfspp/locks.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using namespace hdfs;

// try_lock will always return false, unlock will always throw because it
// can never be locked.
class CantLockMutex : public Mutex {
 public:
  void lock() override {
    throw LockFailure("This mutex cannot be locked");
  }
  void unlock() override {
    throw LockFailure("Unlock");
  }
  std::string str() override {
    return "CantLockMutex";
  }
};

TEST(UserLockTest, DefaultMutexBasics) {
  Mutex *mtx = LockManager::TEST_get_default_mutex();

  // lock and unlock twice to make sure unlock works
  bool locked = false;
  try {
    mtx->lock();
    locked = true;
  } catch (...) {}
  EXPECT_TRUE(locked);
  mtx->unlock();

  locked = false;
  try {
    mtx->lock();
    locked = true;
  } catch (...) {}
  EXPECT_TRUE(locked);
  mtx->unlock();

  EXPECT_EQ(mtx->str(), "DefaultMutex");
}


// Make sure lock manager can only be initialized once unless test reset called
TEST(UserLockTest, LockManager) {
  std::unique_ptr<CantLockMutex> mtx(new CantLockMutex());
  EXPECT_TRUE(mtx != nullptr);

  // Check the default lock
  Mutex *defaultGssapiMtx = LockManager::getGssapiMutex();
  EXPECT_TRUE(defaultGssapiMtx != nullptr);

  // Try a double init.  Should not work
  bool res = LockManager::InitLocks(mtx.get());
  EXPECT_TRUE(res);

  // Check pointer value
  EXPECT_EQ(LockManager::getGssapiMutex(), mtx.get());

  res = LockManager::InitLocks(mtx.get());
  EXPECT_FALSE(res);

  // Make sure test reset still works
  LockManager::TEST_reset_manager();
  res = LockManager::InitLocks(mtx.get());
  EXPECT_TRUE(res);
  LockManager::TEST_reset_manager();
  EXPECT_EQ(LockManager::getGssapiMutex(), defaultGssapiMtx);
}

TEST(UserLockTest, CheckCantLockMutex) {
  std::unique_ptr<CantLockMutex> mtx(new CantLockMutex());
  EXPECT_TRUE(mtx != nullptr);

  bool locked = false;
  try {
    mtx->lock();
  } catch (...) {}
  EXPECT_FALSE(locked);

  bool threw_on_unlock = false;
  try {
    mtx->unlock();
  } catch (const LockFailure& e) {
    threw_on_unlock = true;
  }
  EXPECT_TRUE(threw_on_unlock);

  EXPECT_EQ("CantLockMutex", mtx->str());
}

TEST(UserLockTest, LockGuardBasics) {
  Mutex *goodMtx = LockManager::TEST_get_default_mutex();
  CantLockMutex badMtx;

  // lock/unlock a few times to increase chances of UB if lock is misused
  for(int i=0;i<10;i++) {
    bool caught_exception = false;
    try {
      LockGuard guard(goodMtx);
      // now have a scoped lock
    } catch (const LockFailure& e) {
      caught_exception = true;
    }
    EXPECT_FALSE(caught_exception);
  }

  // still do a few times, but expect it to blow up each time
  for(int i=0;i<10;i++) {
    bool caught_exception = false;
    try {
      LockGuard guard(&badMtx);
      // now have a scoped lock
    } catch (const LockFailure& e) {
      caught_exception = true;
    }
    EXPECT_TRUE(caught_exception);
  }

}

struct Incrementer {
  int64_t& _val;
  int64_t _iters;
  Mutex *_mtx;
  Incrementer(int64_t &val, int64_t iters, Mutex *m)
        : _val(val), _iters(iters), _mtx(m) {}
  void operator()(){
    for(int64_t i=0; i<_iters; i++) {
      LockGuard valguard(_mtx);
      _val += 1;
    }
  }
};

struct Decrementer {
  int64_t& _val;
  int64_t _iters;
  Mutex *_mtx;
  Decrementer(int64_t &val, int64_t iters, Mutex *m)
        : _val(val), _iters(iters), _mtx(m) {}
  void operator()(){
    for(int64_t i=0; i<_iters; i++) {
      LockGuard valguard(_mtx);
      _val -= 1;
    }
  }
};

TEST(UserLockTest, LockGuardConcurrency) {
  Mutex *mtx = LockManager::TEST_get_default_mutex();

  // Prove that these actually mutate the value
  int64_t test_value = 0;
  Incrementer inc(test_value, 1000, mtx);
  inc();
  EXPECT_EQ(test_value, 1000);

  Decrementer dec(test_value, 1000, mtx);
  dec();
  EXPECT_EQ(test_value, 0);

  std::vector<std::thread> workers;
  std::vector<Incrementer> incrementers;
  std::vector<Decrementer> decrementors;

  const int delta = 1024 * 1024;
  const int threads = 2 * 6;
  EXPECT_EQ(threads % 2, 0);

  // a bunch of threads race to increment and decrement the value
  // if all goes well the operations balance out and the value is unchanged
  for(int i=0; i < threads; i++) {
    if(i%2 == 0) {
      incrementers.emplace_back(test_value, delta, mtx);
      workers.emplace_back(incrementers.back());
    } else {
      decrementors.emplace_back(test_value, delta, mtx);
      workers.emplace_back(decrementors.back());
    }
  }

  // join, everything should balance to 0
  for(std::thread& thread : workers) {
    thread.join();
  }
  EXPECT_EQ(test_value, 0);
}


int main(int argc, char *argv[]) {

  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int res = RUN_ALL_TESTS();
  return res;
}
