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

#include "hdfspp/locks.h"

#include <mutex>


namespace hdfs {

LockGuard::LockGuard(Mutex *m) : _mtx(m) {
  if(!m) {
    throw LockFailure("LockGuard passed invalid (null) Mutex pointer");
  }
  _mtx->lock();
}

LockGuard::~LockGuard() {
  if(_mtx) {
    _mtx->unlock();
  }
}


// Basic mutexes to use as default.  Just a wrapper around C++11 std::mutex.
class DefaultMutex : public Mutex {
 public:
  DefaultMutex() {}

  void lock() override {
    // Could throw in here if the implementation couldn't lock for some reason.
    _mtx.lock();
  }

  void unlock() override {
    _mtx.unlock();
  }

  std::string str() override {
    return "DefaultMutex";
  }
 private:
  std::mutex _mtx;
};

DefaultMutex defaultTestMutex;
DefaultMutex defaultGssapiMutex;

// LockManager static var instantiation
Mutex *LockManager::TEST_default_mutex = &defaultTestMutex;
Mutex *LockManager::gssapiMtx = &defaultGssapiMutex;
std::mutex LockManager::_state_lock;
bool LockManager::_finalized = false;

bool LockManager::InitLocks(Mutex *gssapi) {
  std::lock_guard<std::mutex> guard(_state_lock);

  // You get once shot to set this - swapping the locks
  // out while in use gets risky.  It can still be done by
  // using the Mutex as a proxy object if one understands
  // the implied risk of doing so.
  if(_finalized)
    return false;

  gssapiMtx = gssapi;
  _finalized = true;
  return true;
}

Mutex *LockManager::getGssapiMutex() {
  std::lock_guard<std::mutex> guard(_state_lock);
  return gssapiMtx;
}

Mutex *LockManager::TEST_get_default_mutex() {
  return TEST_default_mutex;
}

void LockManager::TEST_reset_manager() {
  _finalized = false;
  // user still responsible for cleanup
  gssapiMtx = &defaultGssapiMutex;
}

} // end namepace hdfs
