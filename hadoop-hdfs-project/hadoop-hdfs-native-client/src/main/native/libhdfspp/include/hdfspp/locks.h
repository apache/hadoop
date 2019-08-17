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

#ifndef COMMON_HDFS_LOCKS_H_
#define COMMON_HDFS_LOCKS_H_

#include <stdexcept>
#include <string>
#include <atomic>
#include <mutex>
#include <memory>

namespace hdfs
{

//
//  Thrown by LockGuard to indicate that it was unable to acquire a mutex
//  what_str should contain info about what caused the failure
//
class LockFailure : public std::runtime_error {
 public:
  LockFailure(const char *what_str) : std::runtime_error(what_str) {};
  LockFailure(const std::string& what_str) : std::runtime_error(what_str) {};
};

//
//  A pluggable mutex type to allow client code to share mutexes it may
//  already use to protect certain system resources.  Certain shared
//  libraries have some procedures that aren't always implemented in a thread
//  safe manner. If libhdfs++ and the code linking it depend on the same
//  library this provides a mechanism to coordinate safe access.
//
//  Interface provided is intended to be similar to std::mutex.  If the lock
//  can't be aquired it may throw LockFailure from the lock method. If lock
//  does fail libhdfs++ is expected fail as cleanly as possible e.g.
//  FileSystem::Mkdirs might return a MutexError but a subsequent call may be
//  successful.
//
class Mutex {
 public:
  virtual ~Mutex() {};
  virtual void lock() = 0;
  virtual void unlock() = 0;
  virtual std::string str() = 0;
};

//
//  LockGuard works in a similar manner to std::lock_guard: it locks the mutex
//  in the constructor and unlocks it in the destructor.
//  Failure to acquire the mutex in the constructor will result in throwing a
//  LockFailure exception.
//
class LockGuard {
 public:
  LockGuard(Mutex *m);
  ~LockGuard();
 private:
  Mutex *_mtx;
};

//
//  Manage instances of hdfs::Mutex that are intended to be global to the
//  process.
//
//  LockManager's InitLocks method provides a mechanism for the calling
//  application to share its own implementations of hdfs::Mutex.  It must be
//  called prior to instantiating any FileSystem objects and can only be
//  called once.  If a lock is not provided a default mutex type wrapping
//  std::mutex is used as a default.
//

class LockManager {
 public:
  // Initializes with a default set of C++11 style mutexes
  static bool InitLocks(Mutex *gssapi);
  static Mutex *getGssapiMutex();

  // Tests only, implementation may no-op on release builds.
  // Reset _finalized to false and set all Mutex* members to default values.
  static void TEST_reset_manager();
  static Mutex *TEST_get_default_mutex();
 private:
  // Used only in tests.
  static Mutex *TEST_default_mutex;
  // Use to synchronize calls into GSSAPI/Kerberos libs
  static Mutex *gssapiMtx;

  // Prevent InitLocks from being called more than once
  // Allows all locks to be set a single time atomically
  static std::mutex _state_lock;
  static bool _finalized;
};

} // end namespace hdfs
#endif
