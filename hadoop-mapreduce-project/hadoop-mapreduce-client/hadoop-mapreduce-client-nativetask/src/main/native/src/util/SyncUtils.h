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

#ifndef SYNCUTILS_H_
#define SYNCUTILS_H_

#include <unistd.h>
#include <string.h>
#ifdef __MACH__
#include <libkern/OSAtomic.h>
#endif
#include <pthread.h>

namespace NativeTask {

class Condition;

class Lock {
public:
  Lock();
  ~Lock();

  void lock();
  void unlock();

private:
  friend class Condition;
  pthread_mutex_t _mutex;

  // No copying
  Lock(const Lock&);
  void operator=(const Lock&);
};

template<typename LockT>
class ScopeLock {
public:
  ScopeLock(LockT & lock)
      : _lock(&lock) {
    _lock->lock();
  }
  ~ScopeLock() {
    _lock->unlock();
  }
private:
  LockT * _lock;

  // No copying
  ScopeLock(const ScopeLock&);
  void operator=(const ScopeLock&);
};


} // namespace NativeTask

#endif /* SYNCUTILS_H_ */
