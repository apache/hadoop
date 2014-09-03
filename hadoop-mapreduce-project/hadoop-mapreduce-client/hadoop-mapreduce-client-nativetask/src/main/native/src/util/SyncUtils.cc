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
#include "lib/jniutils.h"
#include "util/StringUtil.h"
#include "util/SyncUtils.h"

namespace NativeTask {

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    THROW_EXCEPTION_EX(IOException, "pthread %s: %s", label, strerror(result));
  }
}

Lock::Lock() {
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  int ret = pthread_mutex_init(&_mutex, &attr);
  pthread_mutexattr_destroy(&attr);
  if (ret != 0) {
    THROW_EXCEPTION_EX(IOException, "pthread_mutex_init: %s", strerror(ret));
  }
}

Lock::~Lock() {
  PthreadCall("destroy mutex", pthread_mutex_destroy(&_mutex));
}

void Lock::lock() {
  PthreadCall("lock", pthread_mutex_lock(&_mutex));
}

void Lock::unlock() {
  PthreadCall("unlock", pthread_mutex_unlock(&_mutex));
}

} // namespace NativeTask
