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

#include "os/mutexes.h"

#include <pthread.h>
#include <stdio.h>

mutex jvmMutex;
mutex jclassInitMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutexattr_t jvmMutexAttr;

__attribute__((constructor)) static void init() {
  pthread_mutexattr_init(&jvmMutexAttr);
  pthread_mutexattr_settype(&jvmMutexAttr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&jvmMutex, &jvmMutexAttr);
}

int mutexLock(mutex *m) {
  int ret = pthread_mutex_lock(m);
  if (ret) {
    fprintf(stderr, "mutexLock: pthread_mutex_lock failed with error %d\n",
      ret);
  }
  return ret;
}

int mutexUnlock(mutex *m) {
  int ret = pthread_mutex_unlock(m);
  if (ret) {
    fprintf(stderr, "mutexUnlock: pthread_mutex_unlock failed with error %d\n",
      ret);
  }
  return ret;
}
