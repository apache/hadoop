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

#include "os/thread.h"

#include <stdio.h>
#include <windows.h>

/**
 * Defines a helper function that adapts function pointer provided by caller to
 * the type required by CreateThread.
 *
 * @param toRun thread to run
 * @return DWORD result of running thread (always 0)
 */
static DWORD runThread(LPVOID toRun) {
  const thread *t = toRun;
  t->start(t->arg);
  return 0;
}

int threadCreate(thread *t) {
  DWORD ret = 0;
  HANDLE h;
  h = CreateThread(NULL, 0, runThread, t, 0, NULL);
  if (h) {
    t->id = h;
  } else {
    ret = GetLastError();
    fprintf(stderr, "threadCreate: CreateThread failed with error %d\n", ret);
  }
  return ret;
}

int threadJoin(const thread *t) {
  DWORD ret = WaitForSingleObject(t->id, INFINITE);
  switch (ret) {
  case WAIT_OBJECT_0:
    break;
  case WAIT_FAILED:
    ret = GetLastError();
    fprintf(stderr, "threadJoin: WaitForSingleObject failed with error %d\n",
      ret);
    break;
  default:
    fprintf(stderr, "threadJoin: WaitForSingleObject unexpected error %d\n",
      ret);
    break;
  }
  return ret;
}
