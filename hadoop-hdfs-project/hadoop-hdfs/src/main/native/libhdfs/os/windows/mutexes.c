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

#include <windows.h>

mutex hdfsHashMutex;
mutex jvmMutex;

/**
 * Unfortunately, there is no simple static initializer for a critical section.
 * Instead, the API requires calling InitializeCriticalSection.  Since libhdfs
 * lacks an explicit initialization function, there is no obvious existing place
 * for the InitializeCriticalSection calls.  To work around this, we define an
 * initialization function and instruct the linker to set a pointer to that
 * function as a user-defined global initializer.  See discussion of CRT
 * Initialization:
 * http://msdn.microsoft.com/en-us/library/bb918180.aspx
 */
static void __cdecl initializeMutexes(void) {
  InitializeCriticalSection(&hdfsHashMutex);
  InitializeCriticalSection(&jvmMutex);
}
#pragma section(".CRT$XCU", read)
__declspec(allocate(".CRT$XCU"))
const void (__cdecl *pInitialize)(void) = initializeMutexes;

int mutexLock(mutex *m) {
  EnterCriticalSection(m);
  return 0;
}

int mutexUnlock(mutex *m) {
  LeaveCriticalSection(m);
  return 0;
}
