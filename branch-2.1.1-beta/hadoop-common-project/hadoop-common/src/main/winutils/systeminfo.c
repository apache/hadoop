/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with this
* work for additional information regarding copyright ownership. The ASF
* licenses this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
* http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

#include "winutils.h"
#include <psapi.h>
#include <PowrProf.h>

#define PSAPI_VERSION 1
#pragma comment(lib, "psapi.lib")
#pragma comment(lib, "Powrprof.lib")

typedef struct _PROCESSOR_POWER_INFORMATION {
   ULONG  Number;
   ULONG  MaxMhz;
   ULONG  CurrentMhz;
   ULONG  MhzLimit;
   ULONG  MaxIdleState;
   ULONG  CurrentIdleState;
} PROCESSOR_POWER_INFORMATION, *PPROCESSOR_POWER_INFORMATION;

//----------------------------------------------------------------------------
// Function: SystemInfo
//
// Description:
// Returns the resource information about the machine 
//
// Returns:
// EXIT_SUCCESS: On success
// EXIT_FAILURE: otherwise
int SystemInfo() 
{
  size_t vmemSize, vmemFree, memSize, memFree;
  PERFORMANCE_INFORMATION memInfo;
  SYSTEM_INFO sysInfo;
  FILETIME idleTimeFt, kernelTimeFt, userTimeFt;
  ULARGE_INTEGER idleTime, kernelTime, userTime;
  ULONGLONG cpuTimeMs;
  size_t size;
  LPBYTE pBuffer;
  PROCESSOR_POWER_INFORMATION const *ppi;
  ULONGLONG cpuFrequencyKhz;
  NTSTATUS status;

  ZeroMemory(&memInfo, sizeof(PERFORMANCE_INFORMATION));
  memInfo.cb = sizeof(PERFORMANCE_INFORMATION);
  if(!GetPerformanceInfo(&memInfo, sizeof(PERFORMANCE_INFORMATION)))
  {
    ReportErrorCode(L"GetPerformanceInfo", GetLastError());
    return EXIT_FAILURE;
  }
  vmemSize = memInfo.CommitLimit*memInfo.PageSize;
  vmemFree = vmemSize - memInfo.CommitTotal*memInfo.PageSize;
  memSize = memInfo.PhysicalTotal*memInfo.PageSize;
  memFree = memInfo.PhysicalAvailable*memInfo.PageSize;

  GetSystemInfo(&sysInfo);

  if(!GetSystemTimes(&idleTimeFt, &kernelTimeFt, &userTimeFt))
  {
    ReportErrorCode(L"GetSystemTimes", GetLastError());
    return EXIT_FAILURE;
  }
  idleTime.HighPart = idleTimeFt.dwHighDateTime;
  idleTime.LowPart = idleTimeFt.dwLowDateTime;
  kernelTime.HighPart = kernelTimeFt.dwHighDateTime;
  kernelTime.LowPart = kernelTimeFt.dwLowDateTime;
  userTime.HighPart = userTimeFt.dwHighDateTime;
  userTime.LowPart = userTimeFt.dwLowDateTime;

  cpuTimeMs = (kernelTime.QuadPart - idleTime.QuadPart + userTime.QuadPart)/10000;

  // allocate buffer to get info for each processor
  size = sysInfo.dwNumberOfProcessors * sizeof(PROCESSOR_POWER_INFORMATION);
  pBuffer = (BYTE*) LocalAlloc(LPTR, size);
  if(!pBuffer)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    return EXIT_FAILURE;
  }
  status = CallNtPowerInformation(ProcessorInformation, NULL, 0, pBuffer, (long)size);
  if(0 != status)
  {
    fwprintf_s(stderr, L"Error in CallNtPowerInformation. Err:%d\n", status);
    LocalFree(pBuffer);
    return EXIT_FAILURE;
  }
  ppi = (PROCESSOR_POWER_INFORMATION const *)pBuffer;
  cpuFrequencyKhz = ppi->MaxMhz*1000;
  LocalFree(pBuffer);

  fwprintf_s(stdout, L"%Iu,%Iu,%Iu,%Iu,%u,%I64u,%I64u\n", vmemSize, memSize,
    vmemFree, memFree, sysInfo.dwNumberOfProcessors, cpuFrequencyKhz, cpuTimeMs);

  return EXIT_SUCCESS;
}

void SystemInfoUsage()
{
    fwprintf(stdout, L"\
    Usage: systeminfo\n\
    Prints machine information on stdout\n\
    Comma separated list of the following values.\n\
    VirtualMemorySize(bytes),PhysicalMemorySize(bytes),\n\
    FreeVirtualMemory(bytes),FreePhysicalMemory(bytes),\n\
    NumberOfProcessors,CpuFrequency(Khz),\n\
    CpuTime(MilliSec,Kernel+User)\n");
}