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
#include <pdh.h>
#include <pdhmsg.h>

#ifdef PSAPI_VERSION
#undef PSAPI_VERSION
#endif
#define PSAPI_VERSION 1
#pragma comment(lib, "psapi.lib")
#pragma comment(lib, "Powrprof.lib")
#pragma comment(lib, "pdh.lib")

CONST PWSTR COUNTER_PATH_NET_READ_ALL   = L"\\Network Interface(*)\\Bytes Received/Sec";
CONST PWSTR COUNTER_PATH_NET_WRITE_ALL  = L"\\Network Interface(*)\\Bytes Sent/Sec";
CONST PWSTR COUNTER_PATH_DISK_READ_ALL  = L"\\LogicalDisk(*)\\Disk Read Bytes/sec";
CONST PWSTR COUNTER_PATH_DISK_WRITE_ALL = L"\\LogicalDisk(*)\\Disk Write Bytes/sec";

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
  LONGLONG diskRead, diskWrite, netRead, netWrite;

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

  status = GetDiskAndNetwork(&diskRead, &diskWrite, &netRead, &netWrite);
  if(0 != status)
  {
    fwprintf_s(stderr, L"Error in GetDiskAndNetwork. Err:%d\n", status);
    return EXIT_FAILURE;
  }

  fwprintf_s(stdout, L"%Iu,%Iu,%Iu,%Iu,%u,%I64u,%I64u,%I64d,%I64d,%I64d,%I64d\n", vmemSize, memSize,
    vmemFree, memFree, sysInfo.dwNumberOfProcessors, cpuFrequencyKhz, cpuTimeMs,
    diskRead, diskWrite, netRead, netWrite);

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
    CpuTime(MilliSec,Kernel+User),\n\
    DiskRead(bytes),DiskWrite(bytes),\n\
    NetworkRead(bytes),NetworkWrite(bytes)\n");
}

int GetDiskAndNetwork(LONGLONG* diskRead, LONGLONG* diskWrite, LONGLONG* netRead, LONGLONG* netWrite)
{
  int ret = EXIT_SUCCESS;
  PDH_STATUS status = ERROR_SUCCESS;
  PDH_HQUERY hQuery = NULL;
  PDH_HCOUNTER hCounterNetRead = NULL;
  PDH_HCOUNTER hCounterNetWrite = NULL;
  PDH_HCOUNTER hCounterDiskRead = NULL;
  PDH_HCOUNTER hCounterDiskWrite = NULL;
  DWORD i;

  if(status = PdhOpenQuery(NULL, 0, &hQuery))
  {
    fwprintf_s(stderr, L"PdhOpenQuery failed with 0x%x.\n", status);
    ret = EXIT_FAILURE;
    goto cleanup;
  }

  // Add each one of the counters with wild cards
  if(status = PdhAddCounter(hQuery, COUNTER_PATH_NET_READ_ALL, 0, &hCounterNetRead))
  {
    fwprintf_s(stderr, L"PdhAddCounter %s failed with 0x%x.\n", COUNTER_PATH_NET_READ_ALL, status);
    ret = EXIT_FAILURE;
    goto cleanup;
  }
  if(status = PdhAddCounter(hQuery, COUNTER_PATH_NET_WRITE_ALL, 0, &hCounterNetWrite))
  {
    fwprintf_s(stderr, L"PdhAddCounter %s failed with 0x%x.\n", COUNTER_PATH_NET_WRITE_ALL, status);
    ret = EXIT_FAILURE;
    goto cleanup;
  }
  if(status = PdhAddCounter(hQuery, COUNTER_PATH_DISK_READ_ALL, 0, &hCounterDiskRead))
  {
    fwprintf_s(stderr, L"PdhAddCounter %s failed with 0x%x.\n", COUNTER_PATH_DISK_READ_ALL, status);
    ret = EXIT_FAILURE;
    goto cleanup;
  }
  if(status = PdhAddCounter(hQuery, COUNTER_PATH_DISK_WRITE_ALL, 0, &hCounterDiskWrite))
  {
    fwprintf_s(stderr, L"PdhAddCounter %s failed with 0x%x.\n", COUNTER_PATH_DISK_WRITE_ALL, status);
    ret = EXIT_FAILURE;
    goto cleanup;
  }

  if(status = PdhCollectQueryData(hQuery))
  {
    fwprintf_s(stderr, L"PdhCollectQueryData() failed with 0x%x.\n", status);
    ret = EXIT_FAILURE;
    goto cleanup;
  }

  // Read and aggregate counters
  status = ReadTotalCounter(hCounterNetRead, netRead);
  if(ERROR_SUCCESS != status)
  {
    fwprintf_s(stderr, L"ReadTotalCounter(Network Read): Error 0x%x.\n", status);
    ret = EXIT_FAILURE;
  }

  status = ReadTotalCounter(hCounterNetWrite, netWrite);
  if(ERROR_SUCCESS != status)
  {
    fwprintf_s(stderr, L"ReadTotalCounter(Network Write): Error 0x%x.\n", status);
    ret = EXIT_FAILURE;
  }

  status = ReadTotalCounter(hCounterDiskRead, diskRead);
  if(ERROR_SUCCESS != status)
  {
    fwprintf_s(stderr, L"ReadTotalCounter(Disk Read): Error 0x%x.\n", status);
    ret = EXIT_FAILURE;
  }

  status = ReadTotalCounter(hCounterDiskWrite, diskWrite);
  if(ERROR_SUCCESS != status)
  {
    fwprintf_s(stderr, L"ReadTotalCounter(Disk Write): Error 0x%x.\n", status);
    ret = EXIT_FAILURE;
  }

cleanup:
  if (hQuery)
  {
    status = PdhCloseQuery(hQuery);
  }

  return ret;
}

PDH_STATUS ReadTotalCounter(PDH_HCOUNTER hCounter, LONGLONG* ret)
{
  PDH_STATUS status = ERROR_SUCCESS;
  DWORD i = 0;
  DWORD dwBufferSize = 0;
  DWORD dwItemCount = 0;
  PDH_RAW_COUNTER_ITEM *pItems = NULL;

  // Initialize output
  *ret = 0;

  // Get the required size of the pItems buffer
  status = PdhGetRawCounterArray(hCounter, &dwBufferSize, &dwItemCount, NULL);
  if (PDH_MORE_DATA == status)
  {
    pItems = (PDH_RAW_COUNTER_ITEM *) malloc(dwBufferSize);
    if (pItems)
    {
      // Actually query the counter
      status = PdhGetRawCounterArray(hCounter, &dwBufferSize, &dwItemCount, pItems);
      if (ERROR_SUCCESS == status) {
        for (i = 0; i < dwItemCount; i++) {
          if (wcscmp(L"_Total", pItems[i].szName) == 0) {
            *ret = pItems[i].RawValue.FirstValue;
            break;
          } else {
            *ret += pItems[i].RawValue.FirstValue;
          }
        }
      } else {
        *ret = -1;
        goto cleanup;
      }
      // Reset structures
      free(pItems);
      pItems = NULL;
      dwBufferSize = dwItemCount = 0;
    } else {
      *ret = -1;
      status = PDH_NO_DATA;
      goto cleanup;
    }
  } else {
    *ret = -1;
    goto cleanup;
  }

cleanup:
  if (pItems) {
    free(pItems);
  }

  return status;
}