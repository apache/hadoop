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
#include <errno.h>
#include <psapi.h>

#define PSAPI_VERSION 1
#pragma comment(lib, "psapi.lib")

#define ERROR_TASK_NOT_ALIVE 1

// List of different task related command line options supported by
// winutils.
typedef enum TaskCommandOptionType
{
  TaskInvalid,
  TaskCreate,
  TaskIsAlive,
  TaskKill,
  TaskProcessList
} TaskCommandOption;

//----------------------------------------------------------------------------
// Function: ParseCommandLine
//
// Description:
//  Parses the given command line. On success, out param 'command' contains
//  the user specified command.
//
// Returns:
// TRUE: If the command line is valid
// FALSE: otherwise
static BOOL ParseCommandLine(__in int argc,
                             __in wchar_t *argv[],
                             __out TaskCommandOption *command)
{
  *command = TaskInvalid;

  if (wcscmp(argv[0], L"task") != 0 )
  {
    return FALSE;
  }

  if (argc == 3) {
    if (wcscmp(argv[1], L"isAlive") == 0)
    {
      *command = TaskIsAlive;
      return TRUE;
    }
    if (wcscmp(argv[1], L"kill") == 0)
    {
      *command = TaskKill;
      return TRUE;
    }
    if (wcscmp(argv[1], L"processList") == 0)
    {
      *command = TaskProcessList;
      return TRUE;
    }
  }

  if (argc == 4) {
    if (wcscmp(argv[1], L"create") == 0)
    {
      *command = TaskCreate;
      return TRUE;
    }
  }

  return FALSE;
}

//----------------------------------------------------------------------------
// Function: createTask
//
// Description:
//  Creates a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD createTask(_TCHAR* jobObjName, _TCHAR* cmdLine) 
{
  DWORD err = ERROR_SUCCESS;
  DWORD exitCode = EXIT_FAILURE;
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  HANDLE jobObject = NULL;
  JOBOBJECT_EXTENDED_LIMIT_INFORMATION jeli = { 0 };

  // Create un-inheritable job object handle and set job object to terminate 
  // when last handle is closed. So winutils.exe invocation has the only open 
  // job object handle. Exit of winutils.exe ensures termination of job object.
  // Either a clean exit of winutils or crash or external termination.
  jobObject = CreateJobObject(NULL, jobObjName);
  err = GetLastError();
  if(jobObject == NULL || err ==  ERROR_ALREADY_EXISTS)
  {
    return err;
  }
  jeli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
  if(SetInformationJobObject(jobObject, 
                             JobObjectExtendedLimitInformation, 
                             &jeli, 
                             sizeof(jeli)) == 0)
  {
    err = GetLastError();
    CloseHandle(jobObject);
    return err;
  }      

  if(AssignProcessToJobObject(jobObject, GetCurrentProcess()) == 0)
  {
    err = GetLastError();
    CloseHandle(jobObject);
    return err;
  }

  // the child JVM uses this env var to send the task OS process identifier 
  // to the TaskTracker. We pass the job object name.
  if(SetEnvironmentVariable(_T("JVM_PID"), jobObjName) == 0)
  {
    err = GetLastError();
    CloseHandle(jobObject);
    return err;
  }

  ZeroMemory( &si, sizeof(si) );
  si.cb = sizeof(si);
  ZeroMemory( &pi, sizeof(pi) );
  if(CreateProcess(NULL, cmdLine, NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi) == 0)
  {
    err = GetLastError();
    CloseHandle(jobObject);
    return err;
  }
  CloseHandle(pi.hThread);

  // Wait until child process exits.
  WaitForSingleObject( pi.hProcess, INFINITE );
  if(GetExitCodeProcess(pi.hProcess, &exitCode) == 0)
  {
    err = GetLastError();
  }
  CloseHandle( pi.hProcess );

  // Terminate job object so that all spawned processes are also killed.
  // This is needed because once this process closes the handle to the job 
  // object and none of the spawned objects have the handle open (via 
  // inheritance on creation) then it will not be possible for any other external 
  // program (say winutils task kill) to terminate this job object via its name.
  if(TerminateJobObject(jobObject, exitCode) == 0)
  {
    err = GetLastError();
  }

  // comes here only on failure or TerminateJobObject
  CloseHandle(jobObject);

  if(err != ERROR_SUCCESS)
  {
    return err;
  }
  return exitCode;
}

//----------------------------------------------------------------------------
// Function: isTaskAlive
//
// Description:
//  Checks if a task is alive via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD isTaskAlive(const _TCHAR* jobObjName, int* isAlive, int* procsInJob)
{
  PJOBOBJECT_BASIC_PROCESS_ID_LIST procList;
  HANDLE jobObject = NULL;
  int numProcs = 100;

  *isAlive = FALSE;
  
  jobObject = OpenJobObject(JOB_OBJECT_QUERY, FALSE, jobObjName);

  if(jobObject == NULL)
  {
    DWORD err = GetLastError();
    if(err == ERROR_FILE_NOT_FOUND)
    {
      // job object does not exist. assume its not alive
      return ERROR_SUCCESS;
    }
    return err;
  }

  procList = (PJOBOBJECT_BASIC_PROCESS_ID_LIST) LocalAlloc(LPTR, sizeof (JOBOBJECT_BASIC_PROCESS_ID_LIST) + numProcs*32);
  if (!procList)
  {
    DWORD err = GetLastError();
    CloseHandle(jobObject);
    return err;
  }
  if(QueryInformationJobObject(jobObject, JobObjectBasicProcessIdList, procList, sizeof(JOBOBJECT_BASIC_PROCESS_ID_LIST)+numProcs*32, NULL) == 0)
  {
    DWORD err = GetLastError();
    if(err != ERROR_MORE_DATA) 
    {
      CloseHandle(jobObject);
      LocalFree(procList);
      return err;
    }
  }

  if(procList->NumberOfAssignedProcesses > 0)
  {
    *isAlive = TRUE;
    *procsInJob = procList->NumberOfAssignedProcesses;
  }

  LocalFree(procList);

  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: killTask
//
// Description:
//  Kills a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD killTask(_TCHAR* jobObjName)
{
  HANDLE jobObject = OpenJobObject(JOB_OBJECT_TERMINATE, FALSE, jobObjName);
  if(jobObject == NULL)
  {
    DWORD err = GetLastError();
    if(err == ERROR_FILE_NOT_FOUND)
    {
      // job object does not exist. assume its not alive
      return ERROR_SUCCESS;
    }
    return err;
  }

  if(TerminateJobObject(jobObject, 1) == 0)
  {
    return GetLastError();
  }
  CloseHandle(jobObject);

  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: printTaskProcessList
//
// Description:
// Prints resource usage of all processes in the task jobobject
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD printTaskProcessList(const _TCHAR* jobObjName)
{
  DWORD i;
  PJOBOBJECT_BASIC_PROCESS_ID_LIST procList;
  int numProcs = 100;
  HANDLE jobObject = OpenJobObject(JOB_OBJECT_QUERY, FALSE, jobObjName);
  if(jobObject == NULL)
  {
    DWORD err = GetLastError();
    return err;
  }

  procList = (PJOBOBJECT_BASIC_PROCESS_ID_LIST) LocalAlloc(LPTR, sizeof (JOBOBJECT_BASIC_PROCESS_ID_LIST) + numProcs*32);
  if (!procList)
  {
    DWORD err = GetLastError();
    CloseHandle(jobObject);
    return err;
  }
  while(QueryInformationJobObject(jobObject, JobObjectBasicProcessIdList, procList, sizeof(JOBOBJECT_BASIC_PROCESS_ID_LIST)+numProcs*32, NULL) == 0)
  {
    DWORD err = GetLastError();
    if(err != ERROR_MORE_DATA) 
    {
      CloseHandle(jobObject);
      LocalFree(procList);
      return err;
    }
    numProcs = procList->NumberOfAssignedProcesses;
    LocalFree(procList);
    procList = (PJOBOBJECT_BASIC_PROCESS_ID_LIST) LocalAlloc(LPTR, sizeof (JOBOBJECT_BASIC_PROCESS_ID_LIST) + numProcs*32);
    if (!procList)
    {
      DWORD err = GetLastError();
      CloseHandle(jobObject);
      return err;
    }
  }

  for(i=0; i<procList->NumberOfProcessIdsInList; ++i)
  {
    HANDLE hProcess = OpenProcess( PROCESS_QUERY_INFORMATION, FALSE, (DWORD)procList->ProcessIdList[i] );
    if( hProcess != NULL )
    {
      PROCESS_MEMORY_COUNTERS_EX pmc;
      if ( GetProcessMemoryInfo( hProcess, (PPROCESS_MEMORY_COUNTERS)&pmc, sizeof(pmc)) )
      {
        FILETIME create, exit, kernel, user;
        if( GetProcessTimes( hProcess, &create, &exit, &kernel, &user) )
        {
          ULARGE_INTEGER kernelTime, userTime;
          ULONGLONG cpuTimeMs;
          kernelTime.HighPart = kernel.dwHighDateTime;
          kernelTime.LowPart = kernel.dwLowDateTime;
          userTime.HighPart = user.dwHighDateTime;
          userTime.LowPart = user.dwLowDateTime;
          cpuTimeMs = (kernelTime.QuadPart+userTime.QuadPart)/10000;
          _ftprintf_s(stdout, TEXT("%u,%Iu,%Iu,%Iu\n"), procList->ProcessIdList[i], pmc.PrivateUsage, pmc.WorkingSetSize, cpuTimeMs);
        }
      }
      CloseHandle( hProcess );
    }
  }

  LocalFree(procList);
  CloseHandle(jobObject);

  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: Task
//
// Description:
//  Manages a task via a jobobject (create/isAlive/kill). Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// Error code otherwise: otherwise
int Task(int argc, wchar_t *argv[])
{
  DWORD dwErrorCode = ERROR_SUCCESS;
  TaskCommandOption command = TaskInvalid;

  if (!ParseCommandLine(argc, argv, &command)) {
    dwErrorCode = ERROR_INVALID_COMMAND_LINE;

    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    TaskUsage();
    goto TaskExit;
  }

  if (command == TaskCreate)
  {
    // Create the task jobobject
    //
    dwErrorCode = createTask(argv[2], argv[3]);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"createTask", dwErrorCode);
      goto TaskExit;
    }
  } else if (command == TaskIsAlive)
  {
    // Check if task jobobject
    //
    int isAlive;
    int numProcs;
    dwErrorCode = isTaskAlive(argv[2], &isAlive, &numProcs);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"isTaskAlive", dwErrorCode);
      goto TaskExit;
    }

    // Output the result
    if(isAlive == TRUE)
    {
      fwprintf(stdout, L"IsAlive,%d\n", numProcs);
    }
    else
    {
      dwErrorCode = ERROR_TASK_NOT_ALIVE;
      ReportErrorCode(L"isTaskAlive returned false", dwErrorCode);
      goto TaskExit;
    }
  } else if (command == TaskKill)
  {
    // Check if task jobobject
    //
    dwErrorCode = killTask(argv[2]);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"killTask", dwErrorCode);
      goto TaskExit;
    }
  } else if (command == TaskProcessList)
  {
    // Check if task jobobject
    //
    dwErrorCode = printTaskProcessList(argv[2]);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"printTaskProcessList", dwErrorCode);
      goto TaskExit;
    }
  } else
  {
    // Should not happen
    //
    assert(FALSE);
  }

TaskExit:
  return dwErrorCode;
}

void TaskUsage()
{
  // Hadoop code checks for this string to determine if
  // jobobject's are being used.
  // ProcessTree.isSetsidSupported()
  fwprintf(stdout, L"\
    Usage: task create [TASKNAME] [COMMAND_LINE] |\n\
          task isAlive [TASKNAME] |\n\
          task kill [TASKNAME]\n\
          task processList [TASKNAME]\n\
    Creates a new task jobobject with taskname\n\
    Checks if task jobobject is alive\n\
    Kills task jobobject\n\
    Prints to stdout a list of processes in the task\n\
    along with their resource usage. One process per line\n\
    and comma separated info per process\n\
    ProcessId,VirtualMemoryCommitted(bytes),\n\
    WorkingSetSize(bytes),CpuTime(Millisec,Kernel+User)\n");
}
