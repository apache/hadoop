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
#include <malloc.h>

#define PSAPI_VERSION 1
#pragma comment(lib, "psapi.lib")

#define ERROR_TASK_NOT_ALIVE 1

// This exit code for killed processes is compatible with Unix, where a killed
// process exits with 128 + signal.  For SIGKILL, this would be 128 + 9 = 137.
#define KILLED_PROCESS_EXIT_CODE 137

// Name for tracking this logon process when registering with LSA
static const char *LOGON_PROCESS_NAME="Hadoop Container Executor";
// Name for token source, must be less or eq to TOKEN_SOURCE_LENGTH (currently 8) chars
static const char *TOKEN_SOURCE_NAME = "HadoopEx";

// List of different task related command line options supported by
// winutils.
typedef enum TaskCommandOptionType
{
  TaskInvalid,
  TaskCreate,
  TaskCreateAsUser,
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
                             __in_ecount(argc) wchar_t *argv[],
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

  if (argc >= 6) {
    if (wcscmp(argv[1], L"createAsUser") == 0)
    {
      *command = TaskCreateAsUser;
      return TRUE;
    }
  }

  return FALSE;
}

//----------------------------------------------------------------------------
// Function: CreateTaskImpl
//
// Description:
//  Creates a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//  logonHandle may be NULL, in this case the current logon will be utilized for the 
//  created process
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD CreateTaskImpl(__in_opt HANDLE logonHandle, __in PCWSTR jobObjName,__in PWSTR cmdLine) 
{
  DWORD dwErrorCode = ERROR_SUCCESS;
  DWORD exitCode = EXIT_FAILURE;
  DWORD currDirCnt = 0;
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  HANDLE jobObject = NULL;
  JOBOBJECT_EXTENDED_LIMIT_INFORMATION jeli = { 0 };
  void * envBlock = NULL;
  BOOL createProcessResult = FALSE;

  wchar_t* curr_dir = NULL;
  FILE *stream = NULL;

  // Create un-inheritable job object handle and set job object to terminate 
  // when last handle is closed. So winutils.exe invocation has the only open 
  // job object handle. Exit of winutils.exe ensures termination of job object.
  // Either a clean exit of winutils or crash or external termination.
  jobObject = CreateJobObject(NULL, jobObjName);
  dwErrorCode = GetLastError();
  if(jobObject == NULL || dwErrorCode ==  ERROR_ALREADY_EXISTS)
  {
    return dwErrorCode;
  }
  jeli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
  if(SetInformationJobObject(jobObject, 
                             JobObjectExtendedLimitInformation, 
                             &jeli, 
                             sizeof(jeli)) == 0)
  {
    dwErrorCode = GetLastError();
    CloseHandle(jobObject);
    return dwErrorCode;
  }      

  if(AssignProcessToJobObject(jobObject, GetCurrentProcess()) == 0)
  {
    dwErrorCode = GetLastError();
    CloseHandle(jobObject);
    return dwErrorCode;
  }

  // the child JVM uses this env var to send the task OS process identifier 
  // to the TaskTracker. We pass the job object name.
  if(SetEnvironmentVariable(L"JVM_PID", jobObjName) == 0)
  {
    dwErrorCode = GetLastError();
    // We have to explictly Terminate, passing in the error code
    // simply closing the job would kill our own process with success exit status
    TerminateJobObject(jobObject, dwErrorCode);
    return dwErrorCode;
  }

  ZeroMemory( &si, sizeof(si) );
  si.cb = sizeof(si);
  ZeroMemory( &pi, sizeof(pi) );

  if( logonHandle != NULL ) {
    // create user environment for this logon
    if(!CreateEnvironmentBlock(&envBlock,
                              logonHandle,
                              TRUE )) {
        dwErrorCode = GetLastError();
        // We have to explictly Terminate, passing in the error code
        // simply closing the job would kill our own process with success exit status
        TerminateJobObject(jobObject, dwErrorCode);
        return dwErrorCode; 
    }
  }

  // Get the required buffer size first
  currDirCnt = GetCurrentDirectory(0, NULL);
  if (0 < currDirCnt) {
    curr_dir = (wchar_t*) alloca(currDirCnt * sizeof(wchar_t));
    assert(curr_dir);
    currDirCnt = GetCurrentDirectory(currDirCnt, curr_dir);
  }
  
  if (0 == currDirCnt) {
     dwErrorCode = GetLastError();
     // We have to explictly Terminate, passing in the error code
     // simply closing the job would kill our own process with success exit status
     TerminateJobObject(jobObject, dwErrorCode);
     return dwErrorCode;     
  }

  if (logonHandle == NULL) {
    createProcessResult = CreateProcess(
                  NULL,         // ApplicationName
                  cmdLine,      // command line
                  NULL,         // process security attributes
                  NULL,         // thread security attributes
                  TRUE,         // inherit handles
                  0,            // creation flags
                  NULL,         // environment
                  curr_dir,     // current directory
                  &si,          // startup info
                  &pi);         // process info
  }
  else {
    createProcessResult = CreateProcessAsUser(
                  logonHandle,  // logon token handle
                  NULL,         // Application handle
                  cmdLine,      // command line
                  NULL,         // process security attributes
                  NULL,         // thread security attributes
                  FALSE,        // inherit handles
                  CREATE_UNICODE_ENVIRONMENT, // creation flags
                  envBlock,     // environment
                  curr_dir,     // current directory
                  &si,          // startup info
                  &pi);         // process info
  }
  
  if (FALSE == createProcessResult) {
    dwErrorCode = GetLastError();
    if( envBlock != NULL ) {
      DestroyEnvironmentBlock( envBlock );
      envBlock = NULL;
    }
    // We have to explictly Terminate, passing in the error code
    // simply closing the job would kill our own process with success exit status
    TerminateJobObject(jobObject, dwErrorCode);

    // This is tehnically dead code, we cannot reach this condition
    return dwErrorCode;
  }

  CloseHandle(pi.hThread);

  // Wait until child process exits.
  WaitForSingleObject( pi.hProcess, INFINITE );
  if(GetExitCodeProcess(pi.hProcess, &exitCode) == 0)
  {
    dwErrorCode = GetLastError();
  }
  CloseHandle( pi.hProcess );

  if( envBlock != NULL ) {
    DestroyEnvironmentBlock( envBlock );
    envBlock = NULL;
  }

  // Terminate job object so that all spawned processes are also killed.
  // This is needed because once this process closes the handle to the job 
  // object and none of the spawned objects have the handle open (via 
  // inheritance on creation) then it will not be possible for any other external 
  // program (say winutils task kill) to terminate this job object via its name.
  if(TerminateJobObject(jobObject, exitCode) == 0)
  {
    dwErrorCode = GetLastError();
  }

  // comes here only on failure of TerminateJobObject
  CloseHandle(jobObject);

  if(dwErrorCode != ERROR_SUCCESS)
  {
    return dwErrorCode;
  }
  return exitCode;
}

//----------------------------------------------------------------------------
// Function: CreateTask
//
// Description:
//  Creates a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD CreateTask(__in PCWSTR jobObjName,__in PWSTR cmdLine) 
{
  // call with null logon in order to create tasks utilizing the current logon
  return CreateTaskImpl( NULL, jobObjName, cmdLine );
}
//----------------------------------------------------------------------------
// Function: CreateTask
//
// Description:
//  Creates a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD CreateTaskAsUser(__in PCWSTR jobObjName,__in PWSTR user, __in PWSTR pidFilePath, __in PWSTR cmdLine)
{
  DWORD err = ERROR_SUCCESS;
  DWORD exitCode = EXIT_FAILURE;
  ULONG authnPkgId;
  HANDLE lsaHandle = INVALID_HANDLE_VALUE;
  PROFILEINFO  pi;
  BOOL profileIsLoaded = FALSE;
  FILE* pidFile = NULL;

  DWORD retLen = 0;
  HANDLE logonHandle = NULL;

  err = EnablePrivilege(SE_TCB_NAME);
  if( err != ERROR_SUCCESS ) {
    fwprintf(stdout, L"INFO: The user does not have SE_TCB_NAME.\n");
    goto done;
  }
  err = EnablePrivilege(SE_ASSIGNPRIMARYTOKEN_NAME);
  if( err != ERROR_SUCCESS ) {
    fwprintf(stdout, L"INFO: The user does not have SE_ASSIGNPRIMARYTOKEN_NAME.\n");
    goto done;
  }
  err = EnablePrivilege(SE_INCREASE_QUOTA_NAME);
  if( err != ERROR_SUCCESS ) {
    fwprintf(stdout, L"INFO: The user does not have SE_INCREASE_QUOTA_NAME.\n");
    goto done;
  }
  err = EnablePrivilege(SE_RESTORE_NAME);
  if( err != ERROR_SUCCESS ) {
    fwprintf(stdout, L"INFO: The user does not have SE_RESTORE_NAME.\n");
    goto done;
  }

  err = RegisterWithLsa(LOGON_PROCESS_NAME ,&lsaHandle);
  if( err != ERROR_SUCCESS ) goto done;

  err = LookupKerberosAuthenticationPackageId( lsaHandle, &authnPkgId );
  if( err != ERROR_SUCCESS ) goto done;

  err =  CreateLogonForUser(lsaHandle,
    LOGON_PROCESS_NAME, 
    TOKEN_SOURCE_NAME,
    authnPkgId, 
    user, 
    &logonHandle); 
  if( err != ERROR_SUCCESS ) goto done;

  err = LoadUserProfileForLogon(logonHandle, &pi);
  if( err != ERROR_SUCCESS ) goto done;
  profileIsLoaded = TRUE; 

  // Create the PID file

  if (!(pidFile = _wfopen(pidFilePath, "w"))) {
      err = GetLastError();
      goto done;
  }

  if (0 > fprintf_s(pidFile, "%ls", jobObjName)) {
    err = GetLastError();
  }
  
  fclose(pidFile);
    
  if (err != ERROR_SUCCESS) {
    goto done;
  }
  
  err = CreateTaskImpl(logonHandle, jobObjName, cmdLine);

done:	
  if( profileIsLoaded ) {
    UnloadProfileForLogon( logonHandle, &pi );
    profileIsLoaded = FALSE;
  }
  if( logonHandle != NULL ) {
    CloseHandle(logonHandle);
  }

  if (INVALID_HANDLE_VALUE != lsaHandle) {
    UnregisterWithLsa(lsaHandle);
  }

  return err;
}


//----------------------------------------------------------------------------
// Function: IsTaskAlive
//
// Description:
//  Checks if a task is alive via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD IsTaskAlive(const WCHAR* jobObjName, int* isAlive, int* procsInJob)
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
// Function: KillTask
//
// Description:
//  Kills a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD KillTask(PCWSTR jobObjName)
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

  if(TerminateJobObject(jobObject, KILLED_PROCESS_EXIT_CODE) == 0)
  {
    return GetLastError();
  }
  CloseHandle(jobObject);

  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: PrintTaskProcessList
//
// Description:
// Prints resource usage of all processes in the task jobobject
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD PrintTaskProcessList(const WCHAR* jobObjName)
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
    if (procList == NULL)
    {
      err = GetLastError();
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
          fwprintf_s(stdout, L"%Iu,%Iu,%Iu,%I64u\n", procList->ProcessIdList[i], pmc.PrivateUsage, pmc.WorkingSetSize, cpuTimeMs);
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
int Task(__in int argc, __in_ecount(argc) wchar_t *argv[])
{
  DWORD dwErrorCode = ERROR_SUCCESS;
  TaskCommandOption command = TaskInvalid;
  wchar_t* cmdLine = NULL;
  wchar_t buffer[16*1024] = L""; // 32K max command line
  size_t charCountBufferLeft = sizeof(buffer)/sizeof(wchar_t);
  int crtArgIndex = 0;
  size_t argLen = 0;
  size_t wscatErr = 0;
  wchar_t* insertHere = NULL;

  enum {
               ARGC_JOBOBJECTNAME = 2,
               ARGC_USERNAME,
               ARGC_PIDFILE,
               ARGC_COMMAND,
               ARGC_COMMAND_ARGS
       };

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
    dwErrorCode = CreateTask(argv[2], argv[3]);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"CreateTask", dwErrorCode);
      goto TaskExit;
    }
  } else if (command == TaskCreateAsUser)
  {
    // Create the task jobobject as a domain user
    // createAsUser accepts an open list of arguments. All arguments after the command are
    // to be passed as argumrnts to the command itself.Here we're concatenating all 
    // arguments after the command into a single arg entry.
    //
    cmdLine = argv[ARGC_COMMAND];
    if (argc > ARGC_COMMAND_ARGS) {
        crtArgIndex = ARGC_COMMAND;
        insertHere = buffer;
        while (crtArgIndex < argc) {
            argLen = wcslen(argv[crtArgIndex]);
            wscatErr = wcscat_s(insertHere, charCountBufferLeft, argv[crtArgIndex]);
            switch (wscatErr) {
            case 0:
              // 0 means success;
              break;
            case EINVAL:
              dwErrorCode = ERROR_INVALID_PARAMETER;
              goto TaskExit;
            case ERANGE:
              dwErrorCode = ERROR_INSUFFICIENT_BUFFER;
              goto TaskExit;
            default:
              // This case is not MSDN documented.
              dwErrorCode = ERROR_GEN_FAILURE;
              goto TaskExit;
            }
            insertHere += argLen;
            charCountBufferLeft -= argLen;
            insertHere[0] = L' ';
            insertHere += 1;
            charCountBufferLeft -= 1;
            insertHere[0] = 0;
            ++crtArgIndex;
        }
        cmdLine = buffer;
    }

    dwErrorCode = CreateTaskAsUser(
      argv[ARGC_JOBOBJECTNAME], argv[ARGC_USERNAME], argv[ARGC_PIDFILE], cmdLine);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"CreateTaskAsUser", dwErrorCode);
      goto TaskExit;
    }
  } else if (command == TaskIsAlive)
  {
    // Check if task jobobject
    //
    int isAlive;
    int numProcs;
    dwErrorCode = IsTaskAlive(argv[2], &isAlive, &numProcs);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"IsTaskAlive", dwErrorCode);
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
      ReportErrorCode(L"IsTaskAlive returned false", dwErrorCode);
      goto TaskExit;
    }
  } else if (command == TaskKill)
  {
    // Check if task jobobject
    //
    dwErrorCode = KillTask(argv[2]);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"KillTask", dwErrorCode);
      goto TaskExit;
    }
  } else if (command == TaskProcessList)
  {
    // Check if task jobobject
    //
    dwErrorCode = PrintTaskProcessList(argv[2]);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"PrintTaskProcessList", dwErrorCode);
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
          task createAsUser [TASKNAME] [USERNAME] [PIDFILE] [COMMAND_LINE] |\n\
          task isAlive [TASKNAME] |\n\
          task kill [TASKNAME]\n\
          task processList [TASKNAME]\n\
    Creates a new task jobobject with taskname\n\
    Creates a new task jobobject with taskname as the user provided\n\
    Checks if task jobobject is alive\n\
    Kills task jobobject\n\
    Prints to stdout a list of processes in the task\n\
    along with their resource usage. One process per line\n\
    and comma separated info per process\n\
    ProcessId,VirtualMemoryCommitted(bytes),\n\
    WorkingSetSize(bytes),CpuTime(Millisec,Kernel+User)\n");
}
