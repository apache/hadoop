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
#include <authz.h>
#include <sddl.h>

#ifdef PSAPI_VERSION
#undef PSAPI_VERSION
#endif
#define PSAPI_VERSION 1
#pragma comment(lib, "psapi.lib")

#define NM_WSCE_IMPERSONATE_ALLOWED L"yarn.nodemanager.windows-secure-container-executor.impersonate.allowed"
#define NM_WSCE_IMPERSONATE_DENIED  L"yarn.nodemanager.windows-secure-container-executor.impersonate.denied"

// The S4U impersonation access check mask. Arbitrary value (we use 1 for the service access check)
#define SERVICE_IMPERSONATE_MASK 0x00000002


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
// Function: GetLimit
//
// Description:
//  Get the resource limit value in long type given the command line argument.
//
// Returns:
// TRUE: If successfully get the value
// FALSE: otherwise
static BOOL GetLimit(__in const wchar_t *str, __out long *value)
{
  wchar_t *end = NULL;
  if (str == NULL || value == NULL) return FALSE;
  *value = wcstol(str, &end, 10);
  if (end == NULL || *end != '\0')
  {
    *value = -1;
    return FALSE;
  }
  else
  {
    return TRUE;
  }
}

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
                             __out TaskCommandOption *command,
                             __out_opt long *memory,
                             __out_opt long *vcore)
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

  if (argc >= 4 && argc <= 8) {
    if (wcscmp(argv[1], L"create") == 0)
    {
      int i;
      for (i = 2; i < argc - 3; i++)
      {
        if (wcscmp(argv[i], L"-c") == 0)
        {
          if (vcore != NULL && !GetLimit(argv[i + 1], vcore))
          {
            return FALSE;
          }
          else
          {
            i++;
            continue;
          }
        }
        else if (wcscmp(argv[i], L"-m") == 0)
        {
          if (memory != NULL && !GetLimit(argv[i + 1], memory))
          {
            return FALSE;
          }
          else
          {
            i++;
            continue;
          }
        }
        else
        {
          break;
        }
      }
      if (argc - i != 2)
        return FALSE;

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
// Function: BuildImpersonateSecurityDescriptor
//
// Description:
//  Builds the security descriptor for the S4U impersonation permissions
//  This describes what users can be impersonated and what not
//
// Returns:
//   ERROR_SUCCESS: On success
//   GetLastError: otherwise
//
DWORD BuildImpersonateSecurityDescriptor(__out PSECURITY_DESCRIPTOR* ppSD) {
  DWORD dwError = ERROR_SUCCESS;
  size_t countAllowed = 0;
  PSID* allowedSids = NULL;
  size_t countDenied = 0;
  PSID* deniedSids = NULL;
  LPCWSTR value = NULL;
  WCHAR** tokens = NULL;
  size_t len = 0;
  size_t count = 0;
  size_t crt = 0;
  PSECURITY_DESCRIPTOR pSD = NULL;

  dwError = GetConfigValue(wsceConfigRelativePath, NM_WSCE_IMPERSONATE_ALLOWED, &len, &value); 
  if (dwError) {
    ReportErrorCode(L"GetConfigValue:1", dwError);
    goto done;
  }

  if (0 == len) {
    dwError = ERROR_BAD_CONFIGURATION;
    ReportErrorCode(L"GetConfigValue:2", dwError);
    goto done;
  }
  
  dwError = SplitStringIgnoreSpaceW(len, value, L',', &count, &tokens);
  if (dwError) {
    ReportErrorCode(L"SplitStringIgnoreSpaceW:1", dwError);
    goto done;
  }

  allowedSids = LocalAlloc(LPTR, sizeof(PSID) * count);
  if (NULL == allowedSids) {
    dwError = GetLastError();
    ReportErrorCode(L"LocalAlloc:1", dwError);
    goto done;
  }

  for(crt = 0; crt < count; ++crt) {
    dwError = GetSidFromAcctNameW(tokens[crt], &allowedSids[crt]);
    if (dwError) {
      ReportErrorCode(L"GetSidFromAcctNameW:1", dwError);
      goto done;
    }
  }
  countAllowed = count;

  LocalFree(tokens);
  tokens = NULL;

  LocalFree((HLOCAL)value);
  value = NULL;
  
  dwError = GetConfigValue(wsceConfigRelativePath, NM_WSCE_IMPERSONATE_DENIED, &len, &value); 
  if (dwError) {
    ReportErrorCode(L"GetConfigValue:3", dwError);
    goto done;
  }

  if (0 != len) {
    dwError = SplitStringIgnoreSpaceW(len, value, L',', &count, &tokens);
    if (dwError) {
      ReportErrorCode(L"SplitStringIgnoreSpaceW:2", dwError);
      goto done;
    }

    deniedSids = LocalAlloc(LPTR, sizeof(PSID) * count);
    if (NULL == allowedSids) {
      dwError = GetLastError();
      ReportErrorCode(L"LocalAlloc:2", dwError);
      goto done;
    }

    for(crt = 0; crt < count; ++crt) {
      dwError = GetSidFromAcctNameW(tokens[crt], &deniedSids[crt]);
      if (dwError) {
        ReportErrorCode(L"GetSidFromAcctNameW:2", dwError);
        goto done;
      }
    }
    countDenied = count;
  }

  dwError = BuildServiceSecurityDescriptor(
    SERVICE_IMPERSONATE_MASK,
    countAllowed, allowedSids,
    countDenied, deniedSids,
    NULL,
    &pSD);

  if (dwError) {
    ReportErrorCode(L"BuildServiceSecurityDescriptor", dwError);
    goto done;
  }
  
  *ppSD = pSD;
  pSD = NULL;

done:
  if (pSD) LocalFree(pSD);
  if (tokens) LocalFree(tokens);
  if (allowedSids) LocalFree(allowedSids);
  if (deniedSids) LocalFree(deniedSids);
  return dwError;
}

//----------------------------------------------------------------------------
// Function: AddNodeManagerAndUserACEsToObject
//
// Description:
//  Adds ACEs to grant NM and user the provided access mask over a given handle
//
// Returns:
//  ERROR_SUCCESS: on success
//
DWORD AddNodeManagerAndUserACEsToObject(
  __in HANDLE hObject,
  __in LPCWSTR user,
  __in ACCESS_MASK accessMask) {

  DWORD dwError = ERROR_SUCCESS;
  size_t      countTokens = 0;
  size_t      len = 0;
  LPCWSTR     value = NULL;
  WCHAR**     tokens = NULL;
  DWORD       crt = 0;
  PACL        pDacl = NULL;
  PSECURITY_DESCRIPTOR  psdProcess = NULL;
  LPWSTR      lpszOldDacl = NULL, lpszNewDacl = NULL;
  ULONG       daclLen = 0;
  PACL        pNewDacl = NULL;
  ACL_SIZE_INFORMATION si;
  DWORD       dwNewAclSize = 0;
  PACE_HEADER pTempAce = NULL;
  BYTE        sidTemp[SECURITY_MAX_SID_SIZE];
  DWORD       cbSid = SECURITY_MAX_SID_SIZE;
  PSID        tokenSid = NULL;
  // These hard-coded SIDs are allways added
  WELL_KNOWN_SID_TYPE forcesSidTypes[] = {
    WinLocalSystemSid,
    WinBuiltinAdministratorsSid};
  BOOL        logSDs = IsDebuggerPresent(); // Check only once to avoid attach-while-running
 
  
  dwError = GetSecurityInfo(hObject,
      SE_KERNEL_OBJECT,
      DACL_SECURITY_INFORMATION,
      NULL,
      NULL,
      &pDacl,
      NULL,
      &psdProcess);
  if (dwError) {
    ReportErrorCode(L"GetSecurityInfo", dwError);
    goto done;
  }

  // This is debug only output for troubleshooting
  if (logSDs) {
    if (!ConvertSecurityDescriptorToStringSecurityDescriptor(
        psdProcess,
        SDDL_REVISION_1,
        DACL_SECURITY_INFORMATION,
        &lpszOldDacl,
        &daclLen)) {
      dwError = GetLastError();
      ReportErrorCode(L"ConvertSecurityDescriptorToStringSecurityDescriptor", dwError);
      goto done;
    }
  }

  ZeroMemory(&si, sizeof(si));
  if (!GetAclInformation(pDacl, &si, sizeof(si), AclSizeInformation)) {
    dwError = GetLastError();
    ReportErrorCode(L"GetAclInformation", dwError);
    goto done;  
  }

  dwError = GetConfigValue(wsceConfigRelativePath, NM_WSCE_ALLOWED, &len, &value);
  if (ERROR_SUCCESS != dwError) {
    ReportErrorCode(L"GetConfigValue", dwError);
    goto done;
  }

  if (0 == len) {
    dwError = ERROR_BAD_CONFIGURATION;
    ReportErrorCode(L"GetConfigValue", dwError);
    goto done;
  }

  dwError = SplitStringIgnoreSpaceW(len, value, L',', &countTokens, &tokens);
  if (ERROR_SUCCESS != dwError) {
    ReportErrorCode(L"SplitStringIgnoreSpaceW", dwError);
    goto done;
  }

  // We're gonna add 1 ACE for each token found, +1 for user and +1 for each forcesSidTypes[]
  // ACCESS_ALLOWED_ACE struct contains the first DWORD of the SID 
  //
  dwNewAclSize = si.AclBytesInUse + 
      (DWORD)(countTokens + 1 + sizeof(forcesSidTypes)/sizeof(forcesSidTypes[0])) * 
              (sizeof(ACCESS_ALLOWED_ACE) + SECURITY_MAX_SID_SIZE - sizeof(DWORD));

  pNewDacl = (PSID) LocalAlloc(LPTR, dwNewAclSize);
  if (!pNewDacl) {
    dwError = ERROR_OUTOFMEMORY;
    ReportErrorCode(L"LocalAlloc", dwError);
    goto done;
  }

  if (!InitializeAcl(pNewDacl, dwNewAclSize, ACL_REVISION)) {
    dwError = ERROR_OUTOFMEMORY;
    ReportErrorCode(L"InitializeAcl", dwError);
    goto done;
  }

  // Copy over old ACEs
  for (crt = 0; crt < si.AceCount; ++crt) {
    if (!GetAce(pDacl, crt, &pTempAce)) {
      dwError = ERROR_OUTOFMEMORY;
      ReportErrorCode(L"InitializeAcl", dwError);
      goto done;
    }
    if (!AddAce(pNewDacl, ACL_REVISION, MAXDWORD, pTempAce, pTempAce->AceSize)) {
      dwError = ERROR_OUTOFMEMORY;
      ReportErrorCode(L"InitializeAcl", dwError);
      goto done;
    }
  }

  // Add the configured allowed SIDs
  for (crt = 0; crt < countTokens; ++crt) {
    dwError = GetSidFromAcctNameW(tokens[crt], &tokenSid);
    if (ERROR_SUCCESS != dwError) {
      ReportErrorCode(L"GetSidFromAcctNameW", dwError);
      goto done;
    }
    if (!AddAccessAllowedAceEx(
                pNewDacl,
                ACL_REVISION_DS,
                CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
                PROCESS_ALL_ACCESS,
                tokenSid)) {
      dwError = GetLastError();
      ReportErrorCode(L"AddAccessAllowedAceEx:1", dwError);
      goto done;
    }
    LocalFree(tokenSid);
    tokenSid = NULL;
  }

  // add the forced SIDs ACE 
  for (crt = 0; crt < sizeof(forcesSidTypes)/sizeof(forcesSidTypes[0]); ++crt) {
    cbSid = SECURITY_MAX_SID_SIZE;
    if (!CreateWellKnownSid(forcesSidTypes[crt], NULL, &sidTemp, &cbSid)) {
      dwError = GetLastError();
      ReportErrorCode(L"CreateWellKnownSid", dwError);
      goto done;
    }
    if (!AddAccessAllowedAceEx(
              pNewDacl,
              ACL_REVISION_DS,
              CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
              accessMask,
              (PSID) sidTemp)) {
      dwError = GetLastError();
      ReportErrorCode(L"AddAccessAllowedAceEx:2", dwError);
      goto done;
    }
  }
  
  // add the user ACE
  dwError = GetSidFromAcctNameW(user, &tokenSid);
  if (ERROR_SUCCESS != dwError) {
    ReportErrorCode(L"GetSidFromAcctNameW:user", dwError);
    goto done;
  }    

  if (!AddAccessAllowedAceEx(
              pNewDacl,
              ACL_REVISION_DS,
              CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
              PROCESS_ALL_ACCESS,
              tokenSid)) {
    dwError = GetLastError();
    ReportErrorCode(L"AddAccessAllowedAceEx:3", dwError);
    goto done;
  }

  LocalFree(tokenSid);
  tokenSid = NULL;

  dwError = SetSecurityInfo(hObject,
    SE_KERNEL_OBJECT,
    DACL_SECURITY_INFORMATION,
    NULL,
    NULL,
    pNewDacl,
    NULL);
  if (dwError) {
    ReportErrorCode(L"SetSecurityInfo", dwError);
    goto done;
  }

  // This is debug only output for troubleshooting
  if (logSDs) {
    dwError = GetSecurityInfo(hObject,
        SE_KERNEL_OBJECT,
        DACL_SECURITY_INFORMATION,
        NULL,
        NULL,
        &pDacl,
        NULL,
        &psdProcess);
    if (dwError) {
      ReportErrorCode(L"GetSecurityInfo:2", dwError);
      goto done;
    }

    if (!ConvertSecurityDescriptorToStringSecurityDescriptor(
        psdProcess,
        SDDL_REVISION_1,
        DACL_SECURITY_INFORMATION,
        &lpszNewDacl,
        &daclLen)) {
      dwError = GetLastError();
      ReportErrorCode(L"ConvertSecurityDescriptorToStringSecurityDescriptor:2", dwError);
      goto done;
    }

    LogDebugMessage(L"Old DACL: %ls\nNew DACL: %ls\n", lpszOldDacl, lpszNewDacl);
  }
  
done:
  if (tokenSid) LocalFree(tokenSid);
  if (pNewDacl) LocalFree(pNewDacl);
  if (lpszOldDacl) LocalFree(lpszOldDacl);
  if (lpszNewDacl) LocalFree(lpszNewDacl);
  if (psdProcess) LocalFree(psdProcess);

  return dwError;
}

//----------------------------------------------------------------------------
// Function: ValidateImpersonateAccessCheck
//
// Description:
//  Performs the access check for S4U impersonation
//
// Returns:
//   ERROR_SUCCESS: On success
//   ERROR_ACCESS_DENIED, GetLastError: otherwise
//
DWORD ValidateImpersonateAccessCheck(__in HANDLE logonHandle) {
  DWORD dwError = ERROR_SUCCESS;
  PSECURITY_DESCRIPTOR pSD = NULL;
  LUID luidUnused;
  AUTHZ_ACCESS_REQUEST  request;
  AUTHZ_ACCESS_REPLY    reply;
  DWORD authError = ERROR_SUCCESS;
  DWORD saclResult = 0;
  ACCESS_MASK grantedMask = 0;
  AUTHZ_RESOURCE_MANAGER_HANDLE hManager = NULL;
  AUTHZ_CLIENT_CONTEXT_HANDLE hAuthzToken = NULL;

  ZeroMemory(&luidUnused, sizeof(luidUnused));
  ZeroMemory(&request, sizeof(request));
  ZeroMemory(&reply, sizeof(reply));

  dwError = BuildImpersonateSecurityDescriptor(&pSD);
  if (dwError) {
    ReportErrorCode(L"BuildImpersonateSecurityDescriptor", dwError);
    goto done;
  }

  request.DesiredAccess = MAXIMUM_ALLOWED;
  reply.Error = &authError;
  reply.SaclEvaluationResults = &saclResult;
  reply.ResultListLength = 1;
  reply.GrantedAccessMask = &grantedMask;

  if (!AuthzInitializeResourceManager(
        AUTHZ_RM_FLAG_NO_AUDIT,
        NULL,  // pfnAccessCheck
        NULL,  // pfnComputeDynamicGroups
        NULL,  // pfnFreeDynamicGroups
        NULL,  // szResourceManagerName
        &hManager)) {
    dwError = GetLastError();
    ReportErrorCode(L"AuthzInitializeResourceManager", dwError);
    goto done;
  }  

  if (!AuthzInitializeContextFromToken(
        0,
        logonHandle,
        hManager,
        NULL, // expiration time
        luidUnused, // not used
        NULL, // callback args
        &hAuthzToken)) {
    dwError = GetLastError();
    ReportErrorCode(L"AuthzInitializeContextFromToken", dwError);
    goto done;
  }

  if (!AuthzAccessCheck(
        0,
        hAuthzToken,
        &request,
        NULL,   // AuditEvent
        pSD,
        NULL,  // OptionalSecurityDescriptorArray
        0,     // OptionalSecurityDescriptorCount
        &reply,
        NULL  // phAccessCheckResults
        )) {
    dwError = GetLastError();
    ReportErrorCode(L"AuthzAccessCheck", dwError);
    goto done;
  }

  LogDebugMessage(L"AutzAccessCheck: Error:%d sacl:%d access:%d\n",
    authError, saclResult, grantedMask);
  
  if (authError != ERROR_SUCCESS) {
    ReportErrorCode(L"AuthzAccessCheck:REPLY:1", authError);
    dwError = authError;
  } 
  else if (!(grantedMask & SERVICE_IMPERSONATE_MASK)) {
    ReportErrorCode(L"AuthzAccessCheck:REPLY:2", ERROR_ACCESS_DENIED);
    dwError = ERROR_ACCESS_DENIED;
  }

done:  
  if (hAuthzToken) AuthzFreeContext(hAuthzToken);
  if (hManager) AuthzFreeResourceManager(hManager);
  if (pSD) LocalFree(pSD);
  return dwError;
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
DWORD CreateTaskImpl(__in_opt HANDLE logonHandle, __in PCWSTR jobObjName,__in PWSTR cmdLine, 
  __in LPCWSTR userName, __in long memory, __in long cpuRate)
{
  DWORD dwErrorCode = ERROR_SUCCESS;
  DWORD exitCode = EXIT_FAILURE;
  DWORD currDirCnt = 0;
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  HANDLE jobObject = NULL;
  JOBOBJECT_EXTENDED_LIMIT_INFORMATION jeli = { 0 };
  void * envBlock = NULL;
  WCHAR secureJobNameBuffer[MAX_PATH];
  LPCWSTR secureJobName = jobObjName;

  wchar_t* curr_dir = NULL;
  FILE *stream = NULL;

  if (NULL != logonHandle) {
    dwErrorCode = ValidateImpersonateAccessCheck(logonHandle);
    if (dwErrorCode) {
      ReportErrorCode(L"ValidateImpersonateAccessCheck", dwErrorCode);
      return dwErrorCode;
    }

    dwErrorCode = GetSecureJobObjectName(jobObjName, MAX_PATH, secureJobNameBuffer);
    if (dwErrorCode) {
      ReportErrorCode(L"GetSecureJobObjectName", dwErrorCode);
      return dwErrorCode;
    }
    secureJobName = secureJobNameBuffer;
  }

  // Create un-inheritable job object handle and set job object to terminate 
  // when last handle is closed. So winutils.exe invocation has the only open 
  // job object handle. Exit of winutils.exe ensures termination of job object.
  // Either a clean exit of winutils or crash or external termination.
  jobObject = CreateJobObject(NULL, secureJobName);
  dwErrorCode = GetLastError();
  if(jobObject == NULL || dwErrorCode ==  ERROR_ALREADY_EXISTS)
  {
    ReportErrorCode(L"CreateJobObject", dwErrorCode);
    return dwErrorCode;
  }
  jeli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
  if (memory > 0)
  {
    jeli.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
    jeli.ProcessMemoryLimit = ((SIZE_T) memory) * 1024 * 1024;
    jeli.JobMemoryLimit = ((SIZE_T) memory) * 1024 * 1024;
  }
  if(SetInformationJobObject(jobObject, 
                             JobObjectExtendedLimitInformation, 
                             &jeli, 
                             sizeof(jeli)) == 0)
  {
    dwErrorCode = GetLastError();
    ReportErrorCode(L"SetInformationJobObject", dwErrorCode);
    CloseHandle(jobObject);
    return dwErrorCode;
  }
#ifdef NTDDI_WIN8
  if (cpuRate > 0)
  {
    JOBOBJECT_CPU_RATE_CONTROL_INFORMATION jcrci = { 0 };
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    jcrci.ControlFlags = JOB_OBJECT_CPU_RATE_CONTROL_ENABLE |
      JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP;
    jcrci.CpuRate = min(10000, cpuRate);
    if(SetInformationJobObject(jobObject, JobObjectCpuRateControlInformation,
          &jcrci, sizeof(jcrci)) == 0)
    {
      dwErrorCode = GetLastError();
      CloseHandle(jobObject);
      return dwErrorCode;
    }
  }
#endif

  if (logonHandle != NULL) {
    dwErrorCode = AddNodeManagerAndUserACEsToObject(jobObject, userName, JOB_OBJECT_ALL_ACCESS);
    if (dwErrorCode) {
      ReportErrorCode(L"AddNodeManagerAndUserACEsToObject", dwErrorCode);
      CloseHandle(jobObject);
      return dwErrorCode;
    }
  }

  if(AssignProcessToJobObject(jobObject, GetCurrentProcess()) == 0)
  {
    dwErrorCode = GetLastError();
    ReportErrorCode(L"AssignProcessToJobObject", dwErrorCode);
    CloseHandle(jobObject);
    return dwErrorCode;
  }

  // the child JVM uses this env var to send the task OS process identifier 
  // to the TaskTracker. We pass the job object name.
  if(SetEnvironmentVariable(L"JVM_PID", jobObjName) == 0)
  {
    dwErrorCode = GetLastError();
    ReportErrorCode(L"SetEnvironmentVariable", dwErrorCode);
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
        ReportErrorCode(L"CreateEnvironmentBlock", dwErrorCode);
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
     ReportErrorCode(L"GetCurrentDirectory", dwErrorCode);
     // We have to explictly Terminate, passing in the error code
     // simply closing the job would kill our own process with success exit status
     TerminateJobObject(jobObject, dwErrorCode);
     return dwErrorCode;     
  }

  dwErrorCode = ERROR_SUCCESS;

  if (logonHandle == NULL) {
    if (!CreateProcess(
                NULL,         // ApplicationName
                cmdLine,      // command line
                NULL,         // process security attributes
                NULL,         // thread security attributes
                TRUE,         // inherit handles
                0,            // creation flags
                NULL,         // environment
                curr_dir,     // current directory
                &si,          // startup info
                &pi)) {       // process info
         dwErrorCode = GetLastError();
         ReportErrorCode(L"CreateProcess", dwErrorCode);
      }

    // task create (w/o createAsUser) does not need the ACEs change on the process
    goto create_process_done;
  }

  // From here on is the secure S4U implementation for CreateProcessAsUser
  
  // We desire to grant process access to NM so that it can interogate process status
  // and resource utilization. Passing in a security descriptor though results in the 
  // S4U privilege checks being done against that SD and CreateProcessAsUser fails.
  // So instead we create the process suspended and then we add the desired ACEs.
  //
  if (!CreateProcessAsUser(
                logonHandle,  // logon token handle
                NULL,         // Application handle
                cmdLine,      // command line
                NULL,         // process security attributes
                NULL,         // thread security attributes
                FALSE,        // inherit handles
                CREATE_UNICODE_ENVIRONMENT | CREATE_SUSPENDED, // creation flags
                envBlock,     // environment
                curr_dir,     // current directory
                &si,          // startup info
                &pi))  {      // process info 
    dwErrorCode = GetLastError();
    ReportErrorCode(L"CreateProcessAsUser", dwErrorCode);
    goto create_process_done;
  }

  dwErrorCode = AddNodeManagerAndUserACEsToObject(pi.hProcess, userName, PROCESS_ALL_ACCESS);
  if (dwErrorCode) {
    ReportErrorCode(L"AddNodeManagerAndUserACEsToObject", dwErrorCode);
    goto create_process_done;
  }

  if (-1 == ResumeThread(pi.hThread)) {
    dwErrorCode = GetLastError();
    ReportErrorCode(L"ResumeThread", dwErrorCode);
    goto create_process_done;
  }

create_process_done:

  if (dwErrorCode) {
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
DWORD CreateTask(__in PCWSTR jobObjName,__in PWSTR cmdLine, __in long memory, __in long cpuRate)
{
  // call with null logon in order to create tasks utilizing the current logon
  return CreateTaskImpl( NULL, jobObjName, cmdLine, NULL, memory, cpuRate);
}

//----------------------------------------------------------------------------
// Function: CreateTaskAsUser
//
// Description:
//  Creates a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD CreateTaskAsUser(__in PCWSTR jobObjName,
  __in PCWSTR user, __in PCWSTR pidFilePath, __in PWSTR cmdLine)
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
  errno_t pidErrNo = 0;

  err = EnableImpersonatePrivileges();
  if( err != ERROR_SUCCESS ) {
    ReportErrorCode(L"EnableImpersonatePrivileges", err);
    goto done;
  }

  err = RegisterWithLsa(LOGON_PROCESS_NAME ,&lsaHandle);
  if( err != ERROR_SUCCESS ) {
    ReportErrorCode(L"RegisterWithLsa", err);
    goto done;
  }

  err = LookupKerberosAuthenticationPackageId( lsaHandle, &authnPkgId );
  if( err != ERROR_SUCCESS ) {
    ReportErrorCode(L"LookupKerberosAuthenticationPackageId", err);
    goto done;
  }

  err =  CreateLogonTokenForUser(lsaHandle,
    LOGON_PROCESS_NAME, 
    TOKEN_SOURCE_NAME,
    authnPkgId, 
    user, 
    &logonHandle); 
  if( err != ERROR_SUCCESS ) {
    ReportErrorCode(L"CreateLogonTokenForUser", err);
    goto done;
  }

  err = LoadUserProfileForLogon(logonHandle, &pi);
  if( err != ERROR_SUCCESS ) {
    ReportErrorCode(L"LoadUserProfileForLogon", err);
    goto done;
  }
  profileIsLoaded = TRUE; 

  // Create the PID file
  pidErrNo = _wfopen_s(&pidFile, pidFilePath, L"w");
  if (pidErrNo) {
      err = GetLastError();
      ReportErrorCode(L"_wfopen:pidFilePath", err);
      goto done;
  }

  if (0 > fprintf_s(pidFile, "%ls", jobObjName)) {
    err = GetLastError();
  }
  
  fclose(pidFile);
    
  if (err != ERROR_SUCCESS) {
      ReportErrorCode(L"fprintf_s:pidFilePath", err);
      goto done;
  }

  err = CreateTaskImpl(logonHandle, jobObjName, cmdLine, user, -1, -1);

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
  WCHAR secureJobNameBuffer[MAX_PATH];

  *isAlive = FALSE;
  
  jobObject = OpenJobObject(JOB_OBJECT_QUERY, FALSE, jobObjName);
  if(jobObject == NULL)
  {
    // Try Global\...
    DWORD err = GetSecureJobObjectName(jobObjName, MAX_PATH, secureJobNameBuffer);
    if  (err) {
      ReportErrorCode(L"GetSecureJobObjectName", err);
      return err;
    }
    jobObject = OpenJobObject(JOB_OBJECT_QUERY, FALSE, secureJobNameBuffer);
  }
  
  if(jobObject == NULL)
  {
    DWORD err = GetLastError();
    return err;
  }  

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
  WCHAR secureJobNameBuffer[MAX_PATH];
  HANDLE jobObject = NULL;

  jobObject = OpenJobObject(JOB_OBJECT_QUERY, FALSE, jobObjName);
  if(jobObject == NULL)
  {
    // Try Global\...
    DWORD err = GetSecureJobObjectName(jobObjName, MAX_PATH, secureJobNameBuffer);
    if  (err) {
      ReportErrorCode(L"GetSecureJobObjectName", err);
      return err;
    }
    jobObject = OpenJobObject(JOB_OBJECT_QUERY, FALSE, secureJobNameBuffer);
  }
  
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
  long memory = -1;
  long cpuRate = -1;
  wchar_t* cmdLine = NULL;
  wchar_t buffer[16*1024] = L""; // 32K max command line
  size_t charCountBufferLeft = sizeof(buffer)/sizeof(wchar_t);
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

  if (!ParseCommandLine(argc, argv, &command, &memory, &cpuRate)) {
    dwErrorCode = ERROR_INVALID_COMMAND_LINE;

    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    TaskUsage();
    goto TaskExit;
  }

  if (command == TaskCreate)
  {
    // Create the task jobobject
    //
    dwErrorCode = CreateTask(argv[argc-2], argv[argc-1], memory, cpuRate);
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
  ReportErrorCode(L"TaskExit:", dwErrorCode);
  return dwErrorCode;
}

void TaskUsage()
{
  // Hadoop code checks for this string to determine if
  // jobobject's are being used.
  // ProcessTree.isSetsidSupported()
  fwprintf(stdout, L"\
Usage: task create [OPTOINS] [TASKNAME] [COMMAND_LINE]\n\
         Creates a new task job object with taskname and options to set CPU\n\
         and memory limits on the job object\n\
\n\
         OPTIONS: -c [cup rate] set the cpu rate limit on the job object.\n\
                  -m [memory] set the memory limit on the job object.\n\
         The cpu limit is an integral value of percentage * 100. The memory\n\
         limit is an integral number of memory in MB. \n\
         The limit will not be set if 0 or negative value is passed in as\n\
         parameter(s).\n\
\n\
       task createAsUser [TASKNAME] [USERNAME] [PIDFILE] [COMMAND_LINE]\n\
         Creates a new task jobobject with taskname as the user provided\n\
\n\
       task isAlive [TASKNAME]\n\
         Checks if task job object is alive\n\
\n\
       task kill [TASKNAME]\n\
         Kills task job object\n\
\n\
       task processList [TASKNAME]\n\
         Prints to stdout a list of processes in the task\n\
         along with their resource usage. One process per line\n\
         and comma separated info per process\n\
         ProcessId,VirtualMemoryCommitted(bytes),\n\
         WorkingSetSize(bytes),CpuTime(Millisec,Kernel+User)\n");
}
