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
#include "winutils_msg.h"
#include <Winsvc.h>
#include <errno.h>
#include <malloc.h>
#include <strsafe.h>
#include <authz.h>
#include <sddl.h>
#include "hadoopwinutilsvc_h.h"

#pragma comment(lib, "Rpcrt4.lib")
#pragma comment(lib, "advapi32.lib")
#pragma comment(lib, "authz.lib")

LPCWSTR NM_WSCE_ALLOWED     = L"yarn.nodemanager.windows-secure-container-executor.allowed";
LPCWSTR NM_WSCE_JOB_NAME    = L"yarn.nodemanager.windows-secure-container-executor.job-name";
LPCWSTR NM_WSCE_LOCAL_DIRS  = L"yarn.nodemanager.windows-secure-container-executor.local-dirs";

#define SERVICE_ACCESS_MASK 0x00000001

SERVICE_STATUS          gSvcStatus;
SERVICE_STATUS_HANDLE   gSvcStatusHandle;
HANDLE                  ghSvcStopEvent = INVALID_HANDLE_VALUE;
HANDLE                  ghWaitObject = INVALID_HANDLE_VALUE;
HANDLE                  ghEventLog = INVALID_HANDLE_VALUE;
BOOL                    isListenning = FALSE;
PSECURITY_DESCRIPTOR    pAllowedSD = NULL;
LPWSTR*                 gLocalDirs = NULL;
size_t                  gLocalDirsCount = 0;
int*                    gCchLocalDir = NULL;
LPCWSTR                 gJobName = NULL;

VOID SvcError(DWORD dwError);
VOID WINAPI SvcMain(DWORD dwArg, LPTSTR* lpszArgv);
DWORD SvcInit();
DWORD RpcInit();
DWORD AuthInit();
VOID ReportSvcStatus( DWORD dwCurrentState,
                      DWORD dwWin32ExitCode,
                      DWORD dwWaitHint);
VOID WINAPI SvcCtrlHandler( DWORD dwCtrl );
VOID CALLBACK SvcShutdown(
  _In_  PVOID lpParameter,
  _In_  BOOLEAN TimerOrWaitFired);

#define CHECK_ERROR_DONE(status, expected, category, message)       \
  if (status != expected) {                                         \
    ReportSvcCheckError(                                            \
      EVENTLOG_ERROR_TYPE,                                          \
      category,                                                     \
      status,                                                       \
      message);                                                     \
    goto done;                                                      \
  } else {                                                          \
    LogDebugMessage(L"%s: OK\n", message);                          \
  }


#define CHECK_RPC_STATUS_DONE(status, message)                      \
 CHECK_ERROR_DONE(status, RPC_S_OK, SERVICE_CATEGORY, message)

#define CHECK_SVC_STATUS_DONE(status, message)                      \
 CHECK_ERROR_DONE(status, ERROR_SUCCESS, SERVICE_CATEGORY, message)

#define CHECK_UNWIND_RPC(rpcCall) {                                 \
    unwindStatus = rpcCall;                                         \
    if (RPC_S_OK != unwindStatus) {                                 \
      ReportSvcCheckError(                                          \
          EVENTLOG_WARNING_TYPE,                                    \
          SERVICE_CATEGORY,                                         \
          unwindStatus,                                             \
          L#rpcCall);                                               \
      }                                                             \
    }

//----------------------------------------------------------------------------
// Function: ReportSvcCheckError
//
// Description:
//  Reports an error with the system event log and to debugger console (if present)
//
void ReportSvcCheckError(WORD type, WORD category, DWORD dwError, LPCWSTR message) {
    int       len;
    LPWSTR    systemMsg = NULL;
    LPWSTR    appMsg = NULL;
    DWORD     dwReportError;
    LPWSTR    reportMsg = NULL;
    WCHAR     hexError[32];
    LPCWSTR   inserts[] = {message, NULL, NULL, NULL};
    HRESULT   hr;

    hr = StringCbPrintf(hexError, sizeof(hexError), TEXT("%x"), dwError);
    if (SUCCEEDED(hr)) {
      inserts[1] = hexError;
    }
    else {
      inserts[1] = L"(Failed to format dwError as string)";
    }
    
    len = FormatMessageW(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
      NULL, dwError,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPWSTR)&systemMsg, 0, NULL);
  
    if (len) {
      inserts[2] = systemMsg;
    }
    else {
      inserts[2] = L"(Failed to get the system error message)";
    }

    LogDebugMessage(L"%s:%d %.*s\n", message, dwError, len, systemMsg);
  
    if (INVALID_HANDLE_VALUE != ghEventLog) {
      if (!ReportEvent(ghEventLog, type, category, MSG_CHECK_ERROR,
        NULL,         // lpUserSid
        (WORD) 3,     // wNumStrings
        (DWORD) 0,    // dwDataSize
        inserts,      // *lpStrings
        NULL          // lpRawData
        )) {
          // We tried to report and failed. Send to dbg.
          dwReportError = GetLastError();
          len = FormatMessageW(
            FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
            NULL, dwReportError,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
            (LPWSTR)&reportMsg, 0, NULL);
          LogDebugMessage(L"ReportEvent: Error:%d %.*s\n", dwReportError, reportMsg);
      }
    };
    
    if (NULL != systemMsg) LocalFree(systemMsg);
    if (NULL != reportMsg) LocalFree(reportMsg);
}


VOID ReportSvcMessage(WORD type, WORD category, DWORD msgId) {
  DWORD dwError;
  
  if (INVALID_HANDLE_VALUE != ghEventLog) {
    if (!ReportEvent(ghEventLog, type, category, msgId,
      NULL,         // lpUserSid
      (WORD) 0,     // wNumStrings
      (DWORD) 0,    // dwDataSize
      NULL,         // *lpStrings
      NULL          // lpRawData
      )) {
        // We tried to report and failed but debugger is attached. Send to dbg.
        dwError = GetLastError();
        LogDebugMessage(L"ReportEvent: error %d\n", dwError);
    }
  }
}

//----------------------------------------------------------------------------
// Function: IsSidInList
//
// Description:
//  Finds a SID in an array of SID*
//
BOOL IsSidInList(
  __in PSID trustee, 
  __in size_t cAllowedSids, 
  __in_ecount(cAllowedSids) PSID* allowedSids) {

  size_t crtSid = 0;
  
  for (crtSid = 0; crtSid < cAllowedSids; ++crtSid) {
    if (EqualSid(trustee, allowedSids[crtSid])) {
      return TRUE;
    }
  }
  return FALSE;
}


//----------------------------------------------------------------------------
// Function: InitLocalDirs
//
// Description:
//  Validates that the wsceConfigRelativePath file is only writable by Administrators
//
DWORD ValidateConfigurationFile() {
  DWORD dwError = ERROR_SUCCESS;
  WCHAR xmlPath[MAX_PATH];
  PSECURITY_DESCRIPTOR pSd = NULL;
  BOOL daclPresent = FALSE;
  BOOL daclDefaulted = FALSE;
  PACL pDacl = NULL;
  DWORD crt = 0;
  WELL_KNOWN_SID_TYPE allowedSidTypes[] = {
    WinLocalSystemSid,
    WinBuiltinAdministratorsSid};
  ACL_SIZE_INFORMATION aclInfo;
  DWORD cbSid = SECURITY_MAX_SID_SIZE;
  PSID* allowedSids = NULL; 
  int cAllowedSids = 0;
  PSID sidOwner = NULL;
  PSID sidGroup = NULL;

  allowedSids = (PSID*) LocalAlloc(
    LPTR, 
    sizeof(PSID) * sizeof(allowedSidTypes) / sizeof(WELL_KNOWN_SID_TYPE));
  if (NULL == allowedSids) {
    dwError = ERROR_OUTOFMEMORY;
    CHECK_SVC_STATUS_DONE(dwError, L"LocalAlloc");
  }

  for(crt = 0; crt < sizeof(allowedSidTypes) / sizeof(WELL_KNOWN_SID_TYPE); ++crt) {
    allowedSids[crt] = LocalAlloc(LPTR, SECURITY_MAX_SID_SIZE);
    if (NULL == allowedSids[crt]) {
      dwError = ERROR_OUTOFMEMORY;
      CHECK_SVC_STATUS_DONE(dwError, L"LocalAlloc");
    }

    cbSid = SECURITY_MAX_SID_SIZE;
    
    if (!CreateWellKnownSid(
      allowedSidTypes[crt], NULL, allowedSids[crt], &cbSid)) {
      dwError = GetLastError();
      CHECK_SVC_STATUS_DONE(dwError, L"CreateWellKnownSid");
    }
    ++cAllowedSids;
  }

  dwError = BuildPathRelativeToModule(
    wsceConfigRelativePath,
    sizeof(xmlPath)/sizeof(WCHAR),
    xmlPath);
  CHECK_SVC_STATUS_DONE(dwError, L"BuildPathRelativeToModule");

  dwError = GetNamedSecurityInfo(
    xmlPath, 
    SE_FILE_OBJECT,
    DACL_SECURITY_INFORMATION,
    NULL, NULL, NULL, NULL, &pSd);
  CHECK_SVC_STATUS_DONE(dwError, L"GetNamedSecurityInfo");

  if (!GetSecurityDescriptorDacl(
    pSd,
    &daclPresent,
    &pDacl,
    &daclDefaulted)) {
    dwError = GetLastError();
    CHECK_SVC_STATUS_DONE(dwError, L"GetSecurityDescriptorDacl");
  }
    
  if (!pDacl) {
    dwError = ERROR_BAD_CONFIGURATION;
    CHECK_SVC_STATUS_DONE(dwError, L"pDacl");
  }

  ZeroMemory(&aclInfo, sizeof(aclInfo));
  if (!GetAclInformation(pDacl, &aclInfo, sizeof(aclInfo), AclSizeInformation)) {
    dwError = GetLastError();
    CHECK_SVC_STATUS_DONE(dwError, L"GetAclInformation");
  }

  // Inspect all ACEs in the file DACL.
  // Look at all WRITE GRANTs. Make sure the trustee Sid is one of the approved Sid
  //
  for(crt = 0; crt < aclInfo.AceCount; ++crt) {

    ACE_HEADER* aceHdr = NULL;
    if (!GetAce(pDacl, crt, &aceHdr)) {
      dwError = GetLastError();
      CHECK_SVC_STATUS_DONE(dwError, L"GetAce");
    }
    
    if (ACCESS_ALLOWED_ACE_TYPE == aceHdr->AceType) {
      ACCESS_ALLOWED_ACE* pAce = (ACCESS_ALLOWED_ACE*) aceHdr;
      if (WinMasks[WIN_WRITE] & pAce->Mask) {
         if (!IsSidInList((PSID) &pAce->SidStart, cAllowedSids, allowedSids)) {
            dwError = ERROR_BAD_CONFIGURATION;
            CHECK_SVC_STATUS_DONE(dwError, L"!validSidFound");
         }         
      }
    }
  }
  
done:
  if (pSd) LocalFree(pSd);

  if (allowedSids) {
    while (cAllowedSids) {
      LocalFree(allowedSids[cAllowedSids--]);
      }
    LocalFree(allowedSids);
    }
  
  return dwError;
}

//----------------------------------------------------------------------------
// Function: InitJobName
//
// Description:
//  Loads the job name to be used for created processes
//
DWORD InitJobName() {
  DWORD     dwError = ERROR_SUCCESS;
  size_t    len = 0;
  LPCWSTR   value = NULL;
  int       crt = 0;

  // Services can be restarted
  if (gJobName) LocalFree((HLOCAL)gJobName);
  gJobName = NULL;
    
  dwError = GetConfigValue(
    wsceConfigRelativePath,
    NM_WSCE_JOB_NAME, &len, &value);
  CHECK_SVC_STATUS_DONE(dwError, L"GetConfigValue");

  if (len) {
    gJobName = value;
  }
done:
  return dwError;
}


//----------------------------------------------------------------------------
// Function: InitLocalDirs
//
// Description:
//  Loads the configured local dirs
//
DWORD InitLocalDirs() {
  DWORD     dwError = ERROR_SUCCESS;
  size_t    len = 0;
  LPCWSTR   value = NULL;
  size_t    crt = 0;
    

  dwError = GetConfigValue(
    wsceConfigRelativePath,
    NM_WSCE_LOCAL_DIRS, &len, &value);
  CHECK_SVC_STATUS_DONE(dwError, L"GetConfigValue");

  if (0 == len) {
    dwError = ERROR_BAD_CONFIGURATION;
    CHECK_SVC_STATUS_DONE(dwError, NM_WSCE_LOCAL_DIRS);
  }
  
  dwError = SplitStringIgnoreSpaceW(len, value, L',', &gLocalDirsCount, &gLocalDirs);
  CHECK_SVC_STATUS_DONE(dwError, L"SplitStringIgnoreSpaceW");

  if (0 == gLocalDirsCount) {
    dwError = ERROR_BAD_CONFIGURATION;
    CHECK_SVC_STATUS_DONE(dwError, NM_WSCE_LOCAL_DIRS);
  }

  gCchLocalDir = (int*) LocalAlloc(LPTR, sizeof(int) * gLocalDirsCount);
  if (NULL == gCchLocalDir) {
    dwError = ERROR_OUTOFMEMORY;
    CHECK_SVC_STATUS_DONE(dwError, L"LocalAlloc");
  }

  for (crt = 0; crt < gLocalDirsCount; ++crt) {
    gCchLocalDir[crt] = (int) wcsnlen(gLocalDirs[crt], MAX_PATH);
  }

done:
  if (value) LocalFree((HLOCAL)value);
  
  return dwError;
}

//----------------------------------------------------------------------------
// Function: ValidateLocalPath
//
// Description:
//  Validates that a path is within the contained local dirs
//
DWORD ValidateLocalPath(LPCWSTR lpszPath) {
  DWORD   dwError = ERROR_SUCCESS;
  int     compareResult = 0;
  unsigned int  crt = 0;
  int     cchLocalBuffer = 0;
  WCHAR   localBuffer[MAX_PATH+1];
  BOOLEAN nullFound = FALSE;

  // Make a copy of the path and replace / with \ in the process
  while(crt < MAX_PATH && !nullFound) {
    switch(lpszPath[crt]) {
    case L'/':
      localBuffer[crt] = L'\\';
      ++crt;
      break;
    case L'\0':
      // NULL terminator
      nullFound = TRUE;
      break;
    default:
      localBuffer[crt] = lpszPath[crt];
      ++crt;
      break;
    }
  }

  if (FALSE == nullFound) {
    dwError = ERROR_BUFFER_OVERFLOW;
    CHECK_SVC_STATUS_DONE(dwError, L"localBuffer");
  }
  
  localBuffer[crt] = 0;
  cchLocalBuffer = crt;

  for(crt = 0; crt < gLocalDirsCount; ++crt) {

    // use max len gCchLocalDir[crt] to see if it starts with this local dir
    compareResult = CompareStringEx(
      LOCALE_NAME_INVARIANT,
      NORM_IGNORECASE,
      localBuffer, gCchLocalDir[crt] <= cchLocalBuffer ? gCchLocalDir[crt] : cchLocalBuffer, 
      gLocalDirs[crt], gCchLocalDir[crt],
      NULL, // lpVersionInformation
      NULL, // lpReserved
      (LPARAM) NULL); // lParam
    
    if (0 == compareResult) {
      dwError = GetLastError();
      CHECK_SVC_STATUS_DONE(dwError, L"CompareStringEx");
    }
    
    if (CSTR_EQUAL == compareResult) {
      break;
    }
  }

  if (CSTR_EQUAL != compareResult) {
    LogDebugMessage(L"ValidateLocalPath bad path: %s\n", lpszPath);
    dwError = ERROR_BAD_PATHNAME;
  }
  
done:
  return dwError;
}



//----------------------------------------------------------------------------
// Function: RunService
//
// Description:
//  Registers with NT SCM and starts the service
//
// Returns:
// ERROR_SUCCESS: On success
// Error code otherwise: otherwise
DWORD RunService(__in int argc, __in_ecount(argc) wchar_t *argv[])
{
  DWORD dwError= ERROR_SUCCESS;
  int argStart = 1;

  static const SERVICE_TABLE_ENTRY serviceTable[] = {
    { SVCNAME, (LPSERVICE_MAIN_FUNCTION) SvcMain },
    { NULL, NULL }
    };

  ghEventLog = RegisterEventSource(NULL, SVCNAME);
  if (NULL == ghEventLog) {
    dwError = GetLastError();
    CHECK_SVC_STATUS_DONE(dwError, L"RegisterEventSource")
  }

  if (!StartServiceCtrlDispatcher(serviceTable)) {
    dwError = GetLastError();
    CHECK_SVC_STATUS_DONE(dwError, L"StartServiceCtrlDispatcher")
  }

done:
  return dwError;
}

//----------------------------------------------------------------------------
// Function: SvcMain
//
// Description:
//  Service main entry point.
//
VOID WINAPI SvcMain(DWORD dwArg, LPTSTR* lpszArgv) {
  DWORD dwError = ERROR_SUCCESS;

  gSvcStatusHandle = RegisterServiceCtrlHandler( 
        SVCNAME, 
        SvcCtrlHandler);
  if( !gSvcStatusHandle ) { 
    dwError = GetLastError();
    CHECK_SVC_STATUS_DONE(dwError, L"RegisterServiceCtrlHandler")
  } 
  
  // These SERVICE_STATUS members remain as set here
  gSvcStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS; 
  gSvcStatus.dwServiceSpecificExitCode = 0;    

  // Report initial status to the SCM
  ReportSvcStatus( SERVICE_START_PENDING, NO_ERROR, 3000 );

  // Perform service-specific initialization and work.
  dwError = SvcInit();
  
done:
  return;
}

//----------------------------------------------------------------------------
// Function: SvcInit
//
// Description:
//  Initializes the service.
//
DWORD SvcInit() {
  DWORD dwError = ERROR_SUCCESS;

  dwError = EnableImpersonatePrivileges();
  if( dwError != ERROR_SUCCESS ) {
    ReportErrorCode(L"EnableImpersonatePrivileges", dwError);
    goto done;
  }

  // The recommended way to shutdown the service is to use an event
  //  and attach a callback with RegisterWaitForSingleObject
  //
  ghSvcStopEvent = CreateEvent(
                           NULL,    // default security attributes
                           TRUE,    // manual reset event
                           FALSE,   // not signaled
                           NULL);   // no name
  
  if ( ghSvcStopEvent == NULL)
  {
      dwError = GetLastError();
      ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
        dwError, L"CreateEvent");
      ReportSvcStatus( SERVICE_STOPPED, dwError, 0 );
      goto done;
  }

  if (!RegisterWaitForSingleObject (&ghWaitObject,
                            ghSvcStopEvent,
                            SvcShutdown,
                            NULL,
                            INFINITE,
                            WT_EXECUTEONLYONCE)) {
    dwError = GetLastError();
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      dwError, L"RegisterWaitForSingleObject");
    CloseHandle(ghSvcStopEvent);
    ReportSvcStatus( SERVICE_STOPPED, dwError, 0 );
    goto done;
  }

  dwError = ValidateConfigurationFile();
  if (ERROR_SUCCESS != dwError) {
    LogDebugMessage(L"ValidateConfigurationFile failed: %d", dwError);
    SvcError(dwError);
    goto done;
  }

  dwError = AuthInit();
  if (ERROR_SUCCESS != dwError) {
    LogDebugMessage(L"AuthInit failed: %d", dwError);
    SvcError(dwError);
    goto done;
  }

  dwError = InitLocalDirs();
  if (ERROR_SUCCESS != dwError) {
    LogDebugMessage(L"InitLocalDirs failed: %d", dwError);
    SvcError(dwError);
    goto done;
  }

  dwError = InitJobName();
  if (ERROR_SUCCESS != dwError) {
    LogDebugMessage(L"InitJobName failed: %d", dwError);
    SvcError(dwError);
    goto done;
  }

  // Report running status when initialization is  complete.
  ReportSvcStatus( SERVICE_RUNNING, NO_ERROR, 0 );

  dwError = RpcInit();

done:
  return dwError;
}

//----------------------------------------------------------------------------
// Function: RpcAuthorizeCallback
//
// Description:
//  RPC Authorization callback.
//
// Returns:
//  RPC_S_OK for access authorized
//  RPC_S_ACCESS_DENIED for access denied
//
RPC_STATUS CALLBACK RpcAuthorizeCallback (
  RPC_IF_HANDLE  hInterface,
  void* pContext) 
{
  RPC_STATUS                status, 
                            unwindStatus, 
                            authStatus = RPC_S_ACCESS_DENIED;
  DWORD                     dwError;
  LUID                      luidReserved2;
  AUTHZ_ACCESS_REQUEST      request;
  AUTHZ_ACCESS_REPLY        reply;
  AUTHZ_CLIENT_CONTEXT_HANDLE hClientContext = NULL;
  DWORD                     authError = ERROR_SUCCESS;
  DWORD                     saclResult = 0;
  ACCESS_MASK               grantedMask = 0;

  ZeroMemory(&luidReserved2, sizeof(luidReserved2));
  ZeroMemory(&request, sizeof(request));
  ZeroMemory(&reply, sizeof(reply));
  
  status = RpcGetAuthorizationContextForClient(NULL,
        FALSE,         // ImpersonateOnReturn
        NULL,          // Reserved1
        NULL,          // pExpirationTime
        luidReserved2, // Reserved2
        0,             // Reserved3
        NULL,          // Reserved4
        &hClientContext);
  CHECK_RPC_STATUS_DONE(status, L"RpcGetAuthorizationContextForClient");

  request.DesiredAccess = MAXIMUM_ALLOWED;  
  reply.Error = &authError;
  reply.SaclEvaluationResults = &saclResult;
  reply.ResultListLength = 1;
  reply.GrantedAccessMask = &grantedMask;

  if (!AuthzAccessCheck(
    0,
    hClientContext,
    &request,
    NULL,   // AuditEvent
    pAllowedSD,
    NULL,  // OptionalSecurityDescriptorArray
    0,     // OptionalSecurityDescriptorCount
    &reply,
    NULL  // phAccessCheckResults 
    )) {
    dwError = GetLastError();
    CHECK_SVC_STATUS_DONE(dwError, L"AuthzAccessCheck");
  }

  LogDebugMessage(L"AutzAccessCheck: Error:%d sacl:%d access:%d\n", 
    authError, saclResult, grantedMask);
  if (authError == ERROR_SUCCESS && (grantedMask & SERVICE_ACCESS_MASK)) {
    authStatus = RPC_S_OK;
  }
  
done:
  if (NULL != hClientContext) CHECK_UNWIND_RPC(RpcFreeAuthorizationContext(&hClientContext));
  return authStatus;
}

//----------------------------------------------------------------------------
// Function: AuthInit
//
// Description:
//  Initializes the authorization structures (security descriptor).
//
// Notes:
//  This is called from RunService solely for debugging purposed 
//   so that it can be tested by wimply running winutil service from CLI (no SCM)
//
DWORD AuthInit() {
  DWORD       dwError = ERROR_SUCCESS;
  size_t      count = 0;
  size_t      crt  = 0;
  size_t      len = 0;
  LPCWSTR     value = NULL;
  WCHAR**     tokens = NULL;
  LPWSTR      lpszSD = NULL;
  ULONG       cchSD = 0;
  DWORD       dwBufferSize = 0;
  size_t      allowedCount = 0;
  PSID*       allowedSids = NULL;
  

  dwError = GetConfigValue(
    wsceConfigRelativePath,
    NM_WSCE_ALLOWED, &len, &value);
  CHECK_SVC_STATUS_DONE(dwError, L"GetConfigValue");

  if (0 == len) {
    dwError = ERROR_BAD_CONFIGURATION;
    CHECK_SVC_STATUS_DONE(dwError, NM_WSCE_ALLOWED);
  }
  
  dwError = SplitStringIgnoreSpaceW(len, value, L',', &count, &tokens);
  CHECK_SVC_STATUS_DONE(dwError, L"SplitStringIgnoreSpaceW");

  allowedSids = (PSID*) LocalAlloc(LPTR, sizeof(PSID) * count);
  if (NULL == allowedSids) {
    dwError = ERROR_OUTOFMEMORY;
    CHECK_SVC_STATUS_DONE(dwError, L"LocalAlloc");
  }
  
  for (crt = 0; crt < count; ++crt) {
    dwError = GetSidFromAcctNameW(tokens[crt], &allowedSids[crt]);
    CHECK_SVC_STATUS_DONE(dwError, L"GetSidFromAcctNameW");
  }

  allowedCount = count;
  
  dwError = BuildServiceSecurityDescriptor(SERVICE_ACCESS_MASK,
    allowedCount, allowedSids, 0, NULL, NULL, &pAllowedSD);
  CHECK_SVC_STATUS_DONE(dwError, L"BuildServiceSecurityDescriptor");
  
done:
  if (lpszSD) LocalFree(lpszSD);
  if (value) LocalFree((HLOCAL)value);
  if (tokens) LocalFree(tokens);
  return dwError;
}

//----------------------------------------------------------------------------
// Function: RpcInit
//
// Description:
//  Initializes the RPC infrastructure and starts the RPC listenner.
//
DWORD RpcInit() {
  RPC_STATUS  status;
  DWORD       dwError;

  status = RpcServerUseProtseqIf(SVCBINDING, 
                 RPC_C_LISTEN_MAX_CALLS_DEFAULT,
                 HadoopWinutilSvc_v1_0_s_ifspec,
                 NULL);
  if (RPC_S_OK != status) {
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      status, L"RpcServerUseProtseqIf");
    SvcError(status);
    dwError = status;
    goto done;
  }

  status = RpcServerRegisterIfEx(HadoopWinutilSvc_v1_0_s_ifspec,
                 NULL,                                          // MgrTypeUuid
                 NULL,                                          // MgrEpv
                 RPC_IF_ALLOW_LOCAL_ONLY,                       // Flags
                 RPC_C_LISTEN_MAX_CALLS_DEFAULT,                // Max calls
                 RpcAuthorizeCallback);                         // Auth callback
  
  if (RPC_S_OK != status) {
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      status, L"RpcServerRegisterIfEx");
    SvcError(status);
    dwError = status;
    goto done;
  }

  status = RpcServerListen(1, RPC_C_LISTEN_MAX_CALLS_DEFAULT, TRUE);
  if (RPC_S_ALREADY_LISTENING == status) {
    ReportSvcCheckError(EVENTLOG_WARNING_TYPE, SERVICE_CATEGORY, 
      status, L"RpcServerListen");
  }
  else if (RPC_S_OK != status) {
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      status, L"RpcServerListen");
    SvcError(status);
    dwError = status;
    goto done;
  }

  isListenning = TRUE;
  
  ReportSvcMessage(EVENTLOG_INFORMATION_TYPE, SERVICE_CATEGORY, 
      MSG_RPC_SERVICE_HAS_STARTED);
  
done:
  return dwError;
}

//----------------------------------------------------------------------------
// Function: RpcStop
//
// Description:
//  Tears down the RPC infrastructure and stops the RPC listenner.
//
VOID RpcStop() {
  RPC_STATUS  status;
  
  if (isListenning) {

    status = RpcMgmtStopServerListening(NULL);
    isListenning = FALSE;
    
    if (RPC_S_OK != status) {
      ReportSvcCheckError(EVENTLOG_WARNING_TYPE, SERVICE_CATEGORY, 
        status, L"RpcMgmtStopServerListening");
    }
  
    ReportSvcMessage(EVENTLOG_INFORMATION_TYPE, SERVICE_CATEGORY, 
        MSG_RPC_SERVICE_HAS_STOPPED);
  }
}

//----------------------------------------------------------------------------
// Function: CleanupHandles
//
// Description:
//  Cleans up the global service handles.
//
VOID CleanupHandles() {
  if (INVALID_HANDLE_VALUE != ghWaitObject) {
    UnregisterWait(ghWaitObject);
    ghWaitObject = INVALID_HANDLE_VALUE;
  }
  if (INVALID_HANDLE_VALUE != ghSvcStopEvent) {
    CloseHandle(ghSvcStopEvent);
    ghSvcStopEvent = INVALID_HANDLE_VALUE;
  }
  if (INVALID_HANDLE_VALUE != ghEventLog) {
    DeregisterEventSource(ghEventLog);
    ghEventLog = INVALID_HANDLE_VALUE;
  }
}

//----------------------------------------------------------------------------
// Function: SvcError
//
// Description:
//  Aborts the startup sequence. Reports error, stops RPC, cleans up globals.
//
VOID SvcError(DWORD dwError) {
  RpcStop();
  CleanupHandles();
  ReportSvcStatus( SERVICE_STOPPED, dwError, 0 );
}

//----------------------------------------------------------------------------
// Function: SvcShutdown
//
// Description:
//  Callback when the shutdown event is signaled. Stops RPC, cleans up globals.
//
VOID CALLBACK SvcShutdown(
  _In_  PVOID lpParameter,
  _In_  BOOLEAN TimerOrWaitFired) {
  RpcStop();
  CleanupHandles();
  ReportSvcStatus( SERVICE_STOPPED, NO_ERROR, 0 );
}

//----------------------------------------------------------------------------
// Function: SvcCtrlHandler
//
// Description:
//  Callback from SCM for for service events (signals).
//
// Notes:
//   Shutdown is indirect, we set her the STOP_PENDING state and signal the stop event.
//   Signaling the event invokes SvcShutdown which completes the shutdown.
//   This two staged approach allows the SCM handler to complete fast, 
//   not blocking the SCM big fat global lock.
//
VOID WINAPI SvcCtrlHandler( DWORD dwCtrl )
{
   // Handle the requested control code. 

   switch(dwCtrl) 
   {  
      case SERVICE_CONTROL_STOP: 
         ReportSvcStatus(SERVICE_STOP_PENDING, NO_ERROR, 0);

         // Signal the service to stop.
         SetEvent(ghSvcStopEvent);
         
         return;
 
      default: 
         break;
   } 
   
}

//----------------------------------------------------------------------------
// Function: ReportSvcStatus
//
// Description:
//  Updates the service status with the SCM.
//
VOID ReportSvcStatus( DWORD dwCurrentState,
                      DWORD dwWin32ExitCode,
                      DWORD dwWaitHint)
{
    static DWORD dwCheckPoint = 1;
    DWORD dwError;

    // Fill in the SERVICE_STATUS structure.

    gSvcStatus.dwCurrentState = dwCurrentState;
    gSvcStatus.dwWin32ExitCode = dwWin32ExitCode;
    gSvcStatus.dwWaitHint = dwWaitHint;

    if (dwCurrentState == SERVICE_START_PENDING)
        gSvcStatus.dwControlsAccepted = 0;
    else gSvcStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP;

    if ( (dwCurrentState == SERVICE_RUNNING) ||
           (dwCurrentState == SERVICE_STOPPED) )
        gSvcStatus.dwCheckPoint = 0;
    else gSvcStatus.dwCheckPoint = dwCheckPoint++;

    // Report the status of the service to the SCM.
    if (!SetServiceStatus( gSvcStatusHandle, &gSvcStatus)) {
      dwError = GetLastError();
      ReportSvcCheckError(EVENTLOG_WARNING_TYPE, SERVICE_CATEGORY, 
        dwError, L"SetServiceStatus");
    };
}

//----------------------------------------------------------------------------
// Function: WinutilsCreateProcessAsUser
//
// Description:
//  The RPC midl declared function implementation
//
// Returns:
// ERROR_SUCCESS: On success
// Error code otherwise: otherwise
//
// Notes:
//  This is the entry point when the NodeManager does the RPC call
//  Note that the RPC call does not do any S4U work. Is simply spawns (suspended) wintutils
//  using the right command line and the handles over the spwaned process to the NM
//  The actual S4U work occurs in the spawned process, run and monitored by the NM
//
error_status_t WinutilsCreateProcessAsUser( 
    /* [in] */ handle_t IDL_handle,
    /* [in] */ int nmPid,
    /* [in] */ CREATE_PROCESS_REQUEST *request,
    /* [out] */ CREATE_PROCESS_RESPONSE **response) {

  DWORD dwError = ERROR_SUCCESS;
  LPCWSTR inserts[] = {request->cwd, request->jobName, request->user, request->pidFile, request->cmdLine, NULL};
  WCHAR winutilsPath[MAX_PATH];
  WCHAR fullCmdLine[32768];
  HANDLE taskStdInRd = INVALID_HANDLE_VALUE, taskStdInWr = INVALID_HANDLE_VALUE,
    taskStdOutRd = INVALID_HANDLE_VALUE, taskStdOutWr = INVALID_HANDLE_VALUE,
    taskStdErrRd = INVALID_HANDLE_VALUE, taskStdErrWr = INVALID_HANDLE_VALUE,
    hNmProcess = INVALID_HANDLE_VALUE,
    hDuplicateProcess = INVALID_HANDLE_VALUE,
    hDuplicateThread = INVALID_HANDLE_VALUE,
    hDuplicateStdIn  = INVALID_HANDLE_VALUE,
    hDuplicateStdOut = INVALID_HANDLE_VALUE,
    hDuplicateStdErr = INVALID_HANDLE_VALUE,
    hSelfProcess = INVALID_HANDLE_VALUE,
    hJob = INVALID_HANDLE_VALUE;
  BOOL fMustCleanupProcess = FALSE;
  
  HRESULT hr;
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  SECURITY_ATTRIBUTES saTaskStdInOutErr;

  ZeroMemory( &si, sizeof(si) );
  si.cb = sizeof(si);
  ZeroMemory( &pi, sizeof(pi) );
  pi.hProcess = INVALID_HANDLE_VALUE;
  pi.hThread = INVALID_HANDLE_VALUE;
  ZeroMemory( &saTaskStdInOutErr, sizeof(saTaskStdInOutErr));
  

  if (gJobName) {
    hJob = OpenJobObject(JOB_OBJECT_ASSIGN_PROCESS, FALSE, gJobName);
    if (!hJob) {
      dwError = GetLastError();
      ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
        dwError, L"OpenJobObject");
      goto done;
    }
  }


  // NB: GetCurrentProcess returns a pseudo-handle that just so happens 
  // has the value -1, ie. INVALID_HANDLE_VALUE. It cannot fail.
  // 
  hSelfProcess = GetCurrentProcess();

  hNmProcess = OpenProcess(PROCESS_DUP_HANDLE, FALSE, nmPid);
  if (NULL == hNmProcess) {
    dwError = GetLastError();
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      dwError, L"OpenProcess");
    goto done;
  }

  GetModuleFileName(NULL, winutilsPath, sizeof(winutilsPath)/sizeof(WCHAR));
  dwError = GetLastError(); // Always check after GetModuleFileName for ERROR_INSSUFICIENT_BUFFER
  if (dwError) {
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      dwError, L"GetModuleFileName");
    goto done;
  }

  // NB. We can call CreateProcess("wintuls","task create ...") or we can call
  // CreateProcess(NULL, "winutils task create"). Only the second form passes "task" as
  // argv[1], as expected by main. First form passes "task" as argv[0] and main fails.
  
  hr = StringCbPrintf(fullCmdLine, sizeof(fullCmdLine), L"\"%s\" task createAsUser %ls %ls %ls %ls",
    winutilsPath,
    request->jobName, request->user, request->pidFile, request->cmdLine);
  if (FAILED(hr)) {
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      hr, L"StringCbPrintf:fullCmdLine");
    goto done;
  }

  LogDebugMessage(L"[%ls]: %ls %ls\n", request->cwd, winutilsPath, fullCmdLine);

  // stdin/stdout/stderr redirection is handled here
  // We create 3 anonymous named pipes. 
  // Security attributes are required so that the handles can be inherited.
  // We assign one end of the pipe to the process (stdin gets a read end, stdout gets a write end)
  // We then duplicate the other end in the NM process, and we close our own handle
  // Finally we return the duplicate handle values to the NM
  // The NM will attach Java file dscriptors to the duplicated handles and 
  // read/write them as ordinary Java InputStream/OutputStream objects

  si.dwFlags |= STARTF_USESTDHANDLES;

  saTaskStdInOutErr.nLength = sizeof(SECURITY_ATTRIBUTES); 
  saTaskStdInOutErr.bInheritHandle = TRUE; 
  saTaskStdInOutErr.lpSecurityDescriptor = NULL; 

  if (!CreatePipe(&taskStdInRd, &taskStdInWr, &saTaskStdInOutErr, 0)) {
    dwError = GetLastError();
    goto done;
  }
  if (!SetHandleInformation(taskStdInWr, HANDLE_FLAG_INHERIT, FALSE)) {
    dwError = GetLastError();
    goto done;
  }
  si.hStdInput  = taskStdInRd;

  if (!CreatePipe(&taskStdOutRd, &taskStdOutWr, &saTaskStdInOutErr, 0)) {
    dwError = GetLastError();
    goto done;
  }
  if (!SetHandleInformation(taskStdOutRd, HANDLE_FLAG_INHERIT, FALSE)) {
    dwError = GetLastError();
    goto done;
  }
  si.hStdOutput  = taskStdOutWr;

  if (!CreatePipe(&taskStdErrRd, &taskStdErrWr, &saTaskStdInOutErr, 0)) {
    dwError = GetLastError();
    goto done;
  }
  if (!SetHandleInformation(taskStdErrRd, HANDLE_FLAG_INHERIT, FALSE)) {
    dwError = GetLastError();
    goto done;
  }
  si.hStdError  = taskStdErrWr;

  if (!CreateProcess(
    NULL,                     // lpApplicationName,
    fullCmdLine,              // lpCommandLine,
    NULL,                     // lpProcessAttributes,
    NULL,                     // lpThreadAttributes,
    TRUE,                     // bInheritHandles,
    CREATE_SUSPENDED,         // dwCreationFlags,
    NULL,                     // lpEnvironment,
    request->cwd,             // lpCurrentDirectory,
    &si,                      // lpStartupInfo
    &pi)) {                   // lpProcessInformation
    
    dwError = GetLastError();
    ReportSvcCheckError(EVENTLOG_ERROR_TYPE, SERVICE_CATEGORY, 
      dwError, L"CreateProcess");
    goto done;
  }

  fMustCleanupProcess = TRUE;

  LogDebugMessage(L"CreateProcess: pid:%x\n", pi.dwProcessId);

  if (INVALID_HANDLE_VALUE != hJob) {
    if (!AssignProcessToJobObject(hJob, pi.hProcess)) {
      dwError = GetLastError();
      goto done;
    }
  }

  // Grant full access to the container user on the 'winutils task createAsUser ...' helper process
  dwError = AddNodeManagerAndUserACEsToObject(pi.hProcess, request->user, PROCESS_ALL_ACCESS);
  if (dwError) {
    LogDebugMessage(L"failed: AddNodeManagerAndUserACEsToObject\n");
    goto done;
  }

  if (!DuplicateHandle(hSelfProcess, pi.hProcess, hNmProcess,
    &hDuplicateProcess, 0, FALSE, DUPLICATE_SAME_ACCESS)) {
    dwError = GetLastError();
    LogDebugMessage(L"failed: pi.hProcess\n");
    goto done;
  }
  
  if (!DuplicateHandle(hSelfProcess, pi.hThread, hNmProcess,
    &hDuplicateThread, 0, FALSE, DUPLICATE_SAME_ACCESS)) {
    dwError = GetLastError();
    LogDebugMessage(L"failed: pi.hThread\n");
    goto done;
  }

  if (!DuplicateHandle(hSelfProcess, taskStdInWr, hNmProcess,
    &hDuplicateStdIn, 0, FALSE, DUPLICATE_SAME_ACCESS)) {
    dwError = GetLastError();
    LogDebugMessage(L"failed: taskStdInWr\n");
    goto done;
  }

  if (!DuplicateHandle(hSelfProcess, taskStdOutRd, hNmProcess,
    &hDuplicateStdOut, 0, FALSE, DUPLICATE_SAME_ACCESS)) {
    dwError = GetLastError();
    LogDebugMessage(L"failed: taskStdOutRd\n");
    goto done;
  }

  if (!DuplicateHandle(hSelfProcess, taskStdErrRd, hNmProcess,
    &hDuplicateStdErr, 0, FALSE, DUPLICATE_SAME_ACCESS)) {
    dwError = GetLastError();
    LogDebugMessage(L"failed: taskStdErrRd\n");
    goto done;
  }

  *response = (CREATE_PROCESS_RESPONSE*) MIDL_user_allocate(sizeof(CREATE_PROCESS_RESPONSE));
  if (NULL == *response) {
    dwError = ERROR_OUTOFMEMORY;
    LogDebugMessage(L"Failed to allocate CREATE_PROCESS_RESPONSE* response\n");
    goto done;
  }

  // We're now transfering ownership of the duplicated handles to the caller
  // If the RPC call fails *after* this point the handles are leaked inside the NM process
  // Note that there are no more API calls, only assignments. A failure could occur only if
  // foced (process kill) or hardware error (faulty memory, processort bit flip etc).

  // as MIDL has no 'HANDLE' type, the (LONG_PTR) is used instead
  (*response)->hProcess = (LONG_PTR)hDuplicateProcess;
  (*response)->hThread = (LONG_PTR)hDuplicateThread;
  (*response)->hStdIn = (LONG_PTR)hDuplicateStdIn;
  (*response)->hStdOut = (LONG_PTR)hDuplicateStdOut;
  (*response)->hStdErr = (LONG_PTR)hDuplicateStdErr;

  fMustCleanupProcess = FALSE;
  
done:

  if (fMustCleanupProcess) {
    LogDebugMessage(L"Cleaning process: %d due to error:%d\n", pi.dwProcessId, dwError);
    TerminateProcess(pi.hProcess, EXIT_FAILURE);

    // cleanup the duplicate handles inside the NM.

    if (INVALID_HANDLE_VALUE != hDuplicateProcess) {
      DuplicateHandle(hNmProcess, hDuplicateProcess, NULL, NULL, 0, FALSE, DUPLICATE_CLOSE_SOURCE);
    }
    if (INVALID_HANDLE_VALUE != hDuplicateThread) {
      DuplicateHandle(hNmProcess, hDuplicateThread, NULL, NULL, 0, FALSE, DUPLICATE_CLOSE_SOURCE);
    }
    if (INVALID_HANDLE_VALUE != hDuplicateStdIn) {
      DuplicateHandle(hNmProcess, hDuplicateStdIn, NULL, NULL, 0, FALSE, DUPLICATE_CLOSE_SOURCE);
    }
    if (INVALID_HANDLE_VALUE != hDuplicateStdOut) {
      DuplicateHandle(hNmProcess, hDuplicateStdOut, NULL, NULL, 0, FALSE, DUPLICATE_CLOSE_SOURCE);
    }
    if (INVALID_HANDLE_VALUE != hDuplicateStdErr) {
      DuplicateHandle(hNmProcess, hDuplicateStdErr, NULL, NULL, 0, FALSE, DUPLICATE_CLOSE_SOURCE);
    }
  }

  if (INVALID_HANDLE_VALUE != hSelfProcess) CloseHandle(hSelfProcess);
  if (INVALID_HANDLE_VALUE != hNmProcess) CloseHandle(hNmProcess);
  if (INVALID_HANDLE_VALUE != taskStdInRd) CloseHandle(taskStdInRd);
  if (INVALID_HANDLE_VALUE != taskStdInWr) CloseHandle(taskStdInWr);
  if (INVALID_HANDLE_VALUE != taskStdOutRd) CloseHandle(taskStdOutRd);
  if (INVALID_HANDLE_VALUE != taskStdOutWr) CloseHandle(taskStdOutWr);
  if (INVALID_HANDLE_VALUE != taskStdErrRd) CloseHandle(taskStdErrRd);
  if (INVALID_HANDLE_VALUE != taskStdErrWr) CloseHandle(taskStdErrWr);


  // This is closing our own process/thread handles. 
  // If the transfer was succesfull the NM has its own duplicates (if any)
  if (INVALID_HANDLE_VALUE != pi.hThread) CloseHandle(pi.hThread);
  if (INVALID_HANDLE_VALUE != pi.hProcess) CloseHandle(pi.hProcess);

  if (hJob) CloseHandle(hJob);

  return dwError;
}

error_status_t WinutilsCreateFile(
  /* [in] */ handle_t IDL_handle,
  /* [in] */ int nm_pid,
  /* [in] */ CREATEFILE_REQUEST *request,
  /* [out] */ CREATEFILE_RESPONSE **response) {

  DWORD dwError = ERROR_SUCCESS;

  HANDLE hNmProcess = INVALID_HANDLE_VALUE, 
    hFile = INVALID_HANDLE_VALUE,
    hDuplicateFile = INVALID_HANDLE_VALUE,
    hSelfProcess = GetCurrentProcess();

  SECURITY_ATTRIBUTES saFile;

  ZeroMemory( &saFile, sizeof(saFile)); 

  dwError = ValidateLocalPath(request->path);
  CHECK_SVC_STATUS_DONE(dwError,L"ValidateLocalPath request->path");    

  saFile.nLength = sizeof(SECURITY_ATTRIBUTES); 
  saFile.bInheritHandle = TRUE; 
  saFile.lpSecurityDescriptor = NULL;

  hFile = CreateFile(
    request->path,
    request->desiredAccess,
    request->shareMode,
    &saFile,
    request->creationDisposition,
    request->flags,
    NULL); // hTemplate
  if (INVALID_HANDLE_VALUE == hFile) {
    dwError = GetLastError();
    goto done;
  }

  hNmProcess = OpenProcess(PROCESS_DUP_HANDLE, FALSE, nm_pid);
  if (NULL == hNmProcess) {
    dwError = GetLastError();
    goto done;
  }

  if (!DuplicateHandle(hSelfProcess, hFile,
    hNmProcess, &hDuplicateFile,
    0, FALSE, DUPLICATE_SAME_ACCESS)) {
    dwError = GetLastError();
    goto done;
  }

  *response = (CREATEFILE_RESPONSE*) MIDL_user_allocate(sizeof(CREATEFILE_RESPONSE));
  if (NULL == *response) {
    dwError = ERROR_OUTOFMEMORY;
    goto done;
  }

  // As MIDL has no 'HANDLE' type, (LONG_PTR) is used instead
  (*response)->hFile = (LONG_PTR)hDuplicateFile;
  hDuplicateFile = INVALID_HANDLE_VALUE;

done:

  if (INVALID_HANDLE_VALUE != hFile) CloseHandle(hFile);
  if (INVALID_HANDLE_VALUE != hDuplicateFile) {
    DuplicateHandle(hNmProcess, hDuplicateFile, NULL, NULL, 0, FALSE, DUPLICATE_CLOSE_SOURCE);
  }
  if (INVALID_HANDLE_VALUE != hNmProcess) CloseHandle(hNmProcess);

  LogDebugMessage(L"WinutilsCreateFile: %s %d, %d, %d, %d: %d",
    request->path,
    request->desiredAccess,
    request->shareMode,
    request->creationDisposition,
    request->flags,
    dwError);
  
  return dwError;
}

error_status_t WinutilsKillTask( 
    /* [in] */ handle_t IDL_handle,
    /* [in] */ KILLTASK_REQUEST *request) {
  DWORD dwError = ERROR_SUCCESS;
  WCHAR bufferName[MAX_PATH];

  dwError = GetSecureJobObjectName(request->taskName, MAX_PATH, bufferName);
  CHECK_SVC_STATUS_DONE(dwError, L"GetSecureJobObjectName");

  dwError = KillTask(bufferName);

  if (ERROR_ACCESS_DENIED == dwError) {
    // This process runs as LocalSystem with debug privilege enabled
    // The job has a security descriptor that explictly grants JOB_OBJECT_ALL_ACCESS to us
    // If we get ACCESS DENIED it means the job is being unwound
    dwError = ERROR_SUCCESS;
  }
  
done:  
  LogDebugMessage(L"WinutilsKillTask: %s :%d\n", bufferName, dwError);
  return dwError;
}


error_status_t WinutilsDeletePath(
  /* [in] */ handle_t IDL_handle,
  /* [in] */ DELETEPATH_REQUEST *request,
  /* [out] */ DELETEPATH_RESPONSE **response) {

  DWORD dwError = ERROR_SUCCESS;
  BOOL deleted = FALSE;

  dwError = ValidateLocalPath(request->path);
  CHECK_SVC_STATUS_DONE(dwError,L"ValidateLocalPath request->path");

  switch(request->type) {
  case PATH_IS_DIR:
    deleted = RemoveDirectory(request->path);
    if (!deleted) {
      LogDebugMessage(L"Error %d deleting directory %s\n", GetLastError(), request->path);
    }
    break;
  case PATH_IS_FILE:
    deleted = DeleteFile(request->path);
    if (!deleted) {
      LogDebugMessage(L"Error %d deleting file %s\n", GetLastError(), request->path);
    }
    break;
  default:
    dwError = ERROR_BAD_ARGUMENTS;
    CHECK_SVC_STATUS_DONE(dwError, L"request->operation");
  }

  *response = (DELETEPATH_RESPONSE*) MIDL_user_allocate(sizeof(DELETEPATH_RESPONSE));
  if (NULL == *response) {
    dwError = ERROR_OUTOFMEMORY;
    CHECK_SVC_STATUS_DONE(dwError, L"MIDL_user_allocate");
  }

  (*response)->deleted = deleted;

done:

  LogDebugMessage(L"WinutilsDeletePath: %s %d: %d %d",
    request->path,
    request->type,
    deleted,
    dwError);
  
  return dwError;
}

error_status_t WinutilsMkDir( 
    /* [in] */ handle_t IDL_handle,
    /* [in] */ MKDIR_REQUEST *request) {
  DWORD dwError = ERROR_SUCCESS;

  dwError = ValidateLocalPath(request->filePath);
  CHECK_SVC_STATUS_DONE(dwError,L"ValidateLocalPath request->filePath");  

  if (!CreateDirectory(request->filePath, NULL)) {
    dwError = GetLastError();
    CHECK_SVC_STATUS_DONE(dwError, L"CreateDirectory");
  }
  
done:  
  LogDebugMessage(L"WinutilsMkDir: %s :%d\n", request->filePath, dwError);
  return dwError;
}

error_status_t WinutilsChown( 
    /* [in] */ handle_t IDL_handle,
    /* [in] */ CHOWN_REQUEST *request) {
  DWORD dwError = ERROR_SUCCESS;

  dwError = ValidateLocalPath(request->filePath);
  CHECK_SVC_STATUS_DONE(dwError,L"ValidateLocalPath request->filePath");
  
  dwError = ChownImpl(request->ownerName, request->groupName, request->filePath);
  CHECK_SVC_STATUS_DONE(dwError, L"ChownImpl");

done:  
  LogDebugMessage(L"WinutilsChown: %s %s %s :%d\n",
    request->ownerName, request->groupName, request->filePath, dwError);
  return dwError;
}

error_status_t WinutilsChmod( 
    /* [in] */ handle_t IDL_handle,
    /* [in] */ CHMOD_REQUEST *request) {
  DWORD dwError = ERROR_SUCCESS;

  dwError = ValidateLocalPath(request->filePath);
  CHECK_SVC_STATUS_DONE(dwError,L"ValidateLocalPath request->filePath");
  
  dwError = ChangeFileModeByMask(request->filePath, request->mode);
  CHECK_SVC_STATUS_DONE(dwError, L"ChangeFileModeByMask");

done:
  LogDebugMessage(L"WinutilsChmod: %s %o :%d\n",
   request->filePath, request->mode, dwError);
  return dwError;
}

error_status_t WinutilsMoveFile( 
    /* [in] */ handle_t IDL_handle,
    /* [in] */ MOVEFILE_REQUEST *request) {
  DWORD dwError = ERROR_SUCCESS;
  DWORD flags = 0;

  dwError = ValidateLocalPath(request->sourcePath);
  CHECK_SVC_STATUS_DONE(dwError,L"ValidateLocalPath request->sourcePath");

  dwError = ValidateLocalPath(request->destinationPath);
  CHECK_SVC_STATUS_DONE(dwError,L"ValidateLocalPath request->destinationPath");

  switch (request->operation) {
  case MOVE_FILE:
    flags |= MOVEFILE_COPY_ALLOWED;
    if (request->replaceExisting) flags |= MOVEFILE_REPLACE_EXISTING;
    if (!MoveFileEx(request->sourcePath, request->destinationPath, flags)) {
      dwError = GetLastError();
      CHECK_SVC_STATUS_DONE(dwError, L"MoveFileEx");
    }
    break;
  case COPY_FILE:
    if (!request->replaceExisting) flags |= COPY_FILE_FAIL_IF_EXISTS;
    if (!CopyFileEx(request->sourcePath, request->destinationPath,
          NULL, // lpProgressRoutine
          NULL, // lpData
          NULL, // pbCancel
          flags)) {
      dwError = GetLastError();
      CHECK_SVC_STATUS_DONE(dwError, L"CopyFileEx");
    }
    break;
  default:
    dwError = ERROR_BAD_ARGUMENTS;
    CHECK_SVC_STATUS_DONE(dwError, L"request->operation");
  }

done:  
  LogDebugMessage(L"WinutilsMoveFile: %d: %s %s :%d\n",
    request->operation, request->sourcePath, request->destinationPath, dwError);
  return dwError;
}


//----------------------------------------------------------------------------
// Function: ServiceUsage
//
// Description:
//  Prints the CLI arguments for service command.
//
void ServiceUsage()
{
  fwprintf(stdout, L"\
    Usage: service\n\
    Starts the nodemanager Windows Secure Container Executor helper service.\n\
    The service must run as a high privileged account (LocalSystem)\n\
    and is used by the nodemanager WSCE to spawn secure containers on Windows.\n");
}


