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
#include <Winsvc.h>
#include <errno.h>
#include "hadoopwinutilsvc_h.h"

#pragma comment(lib, "Rpcrt4.lib")
#pragma comment(lib, "advapi32.lib")

static ACCESS_MASK CLIENT_MASK = 1;

VOID ReportClientError(LPWSTR lpszLocation, DWORD dwError) {
  LPWSTR      debugMsg = NULL;
  int         len;

  if (IsDebuggerPresent()) {
    len = FormatMessageW(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
      NULL, dwError,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPWSTR)&debugMsg, 0, NULL);

    LogDebugMessage(L"%s: %s: %x: %.*s\n", GetSystemTimeString(), lpszLocation, dwError, len, debugMsg);
  }

  if (NULL != debugMsg)  LocalFree(debugMsg);
}

DWORD PrepareRpcBindingHandle(
  __out RPC_BINDING_HANDLE* pHadoopWinutilsSvcBinding) {
  DWORD       dwError = EXIT_FAILURE;
  RPC_STATUS  status;
  LPWSTR      lpszStringBinding    = NULL;
  RPC_SECURITY_QOS_V3 qos;
  SID_IDENTIFIER_AUTHORITY authNT = SECURITY_NT_AUTHORITY;
  BOOL rpcBindingInit = FALSE;
  PSID        pLocalSystemSid = NULL;
  DWORD       cbSystemSidSize = SECURITY_MAX_SID_SIZE;

  pLocalSystemSid = (PSID) LocalAlloc(LPTR, cbSystemSidSize);
  if (!pLocalSystemSid) {
    dwError = GetLastError();
    ReportClientError(L"LocalAlloc", dwError);
    goto done;
  }

  if (!CreateWellKnownSid(WinLocalSystemSid, NULL, pLocalSystemSid, &cbSystemSidSize)) {
    dwError = GetLastError();
    ReportClientError(L"CreateWellKnownSid", dwError);
    goto done;
  }

  ZeroMemory(&qos, sizeof(qos));
  qos.Version = RPC_C_SECURITY_QOS_VERSION_3;
  qos.Capabilities = RPC_C_QOS_CAPABILITIES_LOCAL_MA_HINT |  RPC_C_QOS_CAPABILITIES_MUTUAL_AUTH;
  qos.IdentityTracking = RPC_C_QOS_IDENTITY_DYNAMIC;
  qos.ImpersonationType = RPC_C_IMP_LEVEL_DEFAULT;
  qos.Sid = pLocalSystemSid;

  status = RpcStringBindingCompose(NULL,
                 SVCBINDING,
                 NULL,
                 SVCNAME,
                 NULL,
                 &lpszStringBinding);
  if (RPC_S_OK != status) {
    ReportClientError(L"RpcStringBindingCompose", status);
    dwError = status;
    goto done;
  }

  status = RpcBindingFromStringBinding(lpszStringBinding, pHadoopWinutilsSvcBinding);

  if (RPC_S_OK != status) {
    ReportClientError(L"RpcBindingFromStringBinding", status);
    dwError = status;
    goto done;
  }
  rpcBindingInit = TRUE;

  status = RpcBindingSetAuthInfoEx(
                  *pHadoopWinutilsSvcBinding,
                  NULL,
                  RPC_C_AUTHN_LEVEL_PKT_PRIVACY,  // AuthnLevel
                  RPC_C_AUTHN_WINNT,              // AuthnSvc
                  NULL,                           // AuthnIdentity (self)
                  RPC_C_AUTHZ_NONE,               // AuthzSvc
                  (RPC_SECURITY_QOS*) &qos);
  if (RPC_S_OK != status) {
    ReportClientError(L"RpcBindingSetAuthInfoEx", status);
    dwError = status;
    goto done;
  }

  dwError = ERROR_SUCCESS;

done:

  if (dwError && rpcBindingInit) RpcBindingFree(pHadoopWinutilsSvcBinding);

  if (pLocalSystemSid) LocalFree(pLocalSystemSid);
  
  if (NULL != lpszStringBinding) {
    status = RpcStringFree(&lpszStringBinding);
    if (RPC_S_OK != status) {
      ReportClientError(L"RpcStringFree", status);
    }
  }
  
  return dwError;
}

DWORD RpcCall_WinutilsKillTask(
  __in LPCWSTR taskName) {

  DWORD       dwError = EXIT_FAILURE;
  ULONG       ulCode;
  KILLTASK_REQUEST request;
  RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
  BOOL rpcBindingInit = FALSE;

  dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
  if (dwError) {
    ReportClientError(L"PrepareRpcBindingHandle", dwError);
    goto done;
  }
  rpcBindingInit = TRUE;

  ZeroMemory(&request, sizeof(request));
  request.taskName = taskName;

  RpcTryExcept {
    dwError = WinutilsKillTask(hHadoopWinutilsSvcBinding, &request);
  }
  RpcExcept(1) {
    ulCode = RpcExceptionCode();
    ReportClientError(L"RpcExcept", ulCode);
    dwError = (DWORD) ulCode;
  }
  RpcEndExcept;

done:
  if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);

  LogDebugMessage(L"RpcCall_WinutilsKillTask: %s :%d\n", taskName, dwError);

  return dwError;
}

DWORD RpcCall_WinutilsMkDir(
  __in LPCWSTR filePath) {

  DWORD       dwError = EXIT_FAILURE;
  ULONG       ulCode;
  MKDIR_REQUEST request;
  RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
  BOOL rpcBindingInit = FALSE;

  dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
  if (dwError) {
    ReportClientError(L"PrepareRpcBindingHandle", dwError);
    goto done;
  }
  rpcBindingInit = TRUE;

  ZeroMemory(&request, sizeof(request));
  request.filePath = filePath;

  RpcTryExcept {
    dwError = WinutilsMkDir(hHadoopWinutilsSvcBinding, &request);
  }
  RpcExcept(1) {
    ulCode = RpcExceptionCode();
    ReportClientError(L"RpcExcept", ulCode);
    dwError = (DWORD) ulCode;
  }
  RpcEndExcept;

done:
  if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);

  LogDebugMessage(L"RpcCall_WinutilsMkDir: %s :%d\n", filePath, dwError);

  return dwError;
}



DWORD RpcCall_WinutilsChown(
  __in LPCWSTR filePath, 
  __in_opt LPCWSTR ownerName, 
  __in_opt LPCWSTR groupName) {

  DWORD       dwError = EXIT_FAILURE;
  ULONG       ulCode;
  CHOWN_REQUEST request;
  RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
  BOOL rpcBindingInit = FALSE;

  dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
  if (dwError) {
    ReportClientError(L"PrepareRpcBindingHandle", dwError);
    goto done;
  }
  rpcBindingInit = TRUE;

  ZeroMemory(&request, sizeof(request));
  request.filePath = filePath;
  request.ownerName = ownerName;
  request.groupName = groupName;

  RpcTryExcept {
    dwError = WinutilsChown(hHadoopWinutilsSvcBinding, &request);
  }
  RpcExcept(1) {
    ulCode = RpcExceptionCode();
    ReportClientError(L"RpcExcept", ulCode);
    dwError = (DWORD) ulCode;
  }
  RpcEndExcept;

done:
  if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);

  LogDebugMessage(L"RpcCall_WinutilsChown: %s %s %s :%d\n",
    ownerName, groupName, filePath, dwError);

  return dwError;
}


DWORD RpcCall_WinutilsChmod(
  __in LPCWSTR filePath, 
  __in int mode) {

  DWORD       dwError = EXIT_FAILURE;
  ULONG       ulCode;
  CHMOD_REQUEST request;
  RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
  BOOL rpcBindingInit = FALSE;

  dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
  if (dwError) {
    ReportClientError(L"PrepareRpcBindingHandle", dwError);
    goto done;
  }
  rpcBindingInit = TRUE;

  ZeroMemory(&request, sizeof(request));
  request.filePath = filePath;
  request.mode = mode;

  RpcTryExcept {
    dwError = WinutilsChmod(hHadoopWinutilsSvcBinding, &request);
  }
  RpcExcept(1) {
    ulCode = RpcExceptionCode();
    ReportClientError(L"RpcExcept", ulCode);
    dwError = (DWORD) ulCode;
  }
  RpcEndExcept;

done:
  if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);

  LogDebugMessage(L"RpcCall_WinutilsChmod: %s %o :%d\n",
    filePath, mode, dwError);

  return dwError;
} 



DWORD RpcCall_WinutilsMoveFile(
  __in int operation,
  __in LPCWSTR sourcePath, 
  __in LPCWSTR destinationPath,
  __in BOOL replaceExisting) {

  DWORD       dwError = EXIT_FAILURE;
  ULONG       ulCode;
  MOVEFILE_REQUEST request;
  RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
  BOOL rpcBindingInit = FALSE;

  dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
  if (dwError) {
    ReportClientError(L"PrepareRpcBindingHandle", dwError);
    goto done;
  }
  rpcBindingInit = TRUE;

  ZeroMemory(&request, sizeof(request));
  request.operation = operation;
  request.sourcePath = sourcePath;
  request.destinationPath = destinationPath;
  request.replaceExisting = replaceExisting;

  RpcTryExcept {
    dwError = WinutilsMoveFile(hHadoopWinutilsSvcBinding, &request);
  }
  RpcExcept(1) {
    ulCode = RpcExceptionCode();
    ReportClientError(L"RpcExcept", ulCode);
    dwError = (DWORD) ulCode;
  }
  RpcEndExcept;

done:
  if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);

  LogDebugMessage(L"RpcCall_WinutilsMoveFile: %s %s %d :%d\n",
    sourcePath, destinationPath, replaceExisting, dwError);

  return dwError;
}

DWORD RpcCall_WinutilsCreateFile(
  __in LPCWSTR path,
  __in DWORD desiredAccess,
  __in DWORD shareMode,
  __in DWORD creationDisposition,
  __in DWORD flags,
  __out HANDLE* hFile) {

  DWORD       dwError = EXIT_FAILURE;
  ULONG       ulCode;
  DWORD       dwSelfPid = GetCurrentProcessId();
  CREATEFILE_REQUEST request;
  CREATEFILE_RESPONSE *response = NULL;
  RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
  BOOL rpcBindingInit = FALSE;

  dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
  if (dwError) {
    ReportClientError(L"PrepareRpcBindingHandle", dwError);
    goto done;
  }
  rpcBindingInit = TRUE;

  ZeroMemory(&request, sizeof(request));
  request.path = path;
  request.desiredAccess = desiredAccess;
  request.shareMode = shareMode;
  request.creationDisposition = creationDisposition;
  request.flags = flags;

  RpcTryExcept {
    dwError = WinutilsCreateFile(hHadoopWinutilsSvcBinding, dwSelfPid, &request, &response);
  }
  RpcExcept(1) {
    ulCode = RpcExceptionCode();
    ReportClientError(L"RpcExcept", ulCode);
    dwError = (DWORD) ulCode;
  }
  RpcEndExcept;

  if (ERROR_SUCCESS == dwError) {
    *hFile = (HANDLE) response->hFile;
  }

done:
  if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);

  if(NULL != response) MIDL_user_free(response);

  LogDebugMessage(L"RpcCall_WinutilsCreateFile: %s %d, %d, %d, %d :%d\n",
    path, desiredAccess, shareMode, creationDisposition, flags, dwError);

  return dwError;
}


DWORD RpcCall_WinutilsDeletePath(
  __in LPCWSTR    path,
  __in BOOL       isDir,
  __out BOOL*     pDeleted) {

  DWORD       dwError = EXIT_FAILURE;
  ULONG       ulCode;
  DELETEPATH_REQUEST request;
  DELETEPATH_RESPONSE *response = NULL;
  RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
  BOOL rpcBindingInit = FALSE;

  pDeleted = FALSE;

  dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
  if (dwError) {
    ReportClientError(L"PrepareRpcBindingHandle", dwError);
    goto done;
  }
  rpcBindingInit = TRUE;

  ZeroMemory(&request, sizeof(request));
  request.path = path;
  request.type = isDir ? PATH_IS_DIR : PATH_IS_FILE;

  RpcTryExcept {
    dwError = WinutilsDeletePath(hHadoopWinutilsSvcBinding, &request, &response);
  }
  RpcExcept(1) {
    ulCode = RpcExceptionCode();
    ReportClientError(L"RpcExcept", ulCode);
    dwError = (DWORD) ulCode;
  }
  RpcEndExcept;

  if (ERROR_SUCCESS == dwError) {
    *pDeleted = response->deleted;
  }

done:
  if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);

  if(NULL != response) MIDL_user_free(response);

  LogDebugMessage(L"RpcCall_WinutilsDeletePath: %s %d: %d %d\n",
    path, isDir, *pDeleted, dwError);

  return dwError;
}


DWORD RpcCall_TaskCreateAsUser(
  LPCWSTR cwd, LPCWSTR jobName, 
  LPCWSTR user, LPCWSTR pidFile, LPCWSTR cmdLine, 
  HANDLE* phProcess, HANDLE* phThread, HANDLE* phStdIn, HANDLE* phStdOut, HANDLE* phStdErr) 
{
    DWORD       dwError = EXIT_FAILURE;
    ULONG       ulCode;
    DWORD       dwSelfPid = GetCurrentProcessId();
    CREATE_PROCESS_REQUEST request;
    CREATE_PROCESS_RESPONSE *response = NULL;
    RPC_BINDING_HANDLE hHadoopWinutilsSvcBinding;
    BOOL rpcBindingInit = FALSE;

    dwError = PrepareRpcBindingHandle(&hHadoopWinutilsSvcBinding);
    if (dwError) {
      ReportClientError(L"PrepareRpcBindingHandle", dwError);
      goto done;
    }
    rpcBindingInit = TRUE;

    ZeroMemory(&request, sizeof(request));
    request.cwd = cwd;
    request.jobName = jobName;
    request.user = user;
    request.pidFile = pidFile;
    request.cmdLine = cmdLine;

    RpcTryExcept {
      dwError = WinutilsCreateProcessAsUser(hHadoopWinutilsSvcBinding, dwSelfPid, &request, &response);
    }
    RpcExcept(1) {
      ulCode = RpcExceptionCode();
      ReportClientError(L"RpcExcept", ulCode);
      dwError = (DWORD) ulCode;
    }
    RpcEndExcept;

    if (ERROR_SUCCESS == dwError) {
      *phProcess = (HANDLE) response->hProcess;
      *phThread = (HANDLE) response->hThread;
      *phStdIn = (HANDLE) response->hStdIn;
      *phStdOut = (HANDLE) response->hStdOut;
      *phStdErr = (HANDLE) response->hStdErr;
    }

done:
    if (rpcBindingInit) RpcBindingFree(&hHadoopWinutilsSvcBinding);
  
    if (NULL != response) {
      MIDL_user_free(response);
    }
    
    return dwError;
}

