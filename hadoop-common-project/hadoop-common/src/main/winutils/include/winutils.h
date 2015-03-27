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

#ifndef UNICODE
#define UNICODE
#endif

#pragma once
#include <stdio.h>
#include <assert.h>
#include <windows.h>
#include <aclapi.h>
#include <accctrl.h>
#include <strsafe.h>
#include <lm.h>
#include <ntsecapi.h>
#include <userenv.h>

#ifdef __cplusplus
extern "C" {
#endif


enum EXIT_CODE
{
  /* Common success exit code shared among all utilities */
  SUCCESS = EXIT_SUCCESS,
  /* Generic failure exit code share among all utilities */
  FAILURE = EXIT_FAILURE,
  /* Failure code indicates the user does not privilege to create symlinks */
  SYMLINK_NO_PRIVILEGE = 2,

  ERROR_TASK_NOT_ALIVE = 1,
  
  // This exit code for killed processes is compatible with Unix, where a killed
  // process exits with 128 + signal.  For SIGKILL, this would be 128 + 9 = 137.
  KILLED_PROCESS_EXIT_CODE = 137,
};


/*
 * The array of 12 months' three-letter abbreviations 
 */
extern const LPCWSTR MONTHS[];

/*
 * The Unix masks
 * The Windows version of <sys/stat.h> does not contain all the POSIX flag/mask
 * definitions. The following masks are used in 'winutils' to represent POSIX
 * permission mode.
 * 
 */
enum UnixAclMask
{
  UX_O_EXECUTE = 00001, // S_IXOTH
  UX_O_WRITE   = 00002, // S_IWOTH
  UX_O_READ    = 00004, // S_IROTH
  UX_G_EXECUTE = 00010, // S_IXGRP
  UX_G_WRITE   = 00020, // S_IWGRP
  UX_G_READ    = 00040, // S_IRGRP
  UX_U_EXECUTE = 00100, // S_IXUSR
  UX_U_WRITE   = 00200, // S_IWUSR
  UX_U_READ    = 00400, // S_IRUSR
  UX_DIRECTORY = 0040000, // S_IFDIR
  UX_REGULAR   = 0100000, // S_IFREG
  UX_SYMLINK   = 0120000, // S_IFLNK
};


/*
 * The WindowsAclMask and WinMasks contain the definitions used to establish
 * the mapping between Unix and Windows.
 */
enum WindowsAclMask
{
  WIN_READ, // The permission(s) that enable Unix read permission
  WIN_WRITE, // The permission(s) that enable Unix write permission
  WIN_EXECUTE, // The permission(s) that enbale Unix execute permission
  WIN_OWNER_SE, // The permissions that are always set for file owners 
  WIN_ALL, // The permissions that all files on Windows should have
  WIN_MASKS_TOTAL
};
extern const ACCESS_MASK WinMasks[];


int Ls(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void LsUsage(LPCWSTR program);

int Chmod(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void ChmodUsage(LPCWSTR program);

int Chown(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void ChownUsage(LPCWSTR program);

int Groups(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void GroupsUsage(LPCWSTR program);

int Hardlink(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void HardlinkUsage();

DWORD KillTask(PCWSTR jobObjName);

int Task(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void TaskUsage();

int Symlink(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void SymlinkUsage();

int Readlink(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void ReadlinkUsage();

int SystemInfo();
void SystemInfoUsage();

DWORD GetFileInformationByName(__in LPCWSTR pathName,  __in BOOL followLink,
  __out LPBY_HANDLE_FILE_INFORMATION lpFileInformation);

DWORD CheckAccessForCurrentUser(
  __in PCWSTR pathName,
  __in ACCESS_MASK requestedAccess,
  __out BOOL *allowed);

DWORD ConvertToLongPath(__in PCWSTR path, __deref_out PWSTR *newPath);

DWORD GetSidFromAcctNameW(__in PCWSTR acctName, __out PSID* ppSid);

DWORD GetAccntNameFromSid(__in PSID pSid, __out LPWSTR *ppAcctName);

void ReportErrorCode(LPCWSTR func, DWORD err);

BOOL IsDirFileInfo(const BY_HANDLE_FILE_INFORMATION *fileInformation);

DWORD FindFileOwnerAndPermission(
  __in LPCWSTR pathName,
  __in BOOL followLink,
  __out_opt LPWSTR *pOwnerName,
  __out_opt LPWSTR *pGroupName,
  __out_opt PINT pMask);

DWORD FindFileOwnerAndPermissionByHandle(
  __in HANDLE fileHandle,
  __out_opt LPWSTR *pOwnerName,
  __out_opt LPWSTR *pGroupName,
  __out_opt PINT pMask);

DWORD DirectoryCheck(__in LPCWSTR pathName, __out LPBOOL result);

DWORD SymbolicLinkCheck(__in LPCWSTR pathName, __out LPBOOL result);

DWORD JunctionPointCheck(__in LPCWSTR pathName, __out LPBOOL result);

DWORD ChangeFileModeByMask(__in LPCWSTR path, INT mode);

DWORD CreateDirectoryWithMode(__in LPCWSTR path, __in INT mode);

DWORD CreateFileWithMode(__in LPCWSTR lpPath, __in DWORD dwDesiredAccess,
    __in DWORD dwShareMode, __in DWORD dwCreationDisposition, __in INT mode,
    __out_opt PHANDLE pHFile);

DWORD GetLocalGroupsForUser(__in LPCWSTR user,
  __out LPLOCALGROUP_USERS_INFO_0 *groups, __out LPDWORD entries);

void GetLibraryName(__in LPCVOID lpAddress, __out LPWSTR *filename);

DWORD EnablePrivilege(__in LPCWSTR privilegeName);

void AssignLsaString(__inout LSA_STRING * target, __in const char *strBuf);

DWORD RegisterWithLsa(__in const char *logonProcessName, __out HANDLE * lsaHandle);

void UnregisterWithLsa(__in HANDLE lsaHandle);

DWORD LookupKerberosAuthenticationPackageId(__in HANDLE lsaHandle, __out ULONG * packageId);

DWORD CreateLogonTokenForUser(__in HANDLE lsaHandle,
                         __in const char * tokenSourceName, 
                         __in const char * tokenOriginName, 
                         __in ULONG authnPkgId, 
                         __in const wchar_t* principalName, 
                         __out HANDLE *tokenHandle);

DWORD LoadUserProfileForLogon(__in HANDLE logonHandle, __out PROFILEINFO * pi);

DWORD UnloadProfileForLogon(__in HANDLE logonHandle, __in PROFILEINFO * pi);

DWORD EnableImpersonatePrivileges();

DWORD RunService(__in int argc, __in_ecount(argc) wchar_t *argv[]);
void ServiceUsage();


DWORD ChangeFileOwnerBySid(__in LPCWSTR path,
  __in_opt PSID pNewOwnerSid, __in_opt PSID pNewGroupSid);

DWORD ChownImpl(
  __in_opt LPCWSTR userName,
  __in_opt LPCWSTR groupName,
  __in LPCWSTR pathName);

LPCWSTR GetSystemTimeString();

VOID LogDebugMessage(LPCWSTR format, ...);

DWORD SplitStringIgnoreSpaceW(
  __in size_t len, 
  __in_ecount(len) LPCWSTR source, 
  __in WCHAR deli, 
  __out size_t* count, __out_ecount(count) WCHAR*** out);

DWORD BuildPathRelativeToModule(
    __in LPCWSTR relativePath, 
    __in size_t len, 
    __out_ecount(len) LPWSTR buffer);

DWORD GetConfigValue(
  __in LPCWSTR relativePath,
  __in LPCWSTR keyName, 
  __out size_t* len, 
  __out_ecount(len) LPCWSTR* value);
DWORD GetConfigValueFromXmlFile(
  __in LPCWSTR xmlFile, 
  __in LPCWSTR keyName, 
  __out size_t* len, 
  __out_ecount(len) LPCWSTR* value);


DWORD BuildServiceSecurityDescriptor(
  __in ACCESS_MASK                    accessMask,
  __in size_t                         grantSidCount,
  __in_ecount(grantSidCount) PSID*    pGrantSids,
  __in size_t                         denySidCount,
  __in_ecount(denySidCount) PSID*     pDenySids,
  __in_opt PSID                       pOwner,
  __out PSECURITY_DESCRIPTOR*         pSD);

DWORD AddNodeManagerAndUserACEsToObject(
  __in HANDLE hProcess,
  __in LPCWSTR user,
  __in ACCESS_MASK accessMask);


DWORD GetSecureJobObjectName(
  __in LPCWSTR      jobName,
  __in size_t       cchSecureJobName,
  __out_ecount(cchSecureJobName) LPWSTR secureJobName);

extern const WCHAR* wsceConfigRelativePath;

extern LPCWSTR NM_WSCE_ALLOWED;


#define SVCNAME       TEXT("hadoopwinutilsvc")
#define SVCBINDING    TEXT("ncalrpc")

DWORD RpcCall_WinutilsKillTask(
  __in LPCWSTR taskName);

DWORD RpcCall_TaskCreateAsUser(
  LPCWSTR cwd, LPCWSTR jobName, 
  LPCWSTR user, LPCWSTR pidFile, LPCWSTR cmdLine, 
  HANDLE* phProcess, HANDLE* phThread, HANDLE* phStdIn, HANDLE* phStdOut, HANDLE* phStdErr);

DWORD RpcCall_WinutilsCreateFile(
  __in LPCWSTR path,
  __in DWORD desiredAccess,
  __in DWORD shareMode,
  __in DWORD creationDisposition,
  __in DWORD flags,
  __out HANDLE* hFile);

DWORD RpcCall_WinutilsMoveFile(
  __in int operation,
  __in LPCWSTR sourcePath, 
  __in LPCWSTR destinationPath,
  __in BOOL replaceExisting);


DWORD RpcCall_WinutilsDeletePath(
  __in LPCWSTR    path,
  __in BOOL       isDir,
  __out BOOL*     pDeleted);

DWORD RpcCall_WinutilsChown(
  __in LPCWSTR filePath, 
  __in_opt LPCWSTR ownerName, 
  __in_opt LPCWSTR groupName);

DWORD RpcCall_WinutilsMkDir(
  __in LPCWSTR filePath);

DWORD RpcCall_WinutilsChmod(
  __in LPCWSTR filePath, 
  __in int mode);

#ifdef __cplusplus
}
#endif


