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
#include <tchar.h>
#include <strsafe.h>
#include <lm.h>

enum EXIT_CODE
{
  /* Common success exit code shared among all utilities */
  SUCCESS = EXIT_SUCCESS,
  /* Generic failure exit code share among all utilities */
  FAILURE = EXIT_FAILURE,
  /* Failure code indicates the user does not privilege to create symlinks */
  SYMLINK_NO_PRIVILEGE = 2,
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


int Ls(int argc, wchar_t *argv[]);
void LsUsage(LPCWSTR program);

int Chmod(int argc, wchar_t *argv[]);
void ChmodUsage(LPCWSTR program);

int Chown(int argc, wchar_t *argv[]);
void ChownUsage(LPCWSTR program);

int Groups(int argc, wchar_t *argv[]);
void GroupsUsage(LPCWSTR program);

int Hardlink(int argc, wchar_t *argv[]);
void HardlinkUsage();

int Task(int argc, wchar_t *argv[]);
void TaskUsage();

int Symlink(int argc, wchar_t *argv[]);
void SymlinkUsage();

int SystemInfo();
void SystemInfoUsage();

DWORD GetFileInformationByName(__in LPCWSTR pathName,  __in BOOL followLink,
  __out LPBY_HANDLE_FILE_INFORMATION lpFileInformation);

DWORD ConvertToLongPath(__in PCWSTR path, __deref_out PWSTR *newPath);

DWORD GetSidFromAcctNameW(LPCWSTR acctName, PSID* ppSid);

DWORD GetAccntNameFromSid(PSID pSid, LPWSTR *ppAcctName);

void ReportErrorCode(LPCWSTR func, DWORD err);

BOOL IsDirFileInfo(const BY_HANDLE_FILE_INFORMATION *fileInformation);

DWORD FindFileOwnerAndPermission(
  __in LPCWSTR pathName,
  __out_opt LPWSTR *pOwnerName,
  __out_opt LPWSTR *pGroupName,
  __out_opt PINT pMask);

DWORD DirectoryCheck(__in LPCWSTR pathName, __out LPBOOL result);

DWORD SymbolicLinkCheck(__in LPCWSTR pathName, __out LPBOOL result);

DWORD JunctionPointCheck(__in LPCWSTR pathName, __out LPBOOL result);

DWORD ChangeFileModeByMask(__in LPCWSTR path, INT mode);

DWORD GetLocalGroupsForUser(__in LPCWSTR user,
  __out LPLOCALGROUP_USERS_INFO_0 *groups, __out LPDWORD entries);

BOOL EnablePrivilege(__in LPCWSTR privilegeName);