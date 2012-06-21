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

/*
 * The array of 12 months' three-letter abbreviations 
 */
extern const LPCWSTR MONTHS[];

/*
 * The Unix masks
 */
enum UnixAclMask
{
  UX_O_EXECUTE = 0x0001,
  UX_O_WRITE   = 0x0002,
  UX_O_READ    = 0x0004,
  UX_G_EXECUTE = 0x0008,
  UX_G_WRITE   = 0x0010,
  UX_G_READ    = 0x0020,
  UX_U_EXECUTE = 0x0040,
  UX_U_WRITE   = 0x0080,
  UX_U_READ    = 0x0100,
  UX_DIRECTORY = 0x0200,
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

int SystemInfo();
void SystemInfoUsage();

DWORD GetFileInformationByName(__in LPCWSTR pathName,
  __out LPBY_HANDLE_FILE_INFORMATION lpFileInformation);

DWORD ConvertToLongPath(__in PCWSTR path, __deref_out PWSTR *newPath);

BOOL GetSidFromAcctNameW(LPCWSTR acctName, PSID* ppSid);

void ReportErrorCode(LPCWSTR func, DWORD err);

BOOL IsDirFileInfo(const BY_HANDLE_FILE_INFORMATION *fileInformation);

BOOL FindFileOwnerAndPermission(
  __in LPCWSTR pathName,
  __out_opt LPWSTR *pOwnerName,
  __out_opt LPWSTR *pGroupName,
  __out_opt PUSHORT pMask);