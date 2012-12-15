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

//----------------------------------------------------------------------------
// Function: ChangeFileOwnerBySid
//
// Description:
//  Change a file or directory ownership by giving new owner and group SIDs
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code: otherwise
//
// Notes:
//  This function is long path safe, i.e. the path will be converted to long
//  path format if not already converted. So the caller does not need to do
//  the converstion before calling the method.
//
static DWORD ChangeFileOwnerBySid(__in LPCWSTR path,
  __in_opt PSID pNewOwnerSid, __in_opt PSID pNewGroupSid)
{
  LPWSTR longPathName = NULL;
  INT oldMode = 0;

  SECURITY_INFORMATION securityInformation = 0;

  DWORD dwRtnCode = ERROR_SUCCESS;

  // Convert the path the the long path
  //
  dwRtnCode = ConvertToLongPath(path, &longPathName);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto ChangeFileOwnerByNameEnd;
  }

  // Get a pointer to the existing owner information and DACL
  //
  dwRtnCode = FindFileOwnerAndPermission(longPathName, NULL, NULL, &oldMode);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto ChangeFileOwnerByNameEnd;
  }

  // We need SeTakeOwnershipPrivilege to set the owner if the caller does not
  // have WRITE_OWNER access to the object; we need SeRestorePrivilege if the
  // SID is not contained in the caller's token, and have the SE_GROUP_OWNER
  // permission enabled.
  //
  if (!EnablePrivilege(L"SeTakeOwnershipPrivilege"))
  {
    fwprintf(stdout, L"INFO: The user does not have SeTakeOwnershipPrivilege.\n");
  }
  if (!EnablePrivilege(L"SeRestorePrivilege"))
  {
    fwprintf(stdout, L"INFO: The user does not have SeRestorePrivilege.\n");
  }

  assert(pNewOwnerSid != NULL || pNewGroupSid != NULL);

  // Set the owners of the file.
  //
  if (pNewOwnerSid != NULL) securityInformation |= OWNER_SECURITY_INFORMATION;
  if (pNewGroupSid != NULL) securityInformation |= GROUP_SECURITY_INFORMATION;
  dwRtnCode = SetNamedSecurityInfoW(
    longPathName,
    SE_FILE_OBJECT,
    securityInformation,
    pNewOwnerSid,
    pNewGroupSid,
    NULL,
    NULL);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto ChangeFileOwnerByNameEnd;
  }

  // Set the permission on the file for the new owner.
  //
  dwRtnCode = ChangeFileModeByMask(longPathName, oldMode);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto ChangeFileOwnerByNameEnd;
  }

ChangeFileOwnerByNameEnd:
  LocalFree(longPathName);
  return dwRtnCode;
}

//----------------------------------------------------------------------------
// Function: Chown
//
// Description:
//	The main method for chown command
//
// Returns:
//	0: on success
//
// Notes:
//
//
int Chown(int argc, wchar_t *argv[])
{
  LPWSTR pathName = NULL;

  LPWSTR ownerInfo = NULL;

  LPWSTR colonPos = NULL;

  LPWSTR userName = NULL;
  size_t userNameLen = 0;

  LPWSTR groupName = NULL;
  size_t groupNameLen = 0;

  PSID pNewOwnerSid = NULL;
  PSID pNewGroupSid = NULL;

  DWORD dwRtnCode = 0;

  int ret = EXIT_FAILURE;

  if (argc >= 3)
  {
    ownerInfo = argv[1];
    pathName = argv[2];
  }
  else
  {
    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    ChownUsage(argv[0]);
    return ret;
  }

  // Parsing the owner name
  //
  if ((colonPos = wcschr(ownerInfo, L':')) != NULL)
  {
    if (colonPos - ownerInfo != 0)
    {
      // Length includes NULL terminator
      userNameLen = colonPos - ownerInfo + 1;
      userName = (LPTSTR)LocalAlloc(LPTR, userNameLen * sizeof(WCHAR));
      if (userName == NULL)
      {
        ReportErrorCode(L"LocalAlloc", GetLastError());
        goto ChownEnd;
      }
      if (FAILED(StringCchCopyNW(userName, userNameLen,
        ownerInfo, userNameLen - 1)))
        goto ChownEnd;
    }

    if (*(colonPos + 1) != 0)
    {
      // Length includes NULL terminator
      groupNameLen = wcslen(ownerInfo) - (colonPos - ownerInfo) + 1;
      groupName = (LPTSTR)LocalAlloc(LPTR, groupNameLen * sizeof(WCHAR));
      if (groupName == NULL)
      {
        ReportErrorCode(L"LocalAlloc", GetLastError());
        goto ChownEnd;
      }
      if (FAILED(StringCchCopyNW(groupName, groupNameLen,
        colonPos + 1, groupNameLen)))
        goto ChownEnd;
    }
  }
  else
  {
    // Length includes NULL terminator
    userNameLen = wcslen(ownerInfo) + 1;
    userName = (LPWSTR)LocalAlloc(LPTR, userNameLen * sizeof(WCHAR));
    if (userName == NULL)
    {
      ReportErrorCode(L"LocalAlloc", GetLastError());
      goto ChownEnd;
    }
    if (FAILED(StringCchCopyNW(userName, userNameLen, ownerInfo, userNameLen)))
      goto ChownEnd;
  }

  // Not allow zero length user name or group name in the parsing results.
  //
  assert(userName == NULL || wcslen(userName) > 0);
  assert(groupName == NULL || wcslen(groupName) > 0);

  // Nothing to change if both names are empty
  //
  if ((userName == NULL) && (groupName == NULL))
  {
    ret = EXIT_SUCCESS;
    goto ChownEnd;
  }

  if (userName != NULL)
  {
    dwRtnCode = GetSidFromAcctNameW(userName, &pNewOwnerSid);
    if (dwRtnCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"GetSidFromAcctName", dwRtnCode);
      fwprintf(stderr, L"Invalid user name: %s\n", userName);
      goto ChownEnd;
    }
  }

  if (groupName != NULL)
  {
    dwRtnCode = GetSidFromAcctNameW(groupName, &pNewGroupSid);
    if (dwRtnCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"GetSidFromAcctName", dwRtnCode);
      fwprintf(stderr, L"Invalid group name: %s\n", groupName);
      goto ChownEnd;
    }
  }

  if (wcslen(pathName) == 0 || wcsspn(pathName, L"/?|><:*\"") != 0)
  {
    fwprintf(stderr, L"Incorrect file name format: %s\n", pathName);
    goto ChownEnd;
  }

  dwRtnCode = ChangeFileOwnerBySid(pathName, pNewOwnerSid, pNewGroupSid);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"ChangeFileOwnerBySid", dwRtnCode);
    goto ChownEnd;
  }

  ret = EXIT_SUCCESS;

ChownEnd:
  LocalFree(userName);
  LocalFree(groupName);
  LocalFree(pNewOwnerSid);
  LocalFree(pNewGroupSid);

  return ret;
}

void ChownUsage(LPCWSTR program)
{
  fwprintf(stdout, L"\
Usage: %s [OWNER][:[GROUP]] [FILE]\n\
Change the owner and/or group of the FILE to OWNER and/or GROUP.\n\
\n\
Note:\n\
On Linux, if a colon but no group name follows the user name, the group of\n\
the files is changed to that user\'s login group. Windows has no concept of\n\
a user's login group. So we do not change the group owner in this case.\n",
program);
}
