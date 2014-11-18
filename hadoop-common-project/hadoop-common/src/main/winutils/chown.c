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
int Chown(__in int argc, __in_ecount(argc) wchar_t *argv[])
{
  LPWSTR pathName = NULL;

  LPWSTR ownerInfo = NULL;

  WCHAR const * colonPos = NULL;

  LPWSTR userName = NULL;
  size_t userNameLen = 0;

  LPWSTR groupName = NULL;
  size_t groupNameLen = 0;

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

	dwRtnCode = ChownImpl(userName, groupName, pathName);
	if (dwRtnCode) {
		goto ChownEnd;
	}

  ret = EXIT_SUCCESS;

ChownEnd:
  LocalFree(userName);
  LocalFree(groupName);

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
