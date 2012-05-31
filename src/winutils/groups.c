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

#pragma comment(lib, "netapi32.lib")

#define SECURITY_WIN32

#include "common.h"
#include <lm.h>

//----------------------------------------------------------------------------
// Function: PrintGroups
//
// Description:
//	Print group names to the console standard output for the given user
//
// Returns:
//	TRUE: on success
//
// Notes:
//   This function could fail on first pass when we fail to find groups for
//   domain account; so we do not report Windows API errors in this function.
//
static BOOL PrintGroups(LPCWSTR userName)
{
  BOOL ret = TRUE;
  LPLOCALGROUP_USERS_INFO_0 pBuf = NULL;
  DWORD dwEntriesRead = 0;
  DWORD dwTotalEntries = 0;
  NET_API_STATUS nStatus;

  // Call the NetUserGetLocalGroups function specifying information level 0.
  // The LG_INCLUDE_INDIRECT flag specifies that the function should also
  // return the names of the local groups in which the user is indirectly a
  // member.
  //
  nStatus = NetUserGetLocalGroups(NULL,
    userName,
    0,
    LG_INCLUDE_INDIRECT,
    (LPBYTE *) &pBuf,
    MAX_PREFERRED_LENGTH,
    &dwEntriesRead,
    &dwTotalEntries);

  if (nStatus == NERR_Success)
  {
    LPLOCALGROUP_USERS_INFO_0 pTmpBuf;
    DWORD i;
    DWORD dwTotalCount = 0;

    if ((pTmpBuf = pBuf) != NULL)
    {
      for (i = 0; i < dwEntriesRead; i++)
      {
        if (pTmpBuf == NULL)
        {
          ret = FALSE;
          break;
        }

        if (i != 0)
        {
          wprintf(L" ");
        }
        wprintf(L"%s", pTmpBuf->lgrui0_name);

        pTmpBuf++;
        dwTotalCount++;
      }
    }
    else
    {
      ret = FALSE;
    }
  }
  else
  {
    ret = FALSE;
  }

  if (pBuf != NULL) NetApiBufferFree(pBuf);
  return ret;
}

//----------------------------------------------------------------------------
// Function: Groups
//
// Description:
//	The main method for groups command
//
// Returns:
//	0: on success
//
// Notes:
//
//
int Groups(int argc, wchar_t *argv[])
{
  LPWSTR input = NULL;

  LPWSTR currentUser = NULL;
  DWORD cchCurrentUser = 0;

  PSID pUserSid = NULL;
  LPWSTR userName = NULL;
  DWORD cchName = 0;
  LPWSTR domainName = NULL;
  DWORD cchDomainName = 0;
  SID_NAME_USE eUse;

  int ret = EXIT_FAILURE;

  if (argc != 2 && argc != 1)
  {
    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    GroupsUsage(argv[0]);
    return EXIT_FAILURE;
  }

  if (argc == 1)
  {
    GetUserNameW(currentUser, &cchCurrentUser);
    if (GetLastError() == ERROR_INSUFFICIENT_BUFFER)
    {
      currentUser = (LPWSTR) LocalAlloc(LPTR,
        (cchCurrentUser + 1) * sizeof(wchar_t));
      if (!currentUser)
      {
        ReportErrorCode(L"LocalAlloc", GetLastError());
        ret = EXIT_FAILURE;
        goto GroupsEnd;
      }
      if (GetUserNameW(currentUser, &cchCurrentUser))
        input = currentUser;
      else
      {
        ReportErrorCode(L"GetUserName", GetLastError());
        ret = EXIT_FAILURE;
        goto GroupsEnd;
      }
    }
    else
    {
      ReportErrorCode(L"GetUserName", GetLastError());
      ret = EXIT_FAILURE;
      goto GroupsEnd;
    }
  }
  else
  {
    input = argv[1];
  }

  if (PrintGroups(input))
  {
    ret = EXIT_SUCCESS;
  }
  else if (GetSidFromAcctNameW(input, &pUserSid))
  {
    LookupAccountSidW(NULL, pUserSid, NULL,
      &cchName, NULL, &cchDomainName, &eUse);

    if (GetLastError() == ERROR_INSUFFICIENT_BUFFER)
    {
      userName = (LPWSTR) LocalAlloc(LPTR, (cchName + 1) * sizeof(wchar_t));
      domainName = (LPWSTR) LocalAlloc(LPTR,
        (cchDomainName + 1) * sizeof(wchar_t));
      if (!userName || !domainName)
      {
        ReportErrorCode(L"LocalAlloc", GetLastError());
        ret = EXIT_FAILURE;
        goto GroupsEnd;
      }
      if (LookupAccountSidW(NULL, pUserSid, userName, &cchName,
        domainName, &cchDomainName, &eUse))
      {
        LPWSTR fullName = (LPWSTR) LocalAlloc(LPTR,
          (cchName + cchDomainName + 2) * sizeof(wchar_t));
        if (!fullName)
        {
          ReportErrorCode(L"LocalAlloc", GetLastError());
          ret = EXIT_FAILURE;
          goto GroupsEnd;
        }
        if (FAILED(StringCchPrintfW(fullName,
          cchName + cchDomainName + 2,
          L"%s\\%s", domainName, userName)))
        {
          ret = EXIT_FAILURE;
          goto GroupsEnd;
        }
        if (!PrintGroups(fullName))
          ret = EXIT_FAILURE;
        else
          ret = EXIT_SUCCESS;
        LocalFree(fullName);
      }
      else
      {
        ret = EXIT_FAILURE;
      }
    }
    else
    {
      ret = EXIT_FAILURE;
    }
  }
  else
  {
    ret = EXIT_FAILURE;
  }

GroupsEnd:
  LocalFree(currentUser);
  LocalFree(pUserSid);
  LocalFree(userName);
  LocalFree(domainName);

  if (ret == EXIT_SUCCESS) wprintf(L"\n");

  return ret;
}

void GroupsUsage(LPCWSTR program)
{
  fwprintf(stdout, L"\
Usage: %s [USERNAME]\n\
Print group information of the specified USERNAME\
(the current user by default).\n",
program);
}
