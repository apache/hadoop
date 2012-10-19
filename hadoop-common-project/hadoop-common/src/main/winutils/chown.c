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
// Function: GetNewAclSize
//
// Description:
//	Compute the extra size of the new ACL if we replace the old owner Sid with
//  the new owner Sid.
//
// Returns:
//	The extra size needed for the new ACL compared with the ACL passed in. If
//  the value is negative, it means the size of the new ACL could be reduced.
//
// Notes:
//
static BOOL GetNewAclSizeDelta(__in PACL pDACL,
  __in PSID pOldOwnerSid, __in PSID pNewOwnerSid, __out PLONG pDelta)
{
  PVOID pAce = NULL;
  DWORD i;
  PSID aceSid = NULL;
  ACE_HEADER *aceHeader = NULL;
  PACCESS_ALLOWED_ACE accessAllowedAce = NULL;
  PACCESS_DENIED_ACE accessDenieddAce = NULL;

  assert(pDACL != NULL && pNewOwnerSid != NULL &&
    pOldOwnerSid != NULL && pDelta != NULL);

  *pDelta = 0;
  for (i = 0; i < pDACL->AceCount; i++)
  {
    if (!GetAce(pDACL, i, &pAce))
    {
      ReportErrorCode(L"GetAce", GetLastError());
      return FALSE;
    }

    aceHeader = (ACE_HEADER *) pAce;
    if (aceHeader->AceType == ACCESS_ALLOWED_ACE_TYPE)
    {
      accessAllowedAce = (PACCESS_ALLOWED_ACE) pAce;
      aceSid = (PSID) &accessAllowedAce->SidStart;
    }
    else if (aceHeader->AceType == ACCESS_DENIED_ACE_TYPE)
    {
      accessDenieddAce = (PACCESS_DENIED_ACE) pAce;
      aceSid = (PSID) &accessDenieddAce->SidStart;
    }
    else
    {
      continue;
    }

    if (EqualSid(pOldOwnerSid, aceSid))
    {
      *pDelta += GetLengthSid(pNewOwnerSid) - GetLengthSid(pOldOwnerSid);
    }
  }

  return TRUE;
}

//----------------------------------------------------------------------------
// Function: AddNewAce
//
// Description:
//	Add an Ace of new owner to the new ACL
//
// Returns:
//	TRUE: on success
//
// Notes:
//  The Ace type should be either ACCESS_ALLOWED_ACE or ACCESS_DENIED_ACE
//
static BOOL AddNewAce(PACL pNewDACL, PVOID pOldAce,
  PSID pOwnerSid, PSID pUserSid)
{
  PVOID pNewAce = NULL;
  DWORD newAceSize = 0;

  assert(pNewDACL != NULL && pOldAce != NULL &&
    pOwnerSid != NULL && pUserSid != NULL);
  assert(((PACE_HEADER)pOldAce)->AceType == ACCESS_ALLOWED_ACE_TYPE ||
    ((PACE_HEADER)pOldAce)->AceType == ACCESS_DENIED_ACE_TYPE);

  newAceSize =  ((PACE_HEADER)pOldAce)->AceSize +
    GetLengthSid(pUserSid) - GetLengthSid(pOwnerSid);
  pNewAce = LocalAlloc(LPTR, newAceSize);
  if (pNewAce == NULL)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    return FALSE;
  }

  ((PACE_HEADER)pNewAce)->AceType = ((PACE_HEADER) pOldAce)->AceType;
  ((PACE_HEADER)pNewAce)->AceFlags = ((PACE_HEADER) pOldAce)->AceFlags;
  ((PACE_HEADER)pNewAce)->AceSize = (WORD) newAceSize;

  if (((PACE_HEADER)pOldAce)->AceType == ACCESS_ALLOWED_ACE_TYPE)
  {
    ((PACCESS_ALLOWED_ACE)pNewAce)->Mask = ((PACCESS_ALLOWED_ACE)pOldAce)->Mask;
    if (!CopySid(GetLengthSid(pUserSid),
      &((PACCESS_ALLOWED_ACE) pNewAce)->SidStart, pUserSid))
    {
      ReportErrorCode(L"CopySid", GetLastError());
      LocalFree(pNewAce);
      return FALSE;
    }
  }
  else
  {
    ((PACCESS_DENIED_ACE)pNewAce)->Mask = ((PACCESS_DENIED_ACE)pOldAce)->Mask;
    if (!CopySid(GetLengthSid(pUserSid),
      &((PACCESS_DENIED_ACE) pNewAce)->SidStart, pUserSid))
    {
      ReportErrorCode(L"CopySid", GetLastError());
      LocalFree(pNewAce);
      return FALSE;
    }
  }

  if (!AddAce(pNewDACL, ACL_REVISION, MAXDWORD,
    pNewAce, ((PACE_HEADER)pNewAce)->AceSize))
  {
    ReportErrorCode(L"AddAce", GetLastError());
    LocalFree(pNewAce);
    return FALSE;
  }

  LocalFree(pNewAce);
  return TRUE;
}

//----------------------------------------------------------------------------
// Function: CreateDaclForNewOwner
//
// Description:
//	Create a new DACL for the new owner
//
// Returns:
//	TRUE: on success
//
// Notes:
//  Caller needs to destroy the memory of the new DACL by calling LocalFree()
//
static BOOL CreateDaclForNewOwner(
  __in PACL pDACL,
  __in_opt PSID pOldOwnerSid,
  __in_opt PSID pNewOwnerSid,
  __in_opt PSID pOldGroupSid,
  __in_opt PSID pNewGroupSid,
  __out PACL *ppNewDACL)
{
  PSID aceSid = NULL;
  PACE_HEADER aceHeader = NULL;
  PACCESS_ALLOWED_ACE accessAllowedAce = NULL;
  PACCESS_DENIED_ACE accessDenieddAce = NULL;
  PVOID pAce = NULL;
  ACL_SIZE_INFORMATION aclSizeInfo;
  LONG delta = 0;
  DWORD dwNewAclSize = 0;
  DWORD dwRtnCode = 0;
  DWORD i;

  assert(pDACL != NULL && ppNewDACL != NULL);
  assert(pOldOwnerSid != NULL && pOldGroupSid != NULL);
  assert(pNewOwnerSid != NULL || pNewGroupSid != NULL);

  if (!GetAclInformation(pDACL, (LPVOID)&aclSizeInfo,
    sizeof(ACL_SIZE_INFORMATION), AclSizeInformation))
  {
    ReportErrorCode(L"GetAclInformation", GetLastError());
    return FALSE;
  }

  dwNewAclSize = aclSizeInfo.AclBytesInUse + aclSizeInfo.AclBytesFree;

  delta = 0;
  if (pNewOwnerSid != NULL &&
    !GetNewAclSizeDelta(pDACL, pOldOwnerSid, pNewOwnerSid, &delta))
  {
    return FALSE;
  }
  dwNewAclSize += delta;

  delta = 0;
  if (pNewGroupSid != NULL &&
    !GetNewAclSizeDelta(pDACL, pOldGroupSid, pNewGroupSid, &delta))
  {
    return FALSE;
  }
  dwNewAclSize += delta;

  *ppNewDACL = (PACL)LocalAlloc(LPTR, dwNewAclSize);
  if (*ppNewDACL == NULL)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    return FALSE;
  }

  if (!InitializeAcl(*ppNewDACL, dwNewAclSize, ACL_REVISION))
  {
    ReportErrorCode(L"InitializeAcl", GetLastError());
    return FALSE;
  }

  // Go through the DACL to change permissions
  //
  for (i = 0; i < pDACL->AceCount; i++)
  {
    if (!GetAce(pDACL, i, &pAce))
    {
      ReportErrorCode(L"GetAce", GetLastError());
      return FALSE;
    }

    aceHeader = (PACE_HEADER) pAce;
    aceSid = NULL;
    if (aceHeader->AceType == ACCESS_ALLOWED_ACE_TYPE)
    {
      accessAllowedAce = (PACCESS_ALLOWED_ACE) pAce;
      aceSid = (PSID) &accessAllowedAce->SidStart;
    }
    else if (aceHeader->AceType == ACCESS_DENIED_ACE_TYPE)
    {
      accessDenieddAce = (PACCESS_DENIED_ACE) pAce;
      aceSid = (PSID) &accessDenieddAce->SidStart;
    }

    if (aceSid != NULL)
    {
      if (pNewOwnerSid != NULL && EqualSid(pOldOwnerSid, aceSid))
      {
        if (!AddNewAce(*ppNewDACL, pAce, pOldOwnerSid, pNewOwnerSid))
          return FALSE;
        else
          continue;
      }
      else if (pNewGroupSid != NULL && EqualSid(pOldGroupSid, aceSid))
      {
        if (!AddNewAce(*ppNewDACL, pAce, pOldGroupSid, pNewGroupSid))
          return FALSE;
        else
          continue;
      }
    }

    // At this point, either:
    // 1. The Ace is not of type ACCESS_ALLOWED_ACE or ACCESS_DENIED_ACE;
    // 2. The Ace does not belong to the owner
    // For both cases, we just add the oringal Ace to the new ACL.
    //
    if (!AddAce(*ppNewDACL, ACL_REVISION, MAXDWORD, pAce, aceHeader->AceSize))
    {
      ReportErrorCode(L"AddAce", dwRtnCode);
      return FALSE;
    }
  }

  return TRUE;
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
  LPWSTR longPathName = NULL;

  LPWSTR ownerInfo = NULL;

  LPWSTR colonPos = NULL;

  LPWSTR userName = NULL;
  size_t userNameLen = 0;

  LPWSTR groupName = NULL;
  size_t groupNameLen = 0;

  PSID pNewOwnerSid = NULL;
  PSID pNewGroupSid = NULL;

  PACL pDACL = NULL;
  PACL pNewDACL = NULL;

  PSID pOldOwnerSid = NULL;
  PSID pOldGroupSid = NULL;

  PSECURITY_DESCRIPTOR pSD = NULL;

  SECURITY_INFORMATION securityInformation;

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

    if (colonPos + 1 != NULL)
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

  if ((userName == NULL || wcslen(userName) == 0) &&
    (groupName == NULL || wcslen(groupName) == 0))
  {
    fwprintf(stderr, L"User name and group name cannot both be empty.");
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

  // Convert the path the the long path
  //
  dwRtnCode = ConvertToLongPath(pathName, &longPathName);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"ConvertToLongPath", dwRtnCode);
    goto ChownEnd;
  }

  // Get a pointer to the existing owner information and DACL
  //
  dwRtnCode = GetNamedSecurityInfoW(
    longPathName,
    SE_FILE_OBJECT, 
    OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION |
    DACL_SECURITY_INFORMATION,
    &pOldOwnerSid,
    &pOldGroupSid,
    &pDACL,
    NULL,
    &pSD);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"GetNamedSecurityInfo", dwRtnCode);
    goto ChownEnd;
  }

  // Create the new DACL
  //
  if (!CreateDaclForNewOwner(pDACL,
    pOldOwnerSid, pNewOwnerSid,
    pOldGroupSid, pNewGroupSid,
    &pNewDACL))
  {
    goto ChownEnd;
  }

  // Set the owner and DACLs in the object's security descriptor. Use
  // PROTECTED_DACL_SECURITY_INFORMATION flag to remove permission inheritance
  //
  securityInformation =
    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION;
  if (pNewOwnerSid != NULL) securityInformation |= OWNER_SECURITY_INFORMATION;
  if (pNewGroupSid != NULL) securityInformation |= GROUP_SECURITY_INFORMATION;
  dwRtnCode = SetNamedSecurityInfoW(
    longPathName,
    SE_FILE_OBJECT,
    securityInformation,
    pNewOwnerSid,
    pNewGroupSid,
    pNewDACL,
    NULL);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"SetNamedSecurityInfo", dwRtnCode);
    goto ChownEnd;
  }

  ret = EXIT_SUCCESS;

ChownEnd:
  LocalFree(userName);
  LocalFree(groupName);
  LocalFree(pNewOwnerSid);
  LocalFree(pNewGroupSid);
  LocalFree(pNewDACL);
  LocalFree(pSD);
  LocalFree(longPathName);

  return ret;
}

void ChownUsage(LPCWSTR program)
{
  fwprintf(stdout, L"\
Usage: %s [OWNER][:[GROUP]] [FILE]\n\
Change the owner and/or group of the FILE to OWNER and/or GROUP.\n",
program);
}
