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

#pragma comment(lib, "authz.lib")
#include "common.h"
#include <authz.h>
/*
 * The array of 12 months' three-letter abbreviations 
 */
const LPCWSTR MONTHS[] = { L"Jan", L"Feb", L"Mar", L"Apr", L"May", L"Jun",
  L"Jul", L"Aug", L"Sep", L"Oct", L"Nov", L"Dec" };

/*
 * The WindowsAclMask and WinMasks contain the definitions used to establish
 * the mapping between Unix and Windows.
 * We set up the mapping with the following rules. 
 *   1. Everyone will have WIN_ALL permissions;
 *   2. Owner will always have WIN_OWNER_SE permissions in addition;
 *   2. When Unix read/write/excute permission is set on the file, the
 *      corresponding Windows allow ACE will be added to the file.
 * More details and explaination can be found in the following white paper:
 *   http://technet.microsoft.com/en-us/library/bb463216.aspx
 */
const ACCESS_MASK WinMasks[WIN_MASKS_TOTAL] =
{
  /* WIN_READ */
  FILE_READ_DATA,
  /* WIN_WRITE */
  FILE_WRITE_DATA | FILE_WRITE_ATTRIBUTES | FILE_APPEND_DATA | FILE_WRITE_EA |
  FILE_DELETE_CHILD,
  /* WIN_EXECUTE */
  FILE_EXECUTE,
  /* WIN_OWNER_SE */
  DELETE | WRITE_DAC | WRITE_OWNER | FILE_WRITE_EA | FILE_WRITE_ATTRIBUTES, 
  /* WIN_ALL */
  READ_CONTROL |  FILE_READ_EA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
};

//----------------------------------------------------------------------------
// Function: GetFileInformationByName
//
// Description:
//  To retrieve the by handle file information given the file name
//
// Returns:
//  ERROR_SUCCESS: on success
//  error code: otherwise
//
// Notes:
//
DWORD GetFileInformationByName(
  __in LPCWSTR pathName,
  __out LPBY_HANDLE_FILE_INFORMATION lpFileInformation)
{
  HANDLE fileHandle = NULL;
  DWORD dwErrorCode = ERROR_SUCCESS;

  assert(lpFileInformation != NULL);

  fileHandle = CreateFileW(
    pathName,
    FILE_READ_ATTRIBUTES,
    FILE_SHARE_READ,
    NULL,
    OPEN_EXISTING,
    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS,
    NULL);
  if (fileHandle == INVALID_HANDLE_VALUE)
  {
    dwErrorCode = GetLastError();
    return dwErrorCode;
  }

  if (!GetFileInformationByHandle(fileHandle, lpFileInformation))
  {
    dwErrorCode = GetLastError();
    CloseHandle(fileHandle);
    return dwErrorCode;
  }

  CloseHandle(fileHandle);

  return dwErrorCode;
}

//----------------------------------------------------------------------------
// Function: IsLongWindowsPath
//
// Description:
//  Checks if the path is longer than MAX_PATH in which case it needs to be
//  prepended with \\?\ for Windows OS to understand it.
//
// Returns:
//  TRUE long path
//  FALSE otherwise
static BOOL IsLongWindowsPath(__in PCWSTR path)
{
  return (wcslen(path) + 1) > MAX_PATH;
}

//----------------------------------------------------------------------------
// Function: ConvertToLongPath
//
// Description:
//  Prepends the path with the \\?\ prefix if the path is longer than MAX_PATH.
//  On success, newPath should be freed with LocalFree(). Given that relative
//  paths cannot be longer than MAX_PATH, we will never prepend the prefix
//  to relative paths.
//
// Returns:
//  ERROR_SUCCESS on success
//  error code on failure
DWORD ConvertToLongPath(__in PCWSTR path, __deref_out PWSTR *newPath)
{
  DWORD dwErrorCode = ERROR_SUCCESS;
  static const PCWSTR LongPathPrefix = L"\\\\?\\";
  BOOL bAppendPrefix = IsLongWindowsPath(path);

  size_t newPathLen = wcslen(path) + (bAppendPrefix ? wcslen(LongPathPrefix) : 0);

  // Allocate the buffer for the output path (+1 for terminating NULL char)
  //
  PWSTR newPathValue = (PWSTR)LocalAlloc(LPTR, (newPathLen + 1) * sizeof(WCHAR));
  if (newPathValue == NULL)
  {
    dwErrorCode = GetLastError();
    goto ConvertToLongPathExit;
  }

  if (bAppendPrefix)
  {
    // Append the prefix to the path
    //
    if (FAILED(StringCchPrintfW(newPathValue, newPathLen + 1, L"%s%s",
      LongPathPrefix, path)))
    {
      dwErrorCode = ERROR_GEN_FAILURE;
      goto ConvertToLongPathExit;
    }
  }
  else
  {
    // Just copy the original value into the output path. In this scenario
    // we are doing extra buffer copy. We decided to trade code simplicity
    // on the call site for small performance impact (extra allocation and
    // buffer copy). As paths are short, the impact is generally small.
    //
    if (FAILED(StringCchPrintfW(newPathValue, newPathLen + 1, L"%s", path)))
    {
      dwErrorCode = ERROR_GEN_FAILURE;
      goto ConvertToLongPathExit;
    }
  }

  *newPath = newPathValue;

ConvertToLongPathExit:
  if (dwErrorCode != ERROR_SUCCESS)
  {
    LocalFree(newPathValue);
    *newPath = NULL;
  }

  return dwErrorCode;
}

//----------------------------------------------------------------------------
// Function: IsDirFileInfo
//
// Description:
//	Test if the given file information is a directory
//
// Returns:
//	TRUE if it is a directory
//  FALSE otherwise
//
// Notes:
//
BOOL IsDirFileInfo(const BY_HANDLE_FILE_INFORMATION *fileInformation)
{
  if ((fileInformation->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    == FILE_ATTRIBUTE_DIRECTORY)
    return TRUE;
  return FALSE;
}

//----------------------------------------------------------------------------
// Function: GetSidFromAcctNameW
//
// Description:
//	To retrieve the SID for a user account
//
// Returns:
//	TRUE: on success
//  FALSE: otherwise
//
// Notes:
//	Caller needs to destroy the memory of Sid by calling LocalFree()
//
BOOL GetSidFromAcctNameW(LPCWSTR acctName, PSID *ppSid)
{
  DWORD dwSidSize = 0;
  DWORD cchDomainName = 0;
  DWORD dwDomainNameSize = 0;
  LPWSTR domainName = NULL;
  SID_NAME_USE eSidType;

  DWORD dwErrorCode;

  // Validate the input parameters.
  //
  assert (acctName != NULL && ppSid != NULL);

  // First pass to retrieve the buffer size.
  //
  LookupAccountName(
    NULL, // Computer name. NULL for the local computer
    acctName,
    NULL, // pSid. NULL to retrieve buffer size
    &dwSidSize,
    NULL, // Domain Name. NULL to retrieve buffer size 
    &cchDomainName,
    &eSidType);

  if((dwErrorCode = GetLastError()) != ERROR_INSUFFICIENT_BUFFER)
  {
    ReportErrorCode(L"LookupAccountName", dwErrorCode);
    return FALSE;
  }
  else
  {
    // Reallocate memory for the buffers.
    //
    *ppSid = (PSID)LocalAlloc(LPTR, dwSidSize);
    if (*ppSid == NULL)
    {
      ReportErrorCode(L"LocalAlloc", GetLastError());
      return FALSE;
    }
    dwDomainNameSize = (cchDomainName + 1) * sizeof(wchar_t);
    domainName = (LPWSTR)LocalAlloc(LPTR, dwDomainNameSize);
    if (domainName == NULL)
    {
      ReportErrorCode(L"LocalAlloc", GetLastError());
      return FALSE;
    }

    // Second pass to retrieve the SID and domain name.
    //
    if (!LookupAccountNameW(
      NULL, // Computer name. NULL for the local computer
      acctName,
      *ppSid,
      &dwSidSize,
      domainName, 
      &cchDomainName,
      &eSidType))
    {
      ReportErrorCode(L"LookupAccountName", GetLastError());
      LocalFree(domainName);
      return FALSE;
    }

    if (IsValidSid(*ppSid) == FALSE)
    {
      LocalFree(domainName);
      return FALSE;
    }
  }

  LocalFree(domainName);
  return TRUE; 
}

//----------------------------------------------------------------------------
// Function: GetUnixAccessMask
//
// Description:
//	Compute the 3 bit Unix mask for the owner, group, or, others
//
// Returns:
//	The 3 bit Unix mask in USHORT
//
// Notes:
//
static USHORT GetUnixAccessMask(ACCESS_MASK Mask)
{
  static const USHORT exe   = 0x0001;
  static const USHORT write = 0x0002;
  static const USHORT read  = 0x0004;
  USHORT mask  = 0;

  if ((Mask & WinMasks[WIN_READ]) == WinMasks[WIN_READ])
    mask |= read;
  if ((Mask & WinMasks[WIN_WRITE]) == WinMasks[WIN_WRITE])
    mask |= write;
  if ((Mask & WinMasks[WIN_EXECUTE]) == WinMasks[WIN_EXECUTE])
    mask |= exe;
  return mask;
}

//----------------------------------------------------------------------------
// Function: GetAccess
//
// Description:
//	Get Windows acces mask by AuthZ methods
//
// Returns:
//	TRUE: on success
//
// Notes:
//
static BOOL GetAccess(AUTHZ_CLIENT_CONTEXT_HANDLE hAuthzClient,
  PSECURITY_DESCRIPTOR psd, PACCESS_MASK pAccessRights)
{
  AUTHZ_ACCESS_REQUEST AccessRequest = {0};
  AUTHZ_ACCESS_REPLY AccessReply = {0};
  BYTE Buffer[1024];

  assert (pAccessRights != NULL);

  //  Do AccessCheck
  AccessRequest.DesiredAccess = MAXIMUM_ALLOWED;
  AccessRequest.PrincipalSelfSid = NULL;
  AccessRequest.ObjectTypeList = NULL;
  AccessRequest.ObjectTypeListLength = 0;
  AccessRequest.OptionalArguments = NULL; 

  RtlZeroMemory(Buffer, sizeof(Buffer));
  AccessReply.ResultListLength = 1;
  AccessReply.GrantedAccessMask = (PACCESS_MASK) (Buffer);
  AccessReply.Error = (PDWORD) (Buffer + sizeof(ACCESS_MASK));

  if (!AuthzAccessCheck(0,
    hAuthzClient,
    &AccessRequest,
    NULL,
    psd,
    NULL,
    0,
    &AccessReply,
    NULL))
  {
    ReportErrorCode(L"AuthzAccessCheck", GetLastError());
  }
  else
  {
    *pAccessRights = (*(PACCESS_MASK)(AccessReply.GrantedAccessMask));
    return TRUE;
  }

  return FALSE;
}

//----------------------------------------------------------------------------
// Function: GetEffectiveRightsForUser
//
// Description:
//	Get Windows acces mask by AuthZ methods
//
// Returns:
//	TRUE: on success
//
// Notes:
//   We run into problems for local user accounts when using the method
//   GetEffectiveRightsFromAcl(). We resort to using AuthZ methods as
//   an alternative way suggested on MSDN:
// http://msdn.microsoft.com/en-us/library/windows/desktop/aa446637.aspx
//
static BOOL GetEffectiveRightsForUser(PSECURITY_DESCRIPTOR psd,
  LPCWSTR userName,
  PACCESS_MASK pAccessRights)
{
  AUTHZ_RESOURCE_MANAGER_HANDLE hManager;
  PSID pSid = NULL;
  LUID unusedId = { 0 };
  AUTHZ_CLIENT_CONTEXT_HANDLE hAuthzClientContext = NULL;
  BOOL ret = FALSE;

  assert (pAccessRights != NULL);

  if (!AuthzInitializeResourceManager(AUTHZ_RM_FLAG_NO_AUDIT,
    NULL, NULL, NULL, NULL, &hManager))
  {
    ReportErrorCode(L"AuthzInitializeResourceManager", GetLastError());
    return FALSE;
  }

  if (GetSidFromAcctNameW(userName, &pSid))
  {
    if(AuthzInitializeContextFromSid(0,
      pSid,
      hManager,
      NULL,
      unusedId,
      NULL,
      &hAuthzClientContext))
    {
      GetAccess(hAuthzClientContext, psd, pAccessRights);
      AuthzFreeContext(hAuthzClientContext);
      ret = TRUE;
    }
    else
    {
      ReportErrorCode(L"AuthzInitializeContextFromSid", GetLastError());
    }
  }

  if(pSid) LocalFree(pSid);

  return ret;
}

//----------------------------------------------------------------------------
// Function: FindFileOwnerAndPermission
//
// Description:
//	Find the owner, primary group and permissions of a file object
//
// Returns:
//	TRUE: on success
//
// Notes:
//
BOOL FindFileOwnerAndPermission(
  __in LPCWSTR pathName,
  __out_opt LPWSTR *pOwnerName,
  __out_opt LPWSTR *pGroupName,
  __out_opt PUSHORT pMask)
{
  DWORD dwRtnCode = 0;
  DWORD dwErrorCode = 0;

  PSECURITY_DESCRIPTOR pSd = NULL;
  DWORD dwSdSize = 0;
  DWORD dwSdSizeNeeded = 0;
 
  PTRUSTEE pOwner = NULL;
  PTRUSTEE pGroup = NULL;

  ACCESS_MASK ownerAccessRights = 0;
  ACCESS_MASK groupAccessRights = 0;
  ACCESS_MASK worldAccessRights = 0;

  BOOL ret = FALSE;

  // Do nothing if the caller request nothing
  //
  if (pOwnerName == NULL && pGroupName == NULL && pMask == NULL)
  {
    return TRUE;
  }

  // Get the owner SID and DACL of the file
  // First pass to get the size needed for the SD
  //
  GetFileSecurity(
    pathName,
    OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION |
    DACL_SECURITY_INFORMATION,
    NULL,
    dwSdSize,
    (LPDWORD)&dwSdSizeNeeded);
  if((dwErrorCode = GetLastError()) != ERROR_INSUFFICIENT_BUFFER)
  {
    ReportErrorCode(L"GetFileSecurity error", dwErrorCode);
    goto FindFileOwnerAndPermissionEnd;
  }
  else
  {
    // Reallocate memory for the buffers
    //
    pSd = (PSECURITY_DESCRIPTOR)LocalAlloc(LPTR, dwSdSizeNeeded);
    if(pSd == NULL)
    {
      ReportErrorCode(L"LocalAlloc", GetLastError());
      goto FindFileOwnerAndPermissionEnd;
    }

    dwSdSize = dwSdSizeNeeded;

    // Second pass to get the Sd
    //
    if (!GetFileSecurity(
      pathName,
      OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION |
      DACL_SECURITY_INFORMATION,
      pSd,
      dwSdSize,
      (LPDWORD)&dwSdSizeNeeded))
    {
      ReportErrorCode(L"GetFileSecurity", GetLastError());
      goto FindFileOwnerAndPermissionEnd;
    }
  }

  // Get file owner and group from Sd
  //
  dwRtnCode = LookupSecurityDescriptorParts(&pOwner, &pGroup,
    NULL, NULL, NULL, NULL, pSd);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"LookupSecurityDescriptorParts", dwRtnCode);
    goto FindFileOwnerAndPermissionEnd;
  }

  assert(pOwner->TrusteeForm == TRUSTEE_IS_NAME);
  assert(pGroup->TrusteeForm == TRUSTEE_IS_NAME);


  if (pOwnerName)
  {
    *pOwnerName = (LPWSTR)LocalAlloc(LPTR,
      (wcslen(pOwner->ptstrName) + 1) * sizeof(TCHAR));
    if (pOwnerName == NULL)
    {
      ReportErrorCode(L"LocalAlloc", GetLastError());
      goto FindFileOwnerAndPermissionEnd;
    }
    if (FAILED(StringCchCopyNW(*pOwnerName, (wcslen(pOwner->ptstrName) + 1),
      pOwner->ptstrName, wcslen(pOwner->ptstrName) + 1)))
      goto FindFileOwnerAndPermissionEnd;
  }

  if (pGroupName)
  {
    *pGroupName = (LPWSTR)LocalAlloc(LPTR,
      (wcslen(pGroup->ptstrName) + 1) * sizeof(TCHAR));
    if (pGroupName == NULL)
    {
      ReportErrorCode(L"LocalAlloc", GetLastError());
      goto FindFileOwnerAndPermissionEnd;
    }
    if (FAILED(StringCchCopyNW(*pGroupName, (wcslen(pGroup->ptstrName) + 1),
      pGroup->ptstrName, wcslen(pGroup->ptstrName) + 1)))
      goto FindFileOwnerAndPermissionEnd;
  }

  if (pMask == NULL)
  {
    ret = TRUE;
    goto FindFileOwnerAndPermissionEnd;
  }

  if (!GetEffectiveRightsForUser(pSd, pOwner->ptstrName, &ownerAccessRights) ||
    !GetEffectiveRightsForUser(pSd, pGroup->ptstrName, &groupAccessRights) ||
    !GetEffectiveRightsForUser(pSd, L"Everyone", &worldAccessRights))
  {
    goto FindFileOwnerAndPermissionEnd;
  }

  *pMask |= GetUnixAccessMask(ownerAccessRights) << 6;
  *pMask |= GetUnixAccessMask(groupAccessRights) << 3;
  *pMask |= GetUnixAccessMask(worldAccessRights);

  ret = TRUE;

FindFileOwnerAndPermissionEnd:
  LocalFree(pOwner);
  LocalFree(pGroup);
  LocalFree(pSd);

  return ret;
}

//----------------------------------------------------------------------------
// Function: ReportErrorCode
//
// Description:
//  Report an error. Use FormatMessage function to get the system error message.
//
// Returns:
//  None
//
// Notes:
//
//
void ReportErrorCode(LPCWSTR func, DWORD err)
{
  DWORD len = 0;
  LPWSTR msg = NULL;

  assert(func != NULL);

  len = FormatMessageW(
    FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
    NULL, err,
    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
    (LPWSTR)&msg, 0, NULL);
  if (len > 0)
  {
    fwprintf(stderr, L"%s error (%d): %s\n", func, err, msg);
  }
  else
  {
    fwprintf(stderr, L"%s error code: %d.\n", func, err);
  }
  if (msg != NULL) LocalFree(msg);
}
