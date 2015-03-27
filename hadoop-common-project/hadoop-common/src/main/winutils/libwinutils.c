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
#pragma comment(lib, "netapi32.lib")
#pragma comment(lib, "Secur32.lib")
#pragma comment(lib, "Userenv.lib")
#pragma comment(lib, "Ntdsapi.lib")

#include "winutils.h"
#include <ctype.h>
#include <Winsvc.h>
#include <authz.h>
#include <sddl.h>
#include <Ntdsapi.h>
#include <malloc.h>

#define WIDEN_STRING(x) WIDEN_STRING_(x)
#define WIDEN_STRING_(x) L ## x
#define STRINGIFY(x) STRINGIFY_(x)
#define STRINGIFY_(x) #x


#pragma message("WSCE config is " STRINGIFY(WSCE_CONFIG_DIR) "\\" STRINGIFY(WSCE_CONFIG_FILE))
const WCHAR* wsceConfigRelativePath = WIDEN_STRING(STRINGIFY(WSCE_CONFIG_DIR)) L"\\" WIDEN_STRING(STRINGIFY(WSCE_CONFIG_FILE));

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
//  If followLink parameter is set to TRUE, we will follow the symbolic link
//  or junction point to get the target file information. Otherwise, the
//  information for the symbolic link or junction point is retrieved.
//
DWORD GetFileInformationByName(
  __in LPCWSTR pathName,
  __in BOOL followLink,
  __out LPBY_HANDLE_FILE_INFORMATION lpFileInformation)
{
  HANDLE fileHandle = INVALID_HANDLE_VALUE;
  BOOL isSymlink = FALSE;
  BOOL isJunction = FALSE;
  DWORD dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS;
  DWORD dwErrorCode = ERROR_SUCCESS;

  assert(lpFileInformation != NULL);

  if (!followLink)
  {
    if ((dwErrorCode = SymbolicLinkCheck(pathName, &isSymlink)) != ERROR_SUCCESS)
      return dwErrorCode;
    if ((dwErrorCode = JunctionPointCheck(pathName, &isJunction)) != ERROR_SUCCESS)
      return dwErrorCode;
    if (isSymlink || isJunction)
      dwFlagsAndAttributes |= FILE_FLAG_OPEN_REPARSE_POINT;
  }

  fileHandle = CreateFileW(
    pathName,
    FILE_READ_ATTRIBUTES,
    FILE_SHARE_READ,
    NULL,
    OPEN_EXISTING,
    dwFlagsAndAttributes,
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
//  Checks if the path is longer than (MAX_PATH - 13) in which case it needs to
//  be prepended with \\?\ for Windows OS to understand it.  The -13 is to
//  account for an additional constraint for directories that it must be possible
//  to append an additional path separator followed by an 8.3 file name.
//
// Returns:
//  TRUE long path
//  FALSE otherwise
static BOOL IsLongWindowsPath(__in PCWSTR path)
{
  return (wcslen(path) + 1) > (MAX_PATH - 13);
}

//----------------------------------------------------------------------------
// Function: IsPrefixedAlready
//
// Description:
//  Checks if the given path is already prepended with \\?\.
//
// Returns:
//  TRUE if yes
//  FALSE otherwise
static BOOL IsPrefixedAlready(__in PCWSTR path)
{
  static const PCWSTR LongPathPrefix = L"\\\\?\\";
  size_t Prefixlen = wcslen(LongPathPrefix);
  size_t i = 0;

  if (path == NULL || wcslen(path) < Prefixlen)
  {
    return FALSE;
  }

  for (i = 0; i < Prefixlen; ++i)
  {
    if (path[i] != LongPathPrefix[i])
    {
      return FALSE;
    }
  }

  return TRUE;
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
  BOOL bAppendPrefix = IsLongWindowsPath(path) && !IsPrefixedAlready(path);
  HRESULT hr = S_OK;

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
    hr = StringCchPrintfW(newPathValue, newPathLen + 1, L"%s%s",
      LongPathPrefix, path);
    if (FAILED(hr))
    {
      dwErrorCode = HRESULT_CODE(hr);
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
    hr = StringCchPrintfW(newPathValue, newPathLen + 1, L"%s", path);
    if (FAILED(hr))
    {
      dwErrorCode = HRESULT_CODE(hr);
      goto ConvertToLongPathExit;
    }
  }

  *newPath = newPathValue;

ConvertToLongPathExit:
  if (dwErrorCode != ERROR_SUCCESS)
  {
    LocalFree(newPathValue);
  }

  return dwErrorCode;
}

//----------------------------------------------------------------------------
// Function: IsDirFileInfo
//
// Description:
//  Test if the given file information is a directory
//
// Returns:
//  TRUE if it is a directory
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
// Function: CheckFileAttributes
//
// Description:
//  Check if the given file has all the given attribute(s)
//
// Returns:
//  ERROR_SUCCESS on success
//  error code otherwise
//
// Notes:
//
static DWORD FileAttributesCheck(
  __in LPCWSTR path, __in DWORD attr, __out PBOOL res)
{
  DWORD attrs = INVALID_FILE_ATTRIBUTES;
  *res = FALSE;
  if ((attrs = GetFileAttributes(path)) != INVALID_FILE_ATTRIBUTES)
    *res = ((attrs & attr) == attr);
  else
    return GetLastError();
  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: IsDirectory
//
// Description:
//  Check if the given file is a directory
//
// Returns:
//  ERROR_SUCCESS on success
//  error code otherwise
//
// Notes:
//
DWORD DirectoryCheck(__in LPCWSTR pathName, __out PBOOL res)
{
  return FileAttributesCheck(pathName, FILE_ATTRIBUTE_DIRECTORY, res);
}

//----------------------------------------------------------------------------
// Function: IsReparsePoint
//
// Description:
//  Check if the given file is a reparse point
//
// Returns:
//  ERROR_SUCCESS on success
//  error code otherwise
//
// Notes:
//
static DWORD ReparsePointCheck(__in LPCWSTR pathName, __out PBOOL res)
{
  return FileAttributesCheck(pathName, FILE_ATTRIBUTE_REPARSE_POINT, res);
}

//----------------------------------------------------------------------------
// Function: CheckReparseTag
//
// Description:
//  Check if the given file is a reparse point of the given tag.
//
// Returns:
//  ERROR_SUCCESS on success
//  error code otherwise
//
// Notes:
//
static DWORD ReparseTagCheck(__in LPCWSTR path, __in DWORD tag, __out PBOOL res)
{
  BOOL isReparsePoint = FALSE;
  HANDLE hFind = INVALID_HANDLE_VALUE;
  WIN32_FIND_DATA findData;
  DWORD dwRtnCode;

  if ((dwRtnCode = ReparsePointCheck(path, &isReparsePoint)) != ERROR_SUCCESS)
    return dwRtnCode;

  if (!isReparsePoint)
  {
    *res = FALSE;
  }
  else
  {
    if ((hFind = FindFirstFile(path, &findData)) == INVALID_HANDLE_VALUE)
    {
      return GetLastError();
    }
    else
    {
      *res = (findData.dwReserved0 == tag);
      FindClose(hFind);
    }
  }
  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: IsSymbolicLink
//
// Description:
//  Check if the given file is a symbolic link.
//
// Returns:
//  ERROR_SUCCESS on success
//  error code otherwise
//
// Notes:
//
DWORD SymbolicLinkCheck(__in LPCWSTR pathName, __out PBOOL res)
{
  return ReparseTagCheck(pathName, IO_REPARSE_TAG_SYMLINK, res);
}

//----------------------------------------------------------------------------
// Function: IsJunctionPoint
//
// Description:
//  Check if the given file is a junction point.
//
// Returns:
//  ERROR_SUCCESS on success
//  error code otherwise
//
// Notes:
//
DWORD JunctionPointCheck(__in LPCWSTR pathName, __out PBOOL res)
{
  return ReparseTagCheck(pathName, IO_REPARSE_TAG_MOUNT_POINT, res);
}

//----------------------------------------------------------------------------
// Function: GetSidFromAcctNameW
//
// Description:
//  To retrieve the SID for a user account
//
// Returns:
//  ERROR_SUCCESS: on success
//  Other error code: otherwise
//
// Notes:
//  Caller needs to destroy the memory of Sid by calling LocalFree()
//
DWORD GetSidFromAcctNameW(__in PCWSTR acctName, __out PSID *ppSid)
{
  DWORD dwSidSize = 0;
  DWORD cchDomainName = 0;
  DWORD dwDomainNameSize = 0;
  LPWSTR domainName = NULL;
  SID_NAME_USE eSidType;

  DWORD dwErrorCode = ERROR_SUCCESS;

  // Validate the input parameters.
  //
  assert (acctName != NULL && ppSid != NULL);

  // Empty name is invalid. However, LookupAccountName() function will return a
  // false Sid, i.e. Sid for 'BUILDIN', for an empty name instead failing. We
  // report the error before calling LookupAccountName() function for this
  // special case. The error code returned here is the same as the last error
  // code set by LookupAccountName() function for an invalid name.
  //
  if (wcslen(acctName) == 0)
    return ERROR_NONE_MAPPED;

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
    return dwErrorCode;
  }
  else
  {
    // Reallocate memory for the buffers.
    //
    *ppSid = (PSID)LocalAlloc(LPTR, dwSidSize);
    if (*ppSid == NULL)
    {
      return GetLastError();
    }
    dwDomainNameSize = (cchDomainName + 1) * sizeof(wchar_t);
    domainName = (LPWSTR)LocalAlloc(LPTR, dwDomainNameSize);
    if (domainName == NULL)
    {
      return GetLastError();
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
      LocalFree(domainName);
      return GetLastError();
    }

    assert(IsValidSid(*ppSid));
  }

  LocalFree(domainName);
  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: GetUnixAccessMask
//
// Description:
//  Compute the 3 bit Unix mask for the owner, group, or, others
//
// Returns:
//  The 3 bit Unix mask in INT
//
// Notes:
//
static INT GetUnixAccessMask(ACCESS_MASK Mask)
{
  static const INT exe   = 0x0001;
  static const INT write = 0x0002;
  static const INT read  = 0x0004;
  INT mask  = 0;

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
//  Get Windows acces mask by AuthZ methods
//
// Returns:
//  ERROR_SUCCESS: on success
//
// Notes:
//
static DWORD GetAccess(AUTHZ_CLIENT_CONTEXT_HANDLE hAuthzClient,
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
    return GetLastError();
  }
  *pAccessRights = (*(const ACCESS_MASK *)(AccessReply.GrantedAccessMask));
  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: GetEffectiveRightsForSid
//
// Description:
//  Get Windows acces mask by AuthZ methods
//
// Returns:
//  ERROR_SUCCESS: on success
//
// Notes:
//   We run into problems for local user accounts when using the method
//   GetEffectiveRightsFromAcl(). We resort to using AuthZ methods as
//   an alternative way suggested on MSDN:
// http://msdn.microsoft.com/en-us/library/windows/desktop/aa446637.aspx
//
static DWORD GetEffectiveRightsForSid(PSECURITY_DESCRIPTOR psd,
  PSID pSid,
  PACCESS_MASK pAccessRights)
{
  AUTHZ_RESOURCE_MANAGER_HANDLE hManager = NULL;
  LUID unusedId = { 0 };
  AUTHZ_CLIENT_CONTEXT_HANDLE hAuthzClientContext = NULL;
  DWORD dwRtnCode = ERROR_SUCCESS;
  DWORD ret = ERROR_SUCCESS;

  assert (pAccessRights != NULL);

  if (!AuthzInitializeResourceManager(AUTHZ_RM_FLAG_NO_AUDIT,
    NULL, NULL, NULL, NULL, &hManager))
  {
    return GetLastError();
  }

  // Pass AUTHZ_SKIP_TOKEN_GROUPS to the function to avoid querying user group
  // information for access check. This allows us to model POSIX permissions
  // on Windows, where a user can have less permissions than a group it
  // belongs to.
  if(!AuthzInitializeContextFromSid(AUTHZ_SKIP_TOKEN_GROUPS,
    pSid, hManager, NULL, unusedId, NULL, &hAuthzClientContext))
  {
    ret = GetLastError();
    goto GetEffectiveRightsForSidEnd;
  }

  if ((dwRtnCode = GetAccess(hAuthzClientContext, psd, pAccessRights))
    != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto GetEffectiveRightsForSidEnd;
  }

GetEffectiveRightsForSidEnd:
  if (hManager != NULL)
  {
    (void)AuthzFreeResourceManager(hManager);
  }
  if (hAuthzClientContext != NULL)
  {
    (void)AuthzFreeContext(hAuthzClientContext);
  }

  return ret;
}

//----------------------------------------------------------------------------
// Function: CheckAccessForCurrentUser
//
// Description:
//   Checks if the current process has the requested access rights on the given
//   path. Based on the following MSDN article:
//   http://msdn.microsoft.com/en-us/library/windows/desktop/ff394771(v=vs.85).aspx
//
// Returns:
//   ERROR_SUCCESS: on success
//
DWORD CheckAccessForCurrentUser(
  __in PCWSTR pathName,
  __in ACCESS_MASK requestedAccess,
  __out BOOL *allowed)
{
  DWORD dwRtnCode = ERROR_SUCCESS;

  LPWSTR longPathName = NULL;
  HANDLE hProcessToken = NULL;
  PSECURITY_DESCRIPTOR pSd = NULL;

  AUTHZ_RESOURCE_MANAGER_HANDLE hManager = NULL;
  AUTHZ_CLIENT_CONTEXT_HANDLE hAuthzClientContext = NULL;
  LUID Luid = {0, 0};

  ACCESS_MASK currentUserAccessRights = 0;

  // Prepend the long path prefix if needed
  dwRtnCode = ConvertToLongPath(pathName, &longPathName);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto CheckAccessEnd;
  }

  // Get SD of the given path. OWNER and DACL security info must be
  // requested, otherwise, AuthzAccessCheck fails with invalid parameter
  // error.
  dwRtnCode = GetNamedSecurityInfo(longPathName, SE_FILE_OBJECT,
    OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION |
    DACL_SECURITY_INFORMATION,
    NULL, NULL, NULL, NULL, &pSd);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto CheckAccessEnd;
  }

  // Get current process token
  if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &hProcessToken))
  {
    dwRtnCode = GetLastError();
    goto CheckAccessEnd;
  }

  if (!AuthzInitializeResourceManager(AUTHZ_RM_FLAG_NO_AUDIT, NULL, NULL,
    NULL, NULL, &hManager))
  {
    dwRtnCode = GetLastError();
    goto CheckAccessEnd;
  }

  if(!AuthzInitializeContextFromToken(0, hProcessToken, hManager, NULL,
    Luid, NULL, &hAuthzClientContext))
  {
    dwRtnCode = GetLastError();
    goto CheckAccessEnd;
  }

  dwRtnCode = GetAccess(hAuthzClientContext, pSd, &currentUserAccessRights);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto CheckAccessEnd;
  }

  *allowed = ((currentUserAccessRights & requestedAccess) == requestedAccess);

CheckAccessEnd:
  LocalFree(longPathName);
  LocalFree(pSd);
  if (hProcessToken != NULL)
  {
    CloseHandle(hProcessToken);
  }
  if (hManager != NULL)
  {
    (void)AuthzFreeResourceManager(hManager);
  }
  if (hAuthzClientContext != NULL)
  {
    (void)AuthzFreeContext(hAuthzClientContext);
  }

  return dwRtnCode;
}


//----------------------------------------------------------------------------
// Function: FindFileOwnerAndPermissionByHandle
//
// Description:
//  Find the owner, primary group and permissions of a file object given the
//  the file object handle. The function will always follow symbolic links.
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code otherwise
//
// Notes:
//  - Caller needs to destroy the memeory of owner and group names by calling
//    LocalFree() function.
//
//  - If the user or group name does not exist, the user or group SID will be
//    returned as the name.
//
DWORD FindFileOwnerAndPermissionByHandle(
  __in HANDLE fileHandle,
  __out_opt LPWSTR *pOwnerName,
  __out_opt LPWSTR *pGroupName,
  __out_opt PINT pMask)
{
  LPWSTR path = NULL;
  DWORD cchPathLen = 0;
  DWORD dwRtnCode = ERROR_SUCCESS;

  DWORD ret = ERROR_SUCCESS;

  dwRtnCode = GetFinalPathNameByHandle(fileHandle, path, cchPathLen, 0);
  if (dwRtnCode == 0)
  {
    ret = GetLastError();
    goto FindFileOwnerAndPermissionByHandleEnd;
  }
  cchPathLen = dwRtnCode;
  path = (LPWSTR) LocalAlloc(LPTR, cchPathLen * sizeof(WCHAR));
  if (path == NULL)
  {
    ret = GetLastError();
    goto FindFileOwnerAndPermissionByHandleEnd;
  }

  dwRtnCode = GetFinalPathNameByHandle(fileHandle, path, cchPathLen, 0);
  if (dwRtnCode != cchPathLen - 1)
  {
    ret = GetLastError();
    goto FindFileOwnerAndPermissionByHandleEnd;
  }

  dwRtnCode = FindFileOwnerAndPermission(path, TRUE, pOwnerName, pGroupName, pMask);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto FindFileOwnerAndPermissionByHandleEnd;
  }

FindFileOwnerAndPermissionByHandleEnd:
  LocalFree(path);
  return ret;
}


//----------------------------------------------------------------------------
// Function: FindFileOwnerAndPermission
//
// Description:
//  Find the owner, primary group and permissions of a file object
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code otherwise
//
// Notes:
//  - Caller needs to destroy the memeory of owner and group names by calling
//    LocalFree() function.
//
//  - If the user or group name does not exist, the user or group SID will be
//    returned as the name.
//
DWORD FindFileOwnerAndPermission(
  __in LPCWSTR pathName,
  __in BOOL followLink,
  __out_opt LPWSTR *pOwnerName,
  __out_opt LPWSTR *pGroupName,
  __out_opt PINT pMask)
{
  DWORD dwRtnCode = 0;
  PSECURITY_DESCRIPTOR pSd = NULL;

  PSID psidOwner = NULL;
  PSID psidGroup = NULL;
  PSID psidEveryone = NULL;
  DWORD cbSid = SECURITY_MAX_SID_SIZE;
  PACL pDacl = NULL;

  BOOL isSymlink = FALSE;
  BY_HANDLE_FILE_INFORMATION fileInformation = {0};

  ACCESS_MASK ownerAccessRights = 0;
  ACCESS_MASK groupAccessRights = 0;
  ACCESS_MASK worldAccessRights = 0;

  DWORD ret = ERROR_SUCCESS;

  // Do nothing if the caller request nothing
  //
  if (pOwnerName == NULL && pGroupName == NULL && pMask == NULL)
  {
    return ret;
  }

  dwRtnCode = GetNamedSecurityInfo(pathName, SE_FILE_OBJECT,
    OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION |
    DACL_SECURITY_INFORMATION,
    &psidOwner, &psidGroup, &pDacl, NULL, &pSd);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto FindFileOwnerAndPermissionEnd;
  }

  if (pOwnerName != NULL)
  {
    dwRtnCode = GetAccntNameFromSid(psidOwner, pOwnerName);
    if (dwRtnCode == ERROR_NONE_MAPPED)
    {
      if (!ConvertSidToStringSid(psidOwner, pOwnerName))
      {
        ret = GetLastError();
        goto FindFileOwnerAndPermissionEnd;
      }
    }
    else if (dwRtnCode != ERROR_SUCCESS)
    {
      ret = dwRtnCode;
      goto FindFileOwnerAndPermissionEnd;
    }
  }

  if (pGroupName != NULL)
  {
    dwRtnCode = GetAccntNameFromSid(psidGroup, pGroupName);
    if (dwRtnCode == ERROR_NONE_MAPPED)
    {
      if (!ConvertSidToStringSid(psidGroup, pGroupName))
      {
        ret = GetLastError();
        goto FindFileOwnerAndPermissionEnd;
      }
    }
    else if (dwRtnCode != ERROR_SUCCESS)
    {
      ret = dwRtnCode;
      goto FindFileOwnerAndPermissionEnd;
    }
  }

  if (pMask == NULL) goto FindFileOwnerAndPermissionEnd;

  dwRtnCode = GetFileInformationByName(pathName,
    followLink, &fileInformation);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto FindFileOwnerAndPermissionEnd;
  }

  dwRtnCode = SymbolicLinkCheck(pathName, &isSymlink);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto FindFileOwnerAndPermissionEnd;
  }

  if (isSymlink)
    *pMask |= UX_SYMLINK;
  else if (IsDirFileInfo(&fileInformation))
    *pMask |= UX_DIRECTORY;
  else
    *pMask |= UX_REGULAR;

  if ((dwRtnCode = GetEffectiveRightsForSid(pSd,
    psidOwner, &ownerAccessRights)) != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto FindFileOwnerAndPermissionEnd;
  }

  if ((dwRtnCode = GetEffectiveRightsForSid(pSd,
    psidGroup, &groupAccessRights)) != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto FindFileOwnerAndPermissionEnd;
  }

  if ((psidEveryone = LocalAlloc(LPTR, cbSid)) == NULL)
  {
    ret = GetLastError();
    goto FindFileOwnerAndPermissionEnd;
  }
  if (!CreateWellKnownSid(WinWorldSid, NULL, psidEveryone, &cbSid))
  {
    ret = GetLastError();
    goto FindFileOwnerAndPermissionEnd;
  }
  if ((dwRtnCode = GetEffectiveRightsForSid(pSd,
    psidEveryone, &worldAccessRights)) != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto FindFileOwnerAndPermissionEnd;
  }

  *pMask |= GetUnixAccessMask(ownerAccessRights) << 6;
  *pMask |= GetUnixAccessMask(groupAccessRights) << 3;
  *pMask |= GetUnixAccessMask(worldAccessRights);

FindFileOwnerAndPermissionEnd:
  LocalFree(psidEveryone);
  LocalFree(pSd);

  return ret;
}

//----------------------------------------------------------------------------
// Function: GetWindowsAccessMask
//
// Description:
//  Get the Windows AccessMask for user, group and everyone based on the Unix
//  permission mask
//
// Returns:
//  none
//
// Notes:
//  none
//
static void GetWindowsAccessMask(INT unixMask,
  ACCESS_MASK *userAllow,
  ACCESS_MASK *userDeny,
  ACCESS_MASK *groupAllow,
  ACCESS_MASK *groupDeny,
  ACCESS_MASK *otherAllow)
{
  assert (userAllow != NULL && userDeny != NULL &&
    groupAllow != NULL && groupDeny != NULL &&
    otherAllow != NULL);

  *userAllow = WinMasks[WIN_ALL] | WinMasks[WIN_OWNER_SE];
  if ((unixMask & UX_U_READ) == UX_U_READ)
    *userAllow |= WinMasks[WIN_READ];

  if ((unixMask & UX_U_WRITE) == UX_U_WRITE)
    *userAllow |= WinMasks[WIN_WRITE];

  if ((unixMask & UX_U_EXECUTE) == UX_U_EXECUTE)
    *userAllow |= WinMasks[WIN_EXECUTE];

  *userDeny = 0;
  if ((unixMask & UX_U_READ) != UX_U_READ &&
    ((unixMask & UX_G_READ) == UX_G_READ ||
    (unixMask & UX_O_READ) == UX_O_READ))
    *userDeny |= WinMasks[WIN_READ];

  if ((unixMask & UX_U_WRITE) != UX_U_WRITE &&
    ((unixMask & UX_G_WRITE) == UX_G_WRITE ||
    (unixMask & UX_O_WRITE) == UX_O_WRITE))
    *userDeny |= WinMasks[WIN_WRITE];

  if ((unixMask & UX_U_EXECUTE) != UX_U_EXECUTE &&
    ((unixMask & UX_G_EXECUTE) == UX_G_EXECUTE ||
    (unixMask & UX_O_EXECUTE) == UX_O_EXECUTE))
    *userDeny |= WinMasks[WIN_EXECUTE];

  *groupAllow = WinMasks[WIN_ALL];
  if ((unixMask & UX_G_READ) == UX_G_READ)
    *groupAllow |= FILE_GENERIC_READ;

  if ((unixMask & UX_G_WRITE) == UX_G_WRITE)
    *groupAllow |= WinMasks[WIN_WRITE];

  if ((unixMask & UX_G_EXECUTE) == UX_G_EXECUTE)
    *groupAllow |= WinMasks[WIN_EXECUTE];

  *groupDeny = 0;
  if ((unixMask & UX_G_READ) != UX_G_READ &&
    (unixMask & UX_O_READ) == UX_O_READ)
    *groupDeny |= WinMasks[WIN_READ];

  if ((unixMask & UX_G_WRITE) != UX_G_WRITE &&
    (unixMask & UX_O_WRITE) == UX_O_WRITE)
    *groupDeny |= WinMasks[WIN_WRITE];

  if ((unixMask & UX_G_EXECUTE) != UX_G_EXECUTE &&
    (unixMask & UX_O_EXECUTE) == UX_O_EXECUTE)
    *groupDeny |= WinMasks[WIN_EXECUTE];

  *otherAllow = WinMasks[WIN_ALL];
  if ((unixMask & UX_O_READ) == UX_O_READ)
    *otherAllow |= WinMasks[WIN_READ];

  if ((unixMask & UX_O_WRITE) == UX_O_WRITE)
    *otherAllow |= WinMasks[WIN_WRITE];

  if ((unixMask & UX_O_EXECUTE) == UX_O_EXECUTE)
    *otherAllow |= WinMasks[WIN_EXECUTE];
}

//----------------------------------------------------------------------------
// Function: GetWindowsDACLs
//
// Description:
//  Get the Windows DACs based the Unix access mask
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code: otherwise
//
// Notes:
//  - Administrators and SYSTEM are always given full permission to the file,
//    unless Administrators or SYSTEM itself is the file owner and the user
//    explictly set the permission to something else. For example, file 'foo'
//    belongs to Administrators, 'chmod 000' on the file will not directly
//    assign Administrators full permission on the file.
//  - Only full permission for Administrators and SYSTEM are inheritable.
//  - CREATOR OWNER is always given full permission and the permission is
//    inheritable, more specifically OBJECT_INHERIT_ACE, CONTAINER_INHERIT_ACE
//    flags are set. The reason is to give the creator of child file full
//    permission, i.e., the child file will have permission mode 700 for
//    a user other than Administrator or SYSTEM.
//
static DWORD GetWindowsDACLs(__in INT unixMask,
  __in PSID pOwnerSid, __in PSID pGroupSid, __out PACL *ppNewDACL)
{
  DWORD winUserAccessDenyMask;
  DWORD winUserAccessAllowMask;
  DWORD winGroupAccessDenyMask;
  DWORD winGroupAccessAllowMask;
  DWORD winOtherAccessAllowMask;

  PSID pEveryoneSid = NULL;
  DWORD cbEveryoneSidSize = SECURITY_MAX_SID_SIZE;

  PSID pSystemSid = NULL;
  DWORD cbSystemSidSize = SECURITY_MAX_SID_SIZE;
  BOOL bAddSystemAcls = FALSE;

  PSID pAdministratorsSid = NULL;
  DWORD cbAdministratorsSidSize = SECURITY_MAX_SID_SIZE;
  BOOL bAddAdministratorsAcls = FALSE;

  PSID pCreatorOwnerSid = NULL;
  DWORD cbCreatorOwnerSidSize = SECURITY_MAX_SID_SIZE;

  PACL pNewDACL = NULL;
  DWORD dwNewAclSize = 0;

  DWORD ret = ERROR_SUCCESS;

  GetWindowsAccessMask(unixMask,
    &winUserAccessAllowMask, &winUserAccessDenyMask,
    &winGroupAccessAllowMask, &winGroupAccessDenyMask,
    &winOtherAccessAllowMask);

  // Create a well-known SID for the Everyone group
  //
  if ((pEveryoneSid = LocalAlloc(LPTR, cbEveryoneSidSize)) == NULL)
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!CreateWellKnownSid(WinWorldSid, NULL, pEveryoneSid, &cbEveryoneSidSize))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }

  // Create a well-known SID for the Administrators group
  //
  if ((pAdministratorsSid = LocalAlloc(LPTR, cbAdministratorsSidSize)) == NULL)
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!CreateWellKnownSid(WinBuiltinAdministratorsSid, NULL,
    pAdministratorsSid, &cbAdministratorsSidSize))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!EqualSid(pAdministratorsSid, pOwnerSid)
    && !EqualSid(pAdministratorsSid, pGroupSid))
    bAddAdministratorsAcls = TRUE;

  // Create a well-known SID for the SYSTEM
  //
  if ((pSystemSid = LocalAlloc(LPTR, cbSystemSidSize)) == NULL)
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!CreateWellKnownSid(WinLocalSystemSid, NULL,
    pSystemSid, &cbSystemSidSize))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!EqualSid(pSystemSid, pOwnerSid)
    && !EqualSid(pSystemSid, pGroupSid))
    bAddSystemAcls = TRUE;

  // Create a well-known SID for the Creator Owner
  //
  if ((pCreatorOwnerSid = LocalAlloc(LPTR, cbCreatorOwnerSidSize)) == NULL)
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!CreateWellKnownSid(WinCreatorOwnerSid, NULL,
    pCreatorOwnerSid, &cbCreatorOwnerSidSize))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }

  // Create the new DACL
  //
  dwNewAclSize = sizeof(ACL);
  dwNewAclSize += sizeof(ACCESS_ALLOWED_ACE) +
    GetLengthSid(pOwnerSid) - sizeof(DWORD);
  if (winUserAccessDenyMask)
    dwNewAclSize += sizeof(ACCESS_DENIED_ACE) +
    GetLengthSid(pOwnerSid) - sizeof(DWORD);
  dwNewAclSize += sizeof(ACCESS_ALLOWED_ACE) +
    GetLengthSid(pGroupSid) - sizeof(DWORD);
  if (winGroupAccessDenyMask)
    dwNewAclSize += sizeof(ACCESS_DENIED_ACE) +
    GetLengthSid(pGroupSid) - sizeof(DWORD);
  dwNewAclSize += sizeof(ACCESS_ALLOWED_ACE) +
    GetLengthSid(pEveryoneSid) - sizeof(DWORD);

  if (bAddSystemAcls)
  {
    dwNewAclSize += sizeof(ACCESS_ALLOWED_ACE) +
      cbSystemSidSize - sizeof(DWORD);
  }

  if (bAddAdministratorsAcls)
  {
    dwNewAclSize += sizeof(ACCESS_ALLOWED_ACE) +
      cbAdministratorsSidSize - sizeof(DWORD);
  }

  dwNewAclSize += sizeof(ACCESS_ALLOWED_ACE) +
    cbCreatorOwnerSidSize - sizeof(DWORD);

  pNewDACL = (PACL)LocalAlloc(LPTR, dwNewAclSize);
  if (pNewDACL == NULL)
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!InitializeAcl(pNewDACL, dwNewAclSize, ACL_REVISION))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }

  if (!AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    GENERIC_ALL, pCreatorOwnerSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }

  if (bAddSystemAcls &&
    !AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    GENERIC_ALL, pSystemSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }

  if (bAddAdministratorsAcls &&
    !AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    GENERIC_ALL, pAdministratorsSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }

  if (winUserAccessDenyMask &&
    !AddAccessDeniedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    winUserAccessDenyMask, pOwnerSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    winUserAccessAllowMask, pOwnerSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (winGroupAccessDenyMask &&
    !AddAccessDeniedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    winGroupAccessDenyMask, pGroupSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    winGroupAccessAllowMask, pGroupSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
    winOtherAccessAllowMask, pEveryoneSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }

  *ppNewDACL = pNewDACL;

GetWindowsDACLsEnd:
  LocalFree(pEveryoneSid);
  LocalFree(pAdministratorsSid);
  LocalFree(pSystemSid);
  LocalFree(pCreatorOwnerSid);
  if (ret != ERROR_SUCCESS) LocalFree(pNewDACL);
  
  return ret;
}

//----------------------------------------------------------------------------
// Function: ChangeFileModeByMask
//
// Description:
//  Change a file or direcotry at the path to Unix mode
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
DWORD ChangeFileModeByMask(__in LPCWSTR path, INT mode)
{
  LPWSTR longPathName = NULL;
  PACL pNewDACL = NULL;
  PSID pOwnerSid = NULL;
  PSID pGroupSid = NULL;
  PSECURITY_DESCRIPTOR pSD = NULL;

  SECURITY_DESCRIPTOR_CONTROL control;
  DWORD revision = 0;

  PSECURITY_DESCRIPTOR pAbsSD = NULL;
  PSECURITY_DESCRIPTOR pNonNullSD = NULL;
  PACL pAbsDacl = NULL;
  PACL pAbsSacl = NULL;
  PSID pAbsOwner = NULL;
  PSID pAbsGroup = NULL;

  DWORD dwRtnCode = 0;
  DWORD dwErrorCode = 0;

  DWORD ret = ERROR_SUCCESS;

  dwRtnCode = ConvertToLongPath(path, &longPathName);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto ChangeFileModeByMaskEnd;
  }

  // Get owner and group Sids
  //
  dwRtnCode = GetNamedSecurityInfoW(
    longPathName,
    SE_FILE_OBJECT, 
    OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION,
    &pOwnerSid,
    &pGroupSid,
    NULL,
    NULL,
    &pSD);
  if (ERROR_SUCCESS != dwRtnCode)
  {
    ret = dwRtnCode;
    goto ChangeFileModeByMaskEnd; 
  }

  // SetSecurityDescriptorDacl function used below only accepts security
  // descriptor in absolute format, meaning that its members must be pointers to
  // other structures, rather than offsets to contiguous data.
  // To determine whether a security descriptor is self-relative or absolute,
  // call the GetSecurityDescriptorControl function and check the
  // SE_SELF_RELATIVE flag of the SECURITY_DESCRIPTOR_CONTROL parameter.
  //
  if (!GetSecurityDescriptorControl(pSD, &control, &revision))
  {
    ret = GetLastError();
    goto ChangeFileModeByMaskEnd;
  }

  // If the security descriptor is self-relative, we use MakeAbsoluteSD function
  // to convert it to absolute format.
  //
  if ((control & SE_SELF_RELATIVE) == SE_SELF_RELATIVE)
  {
    DWORD absSDSize = 0;
    DWORD daclSize = 0;
    DWORD saclSize = 0;
    DWORD ownerSize = 0;
    DWORD primaryGroupSize = 0;
    MakeAbsoluteSD(pSD, NULL, &absSDSize, NULL, &daclSize, NULL,
      &saclSize, NULL, &ownerSize, NULL, &primaryGroupSize);
    if ((dwErrorCode = GetLastError()) != ERROR_INSUFFICIENT_BUFFER)
    {
      ret = dwErrorCode;
      goto ChangeFileModeByMaskEnd;
    }

    if ((pAbsSD = (PSECURITY_DESCRIPTOR) LocalAlloc(LPTR, absSDSize)) == NULL)
    {
      ret = GetLastError();
      goto ChangeFileModeByMaskEnd;
    }
    if ((pAbsDacl = (PACL) LocalAlloc(LPTR, daclSize)) == NULL)
    {
      ret = GetLastError();
      goto ChangeFileModeByMaskEnd;
    }
    if ((pAbsSacl = (PACL) LocalAlloc(LPTR, saclSize)) == NULL)
    {
      ret = GetLastError();
      goto ChangeFileModeByMaskEnd;
    }
    if ((pAbsOwner = (PSID) LocalAlloc(LPTR, ownerSize)) == NULL)
    {
      ret = GetLastError();
      goto ChangeFileModeByMaskEnd;
    }
    if ((pAbsGroup = (PSID) LocalAlloc(LPTR, primaryGroupSize)) == NULL)
    {
      ret = GetLastError();
      goto ChangeFileModeByMaskEnd;
    }

    if (!MakeAbsoluteSD(pSD, pAbsSD, &absSDSize, pAbsDacl, &daclSize, pAbsSacl,
      &saclSize, pAbsOwner, &ownerSize, pAbsGroup, &primaryGroupSize))
    {
      ret = GetLastError();
      goto ChangeFileModeByMaskEnd;
    }
  }

  // Get Windows DACLs based on Unix access mask
  //
  if ((dwRtnCode = GetWindowsDACLs(mode, pOwnerSid, pGroupSid, &pNewDACL))
    != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto ChangeFileModeByMaskEnd;
  }

  // Set the DACL information in the security descriptor; if a DACL is already
  // present in the security descriptor, the DACL is replaced. The security
  // descriptor is then used to set the security of a file or directory.
  //
  pNonNullSD = (pAbsSD != NULL) ? pAbsSD : pSD;
  if (!SetSecurityDescriptorDacl(pNonNullSD, TRUE, pNewDACL, FALSE))
  {
    ret = GetLastError();
    goto ChangeFileModeByMaskEnd;
  }

  // MSDN states "This function is obsolete. Use the SetNamedSecurityInfo
  // function instead." However we have the following problem when using
  // SetNamedSecurityInfo:
  //  - When PROTECTED_DACL_SECURITY_INFORMATION is not passed in as part of
  //    security information, the object will include inheritable permissions
  //    from its parent.
  //  - When PROTECTED_DACL_SECURITY_INFORMATION is passsed in to set
  //    permissions on a directory, the child object of the directory will lose
  //    inheritable permissions from their parent (the current directory).
  // By using SetFileSecurity, we have the nice property that the new
  // permissions of the object does not include the inheritable permissions from
  // its parent, and the child objects will not lose their inherited permissions
  // from the current object.
  //
  if (!SetFileSecurity(longPathName, DACL_SECURITY_INFORMATION, pNonNullSD))
  {
    ret = GetLastError();
    goto ChangeFileModeByMaskEnd;
  }

ChangeFileModeByMaskEnd:
  pNonNullSD = NULL;
  LocalFree(longPathName);
  LocalFree(pSD);
  LocalFree(pNewDACL);
  LocalFree(pAbsDacl);
  LocalFree(pAbsSacl);
  LocalFree(pAbsOwner);
  LocalFree(pAbsGroup);
  LocalFree(pAbsSD);

  return ret;
}

//----------------------------------------------------------------------------
// Function: GetTokenInformationByClass
//
// Description:
//  Gets a class of information from a token.  On success, this function has
//  dynamically allocated memory and set the ppTokenInformation parameter to
//  point to it.  The caller owns this memory and is reponsible for releasing it
//  by calling LocalFree.
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code: otherwise
//
static DWORD GetTokenInformationByClass(__in HANDLE hToken,
    __in TOKEN_INFORMATION_CLASS class, __out_opt LPVOID *ppTokenInformation) {
  DWORD dwRtnCode = ERROR_SUCCESS;
  LPVOID pTokenInformation = NULL;
  DWORD dwSize = 0;

  // Call GetTokenInformation first time to get the required buffer size.
  if (!GetTokenInformation(hToken, class, NULL, 0, &dwSize)) {
    dwRtnCode = GetLastError();
    if (dwRtnCode != ERROR_INSUFFICIENT_BUFFER) {
      return dwRtnCode;
    }
  }

  // Allocate memory.
  pTokenInformation = LocalAlloc(LPTR, dwSize);
  if (!pTokenInformation) {
    return GetLastError();
  }

  // Call GetTokenInformation second time to fill our buffer with data.
  if (!GetTokenInformation(hToken, class, pTokenInformation, dwSize, &dwSize)) {
    LocalFree(pTokenInformation);
    return GetLastError();
  }

  *ppTokenInformation = pTokenInformation;
  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: GetWindowsDACLsForCreate
//
// Description:
//  Get the Windows discretionary access control list equivalent to the given
//  mode, suitable for creating a new file or directory.  Ownership is assumed
//  to be the current process owner and primary group.  On success, this function
//  has dynamically allocated memory and set the ppDACL parameter to point to it.
//  The caller owns this memory and is reponsible for releasing it by calling
//  LocalFree.
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code: otherwise
//
static DWORD GetWindowsDACLsForCreate(__in INT mode, __out PACL *ppDACL) {
  DWORD dwRtnCode = ERROR_SUCCESS;
  HANDLE hToken = NULL;
  DWORD dwSize = 0;
  PTOKEN_OWNER pTokenOwner = NULL;
  PTOKEN_PRIMARY_GROUP pTokenPrimaryGroup = NULL;
  PSID pOwnerSid = NULL, pGroupSid = NULL;
  PACL pDACL = NULL;

  if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &hToken)) {
    dwRtnCode = GetLastError();
    goto done;
  }

  dwRtnCode = GetTokenInformationByClass(hToken, TokenOwner, &pTokenOwner);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }
  pOwnerSid = pTokenOwner->Owner;

  dwRtnCode = GetTokenInformationByClass(hToken, TokenPrimaryGroup,
      &pTokenPrimaryGroup);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }
  pGroupSid = pTokenPrimaryGroup->PrimaryGroup;

  dwRtnCode = GetWindowsDACLs(mode, pOwnerSid, pGroupSid, &pDACL);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  *ppDACL = pDACL;

done:
  if (hToken) {
    CloseHandle(hToken);
  }
  LocalFree(pTokenOwner);
  LocalFree(pTokenPrimaryGroup);
  return dwRtnCode;
}

//----------------------------------------------------------------------------
// Function: CreateSecurityDescriptorForCreate
//
// Description:
//  Creates a security descriptor with the given DACL, suitable for creating a
//  new file or directory.  On success, this function has dynamically allocated
//  memory and set the ppSD parameter to point to it.  The caller owns this
//  memory and is reponsible for releasing it by calling LocalFree.
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code: otherwise
//
static DWORD CreateSecurityDescriptorForCreate(__in PACL pDACL,
    __out PSECURITY_DESCRIPTOR *ppSD) {
  DWORD dwRtnCode = ERROR_SUCCESS;
  PSECURITY_DESCRIPTOR pSD = NULL;

  pSD = LocalAlloc(LPTR, SECURITY_DESCRIPTOR_MIN_LENGTH);
  if (!pSD) {
    dwRtnCode = GetLastError();
    goto done;
  }

  if (!InitializeSecurityDescriptor(pSD, SECURITY_DESCRIPTOR_REVISION)) {
    dwRtnCode = GetLastError();
    goto done;
  }

  if (!SetSecurityDescriptorDacl(pSD, TRUE, pDACL, FALSE)) {
    dwRtnCode = GetLastError();
    goto done;
  }

  *ppSD = pSD;

done:
  if (dwRtnCode != ERROR_SUCCESS) {
    LocalFree(pSD);
  }
  return dwRtnCode;
}

//----------------------------------------------------------------------------
// Function: CreateDirectoryWithMode
//
// Description:
//  Create a directory with initial security descriptor containing a
//  discretionary access control list equivalent to the given mode.
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code: otherwise
//
// Notes:
//  This function is long path safe, i.e. the path will be converted to long
//  path format if not already converted. So the caller does not need to do
//  the conversion before calling the method.
//
DWORD CreateDirectoryWithMode(__in LPCWSTR lpPath, __in INT mode) {
  DWORD dwRtnCode = ERROR_SUCCESS;
  LPWSTR lpLongPath = NULL;
  PACL pDACL = NULL;
  PSECURITY_DESCRIPTOR pSD = NULL;
  SECURITY_ATTRIBUTES sa;

  dwRtnCode = ConvertToLongPath(lpPath, &lpLongPath);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  dwRtnCode = GetWindowsDACLsForCreate(mode, &pDACL);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  dwRtnCode = CreateSecurityDescriptorForCreate(pDACL, &pSD);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  sa.nLength = sizeof(SECURITY_ATTRIBUTES);
  sa.lpSecurityDescriptor = pSD;
  sa.bInheritHandle = FALSE;

  if (!CreateDirectoryW(lpLongPath, &sa)) {
    dwRtnCode = GetLastError();
  }

done:
  LocalFree(lpLongPath);
  LocalFree(pDACL);
  LocalFree(pSD);
  return dwRtnCode;
}

//----------------------------------------------------------------------------
// Function: CreateFileWithMode
//
// Description:
//  Create a file with initial security descriptor containing a discretionary
//  access control list equivalent to the given mode.
//
// Returns:
//  ERROR_SUCCESS: on success
//  Error code: otherwise
//
// Notes:
//  This function is long path safe, i.e. the path will be converted to long
//  path format if not already converted. So the caller does not need to do
//  the conversion before calling the method.
//
DWORD CreateFileWithMode(__in LPCWSTR lpPath, __in DWORD dwDesiredAccess,
    __in DWORD dwShareMode, __in DWORD dwCreationDisposition, __in INT mode,
    __out PHANDLE pHFile) {
  DWORD dwRtnCode = ERROR_SUCCESS;
  LPWSTR lpLongPath = NULL;
  PACL pDACL = NULL;
  PSECURITY_DESCRIPTOR pSD = NULL;
  SECURITY_ATTRIBUTES sa;
  DWORD dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
  HANDLE hFile = INVALID_HANDLE_VALUE;

  dwRtnCode = ConvertToLongPath(lpPath, &lpLongPath);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  dwRtnCode = GetWindowsDACLsForCreate(mode, &pDACL);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  dwRtnCode = CreateSecurityDescriptorForCreate(pDACL, &pSD);
  if (dwRtnCode != ERROR_SUCCESS) {
    goto done;
  }

  sa.nLength = sizeof(SECURITY_ATTRIBUTES);
  sa.lpSecurityDescriptor = pSD;
  sa.bInheritHandle = FALSE;

  hFile = CreateFileW(lpLongPath, dwDesiredAccess, dwShareMode, &sa,
    dwCreationDisposition, dwFlagsAndAttributes, NULL);
  if (hFile == INVALID_HANDLE_VALUE) {
    dwRtnCode = GetLastError();
    goto done;
  }

  *pHFile = hFile;

done:
  LocalFree(lpLongPath);
  LocalFree(pDACL);
  LocalFree(pSD);
  return dwRtnCode;
}

//----------------------------------------------------------------------------
// Function: GetAccntNameFromSid
//
// Description:
//  To retrieve an account name given the SID
//
// Returns:
//  ERROR_SUCCESS: on success
//  Other error code: otherwise
//
// Notes:
//  Caller needs to destroy the memory of account name by calling LocalFree()
//
DWORD GetAccntNameFromSid(__in PSID pSid, __out PWSTR *ppAcctName)
{
  LPWSTR lpName = NULL;
  DWORD cchName = 0;
  LPWSTR lpDomainName = NULL;
  DWORD cchDomainName = 0;
  SID_NAME_USE eUse = SidTypeUnknown;
  DWORD cchAcctName = 0;
  DWORD dwErrorCode = ERROR_SUCCESS;
  HRESULT hr = S_OK;

  DWORD ret = ERROR_SUCCESS;

  assert(ppAcctName != NULL);

  // NOTE:
  // MSDN says the length returned for the buffer size including the terminating
  // null character. However we found it is not true during debuging.
  //
  LookupAccountSid(NULL, pSid, NULL, &cchName, NULL, &cchDomainName, &eUse);
  if ((dwErrorCode = GetLastError()) != ERROR_INSUFFICIENT_BUFFER)
    return dwErrorCode;
  lpName = (LPWSTR) LocalAlloc(LPTR, (cchName + 1) * sizeof(WCHAR));
  if (lpName == NULL)
  {
    ret = GetLastError();
    goto GetAccntNameFromSidEnd;
  }
  lpDomainName = (LPWSTR) LocalAlloc(LPTR, (cchDomainName + 1) * sizeof(WCHAR));
  if (lpDomainName == NULL)
  {
    ret = GetLastError();
    goto GetAccntNameFromSidEnd;
  }

  if (!LookupAccountSid(NULL, pSid,
    lpName, &cchName, lpDomainName, &cchDomainName, &eUse))
  {
    ret = GetLastError();
    goto GetAccntNameFromSidEnd;
  }

  // Buffer size = name length + 1 for '\' + domain length + 1 for NULL
  cchAcctName = cchName + cchDomainName + 2;
  *ppAcctName = (LPWSTR) LocalAlloc(LPTR, cchAcctName * sizeof(WCHAR));
  if (*ppAcctName == NULL)
  {
    ret = GetLastError();
    goto GetAccntNameFromSidEnd;
  }

  hr = StringCchCopyW(*ppAcctName, cchAcctName, lpDomainName);
  if (FAILED(hr))
  {
    ret = HRESULT_CODE(hr);
    goto GetAccntNameFromSidEnd;
  }

  hr = StringCchCatW(*ppAcctName, cchAcctName, L"\\");
  if (FAILED(hr))
  {
    ret = HRESULT_CODE(hr);
    goto GetAccntNameFromSidEnd;
  }

  hr = StringCchCatW(*ppAcctName, cchAcctName, lpName);
  if (FAILED(hr))
  {
    ret = HRESULT_CODE(hr);
    goto GetAccntNameFromSidEnd;
  }

GetAccntNameFromSidEnd:
  LocalFree(lpName);
  LocalFree(lpDomainName);
  if (ret != ERROR_SUCCESS)
  {
    LocalFree(*ppAcctName);
    *ppAcctName = NULL;
  }
  return ret;
}

//----------------------------------------------------------------------------
// Function: GetLocalGroupsForUser
//
// Description:
//  Get an array of groups for the given user.
//
// Returns:
//  ERROR_SUCCESS on success
//  Other error code on failure
//
// Notes:
// - NetUserGetLocalGroups() function only accepts full user name in the format
//   [domain name]\[username]. The user input to this function can be only the
//   username. In this case, NetUserGetLocalGroups() will fail on the first try,
//   and we will try to find full user name using LookupAccountNameW() method,
//   and call NetUserGetLocalGroups() function again with full user name.
//   However, it is not always possible to find full user name given only user
//   name. For example, a computer named 'win1' joined domain 'redmond' can have
//   two different users, 'win1\alex' and 'redmond\alex'. Given only 'alex', we
//   cannot tell which one is correct.
//
// - Caller needs to destroy the memory of groups by using the
//   NetApiBufferFree() function
//
DWORD GetLocalGroupsForUser(
  __in LPCWSTR user,
  __out LPLOCALGROUP_USERS_INFO_0 *groups,
  __out LPDWORD entries)
{
  DWORD dwEntriesRead = 0;
  DWORD dwTotalEntries = 0;
  NET_API_STATUS nStatus = NERR_Success;

  PSID pUserSid = NULL;
  LPWSTR fullName = NULL;

  DWORD dwRtnCode = ERROR_SUCCESS;

  DWORD ret = ERROR_SUCCESS;

  *groups = NULL;
  *entries = 0;

  nStatus = NetUserGetLocalGroups(NULL,
    user,
    0,
    0,
    (LPBYTE *) groups,
    MAX_PREFERRED_LENGTH,
    &dwEntriesRead,
    &dwTotalEntries);

  if (nStatus == NERR_Success)
  {
    *entries = dwEntriesRead;
    return ERROR_SUCCESS;
  }
  else if (nStatus != NERR_UserNotFound)
  {
    return nStatus;
  }

  if ((dwRtnCode = GetSidFromAcctNameW(user, &pUserSid)) != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto GetLocalGroupsForUserEnd;
  }

  if ((dwRtnCode = GetAccntNameFromSid(pUserSid, &fullName)) != ERROR_SUCCESS)
  {
    ret = dwRtnCode;
    goto GetLocalGroupsForUserEnd;
  }

  nStatus = NetUserGetLocalGroups(NULL,
    fullName,
    0,
    0,
    (LPBYTE *) groups,
    MAX_PREFERRED_LENGTH,
    &dwEntriesRead,
    &dwTotalEntries);
  if (nStatus != NERR_Success)
  {
    // NERR_DCNotFound (2453) and NERR_UserNotFound (2221) are not published
    // Windows System Error Code. All other error codes returned by
    // NetUserGetLocalGroups() are valid System Error Codes according to MSDN.
    ret = nStatus;
    goto GetLocalGroupsForUserEnd;
  }

  *entries = dwEntriesRead;

GetLocalGroupsForUserEnd:
  LocalFree(pUserSid);
  LocalFree(fullName);
  return ret;
}


//----------------------------------------------------------------------------
// Function: EnablePrivilege
//
// Description:
//  Check if the process has the given privilege. If yes, enable the privilege
//  to the process's access token.
//
// Returns:
//  ERROR_SUCCESS on success
//  GetLastError() on error
//
// Notes:
//
DWORD EnablePrivilege(__in LPCWSTR privilegeName)
{
  HANDLE hToken = INVALID_HANDLE_VALUE;
  TOKEN_PRIVILEGES tp = { 0 };
  DWORD dwErrCode = ERROR_SUCCESS;

  if (!OpenProcessToken(GetCurrentProcess(),
    TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken))
  {
    dwErrCode = GetLastError();
    ReportErrorCode(L"OpenProcessToken", dwErrCode);
    return dwErrCode;
  }

  tp.PrivilegeCount = 1;
  if (!LookupPrivilegeValueW(NULL,
    privilegeName, &(tp.Privileges[0].Luid)))
  {
    dwErrCode = GetLastError();
    ReportErrorCode(L"LookupPrivilegeValue", dwErrCode);
    CloseHandle(hToken);
    return dwErrCode;
  }
  tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

  // As stated on MSDN, we need to use GetLastError() to check if
  // AdjustTokenPrivileges() adjusted all of the specified privileges.
  //
  if (!AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL)) {
    dwErrCode = GetLastError();
  }
  CloseHandle(hToken);

  return dwErrCode;
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
    LogDebugMessage(L"%s error (%d): %s\n", func, err, msg);
    fwprintf(stderr, L"%s error (%d): %s\n", func, err, msg);
  }
  else
  {
    LogDebugMessage(L"%s error code: %d.\n", func, err);
    fwprintf(stderr, L"%s error code: %d.\n", func, err);
  }

  if (msg != NULL) LocalFree(msg);
}

//----------------------------------------------------------------------------
// Function: GetLibraryName
//
// Description:
//  Given an address, get the file name of the library from which it was loaded.
//
// Notes:
// - The function allocates heap memory and points the filename out parameter to
//   the newly allocated memory, which will contain the name of the file.
//
// - If there is any failure, then the function frees the heap memory it
//   allocated and sets the filename out parameter to NULL.
//
void GetLibraryName(LPCVOID lpAddress, LPWSTR *filename)
{
  SIZE_T ret = 0;
  DWORD size = MAX_PATH;
  HMODULE mod = NULL;
  DWORD err = ERROR_SUCCESS;

  MEMORY_BASIC_INFORMATION mbi;
  ret = VirtualQuery(lpAddress, &mbi, sizeof(mbi));
  if (ret == 0) goto cleanup;
  mod = mbi.AllocationBase;

  do {
    *filename = (LPWSTR) realloc(*filename, size * sizeof(WCHAR));
    if (*filename == NULL) goto cleanup;
    GetModuleFileName(mod, *filename, size);
    size <<= 1;
    err = GetLastError();
  } while (err == ERROR_INSUFFICIENT_BUFFER);

  if (err != ERROR_SUCCESS) goto cleanup;

  return;

cleanup:
  if (*filename != NULL)
  {
    free(*filename);
    *filename = NULL;
  }
}

// Function: AssignLsaString
//
// Description:
//  fills in values of LSA_STRING struct to point to a string buffer
//
// Returns:
//  None
//
//  IMPORTANT*** strBuf is not copied. It must be globally immutable
//
void AssignLsaString(__inout LSA_STRING * target, __in const char *strBuf)
{
  target->Length = (USHORT)(sizeof(char)*strlen(strBuf));
  target->MaximumLength = target->Length;
  target->Buffer = (char *)(strBuf);
}

//----------------------------------------------------------------------------
// Function: RegisterWithLsa
//
// Description:
//  Registers with local security authority and sets handle for use in later LSA
//  operations
//
// Returns:
//  ERROR_SUCCESS on success
//  Other error code on failure
//
// Notes:
//
DWORD RegisterWithLsa(__in const char *logonProcessName, __out HANDLE * lsaHandle) 
{
  LSA_STRING processName; 
  LSA_OPERATIONAL_MODE o_mode; // never useful as per msdn docs
  NTSTATUS registerStatus;
  *lsaHandle = 0;
  
  AssignLsaString(&processName, logonProcessName);
  registerStatus = LsaRegisterLogonProcess(&processName, lsaHandle, &o_mode); 
  
  return LsaNtStatusToWinError( registerStatus );
}

//----------------------------------------------------------------------------
// Function: UnregisterWithLsa
//
// Description:
//  Closes LSA handle allocated by RegisterWithLsa()
//
// Returns:
//  None
//
// Notes:
//
void UnregisterWithLsa(__in HANDLE lsaHandle)
{
  LsaClose(lsaHandle);
}

//----------------------------------------------------------------------------
// Function: LookupKerberosAuthenticationPackageId
//
// Description:
//  Looks of the current id (integer index) of the Kerberos authentication package on the local
//  machine.
//
// Returns:
//  ERROR_SUCCESS on success
//  Other error code on failure
//
// Notes:
//
DWORD LookupKerberosAuthenticationPackageId(__in HANDLE lsaHandle, __out ULONG * packageId)
{
  NTSTATUS lookupStatus; 
  LSA_STRING pkgName;

  AssignLsaString(&pkgName, MICROSOFT_KERBEROS_NAME_A);
  lookupStatus = LsaLookupAuthenticationPackage(lsaHandle, &pkgName, packageId);
  return LsaNtStatusToWinError( lookupStatus );
}
  
//----------------------------------------------------------------------------
// Function: CreateLogonTokenForUser
//
// Description:
//  Contacts the local LSA and performs a logon without credential for the 
//  given principal. This logon token will be local machine only and have no 
//  network credentials attached.
//
// Returns:
//  ERROR_SUCCESS on success
//  Other error code on failure
//
// Notes:
//  This call assumes that all required privileges have already been enabled (TCB etc).
//  IMPORTANT ****  tokenOriginName must be immutable!
//
DWORD CreateLogonTokenForUser(__in HANDLE lsaHandle,
                         __in const char * tokenSourceName, 
                         __in const char * tokenOriginName, // must be immutable, will not be copied!
                         __in ULONG authnPkgId, 
                         __in const wchar_t* principalName, 
                         __out HANDLE *tokenHandle) 
{ 
  DWORD logonStatus = ERROR_ASSERTION_FAILURE; // Failure to set status should trigger error
  TOKEN_SOURCE tokenSource;
  LSA_STRING originName;
  void * profile = NULL;

  // from MSDN:
  // The ClientUpn and ClientRealm members of the KERB_S4U_LOGON 
  // structure must point to buffers in memory that are contiguous 
  // to the structure itself. The value of the 
  // AuthenticationInformationLength parameter must take into 
  // account the length of these buffers.
  const int principalNameBufLen = lstrlen(principalName)*sizeof(*principalName);
  const int totalAuthInfoLen = sizeof(KERB_S4U_LOGON) + principalNameBufLen;
  KERB_S4U_LOGON* s4uLogonAuthInfo = (KERB_S4U_LOGON*)calloc(totalAuthInfoLen, 1);
  if (s4uLogonAuthInfo == NULL ) {
    logonStatus = ERROR_NOT_ENOUGH_MEMORY;
    goto done;
  }
  s4uLogonAuthInfo->MessageType = KerbS4ULogon;
  s4uLogonAuthInfo->ClientUpn.Buffer = (wchar_t*)((char*)s4uLogonAuthInfo + sizeof *s4uLogonAuthInfo);
  CopyMemory(s4uLogonAuthInfo->ClientUpn.Buffer, principalName, principalNameBufLen);
  s4uLogonAuthInfo->ClientUpn.Length        = (USHORT)principalNameBufLen;
  s4uLogonAuthInfo->ClientUpn.MaximumLength = (USHORT)principalNameBufLen;

  AllocateLocallyUniqueId(&tokenSource.SourceIdentifier);
  StringCchCopyA(tokenSource.SourceName, TOKEN_SOURCE_LENGTH, tokenSourceName );
  AssignLsaString(&originName, tokenOriginName);

  {
    DWORD cbProfile = 0;
    LUID logonId;
    QUOTA_LIMITS quotaLimits;
    NTSTATUS subStatus;

    NTSTATUS logonNtStatus = LsaLogonUser(lsaHandle,
      &originName,
      Batch, // SECURITY_LOGON_TYPE
      authnPkgId,
      s4uLogonAuthInfo, 
      totalAuthInfoLen,
      0,
      &tokenSource,
      &profile, 
      &cbProfile,
      &logonId, 
      tokenHandle,
      &quotaLimits,
      &subStatus);
    logonStatus = LsaNtStatusToWinError( logonNtStatus );
  }
done:
  // clean up
  if (s4uLogonAuthInfo != NULL) {
    free(s4uLogonAuthInfo);
  }
  if (profile != NULL) {
    LsaFreeReturnBuffer(profile);
  }
  return logonStatus;
}

// NOTE: must free allocatedName
DWORD GetNameFromLogonToken(__in HANDLE logonToken, __out wchar_t **allocatedName)
{
  DWORD userInfoSize = 0;
  PTOKEN_USER  user = NULL;
  DWORD userNameSize = 0;
  wchar_t * userName = NULL;
  DWORD domainNameSize = 0; 
  wchar_t * domainName = NULL;
  SID_NAME_USE sidUse = SidTypeUnknown;
  DWORD getNameStatus = ERROR_ASSERTION_FAILURE; // Failure to set status should trigger error
  BOOL tokenInformation = FALSE;

  // call for sid size then alloc and call for sid
  tokenInformation = GetTokenInformation(logonToken, TokenUser, NULL, 0, &userInfoSize);
  assert (FALSE == tokenInformation);
  
  // last call should have failed and filled in allocation size
  if ((getNameStatus = GetLastError()) != ERROR_INSUFFICIENT_BUFFER)
  {
    goto done; 
  }
  user = (PTOKEN_USER)calloc(userInfoSize,1);
  if (user == NULL)
  {
    getNameStatus = ERROR_NOT_ENOUGH_MEMORY;
    goto done;
  }
  if (!GetTokenInformation(logonToken, TokenUser, user, userInfoSize, &userInfoSize)) {
      getNameStatus = GetLastError();
      goto done;
  }
  LookupAccountSid( NULL, user->User.Sid, NULL, &userNameSize, NULL, &domainNameSize, &sidUse );
  // last call should have failed and filled in allocation size
  if ((getNameStatus = GetLastError()) != ERROR_INSUFFICIENT_BUFFER)
  {
    goto done;
  }
  userName = (wchar_t *)calloc(userNameSize, sizeof(wchar_t));
  if (userName == NULL) {
    getNameStatus = ERROR_NOT_ENOUGH_MEMORY;
    goto done;
  }
  domainName = (wchar_t *)calloc(domainNameSize, sizeof(wchar_t));
  if (domainName == NULL) {
    getNameStatus = ERROR_NOT_ENOUGH_MEMORY;
    goto done;
  }
  if (!LookupAccountSid( NULL, user->User.Sid, userName, &userNameSize, domainName, &domainNameSize, &sidUse )) {
      getNameStatus = GetLastError();
      goto done;
  }

  getNameStatus = ERROR_SUCCESS;
  *allocatedName = userName;
  userName = NULL;
done:
  if (user != NULL) {
    free( user );
    user = NULL;
  }
  if (userName != NULL) {
    free( userName );
    userName = NULL;
  }
  if (domainName != NULL) {
    free( domainName );
    domainName = NULL;
  }
  return getNameStatus;
}

DWORD LoadUserProfileForLogon(__in HANDLE logonHandle, __out PROFILEINFO * pi)
{
  wchar_t *userName = NULL;
  DWORD loadProfileStatus = ERROR_ASSERTION_FAILURE; // Failure to set status should trigger error

  loadProfileStatus = GetNameFromLogonToken( logonHandle, &userName );
  if (loadProfileStatus != ERROR_SUCCESS) {
    goto done;
  }

  assert(pi);

  ZeroMemory( pi, sizeof(*pi) );
  pi->dwSize = sizeof(*pi); 
  pi->lpUserName = userName;
  pi->dwFlags = PI_NOUI;

  // if the profile does not exist it will be created
  if ( !LoadUserProfile( logonHandle, pi ) ) {      
    loadProfileStatus = GetLastError();
    goto done;
  }

  loadProfileStatus = ERROR_SUCCESS;
done:
  return loadProfileStatus;
}



DWORD UnloadProfileForLogon(__in HANDLE logonHandle, __in PROFILEINFO * pi)
{
  DWORD touchProfileStatus = ERROR_ASSERTION_FAILURE; // Failure to set status should trigger error

  assert(pi);

  if ( !UnloadUserProfile(logonHandle, pi->hProfile ) ) {
    touchProfileStatus = GetLastError();
    goto done;
  }
  if (pi->lpUserName != NULL) {
    free(pi->lpUserName);
    pi->lpUserName = NULL;
  }
  ZeroMemory( pi, sizeof(*pi) );

  touchProfileStatus = ERROR_SUCCESS;
done:
  return touchProfileStatus;
}


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
DWORD ChangeFileOwnerBySid(__in LPCWSTR path,
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
  dwRtnCode = FindFileOwnerAndPermission(longPathName, FALSE, NULL, NULL, &oldMode);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    goto ChangeFileOwnerByNameEnd;
  }

  // We need SeTakeOwnershipPrivilege to set the owner if the caller does not
  // have WRITE_OWNER access to the object; we need SeRestorePrivilege if the
  // SID is not contained in the caller's token, and have the SE_GROUP_OWNER
  // permission enabled.
  //
  if (EnablePrivilege(L"SeTakeOwnershipPrivilege") != ERROR_SUCCESS)
  {
    fwprintf(stdout, L"INFO: The user does not have SeTakeOwnershipPrivilege.\n");
  }
  if (EnablePrivilege(L"SeRestorePrivilege") != ERROR_SUCCESS)
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


//-----------------------------------------------------------------------------
// Function: GetSecureJobObjectName
//
// Description:
//  Creates a job object name usable in a secure environment: adds the Golbal\
//

DWORD GetSecureJobObjectName(
  __in LPCWSTR      jobName,
  __in size_t       cchSecureJobName,
  __out_ecount(cchSecureJobName) LPWSTR secureJobName) {

  HRESULT hr = StringCchPrintf(secureJobName, cchSecureJobName,
    L"Global\\%s", jobName);

  if (FAILED(hr)) {
    return HRESULT_CODE(hr);
  }

  return ERROR_SUCCESS;
}

//-----------------------------------------------------------------------------
// Function: EnableImpersonatePrivileges
//
// Description:
//  Enables the required privileges for S4U impersonation
//
// Returns:
// ERROR_SUCCESS: On success
//
DWORD EnableImpersonatePrivileges() {
  DWORD dwError = ERROR_SUCCESS;
  LPCWSTR privilege = NULL;
  int crt = 0;

  LPCWSTR privileges[] = {
    SE_IMPERSONATE_NAME,
    SE_TCB_NAME,
    SE_ASSIGNPRIMARYTOKEN_NAME,
    SE_INCREASE_QUOTA_NAME,
    SE_RESTORE_NAME,
    SE_DEBUG_NAME,
    SE_SECURITY_NAME,
    };

  for (crt = 0; crt < sizeof(privileges)/sizeof(LPCWSTR); ++crt) {
    LPCWSTR privilege = privileges[crt];
    dwError = EnablePrivilege(privilege);
    if( dwError != ERROR_SUCCESS ) {
      LogDebugMessage(L"Failed to enable privilege: %s\n", privilege);
      ReportErrorCode(L"EnablePrivilege", dwError);
      goto done;
    }    
  }

done:
  return dwError;
}


//-----------------------------------------------------------------------------
// Function: KillTask
//
// Description:
//  Kills a task via a jobobject. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// ERROR_SUCCESS: On success
// GetLastError: otherwise
DWORD KillTask(PCWSTR jobObjName)
{
  DWORD dwError = ERROR_SUCCESS;
  
  HANDLE jobObject = OpenJobObject(JOB_OBJECT_TERMINATE, FALSE, jobObjName);
  if(jobObject == NULL)
  {
    dwError = GetLastError();
    if(dwError == ERROR_FILE_NOT_FOUND)
    {      
      // job object does not exist. assume its not alive
      dwError = ERROR_SUCCESS;
    }
    goto done;
  }

  if(TerminateJobObject(jobObject, KILLED_PROCESS_EXIT_CODE) == 0)
  {
    dwError = GetLastError();
  }

done:
  CloseHandle(jobObject);
  
  return dwError;
}

DWORD ChownImpl(
  __in_opt LPCWSTR userName,
  __in_opt LPCWSTR groupName,
  __in LPCWSTR pathName) {

  DWORD dwError;

  PSID pNewOwnerSid = NULL;
  PSID pNewGroupSid = NULL;

  if (userName != NULL)
  {
    dwError = GetSidFromAcctNameW(userName, &pNewOwnerSid);
    if (dwError != ERROR_SUCCESS)
    {
      ReportErrorCode(L"GetSidFromAcctName", dwError);
      fwprintf(stderr, L"Invalid user name: %s\n", userName);
      goto done;
    }
  }

  if (groupName != NULL)
  {
    dwError = GetSidFromAcctNameW(groupName, &pNewGroupSid);
    if (dwError != ERROR_SUCCESS)
    {
      ReportErrorCode(L"GetSidFromAcctName", dwError);
      fwprintf(stderr, L"Invalid group name: %s\n", groupName);
      goto done;
    }
  }

  if (wcslen(pathName) == 0 || wcsspn(pathName, L"/?|><:*\"") != 0)
  {
    fwprintf(stderr, L"Incorrect file name format: %s\n", pathName);
    goto done;
  }

  dwError = ChangeFileOwnerBySid(pathName, pNewOwnerSid, pNewGroupSid);
  if (dwError != ERROR_SUCCESS)
  {
    ReportErrorCode(L"ChangeFileOwnerBySid", dwError);
    goto done;
  }
done:
  LocalFree(pNewOwnerSid);
  LocalFree(pNewGroupSid);

  return dwError;
}



LPCWSTR GetSystemTimeString() {
  __declspec(thread) static WCHAR buffer[1024];
  DWORD dwError;
  FILETIME ftime;
  SYSTEMTIME systime;
  LARGE_INTEGER counter, frequency;
  int subSec;
  double qpc;
  HRESULT hr;
  buffer[0] = L'\0';

  // GetSystemTimePreciseAsFileTime is only available in Win8+ and our libs do not link against it

  GetSystemTimeAsFileTime(&ftime);

  if (!FileTimeToSystemTime(&ftime, &systime)) {
    dwError = GetLastError();
    LogDebugMessage(L"FileTimeToSystemTime error:%d\n", dwError);
    goto done;
  }

  // Get the ms from QPC. GetSystemTimeAdjustment is ignored...
  
  QueryPerformanceCounter(&counter);
  QueryPerformanceFrequency(&frequency);

  qpc = (double) counter.QuadPart / (double) frequency.QuadPart;
  subSec = (int)((qpc - (long)qpc) * 1000000);

  hr = StringCbPrintf(buffer, sizeof(buffer), L"%02d:%02d:%02d.%06d", 
    (int)systime.wHour, (int)systime.wMinute, (int)systime.wSecond, (int)subSec);

  if (FAILED(hr)) {
    LogDebugMessage(L"StringCbPrintf error:%d\n", hr);
  }
done:
  return buffer;
}


//----------------------------------------------------------------------------
// Function: LogDebugMessage
//
// Description:
//  Sends a message to the debugger console, if one is attached
//
// Notes:
//  Native debugger: windbg, ntsd, cdb, visual studio
//
VOID LogDebugMessage(LPCWSTR format, ...) {
  wchar_t buffer[8192];
  va_list args;
  HRESULT hr;

  if (!IsDebuggerPresent()) return;

  va_start(args, format);
  hr = StringCbVPrintf(buffer, sizeof(buffer), format, args);
  if (SUCCEEDED(hr)) {
    OutputDebugString(buffer);
  }
  va_end(args);
}

//----------------------------------------------------------------------------
// Function: SplitStringIgnoreSpaceW
//
// Description:
//  splits a null-terminated string based on a delimiter
//
// Returns:
//  ERROR_SUCCESS: on success
//  error code: otherwise
//
// Notes:
//  The tokes are also null-terminated
//  Caller should use LocalFree to clear outTokens
//
DWORD SplitStringIgnoreSpaceW(
  __in size_t len, 
  __in_ecount(len) LPCWSTR source, 
  __in WCHAR deli, 
  __out size_t* count, 
  __out_ecount(count) WCHAR*** outTokens) {
  
  size_t tokenCount = 0;
  size_t crtSource;
  size_t crtToken = 0;
  const WCHAR* lpwszTokenStart = NULL;
  const WCHAR* lpwszTokenEnd = NULL;
  WCHAR* lpwszBuffer = NULL;
  size_t tokenLength = 0;
  size_t cchBufferLength = 0;
  WCHAR crt;
  WCHAR** tokens = NULL;
  enum {BLANK, TOKEN, DELIMITER} State = BLANK;

  for(crtSource = 0; crtSource < len; ++crtSource) {
    crt = source[crtSource];
    switch(State) {
    case BLANK: // intentional fallthrough
    case DELIMITER:
      if (crt == deli) {
        State = DELIMITER;
      } 
      else if (!iswspace(crt)) {
        ++tokenCount;
        lpwszTokenEnd = lpwszTokenStart = source + crtSource;
        State = TOKEN;
      }
      else {
        State = BLANK;
      }
      break;
    case TOKEN:
      if (crt == deli) {
        State = DELIMITER;
        cchBufferLength += lpwszTokenEnd - lpwszTokenStart + 2;
      }
      else if (!iswspace(crt)) {
        lpwszTokenEnd = source + crtSource;
      }
      break;
    }
  }

  if (State == TOKEN) {
    cchBufferLength += lpwszTokenEnd - lpwszTokenStart + 2;
  }

  LogDebugMessage(L"counted %d [buffer:%d] tokens in %s\n", tokenCount, cchBufferLength, source);

  #define COPY_CURRENT_TOKEN                                              \
    tokenLength = lpwszTokenEnd - lpwszTokenStart + 1;                    \
    tokens[crtToken] = lpwszBuffer;                                       \
    memcpy(tokens[crtToken], lpwszTokenStart, tokenLength*sizeof(WCHAR)); \
    tokens[crtToken][tokenLength] = L'\0';                                \
    lpwszBuffer += (tokenLength+1);                                       \
    ++crtToken;

  if (tokenCount) {

    // We use one contigous memory for both the pointer arrays and the data copy buffers
    // We cannot use in-place references (zero-copy) because the function users 
    // need null-terminated strings for the tokens
    
    tokens = (WCHAR**) LocalAlloc(LPTR, 
       sizeof(WCHAR*) * tokenCount +      // for the pointers
       sizeof(WCHAR) * cchBufferLength);  // for the data

    // Data will be copied after the array
    lpwszBuffer = (WCHAR*)(((BYTE*)tokens) + (sizeof(WCHAR*) * tokenCount));
       
    State = BLANK;

    for(crtSource = 0; crtSource < len; ++crtSource) {
      crt = source[crtSource];
      switch(State) {
      case DELIMITER: // intentional fallthrough
      case BLANK:
        if (crt == deli) {
          State = DELIMITER;
        } 
        else if (!iswspace(crt)) {
          lpwszTokenEnd = lpwszTokenStart = source + crtSource;
          State = TOKEN;
        }
        else {
          State = BLANK;
        }
        break;
      case TOKEN:
        if (crt == deli) {
          COPY_CURRENT_TOKEN;
          State = DELIMITER;
        }
        else if (!iswspace(crt)) {
          lpwszTokenEnd = source + crtSource;
        }
        break;
      }
    }

    // Copy out last token, if any
    if (TOKEN == State) {
      COPY_CURRENT_TOKEN;
    }
  }

  *count = tokenCount;
  *outTokens = tokens;

  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: BuildServiceSecurityDescriptor
//
// Description:
//  Builds a security descriptor for an arbitrary object
//
// Returns:
//  ERROR_SUCCESS: on success
//  error code: otherwise
//
// Notes:
//  The SD is a of the self-contained flavor (offsets, not pointers)
//  Caller should use LocalFree to clear allocated pSD
//
DWORD BuildServiceSecurityDescriptor(
  __in ACCESS_MASK                    accessMask,
  __in size_t                         grantSidCount,
  __in_ecount(grantSidCount) PSID*    pGrantSids,
  __in size_t                         denySidCount,
  __in_ecount(denySidCount) PSID*     pDenySids,
  __in_opt PSID                       pOwner,
  __out PSECURITY_DESCRIPTOR*         pSD) {

  DWORD                 dwError = ERROR_SUCCESS;
  unsigned int          crt  = 0;
  int                   len = 0;
  EXPLICIT_ACCESS*      eas = NULL;
  LPWSTR                lpszSD = NULL;
  ULONG                 cchSD = 0;
  HANDLE                hToken = INVALID_HANDLE_VALUE;
  DWORD                 dwBufferSize = 0;
  PTOKEN_USER           pTokenUser = NULL;
  PTOKEN_PRIMARY_GROUP  pTokenGroup = NULL;
  PSECURITY_DESCRIPTOR  pTempSD = NULL;
  ULONG                 cbSD = 0;
  TRUSTEE               owner, group;

  ZeroMemory(&owner, sizeof(owner));

  // We'll need our own SID to add as SD owner
  if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &hToken)) {
    dwError = GetLastError();
    LogDebugMessage(L"OpenProcessToken: %d\n", dwError);
    goto done;  
  }

  if (NULL == pOwner) {
    if (!GetTokenInformation(hToken, TokenUser, NULL, 0, &dwBufferSize)) {
      dwError = GetLastError();
      if (ERROR_INSUFFICIENT_BUFFER != dwError) {
        LogDebugMessage(L"GetTokenInformation: %d\n", dwError);
        goto done;
      }
    }

    pTokenUser = (PTOKEN_USER) LocalAlloc(LPTR, dwBufferSize);
    if (NULL == pTokenUser) {
      dwError = GetLastError();
      LogDebugMessage(L"LocalAlloc:pTokenUser: %d\n", dwError);
      goto done; 
    }

    if (!GetTokenInformation(hToken, TokenUser, pTokenUser, dwBufferSize, &dwBufferSize)) {
      dwError = GetLastError();
      LogDebugMessage(L"GetTokenInformation: %d\n", dwError);
      goto done; 
    }

    if (!IsValidSid(pTokenUser->User.Sid)) {
      dwError = ERROR_INVALID_PARAMETER;
      LogDebugMessage(L"IsValidSid: %d\n", dwError);
      goto done;
    }
    pOwner = pTokenUser->User.Sid;
  }

  dwBufferSize = 0;
  if (!GetTokenInformation(hToken, TokenPrimaryGroup, NULL, 0, &dwBufferSize)) {
    dwError = GetLastError();
    if (ERROR_INSUFFICIENT_BUFFER != dwError) {
      LogDebugMessage(L"GetTokenInformation: %d\n", dwError);
      goto done;
    }
  }

  pTokenGroup = (PTOKEN_PRIMARY_GROUP) LocalAlloc(LPTR, dwBufferSize);
  if (NULL == pTokenGroup) {
    dwError = GetLastError();
    LogDebugMessage(L"LocalAlloc:pTokenGroup: %d\n", dwError);
    goto done; 
  }

  if (!GetTokenInformation(hToken, TokenPrimaryGroup, pTokenGroup, dwBufferSize, &dwBufferSize)) {
    dwError = GetLastError();
    LogDebugMessage(L"GetTokenInformation: %d\n", dwError);
    goto done; 
  }

  if (!IsValidSid(pTokenGroup->PrimaryGroup)) {
    dwError = ERROR_INVALID_PARAMETER;
    LogDebugMessage(L"IsValidSid: %d\n", dwError);
    goto done;
  }  

  owner.TrusteeForm = TRUSTEE_IS_SID;
  owner.TrusteeType = TRUSTEE_IS_UNKNOWN;
  owner.ptstrName = (LPWSTR) pOwner;

  group.TrusteeForm = TRUSTEE_IS_SID;
  group.TrusteeType = TRUSTEE_IS_UNKNOWN;
  group.ptstrName = (LPWSTR) pTokenGroup->PrimaryGroup;

  eas = (EXPLICIT_ACCESS*) LocalAlloc(LPTR, sizeof(EXPLICIT_ACCESS) * (grantSidCount + denySidCount));
  if (NULL == eas) {
    dwError = ERROR_OUTOFMEMORY;
    LogDebugMessage(L"LocalAlloc: %d\n", dwError);
    goto done;
  }

  // Build the granted list
  for (crt = 0; crt < grantSidCount; ++crt) {
    eas[crt].grfAccessPermissions = accessMask;
    eas[crt].grfAccessMode = GRANT_ACCESS;
    eas[crt].grfInheritance = NO_INHERITANCE;
    eas[crt].Trustee.TrusteeForm = TRUSTEE_IS_SID;
    eas[crt].Trustee.TrusteeType = TRUSTEE_IS_UNKNOWN;
    eas[crt].Trustee.ptstrName = (LPWSTR) pGrantSids[crt];
    eas[crt].Trustee.pMultipleTrustee = NULL;
    eas[crt].Trustee.MultipleTrusteeOperation = NO_MULTIPLE_TRUSTEE;
  }

  // Build the deny list
  for (; crt < grantSidCount + denySidCount; ++crt) {
    eas[crt].grfAccessPermissions = accessMask;
    eas[crt].grfAccessMode = DENY_ACCESS;
    eas[crt].grfInheritance = NO_INHERITANCE;
    eas[crt].Trustee.TrusteeForm = TRUSTEE_IS_SID;
    eas[crt].Trustee.TrusteeType = TRUSTEE_IS_UNKNOWN;
    eas[crt].Trustee.ptstrName = (LPWSTR) pDenySids[crt - grantSidCount];
    eas[crt].Trustee.pMultipleTrustee = NULL;
    eas[crt].Trustee.MultipleTrusteeOperation = NO_MULTIPLE_TRUSTEE;
  }

  dwError = BuildSecurityDescriptor(
    &owner,
    &group,
    crt,
    eas,
    0,    // cCountOfAuditEntries
    NULL, // pListOfAuditEntries
    NULL, // pOldSD
    &cbSD, 
    &pTempSD);
  if (ERROR_SUCCESS != dwError) {
    LogDebugMessage(L"BuildSecurityDescriptor: %d\n", dwError);
    goto done;
  }
  
  *pSD = pTempSD;
  pTempSD = NULL;

  if (IsDebuggerPresent()) {
    ConvertSecurityDescriptorToStringSecurityDescriptor(*pSD, 
      SDDL_REVISION_1,
      DACL_SECURITY_INFORMATION,
      &lpszSD,
      &cchSD);
    LogDebugMessage(L"pSD: %.*s\n", cchSD, lpszSD);
  }
  
done:
  if (eas) LocalFree(eas);
  if (pTokenUser) LocalFree(pTokenUser);
  if (INVALID_HANDLE_VALUE != hToken) CloseHandle(hToken);
  if (lpszSD) LocalFree(lpszSD);
  if (pTempSD) LocalFree(pTempSD);
  return dwError;
}

//----------------------------------------------------------------------------
// Function: MIDL_user_allocate
//
// Description:
//  Hard-coded function name used by RPC midl code for allocations
//
// Notes:
//  Must match the de-allocation mechanism used in MIDL_user_free
//
void __RPC_FAR * __RPC_USER MIDL_user_allocate(size_t len)
{
    return LocalAlloc(LPTR, len);
}
 
 //----------------------------------------------------------------------------
 // Function: MIDL_user_free
 //
 // Description:
 //  Hard-coded function name used by RPC midl code for deallocations
 //
 // NoteS:
 //  Must match the allocation mechanism used in MIDL_user_allocate
 //
void __RPC_USER MIDL_user_free(void __RPC_FAR * ptr)
{
    LocalFree(ptr);
}

