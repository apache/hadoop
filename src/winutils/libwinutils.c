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
#include "winutils.h"
#include <authz.h>
#include <sddl.h>

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
// Function: CheckFileAttributes
//
// Description:
//	Check if the given file has all the given attribute(s)
//
// Returns:
//	ERROR_SUCCESS on success
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
//	Check if the given file is a directory
//
// Returns:
//	ERROR_SUCCESS on success
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
//	Check if the given file is a reparse point
//
// Returns:
//	ERROR_SUCCESS on success
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
//	Check if the given file is a reparse point of the given tag.
//
// Returns:
//	ERROR_SUCCESS on success
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
//	Check if the given file is a symbolic link.
//
// Returns:
//	ERROR_SUCCESS on success
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
//	Check if the given file is a junction point.
//
// Returns:
//	ERROR_SUCCESS on success
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
//	To retrieve the SID for a user account
//
// Returns:
//	ERROR_SUCCESS: on success
//  Other error code: otherwise
//
// Notes:
//	Caller needs to destroy the memory of Sid by calling LocalFree()
//
DWORD GetSidFromAcctNameW(LPCWSTR acctName, PSID *ppSid)
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
//	Compute the 3 bit Unix mask for the owner, group, or, others
//
// Returns:
//	The 3 bit Unix mask in INT
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
//	Get Windows acces mask by AuthZ methods
//
// Returns:
//	ERROR_SUCCESS: on success
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
  *pAccessRights = (*(PACCESS_MASK)(AccessReply.GrantedAccessMask));
  return ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: GetEffectiveRightsForSid
//
// Description:
//	Get Windows acces mask by AuthZ methods
//
// Returns:
//	ERROR_SUCCESS: on success
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
  AUTHZ_RESOURCE_MANAGER_HANDLE hManager;
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
  if (!AuthzFreeContext(hAuthzClientContext))
  {
    ret = GetLastError();
    goto GetEffectiveRightsForSidEnd;
  }

GetEffectiveRightsForSidEnd:
  return ret;
}

//----------------------------------------------------------------------------
// Function: FindFileOwnerAndPermission
//
// Description:
//	Find the owner, primary group and permissions of a file object
//
// Returns:
//	ERROR_SUCCESS: on success
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
    NO_PROPAGATE_INHERIT_ACE,
    winUserAccessDenyMask, pOwnerSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    NO_PROPAGATE_INHERIT_ACE,
    winUserAccessAllowMask, pOwnerSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (winGroupAccessDenyMask &&
    !AddAccessDeniedAceEx(pNewDACL, ACL_REVISION,
    NO_PROPAGATE_INHERIT_ACE,
    winGroupAccessDenyMask, pGroupSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    NO_PROPAGATE_INHERIT_ACE,
    winGroupAccessAllowMask, pGroupSid))
  {
    ret = GetLastError();
    goto GetWindowsDACLsEnd;
  }
  if (!AddAccessAllowedAceEx(pNewDACL, ACL_REVISION,
    NO_PROPAGATE_INHERIT_ACE,
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
  if (!SetSecurityDescriptorDacl(pAbsSD, TRUE, pNewDACL, FALSE))
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
  if (!SetFileSecurity(longPathName, DACL_SECURITY_INFORMATION, pAbsSD))
  {
    ret = GetLastError();
    goto ChangeFileModeByMaskEnd;
  }

ChangeFileModeByMaskEnd:
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
// Function: GetAccntNameFromSid
//
// Description:
//	To retrieve an account name given the SID
//
// Returns:
//	ERROR_SUCCESS: on success
//  Other error code: otherwise
//
// Notes:
//	Caller needs to destroy the memory of account name by calling LocalFree()
//
DWORD GetAccntNameFromSid(PSID pSid, LPWSTR *ppAcctName)
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
//	Get an array of groups for the given user.
//
// Returns:
//	ERROR_SUCCESS on success
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
//	Check if the process has the given privilege. If yes, enable the privilege
//  to the process's access token.
//
// Returns:
//	TRUE: on success
//
// Notes:
//
BOOL EnablePrivilege(__in LPCWSTR privilegeName)
{
  HANDLE hToken = INVALID_HANDLE_VALUE;
  TOKEN_PRIVILEGES tp = { 0 };
  DWORD dwErrCode = ERROR_SUCCESS;

  if (!OpenProcessToken(GetCurrentProcess(),
    TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken))
  {
    ReportErrorCode(L"OpenProcessToken", GetLastError());
    return FALSE;
  }

  tp.PrivilegeCount = 1;
  if (!LookupPrivilegeValueW(NULL,
    privilegeName, &(tp.Privileges[0].Luid)))
  {
    ReportErrorCode(L"LookupPrivilegeValue", GetLastError());
    CloseHandle(hToken);
    return FALSE;
  }
  tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

  // As stated on MSDN, we need to use GetLastError() to check if
  // AdjustTokenPrivileges() adjusted all of the specified privileges.
  //
  AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL);
  dwErrCode = GetLastError();
  CloseHandle(hToken);

  return dwErrCode == ERROR_SUCCESS;
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
