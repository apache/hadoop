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

#include "common.h"

//----------------------------------------------------------------------------
// Function: GetMaskString
//
// Description:
//	Get the mask string that are used for output to the console.
//
// Returns:
//	TRUE: on success
//
// Notes:
//
static BOOL GetMaskString(USHORT accessMask, LPWSTR maskString)
{
  if(wcslen(maskString) != 10)
    return FALSE;

  if ((accessMask & UX_DIRECTORY) == UX_DIRECTORY)
    maskString[0] = L'd';

  if ((accessMask & UX_U_READ) == UX_U_READ)
    maskString[1] = L'r';
  else
    maskString[1] = L'-';
  if ((accessMask & UX_U_WRITE) == UX_U_WRITE)
    maskString[2] = L'w';
  else
    maskString[2] = L'-';
  if ((accessMask & UX_U_EXECUTE) == UX_U_EXECUTE)
    maskString[3] = L'x';
  else
    maskString[3] = L'-';

  if ((accessMask & UX_G_READ) == UX_G_READ)
    maskString[4] = L'r';
  else
    maskString[4] = L'-';
  if ((accessMask & UX_G_WRITE) == UX_G_WRITE)
    maskString[5] = L'w';
  else
    maskString[5] = L'-';
  if ((accessMask & UX_G_EXECUTE) == UX_G_EXECUTE)
    maskString[6] = L'x';
  else
    maskString[6] = L'-';

  if ((accessMask & UX_O_READ) == UX_O_READ)
    maskString[7] = L'r';
  else
    maskString[7] = L'-';
  if ((accessMask & UX_O_WRITE) == UX_O_WRITE)
    maskString[8] = L'w';
  else
    maskString[8] = L'-';
  if ((accessMask & UX_O_EXECUTE) == UX_O_EXECUTE)
    maskString[9] = L'x';
  else
    maskString[9] = L'-';

  return TRUE;
}

//----------------------------------------------------------------------------
// Function: LsPrintLine
//
// Description:
//	Print one line of 'ls' command given all the information needed
//
// Returns:
//	None
//
// Notes:
//
static BOOL LsPrintLine(
  const USHORT mask,
  const DWORD hardlinkCount,
  LPCWSTR ownerName,
  LPCWSTR groupName,
  const FILETIME *lpFileWritetime,
  const LARGE_INTEGER fileSize,
  LPCWSTR path)
{
  // 'd' + 'rwx' for user, group, other
  static const size_t ck_ullMaskLen = 1 + 3 * 3;

  LPWSTR maskString = NULL;
  SYSTEMTIME stFileWriteTime;
  BOOL ret = FALSE;

  maskString = (LPWSTR)LocalAlloc(LPTR, (ck_ullMaskLen+1)*sizeof(WCHAR));
  if (maskString == NULL)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    return FALSE;
  }

  // Build mask string from ushort mask
  if (FAILED(StringCchCopyW(maskString, (ck_ullMaskLen+1), L"----------")))
  {
    goto LsPrintLineEnd;
  }

  if (!GetMaskString(mask, maskString))
  {
    goto LsPrintLineEnd;
  }

  // Convert file time to system time
  if (!FileTimeToSystemTime(lpFileWritetime, &stFileWriteTime))
  {
    goto LsPrintLineEnd;
  }

  fwprintf(stdout, L"%10s %d %s %s %lld %3s %2d %4d %s\n",
    maskString, hardlinkCount, ownerName, groupName, fileSize.QuadPart,
    MONTHS[stFileWriteTime.wMonth], stFileWriteTime.wDay,
    stFileWriteTime.wYear, path);

  ret = TRUE;

LsPrintLineEnd:
  LocalFree(maskString);

  return ret;
}

//----------------------------------------------------------------------------
// Function: Ls
//
// Description:
//	The main method for ls command
//
// Returns:
//	0: on success
//
// Notes:
//
int Ls(int argc, wchar_t *argv[])
{
  LPWSTR pathName = NULL;
  LPWSTR longPathName = NULL;

  BY_HANDLE_FILE_INFORMATION fileInformation;

  LPWSTR ownerName = NULL;
  LPWSTR groupName = NULL;
  USHORT accessMask = 0x0000;
  DWORD dwErrorCode = ERROR_SUCCESS;

  LARGE_INTEGER fileSize;

  int ret = EXIT_FAILURE;

  if (argc > 2)
  {
    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    LsUsage(argv[0]);
    return EXIT_FAILURE;
  }

  if (argc == 2)
    pathName = argv[1];

  if (pathName == NULL || wcslen(pathName) == 0)
    pathName = L".";

  if (wcsspn(pathName, L"/?|><:*\"") != 0)
  {
    fwprintf(stderr, L"Incorrect file name format: %s\n", pathName);
    return EXIT_FAILURE;
  }

  // Convert the path the the long path
  //
  dwErrorCode = ConvertToLongPath(pathName, &longPathName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"ConvertToLongPath", dwErrorCode);
    goto LsEnd;
  }

  dwErrorCode = GetFileInformationByName(longPathName, &fileInformation);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"GetFileInformationByName", dwErrorCode);
    goto LsEnd;
  }

  if (IsDirFileInfo(&fileInformation))
  {
    accessMask |= UX_DIRECTORY;
  }

  if (!FindFileOwnerAndPermission(longPathName,
    &ownerName, &groupName, &accessMask))
    goto LsEnd;

  fileSize.HighPart = fileInformation.nFileSizeHigh;
  fileSize.LowPart = fileInformation.nFileSizeLow;

  // Print output using the input path name (not the long one)
  //
  if (!LsPrintLine(accessMask,
    fileInformation.nNumberOfLinks,
    ownerName, groupName,
    &fileInformation.ftLastWriteTime,
    fileSize,
    pathName))
    goto LsEnd;

  ret = EXIT_SUCCESS;

LsEnd:
  LocalFree(ownerName);
  LocalFree(groupName);
  LocalFree(longPathName);

  return ret;
}

void LsUsage(LPCWSTR program)
{
  fwprintf(stdout, L"\
Usage: %s [FILE]\n\
List information about the FILE (the current directory by default).\n\
Using long listing format and list directory entries instead of contents,\n\
and do not dereference symbolic links.\n\
Provide equivalent or similar function as 'ls -ld' on GNU/Linux.\n",
program);
}
