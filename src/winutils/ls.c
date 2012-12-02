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
// Function: GetMaskString
//
// Description:
//	Get the mask string that are used for output to the console.
//
// Returns:
//	TRUE: on success
//
// Notes:
//  The function only sets the existed permission in the mask string. If the
//  permission does not exist, the corresponding character in mask string is not
//  altered. The caller need to initilize the mask string to be all '-' to get
//  the correct mask string.
//
static BOOL GetMaskString(INT accessMask, LPWSTR maskString)
{
  if(wcslen(maskString) != 10)
    return FALSE;

  if ((accessMask & UX_DIRECTORY) == UX_DIRECTORY)
    maskString[0] = L'd';
  else if ((accessMask & UX_SYMLINK) == UX_SYMLINK)
    maskString[0] = L'l';

  if ((accessMask & UX_U_READ) == UX_U_READ)
    maskString[1] = L'r';
  if ((accessMask & UX_U_WRITE) == UX_U_WRITE)
    maskString[2] = L'w';
  if ((accessMask & UX_U_EXECUTE) == UX_U_EXECUTE)
    maskString[3] = L'x';

  if ((accessMask & UX_G_READ) == UX_G_READ)
    maskString[4] = L'r';
  if ((accessMask & UX_G_WRITE) == UX_G_WRITE)
    maskString[5] = L'w';
  if ((accessMask & UX_G_EXECUTE) == UX_G_EXECUTE)
    maskString[6] = L'x';

  if ((accessMask & UX_O_READ) == UX_O_READ)
    maskString[7] = L'r';
  if ((accessMask & UX_O_WRITE) == UX_O_WRITE)
    maskString[8] = L'w';
  if ((accessMask & UX_O_EXECUTE) == UX_O_EXECUTE)
    maskString[9] = L'x';

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
//  if useSeparator is false, separates the output tokens with a space
//  character, otherwise, with a pipe character
//
static BOOL LsPrintLine(
  const INT mask,
  const DWORD hardlinkCount,
  LPCWSTR ownerName,
  LPCWSTR groupName,
  const FILETIME *lpFileWritetime,
  const LARGE_INTEGER fileSize,
  LPCWSTR path,
  BOOL useSeparator)
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

  // Build mask string from mask mode
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

  if (useSeparator)
  {
    fwprintf(stdout, L"%10s|%d|%s|%s|%lld|%3s|%2d|%4d|%s\n",
      maskString, hardlinkCount, ownerName, groupName, fileSize.QuadPart,
      MONTHS[stFileWriteTime.wMonth-1], stFileWriteTime.wDay,
      stFileWriteTime.wYear, path);
  }
  else
  {
    fwprintf(stdout, L"%10s %d %s %s %lld %3s %2d %4d %s\n",
      maskString, hardlinkCount, ownerName, groupName, fileSize.QuadPart,
      MONTHS[stFileWriteTime.wMonth-1], stFileWriteTime.wDay,
      stFileWriteTime.wYear, path);
  }

  ret = TRUE;

LsPrintLineEnd:
  LocalFree(maskString);

  return ret;
}

// List of command line options supported by "winutils ls"
enum CmdLineOption
{
  CmdLineOptionFollowSymlink = 0x1,  // "-L"
  CmdLineOptionSeparator = 0x2  // "-F"
  // options should be powers of 2 (aka next is 0x4)
};

static wchar_t* CurrentDir = L".";

//----------------------------------------------------------------------------
// Function: ParseCommandLine
//
// Description:
//   Parses the command line
//
// Returns:
//   TRUE on the valid command line, FALSE otherwise
//
BOOL ParseCommandLine(
  int argc, wchar_t *argv[], wchar_t** path, int *optionsMask)
{
  int MaxOptions = 2; // Should be equal to the number of elems in CmdLineOption
  int i = 0;

  assert(optionsMask != NULL);
  assert(argv != NULL);
  assert(path != NULL);

  *optionsMask = 0;

  if (argc == 1)
  {
    // no path specified, assume "."
    *path = CurrentDir;
    return TRUE;
  }

  if (argc == 2)
  {
    // only path specified, no other options
    *path = argv[1];
    return TRUE;
  }

  if (argc > 2 + MaxOptions)
  {
    // too many parameters
    return FALSE;
  }

  for (i = 1; i < argc - 1; ++i)
  {
    if (wcscmp(argv[i], L"-L") == 0)
    {
      // Check if this option was already specified
      BOOL alreadySet = *optionsMask & CmdLineOptionFollowSymlink;
      if (alreadySet)
        return FALSE;

      *optionsMask |= CmdLineOptionFollowSymlink;
    }
    else if (wcscmp(argv[i], L"-F") == 0)
    {
      // Check if this option was already specified
      BOOL alreadySet = *optionsMask & CmdLineOptionSeparator;
      if (alreadySet)
        return FALSE;

      *optionsMask |= CmdLineOptionSeparator;
    }
    else
    {
      return FALSE;
    }
  }

  *path = argv[argc - 1];

  return TRUE;
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
  INT unixAccessMode = 0;
  DWORD dwErrorCode = ERROR_SUCCESS;

  LARGE_INTEGER fileSize;

  BOOL isSymlink = FALSE;

  int ret = EXIT_FAILURE;
  int optionsMask = 0;

  if (!ParseCommandLine(argc, argv, &pathName, &optionsMask))
  {
    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    LsUsage(argv[0]);
    return EXIT_FAILURE;
  }

  assert(pathName != NULL);

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

  dwErrorCode = GetFileInformationByName(
    longPathName, optionsMask & CmdLineOptionFollowSymlink, &fileInformation);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"GetFileInformationByName", dwErrorCode);
    goto LsEnd;
  }

  dwErrorCode = SymbolicLinkCheck(longPathName, &isSymlink);
  if (dwErrorCode != ERROR_SUCCESS)
  {
     ReportErrorCode(L"IsSymbolicLink", dwErrorCode);
     goto LsEnd;
  }

  if (isSymlink)
    unixAccessMode |= UX_SYMLINK;
  else if (IsDirFileInfo(&fileInformation))
    unixAccessMode |= UX_DIRECTORY;

  dwErrorCode = FindFileOwnerAndPermission(longPathName,
    &ownerName, &groupName, &unixAccessMode);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"FindFileOwnerAndPermission", dwErrorCode);
    goto LsEnd;
  }

  fileSize.HighPart = fileInformation.nFileSizeHigh;
  fileSize.LowPart = fileInformation.nFileSizeLow;

  // Print output using the input path name (not the long one)
  //
  if (!LsPrintLine(unixAccessMode,
    fileInformation.nNumberOfLinks,
    ownerName, groupName,
    &fileInformation.ftLastWriteTime,
    fileSize,
    pathName,
    optionsMask & CmdLineOptionSeparator))
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
Usage: %s [OPTIONS] [FILE]\n\
List information about the FILE (the current directory by default).\n\
Using long listing format and list directory entries instead of contents,\n\
and do not dereference symbolic links.\n\
Provides equivalent or similar function as 'ls -ld' on GNU/Linux.\n\
\n\
OPTIONS: -L dereference symbolic links\n\
         -F format the output by separating tokens with a pipe\n",
program);
}
