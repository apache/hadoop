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
// The Windows SDK does not include the definition of REPARSE_DATA_BUFFER. To
// avoid adding a dependency on the WDK we define the structure here.
// Reference: http://msdn.microsoft.com/en-us/library/ff552012.aspx
//
#pragma warning(push)
#pragma warning(disable: 4201)  // nonstandard extension: nameless struct/union
#pragma pack(push, 1)
typedef struct _REPARSE_DATA_BUFFER {
  ULONG  ReparseTag;
  USHORT ReparseDataLength;
  USHORT Reserved;
  union {
    struct {
      USHORT SubstituteNameOffset;
      USHORT SubstituteNameLength;
      USHORT PrintNameOffset;
      USHORT PrintNameLength;
      ULONG  Flags;
      WCHAR  PathBuffer[1];
    } SymbolicLinkReparseBuffer;
    struct {
      USHORT SubstituteNameOffset;
      USHORT SubstituteNameLength;
      USHORT PrintNameOffset;
      USHORT PrintNameLength;
      WCHAR  PathBuffer[1];
    } MountPointReparseBuffer;
    struct {
      UCHAR DataBuffer[1];
    } GenericReparseBuffer;
  };
} REPARSE_DATA_BUFFER, *PREPARSE_DATA_BUFFER;
#pragma pack(pop)
#pragma warning(pop)


//----------------------------------------------------------------------------
// Function: Readlink
//
// Description:
//  Prints the target of a symbolic link to stdout.
//
//  The return codes and output are modeled after the UNIX readlink command. 
//  Hence no error messages are printed. Unlike the UNIX readlink, no options
//  are accepted.
//
// Returns:
//  0: on success
//  1: on all errors
//
// Notes:
//
int Readlink(__in int argc, __in_ecount(argc) wchar_t *argv[])
{
  DWORD bytesReturned;
  DWORD bufferSize = 1024;                  // Start off with a 1KB buffer.
  HANDLE hFile = INVALID_HANDLE_VALUE;
  PWSTR longLinkName = NULL;
  PWCHAR printName = NULL;
  PREPARSE_DATA_BUFFER pReparseData = NULL;
  USHORT printNameLength;
  USHORT printNameOffset;
  DWORD result;
  BOOLEAN succeeded = FALSE;
  
  if (argc != 2)
  {
    ReadlinkUsage();
    goto Cleanup;
  }

  if (ConvertToLongPath(argv[1], &longLinkName) != ERROR_SUCCESS)
  {
    goto Cleanup;
  }

  // Get a handle to the link to issue the FSCTL.
  // FILE_FLAG_BACKUP_SEMANTICS is needed to open directories.
  // FILE_FLAG_OPEN_REPARSE_POINT disables normal reparse point processing
  // so we can query the symlink.
  //
  hFile = CreateFileW(longLinkName,
                      0,        // no rights needed to issue the FSCTL.
                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                      NULL,
                      OPEN_EXISTING,
                      FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                      NULL);

  if (hFile == INVALID_HANDLE_VALUE) 
  {
    goto Cleanup;
  }

  for (;;)
  {
    pReparseData = (PREPARSE_DATA_BUFFER) LocalAlloc(LMEM_FIXED, bufferSize);

    if (pReparseData == NULL) 
    {
      goto Cleanup;
    }

    // Issue the FSCTL to query the link information.
    //
    result = DeviceIoControl(hFile,
                             FSCTL_GET_REPARSE_POINT,
                             NULL,
                             0,
                             pReparseData,
                             bufferSize,
                             &bytesReturned,
                             NULL);

    if (result != 0)
    {
      // Success!
      //
      break;
    }
    else if ((GetLastError() == ERROR_INSUFFICIENT_BUFFER) ||
             (GetLastError() == ERROR_MORE_DATA))
    {
      // Retry with a larger buffer.
      //
      LocalFree(pReparseData);
      bufferSize *= 2;
    }
    else
    {
      // Unrecoverable error.
      //
      goto Cleanup;
    }
  }

  if (pReparseData->ReparseTag != IO_REPARSE_TAG_SYMLINK) 
  {
    // Doesn't look like a symlink.
    //
    goto Cleanup;
  }

  // MSDN does not guarantee that the embedded paths in REPARSE_DATA_BUFFER
  // will be NULL terminated. So we copy the string to a separate buffer and
  // NULL terminate it before printing.
  //
  printNameLength = pReparseData->SymbolicLinkReparseBuffer.PrintNameLength;
  printNameOffset = pReparseData->SymbolicLinkReparseBuffer.PrintNameOffset;
  printName = (PWCHAR) LocalAlloc(LMEM_FIXED, printNameLength + 1);

  if (printName == NULL) 
  {
    goto Cleanup;
  }

  memcpy(
      printName,
      pReparseData->SymbolicLinkReparseBuffer.PathBuffer + printNameOffset,
      printNameLength);

  printName[printNameLength / sizeof(WCHAR)] = L'\0';

  fwprintf(stdout, L"%ls", printName);
  succeeded = TRUE;

Cleanup:
  if (hFile != INVALID_HANDLE_VALUE) 
  {
    CloseHandle(hFile);
  }

  if (printName != NULL) 
  {
    LocalFree(printName);
  }

  if (pReparseData != NULL)
  {
    LocalFree(pReparseData);
  }

  if (longLinkName != NULL)
  {
    LocalFree(longLinkName);
  }

  return (succeeded ? EXIT_SUCCESS : EXIT_FAILURE);
}

void ReadlinkUsage()
{
    fwprintf(stdout, L"\
Usage: readlink [LINKNAME]\n\
Prints the target of a symbolic link\n\
The output and returned error codes are similar to the UNIX\n\
readlink command. However no options are accepted.\n\
\n\
0 is returned on success.\n\
1 is returned for all errors.\n\
\n");
}

