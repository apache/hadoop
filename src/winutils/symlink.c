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
// Function: Symlink
//
// Description:
//	The main method for symlink command
//
// Returns:
//	0: on success
//
// Notes:
//
int Symlink(int argc, wchar_t *argv[])
{
  PWSTR longLinkName = NULL;
  PWSTR longFileName = NULL;
  DWORD dwErrorCode = ERROR_SUCCESS;

  BOOL isDir = FALSE;

  DWORD dwRtnCode = ERROR_SUCCESS;
  DWORD dwFlag = 0;

  int ret = SUCCESS;

  if (argc != 3)
  {
    SymlinkUsage();
    return FAILURE;
  }

  dwErrorCode = ConvertToLongPath(argv[1], &longLinkName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ret = FAILURE;
    goto SymlinkEnd;
  }
  dwErrorCode = ConvertToLongPath(argv[2], &longFileName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ret = FAILURE;
    goto SymlinkEnd;
  }

  // Check if the the process's access token has the privilege to create
  // symbolic links. Without this step, the call to CreateSymbolicLink() from
  // users have the privilege to create symbolic links will still succeed.
  // This is just an additional step to do the privilege check by not using
  // error code from CreateSymbolicLink() method.
  //
  if (!EnablePrivilege(L"SeCreateSymbolicLinkPrivilege"))
  {
    fwprintf(stderr,
      L"No privilege to create symbolic links.\n");
    ret = SYMLINK_NO_PRIVILEGE;
    goto SymlinkEnd;
  }

  if ((dwRtnCode = DirectoryCheck(longFileName, &isDir)) != ERROR_SUCCESS)
  {
    ReportErrorCode(L"DirectoryCheck", dwRtnCode);
    ret = FAILURE;
    goto SymlinkEnd;
  }

  if (isDir)
    dwFlag = SYMBOLIC_LINK_FLAG_DIRECTORY;

  if (!CreateSymbolicLinkW(longLinkName, longFileName, dwFlag))
  {
    ReportErrorCode(L"CreateSymbolicLink", GetLastError());
    ret = FAILURE;
    goto SymlinkEnd;
  }

SymlinkEnd:
  LocalFree(longLinkName);
  LocalFree(longFileName);
  return ret;
}

void SymlinkUsage()
{
    fwprintf(stdout, L"\
Usage: symlink [LINKNAME] [FILENAME]\n\
Creates a symbolic link\n\
\n\
0 is returned on success.\n\
2 is returned if the user does no have privilege to create symbolic links.\n\
1 is returned for all other errors.\n\
\n\
The default security settings in Windows disallow non-elevated administrators\n\
and all non-administrators from creating symbolic links. The security settings\n\
for symbolic links can be changed in the Local Security Policy management\n\
console.\n");
}

