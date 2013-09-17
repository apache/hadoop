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

// List of different hardlink related command line options supported by
// winutils.
typedef enum HardLinkCommandOptionType
{
  HardLinkInvalid,
  HardLinkCreate,
  HardLinkStat
} HardLinkCommandOption;

//----------------------------------------------------------------------------
// Function: ParseCommandLine
//
// Description:
//  Parses the given command line. On success, out param 'command' contains
//  the user specified command.
//
// Returns:
// TRUE: If the command line is valid
// FALSE: otherwise
static BOOL ParseCommandLine(__in int argc,
                             __in_ecount(argc) wchar_t *argv[],
                             __out HardLinkCommandOption *command)
{
  *command = HardLinkInvalid;

  if (argc != 3 && argc != 4) {
    return FALSE;
  }

  if (argc == 3) {
    if (wcscmp(argv[0], L"hardlink") != 0 || wcscmp(argv[1], L"stat") != 0)
    {
      return FALSE;
    }

    *command = HardLinkStat;
  }

  if (argc == 4) {
    if (wcscmp(argv[0], L"hardlink") != 0 || wcscmp(argv[1], L"create") != 0)
    {
      return FALSE;
    }

    *command = HardLinkCreate;
  }

  assert(*command != HardLinkInvalid);

  return TRUE;
}

//----------------------------------------------------------------------------
// Function: HardlinkStat
//
// Description:
//  Computes the number of hard links for a given file.
//
// Returns:
// ERROR_SUCCESS: On success
// error code: otherwise
static DWORD HardlinkStat(__in LPCWSTR fileName, __out DWORD *puHardLinkCount)
{
  BY_HANDLE_FILE_INFORMATION fileInformation;
  DWORD dwErrorCode = ERROR_SUCCESS;
  PWSTR longFileName = NULL;

  // First convert input paths to long paths
  //
  dwErrorCode = ConvertToLongPath(fileName, &longFileName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    goto HardlinkStatExit;
  }

  // Get file information which contains the hard link count
  //
  dwErrorCode = GetFileInformationByName(longFileName, FALSE, &fileInformation);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    goto HardlinkStatExit;
  }

  *puHardLinkCount = fileInformation.nNumberOfLinks;

HardlinkStatExit:
  LocalFree(longFileName);

  return dwErrorCode;
}

//----------------------------------------------------------------------------
// Function: HardlinkCreate
//
// Description:
//  Creates a hard link for a given file under the given name.
//
// Returns:
// ERROR_SUCCESS: On success
// error code: otherwise
static DWORD HardlinkCreate(__in LPCWSTR linkName, __in LPCWSTR fileName)
{
  PWSTR longLinkName = NULL;
  PWSTR longFileName = NULL;
  DWORD dwErrorCode = ERROR_SUCCESS;

  // First convert input paths to long paths
  //
  dwErrorCode = ConvertToLongPath(linkName, &longLinkName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    goto HardlinkCreateExit;
  }

  dwErrorCode = ConvertToLongPath(fileName, &longFileName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    goto HardlinkCreateExit;
  }

  // Create the hard link
  //
  if (!CreateHardLink(longLinkName, longFileName, NULL))
  {
    dwErrorCode = GetLastError();
  }

HardlinkCreateExit:
  LocalFree(longLinkName);
  LocalFree(longFileName);

  return dwErrorCode;
}

//----------------------------------------------------------------------------
// Function: Hardlink
//
// Description:
//  Creates a hard link for a given file under the given name. Outputs the
//  appropriate information to stdout on success, or stderr on failure.
//
// Returns:
// EXIT_SUCCESS: On success
// EXIT_FAILURE: otherwise
int Hardlink(__in int argc, __in_ecount(argc) wchar_t *argv[])
{
  DWORD dwErrorCode = ERROR_SUCCESS;
  int ret = EXIT_FAILURE;
  HardLinkCommandOption command = HardLinkInvalid;

  if (!ParseCommandLine(argc, argv, &command)) {
    dwErrorCode = ERROR_INVALID_COMMAND_LINE;

    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    HardlinkUsage();
    goto HardLinkExit;
  }

  if (command == HardLinkStat)
  {
    // Compute the number of hard links
    //
    DWORD uHardLinkCount = 0;
    dwErrorCode = HardlinkStat(argv[2], &uHardLinkCount);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"HardlinkStat", dwErrorCode);
      goto HardLinkExit;
    }

    // Output the result
    //
    fwprintf(stdout, L"%d\n", uHardLinkCount);

  } else if (command == HardLinkCreate)
  {
    // Create the hard link
    //
    dwErrorCode = HardlinkCreate(argv[2], argv[3]);
    if (dwErrorCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"HardlinkCreate", dwErrorCode);
      goto HardLinkExit;
    }

    // Output the success message
    //
    fwprintf(stdout, L"Hardlink created for %s <<===>> %s\n", argv[2], argv[3]);

  } else
  {
    // Should not happen
    //
    assert(FALSE);
  }

  ret = EXIT_SUCCESS;

HardLinkExit:

  return ret;
}

void HardlinkUsage()
{
    fwprintf(stdout, L"\
Usage: hardlink create [LINKNAME] [FILENAME] |\n\
       hardlink stat [FILENAME]\n\
Creates a new hardlink on the existing file or displays the number of links\n\
for the given file\n");
}