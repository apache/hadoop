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

static void Usage(LPCWSTR program);

int wmain(int argc, wchar_t* argv[])
{
  LPCWSTR cmd = NULL;

  if (argc < 2)
  {
    Usage(argv[0]);
    return EXIT_FAILURE;
  }

  cmd = argv[1];

  if (wcscmp(L"ls", cmd) == 0)
  {
    return Ls(argc - 1, argv + 1);
  }
  else if (wcscmp(L"chmod", cmd) == 0)
  {
    return Chmod(argc - 1, argv + 1);
  }
  else if (wcscmp(L"chown", cmd) == 0)
  {
    return Chown(argc - 1, argv + 1);
  }
  else if (wcscmp(L"groups", cmd) == 0)
  {
    return Groups(argc - 1, argv + 1);
  }
  else if (wcscmp(L"hardlink", cmd) == 0)
  {
    return Hardlink(argc - 1, argv + 1);
  }
  else if (wcscmp(L"symlink", cmd) == 0)
  {
    return Symlink(argc - 1, argv + 1);
  }
  else if (wcscmp(L"task", cmd) == 0)
  {
    return Task(argc - 1, argv + 1);
  }
  else if (wcscmp(L"systeminfo", cmd) == 0)
  {
    return SystemInfo();
  }
  else if (wcscmp(L"help", cmd) == 0)
  {
    Usage(argv[0]);
    return EXIT_SUCCESS;
  }
  else
  {
    Usage(argv[0]);
    return EXIT_FAILURE;
  }
}

static void Usage(LPCWSTR program)
{
  fwprintf(stdout, L"Usage: %s [command] ...\n\
Provide basic command line utilities for Hadoop on Windows.\n\n\
The available commands and their usages are:\n\n", program);

  fwprintf(stdout, L"%-15s%s\n\n", L"chmod", L"Change file mode bits.");
  ChmodUsage(L"chmod");
  fwprintf(stdout, L"\n\n");

  fwprintf(stdout, L"%-15s%s\n\n", L"chown", L"Change file owner.");
  ChownUsage(L"chown");
  fwprintf(stdout, L"\n\n");

  fwprintf(stdout, L"%-15s%s\n\n", L"groups", L"List user groups.");
  GroupsUsage(L"groups");
  fwprintf(stdout, L"\n\n");

  fwprintf(stdout, L"%-15s%s\n\n", L"hardlink", L"Hard link operations.");
  HardlinkUsage();
  fwprintf(stdout, L"\n\n");

  fwprintf(stdout, L"%-15s%s\n\n", L"ls", L"List file information.");
  LsUsage(L"ls");
  fwprintf(stdout, L"\n\n");
 
  fwprintf(stdout, L"%-10s%s\n\n", L"symlink", L"Create a symbolic link.");
  SymlinkUsage();
  fwprintf(stdout, L"\n\n");

  fwprintf(stdout, L"%-15s%s\n\n", L"systeminfo", L"System information.");
  SystemInfoUsage();
  fwprintf(stdout, L"\n\n");

  fwprintf(stdout, L"%-15s%s\n\n", L"task", L"Task operations.");
  TaskUsage();
  fwprintf(stdout, L"\n\n");
}
