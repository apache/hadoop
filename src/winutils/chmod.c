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
#include <errno.h>

enum CHMOD_WHO
{
  CHMOD_WHO_NONE  =    0,
  CHMOD_WHO_OTHER =   07,
  CHMOD_WHO_GROUP =  070,
  CHMOD_WHO_USER  = 0700,
  CHMOD_WHO_ALL   = CHMOD_WHO_OTHER | CHMOD_WHO_GROUP | CHMOD_WHO_USER
};

enum CHMOD_OP
{
  CHMOD_OP_INVALID,
  CHMOD_OP_PLUS,
  CHMOD_OP_MINUS,
  CHMOD_OP_EQUAL,
};

enum CHMOD_PERM
{
  CHMOD_PERM_NA =  00,
  CHMOD_PERM_R  =  01,
  CHMOD_PERM_W  =  02,
  CHMOD_PERM_X  =  04,
  CHMOD_PERM_LX = 010,
};

/*
 * We use the following struct to build a linked list of mode change actions.
 * The mode is described by the following grammar:
 *  mode         ::= clause [, clause ...]
 *  clause       ::= [who ...] [action ...]
 *  action       ::= op [perm ...] | op [ref]
 *  who          ::= a | u | g | o
 *  op           ::= + | - | =
 *  perm         ::= r | w | x | X
 *  ref          ::= u | g | o
 */
typedef struct _MODE_CHANGE_ACTION
{
  USHORT who;
  USHORT op;
  USHORT perm;
  USHORT ref;
  struct _MODE_CHANGE_ACTION *next_action;
} MODE_CHANGE_ACTION, *PMODE_CHANGE_ACTION;

const MODE_CHANGE_ACTION INIT_MODE_CHANGE_ACTION = {
  CHMOD_WHO_NONE, CHMOD_OP_INVALID, CHMOD_PERM_NA, CHMOD_WHO_NONE, NULL
};

static BOOL ParseOctalMode(LPCWSTR tsMask, INT *uMask);

static BOOL ParseMode(LPCWSTR modeString, PMODE_CHANGE_ACTION *actions);

static BOOL FreeActions(PMODE_CHANGE_ACTION actions);

static BOOL ParseCommandLineArguments(__in int argc, __in wchar_t *argv[],
  __out BOOL *rec, __out_opt INT *mask,
  __out_opt PMODE_CHANGE_ACTION *actions, __out LPCWSTR *path);

static BOOL ChangeFileModeByActions(__in LPCWSTR path,
  PMODE_CHANGE_ACTION actions);

static BOOL ChangeFileMode(__in LPCWSTR path, __in_opt INT mode,
  __in_opt PMODE_CHANGE_ACTION actions);

static BOOL ChangeFileModeRecursively(__in LPCWSTR path, __in_opt INT mode,
  __in_opt PMODE_CHANGE_ACTION actions);


//----------------------------------------------------------------------------
// Function: Chmod
//
// Description:
//  The main method for chmod command
//
// Returns:
//  0: on success
//
// Notes:
//
int Chmod(int argc, wchar_t *argv[])
{
  LPWSTR pathName = NULL;
  LPWSTR longPathName = NULL;

  BOOL recursive = FALSE;

  PMODE_CHANGE_ACTION actions = NULL;

  INT unixAccessMask = 0;

  DWORD dwRtnCode = 0;

  int ret = EXIT_FAILURE;

  // Parsing chmod arguments
  //
  if (!ParseCommandLineArguments(argc, argv,
    &recursive, &unixAccessMask, &actions, &pathName))
  {
    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    ChmodUsage(argv[0]);
    return EXIT_FAILURE;
  }

  // Convert the path the the long path
  //
  dwRtnCode = ConvertToLongPath(pathName, &longPathName);
  if (dwRtnCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"ConvertToLongPath", dwRtnCode);
    goto ChmodEnd;
  }

  if (!recursive)
  {
    if (ChangeFileMode(longPathName, unixAccessMask, actions))
    {
      ret = EXIT_SUCCESS;
    }
  }
  else
  {
    if (ChangeFileModeRecursively(longPathName, unixAccessMask, actions))
    {
      ret = EXIT_SUCCESS;
    }
  }

ChmodEnd:
  FreeActions(actions);
  LocalFree(longPathName);

  return ret;
}

//----------------------------------------------------------------------------
// Function: ChangeFileMode
//
// Description:
//  Wrapper function for change file mode. Choose either change by action or by
//  access mask.
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//
static BOOL ChangeFileMode(__in LPCWSTR path, __in_opt INT unixAccessMask,
  __in_opt PMODE_CHANGE_ACTION actions)
{
  if (actions != NULL)
    return ChangeFileModeByActions(path, actions);
  else
  {
    DWORD dwRtnCode = ChangeFileModeByMask(path, unixAccessMask);
    if (dwRtnCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"ChangeFileModeByMask", dwRtnCode);
      return FALSE;
    }
    return TRUE;
  }
}

//----------------------------------------------------------------------------
// Function: ChangeFileModeRecursively
//
// Description:
//  Travel the directory recursively to change the permissions.
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//  The recursion works in the following way:
//    - If the path is not a directory, change its mode and return.
//      Symbolic links and junction points are not considered as directories.
//    - Otherwise, call the method on all its children, then change its mode.
//
static BOOL ChangeFileModeRecursively(__in LPCWSTR path, __in_opt INT mode,
  __in_opt PMODE_CHANGE_ACTION actions)
{
  BOOL isDir = FALSE;
  BOOL isSymlink = FALSE;
  LPWSTR dir = NULL;

  size_t pathSize = 0;
  size_t dirSize = 0;

  HANDLE hFind = INVALID_HANDLE_VALUE;
  WIN32_FIND_DATA ffd;
  DWORD dwRtnCode = ERROR_SUCCESS;
  BOOL ret = FALSE;

  if ((dwRtnCode = DirectoryCheck(path, &isDir)) != ERROR_SUCCESS)
  {
    ReportErrorCode(L"IsDirectory", dwRtnCode);
    return FALSE;
  }
  if ((dwRtnCode = SymbolicLinkCheck(path, &isSymlink)) != ERROR_SUCCESS)
  {
    ReportErrorCode(L"IsSymbolicLink", dwRtnCode);
    return FALSE;
  }

  if (isSymlink || !isDir)
  {
     if (ChangeFileMode(path, mode, actions))
       return TRUE;
     else
       return FALSE;
  }

  if (FAILED(StringCchLengthW(path, STRSAFE_MAX_CCH - 3, &pathSize)))
  {
    return FALSE;
  }
  dirSize = pathSize + 3;
  dir = (LPWSTR)LocalAlloc(LPTR, dirSize * sizeof(WCHAR));
  if (dir == NULL)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    goto ChangeFileModeRecursivelyEnd;
  }

  if (FAILED(StringCchCopyW(dir, dirSize, path)) ||
    FAILED(StringCchCatW(dir, dirSize, L"\\*")))
  {
    goto ChangeFileModeRecursivelyEnd;
  }

  hFind = FindFirstFile(dir, &ffd);
  if (hFind == INVALID_HANDLE_VALUE)
  {
    ReportErrorCode(L"FindFirstFile", GetLastError());
    goto ChangeFileModeRecursivelyEnd;
  }

  do
  {
    LPWSTR filename = NULL;
    LPWSTR longFilename = NULL;
    size_t filenameSize = 0;

    if (wcscmp(ffd.cFileName, L".") == 0 ||
      wcscmp(ffd.cFileName, L"..") == 0)
      continue;

    filenameSize = pathSize + wcslen(ffd.cFileName) + 2;
    filename = (LPWSTR)LocalAlloc(LPTR, filenameSize * sizeof(WCHAR));
    if (filename == NULL)
    {
      ReportErrorCode(L"LocalAlloc", GetLastError());
      goto ChangeFileModeRecursivelyEnd;
    }

    if (FAILED(StringCchCopyW(filename, filenameSize, path)) ||
      FAILED(StringCchCatW(filename, filenameSize, L"\\")) ||
      FAILED(StringCchCatW(filename, filenameSize, ffd.cFileName)))
    {
      LocalFree(filename);
      goto ChangeFileModeRecursivelyEnd;
    }
     
    // The child fileanme is not prepended with long path prefix.
    // Convert the filename to long path format.
    //
    dwRtnCode = ConvertToLongPath(filename, &longFilename);
    LocalFree(filename);
    if (dwRtnCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"ConvertToLongPath", dwRtnCode);
      LocalFree(longFilename);
      goto ChangeFileModeRecursivelyEnd;
    }

    if(!ChangeFileModeRecursively(longFilename, mode, actions))
    {
      LocalFree(longFilename);
      goto ChangeFileModeRecursivelyEnd;
    }

    LocalFree(longFilename);

  } while (FindNextFileW(hFind, &ffd));

  if (!ChangeFileMode(path, mode, actions))
  {
    goto ChangeFileModeRecursivelyEnd;
  }

  ret = TRUE;

ChangeFileModeRecursivelyEnd:
  LocalFree(dir);

  return ret;
}

//----------------------------------------------------------------------------
// Function: ParseCommandLineArguments
//
// Description:
//  Parse command line arguments for chmod.
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//	1. Recursive is only set on directories
//  2. 'actions' is NULL if the mode is octal
//
static BOOL ParseCommandLineArguments(__in int argc, __in wchar_t *argv[],
  __out BOOL *rec,
  __out_opt INT *mask,
  __out_opt PMODE_CHANGE_ACTION *actions,
  __out LPCWSTR *path)
{
  LPCWSTR maskString;
  BY_HANDLE_FILE_INFORMATION fileInfo;
  DWORD dwRtnCode = ERROR_SUCCESS;

  assert(path != NULL);

  if (argc != 3 && argc != 4)
    return FALSE;

  *rec = FALSE;
  if (argc == 4)
  {
    maskString = argv[2];
    *path = argv[3];

    if (wcscmp(argv[1], L"-R") == 0)
    {
      // Check if the given path name is a file or directory
      // Only set recursive flag if the given path is a directory
      //
      dwRtnCode = GetFileInformationByName(*path, FALSE, &fileInfo);
      if (dwRtnCode != ERROR_SUCCESS)
      {
        ReportErrorCode(L"GetFileInformationByName", dwRtnCode);
        return FALSE;
      }

      if (IsDirFileInfo(&fileInfo))
      {
        *rec = TRUE;
      }
    }
    else
      return FALSE;
  }
  else
  {
    maskString = argv[1];
    *path = argv[2];
  }

  if (ParseOctalMode(maskString, mask))
  {
    return TRUE;
  }
  else if (ParseMode(maskString, actions))
  {
    return TRUE;
  }

  return FALSE;
}

//----------------------------------------------------------------------------
// Function: FreeActions
//
// Description:
//  Free a linked list of mode change actions given the head node.
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//  none
//
static BOOL FreeActions(PMODE_CHANGE_ACTION actions)
{
  PMODE_CHANGE_ACTION curr = NULL;
  PMODE_CHANGE_ACTION next = NULL;
  
  // Nothing to free if NULL is passed in
  //
  if (actions == NULL)
  {
    return TRUE;
  }

  curr = actions;
  while (curr != NULL)
  {
    next = curr->next_action;
    LocalFree(curr);
    curr = next;
  }
  actions = NULL;

  return TRUE;
}

//----------------------------------------------------------------------------
// Function: ComputeNewMode
//
// Description:
//  Compute a new mode based on the old mode and a mode change action.
//
// Returns:
//  The newly computed mode
//
// Notes:
//  Apply 'rwx' permission mask or reference permission mode according to the
//  '+', '-', or '=' operator.
//
static INT ComputeNewMode(__in INT oldMode,
  __in USHORT who, __in USHORT op,
  __in USHORT perm, __in USHORT ref)
{
  static const INT readMask  = 0444;
  static const INT writeMask = 0222;
  static const INT exeMask   = 0111;

  INT mask = 0;
  INT mode = 0;

  // Operations are exclusive, and cannot be invalid
  //
  assert(op == CHMOD_OP_EQUAL || op == CHMOD_OP_PLUS || op == CHMOD_OP_MINUS);

  // Nothing needs to be changed if there is not permission or reference
  //
  if(perm == CHMOD_PERM_NA && ref == CHMOD_WHO_NONE)
  {
    return oldMode;
  }

  // We should have only permissions or a reference target, not both.
  //
  assert((perm != CHMOD_PERM_NA && ref == CHMOD_WHO_NONE) ||
    (perm == CHMOD_PERM_NA && ref != CHMOD_WHO_NONE));

  if (perm != CHMOD_PERM_NA)
  {
    if ((perm & CHMOD_PERM_R) == CHMOD_PERM_R)
      mask |= readMask;
    if ((perm & CHMOD_PERM_W) == CHMOD_PERM_W)
      mask |= writeMask;
    if ((perm & CHMOD_PERM_X) == CHMOD_PERM_X)
      mask |= exeMask;
    if (((perm & CHMOD_PERM_LX) == CHMOD_PERM_LX))
    {
      // It applies execute permissions to directories regardless of their
      // current permissions and applies execute permissions to a file which
      // already has at least 1 execute permission bit already set (either user,
      // group or other). It is only really useful when used with '+' and
      // usually in combination with the -R option for giving group or other
      // access to a big directory tree without setting execute permission on
      // normal files (such as text files), which would normally happen if you
      // just used "chmod -R a+rx .", whereas with 'X' you can do
      // "chmod -R a+rX ." instead (Source: Wikipedia)
      //
      if ((oldMode & UX_DIRECTORY) == UX_DIRECTORY || (oldMode & exeMask))
        mask |= exeMask;
    }
  }
  else if (ref != CHMOD_WHO_NONE)
  {
    mask |= oldMode & ref;
    switch(ref)
    {
    case CHMOD_WHO_GROUP:
      mask |= mask >> 3;
      mask |= mask << 3;
      break;
    case CHMOD_WHO_OTHER:
      mask |= mask << 3;
      mask |= mask << 6;
      break;
    case CHMOD_WHO_USER:
      mask |= mask >> 3;
      mask |= mask >> 6;
      break;
    default:
      // Reference modes can only be U/G/O and are exclusive
      assert(FALSE);
    }
  }

  mask &= who;

  if (op == CHMOD_OP_EQUAL)
  {
    mode = (oldMode & (~who)) | mask;
  }
  else if (op == CHMOD_OP_MINUS)
  {
    mode = oldMode & (~mask);
  }
  else if (op == CHMOD_OP_PLUS)
  {
    mode = oldMode | mask;
  }

  return mode;
}

//----------------------------------------------------------------------------
// Function: ConvertActionsToMask
//
// Description:
//  Convert a linked list of mode change actions to the Unix permission mask
//  given the head node.
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//  none
//
static BOOL ConvertActionsToMask(__in LPCWSTR path,
  __in PMODE_CHANGE_ACTION actions, __out PINT puMask)
{
  PMODE_CHANGE_ACTION curr = NULL;

  BY_HANDLE_FILE_INFORMATION fileInformation;
  DWORD dwErrorCode = ERROR_SUCCESS;

  INT mode = 0;

  dwErrorCode = GetFileInformationByName(path, FALSE, &fileInformation);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"GetFileInformationByName", dwErrorCode);
    return FALSE;
  }
  if (IsDirFileInfo(&fileInformation))
  {
    mode |= UX_DIRECTORY;
  }
  dwErrorCode = FindFileOwnerAndPermission(path, NULL, NULL, &mode);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ReportErrorCode(L"FindFileOwnerAndPermission", dwErrorCode);
    return FALSE;
  }
  *puMask = mode;

  // Nothing to change if NULL is passed in
  //
  if (actions == NULL)
  {
    return TRUE;
  }

  for (curr = actions; curr != NULL; curr = curr->next_action)
  {
    mode = ComputeNewMode(mode, curr->who, curr->op, curr->perm, curr->ref);
  }

  *puMask = mode;
  return TRUE;
}

//----------------------------------------------------------------------------
// Function: ChangeFileModeByActions
//
// Description:
//  Change a file mode through a list of actions.
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//  none
//
static BOOL ChangeFileModeByActions(__in LPCWSTR path,
  PMODE_CHANGE_ACTION actions)
{
  INT mask = 0;

  if (ConvertActionsToMask(path, actions, &mask))
  {
    DWORD dwRtnCode = ChangeFileModeByMask(path, mask);
    if (dwRtnCode != ERROR_SUCCESS)
    {
      ReportErrorCode(L"ChangeFileModeByMask", dwRtnCode);
      return FALSE;
    }
    return TRUE;
  }
  else
    return FALSE;
}

//----------------------------------------------------------------------------
// Function: ParseMode
//
// Description:
//  Convert a mode string into a linked list of actions
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//  Take a state machine approach to parse the mode. Each mode change action
//  will be a node in the output linked list. The state machine has five state,
//  and each will only transit to the next; the end state can transit back to
//  the first state, and thus form a circle. In each state, if we see a
//  a character not belongs to the state, we will move to next state. WHO, PERM,
//  and REF states are optional; OP and END states are required; and errors
//  will only be reported at the latter two states.
//
static BOOL ParseMode(LPCWSTR modeString, PMODE_CHANGE_ACTION *pActions)
{
  enum __PARSE_MODE_ACTION_STATE
  {
    PARSE_MODE_ACTION_WHO_STATE,
    PARSE_MODE_ACTION_OP_STATE,
    PARSE_MODE_ACTION_PERM_STATE,
    PARSE_MODE_ACTION_REF_STATE,
    PARSE_MODE_ACTION_END_STATE
  } state = PARSE_MODE_ACTION_WHO_STATE;

  MODE_CHANGE_ACTION action = INIT_MODE_CHANGE_ACTION;
  PMODE_CHANGE_ACTION actionsEnd = NULL;
  PMODE_CHANGE_ACTION actionsLast = NULL;
  USHORT lastWho;
  WCHAR c = 0;
  size_t len = 0;
  size_t i = 0;

  assert(modeString != NULL && pActions != NULL);

  if (FAILED(StringCchLengthW(modeString, STRSAFE_MAX_CCH, &len)))
  {
    return FALSE;
  }

  actionsEnd = *pActions;
  while(i <= len)
  {
    c = modeString[i];
    if (state == PARSE_MODE_ACTION_WHO_STATE)
    {
      switch (c)
      {
      case L'a':
        action.who |= CHMOD_WHO_ALL;
        i++;
        break;
      case L'u':
        action.who |= CHMOD_WHO_USER;
        i++;
        break;
      case L'g':
        action.who |= CHMOD_WHO_GROUP;
        i++;
        break;
      case L'o':
        action.who |= CHMOD_WHO_OTHER;
        i++;
        break;
      default:
        state = PARSE_MODE_ACTION_OP_STATE;
      } // WHO switch
    }
    else if (state == PARSE_MODE_ACTION_OP_STATE)
    {
      switch (c)
      {
      case L'+':
        action.op = CHMOD_OP_PLUS;
        break;
      case L'-':
        action.op = CHMOD_OP_MINUS;
        break;
      case L'=':
        action.op = CHMOD_OP_EQUAL;
        break;
      default:
        fwprintf(stderr, L"Invalid mode: '%s'\n", modeString);
        FreeActions(*pActions);
        return FALSE;
      } // OP switch
      i++;
      state = PARSE_MODE_ACTION_PERM_STATE;
    }
    else if (state == PARSE_MODE_ACTION_PERM_STATE)
    {
      switch (c)
      {
      case L'r':
        action.perm |= CHMOD_PERM_R;
        i++;
        break;
      case L'w':
        action.perm |= CHMOD_PERM_W;
        i++;
        break;
      case L'x':
        action.perm |= CHMOD_PERM_X;
        i++;
        break;
      case L'X':
        action.perm |= CHMOD_PERM_LX;
        i++;
        break;
      default:
        state = PARSE_MODE_ACTION_REF_STATE;
      } // PERM switch
    }
    else if (state == PARSE_MODE_ACTION_REF_STATE)
    {
      switch (c)
      {
      case L'u':
        action.ref = CHMOD_WHO_USER;
        i++;
        break;
      case L'g':
        action.ref = CHMOD_WHO_GROUP;
        i++;
        break;
      case L'o':
        action.ref = CHMOD_WHO_OTHER;
        i++;
        break;
      default:
        state = PARSE_MODE_ACTION_END_STATE;
      } // REF switch
    }
    else if (state == PARSE_MODE_ACTION_END_STATE)
    {
      switch (c)
      {
      case NULL:
      case L',':
        i++;
      case L'+':
      case L'-':
      case L'=':
        state = PARSE_MODE_ACTION_WHO_STATE;

        // Append the current action to the end of the linked list
        //
        assert(actionsEnd == NULL);
        // Allocate memory
        actionsEnd = (PMODE_CHANGE_ACTION) LocalAlloc(LPTR,
          sizeof(MODE_CHANGE_ACTION));
        if (actionsEnd == NULL)
        {
          ReportErrorCode(L"LocalAlloc", GetLastError());
          FreeActions(*pActions);
          return FALSE;
        }
        if (action.who == CHMOD_WHO_NONE) action.who = CHMOD_WHO_ALL;
        // Copy the action to the new node
        *actionsEnd = action;
        // Append to the last node in the linked list
        if (actionsLast != NULL) actionsLast->next_action = actionsEnd;
        // pActions should point to the head of the linked list
        if (*pActions == NULL) *pActions = actionsEnd;
        // Update the two pointers to point to the last node and the tail
        actionsLast = actionsEnd;
        actionsEnd = actionsLast->next_action;

        // Reset action
        //
        lastWho = action.who;
        action = INIT_MODE_CHANGE_ACTION;
        if (c != L',')
        {
          action.who = lastWho;
        }

        break;
      default:
        fwprintf(stderr, L"Invalid mode: '%s'\n", modeString);
        FreeActions(*pActions);
        return FALSE;
      } // END switch
    }
  } // while
  return TRUE;
}

//----------------------------------------------------------------------------
// Function: ParseOctalMode
//
// Description:
//  Convert the 3 or 4 digits Unix mask string into the binary representation
//  of the Unix access mask, i.e. 9 bits each an indicator of the permission
//  of 'rwxrwxrwx', i.e. user's, group's, and owner's read, write, and
//  execute/search permissions.
//
// Returns:
//  TRUE: on success
//  FALSE: otherwise
//
// Notes:
//	none
//
static BOOL ParseOctalMode(LPCWSTR tsMask, INT *uMask)
{
  size_t tsMaskLen = 0;
  DWORD i;
  LONG l;
  WCHAR *end;

  if (uMask == NULL)
    return FALSE;

  if (FAILED(StringCchLengthW(tsMask, STRSAFE_MAX_CCH, &tsMaskLen)))
    return FALSE;

  if (tsMaskLen == 0 || tsMaskLen > 4)
  {
    return FALSE;
  }

  for (i = 0; i < tsMaskLen; i++)
  {
    if (!(tsMask[tsMaskLen - i - 1] >= L'0' &&
      tsMask[tsMaskLen - i - 1] <= L'7'))
      return FALSE;
  }

  errno = 0;
  if (tsMaskLen == 4)
    // Windows does not have any equivalent of setuid/setgid and sticky bit.
    // So the first bit is omitted for the 4 digit octal mode case.
    //
    l = wcstol(tsMask + 1, &end, 8);
  else
    l = wcstol(tsMask, &end, 8);

  if (errno || l > 0x0777 || l < 0 || *end != 0)
  {
    return FALSE;
  }

  *uMask = (INT) l;

  return TRUE;
}

void ChmodUsage(LPCWSTR program)
{
  fwprintf(stdout, L"\
Usage: %s [OPTION] OCTAL-MODE [FILE]\n\
   or: %s [OPTION] MODE [FILE]\n\
Change the mode of the FILE to MODE.\n\
\n\
   -R: change files and directories recursively\n\
\n\
Each MODE is of the form '[ugoa]*([-+=]([rwxX]*|[ugo]))+'.\n",
program, program);
}
