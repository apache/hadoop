package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.fs.permission.FsAction;

public class PermissionMapping {
  /**
   * public enum AbfsRestOperationType {
   case CreateFileSystem,
   case GetFileSystemProperties,
   case SetFileSystemProperties,
   case ListPaths,
   case DeleteFileSystem,
   case CreatePath,
   case RenamePath,
   case GetAcl,
   case GetPathProperties,
   case GetPathStatus,
   case SetAcl,
   case SetOwner,
   case SetPathProperties,
   case SetPermissions,
   case Append,
   case Flush,
   case ReadFile,
   case DeletePath
   * }
   * @return
   */
  
  public static FsAction getRequiredOpPermission(
      AbfsRestOperationType operationType) {
    switch (operationType) {
    case CreateFileSystem:
    case GetFileSystemProperties:
    case SetFileSystemProperties:
    case DeleteFileSystem:
      return FsAction.NONE; // TODO: TBD based on FE checks

    case CreatePath:
    case SetOwner:
    case SetPathProperties:
    case SetPermissions:
    case Append:
    case Flush:
    case DeletePath:
      return FsAction.WRITE;

    case ListPaths:
    case GetAcl:
    case GetPathProperties:
    case GetPathStatus:
    case SetAcl:
    case ReadFile:
      return FsAction.READ;

    case RenamePath:
      return FsAction.READ_WRITE;
    }

    // When not sure about permission required,
    // Assume all permissions are needed
    return FsAction.ALL;
  }
}
