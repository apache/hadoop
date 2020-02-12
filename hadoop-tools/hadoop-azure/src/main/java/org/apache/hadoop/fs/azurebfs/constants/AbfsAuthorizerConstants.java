/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;

/**
 * Defines the string literals that need to be set to authorizer as action
 */
public class AbfsAuthorizerConstants {

  public static String RENAME_SOURCE_ACTION = "rename-source";
  public static String RENAME_DESTINATION_ACTION = "rename-destination";

  public static String CONCAT_SOURCE_ACTION = "concat-source";
  public static String CONCAT_TARGET_ACTION = "concat-target";

  public static String CHECKACCESS_ACTION_PREFIX_ACTION = "access-";

  public static String LISTSTATUS_ACTION = "liststatus";
  public static String DELETE_ACTION = "delete";
  public static String CREATEFILE_ACTION = "create";
  public static String MKDIR_ACTION = "mkdir";
  public static String GETACL_ACTION = "getaclstatus";
  public static String GETFILESTATUS_ACTION = "getfilestatus";
  public static String SETACL_ACTION = "setacl"; // Modify, removeacl, setacl
  public static String SETOWNER_ACTION = "setowner";
  public static String SETPERMISSION_ACTION = "setpermission";
  public static String APPEND_ACTION = "write";
  public static String READ_ACTION = "read";
  public static String EXECUTE_ACTION = "execute";

  /**
   * Converts AbfsRestOperation to Authorizer action
   * @param opType
   * @return
   */
  public static String getAction(AbfsRestOperationType opType)
      throws InvalidAbfsRestOperationException {
    switch (opType) {
    case ListPaths:
      return LISTSTATUS_ACTION;
    case RenamePath:
      return RENAME_DESTINATION_ACTION;
    case GetAcl:
      return GETACL_ACTION;
    case GetPathStatus:
      return GETFILESTATUS_ACTION;
    case SetAcl:
      return SETACL_ACTION;
    case SetOwner:
      return SETOWNER_ACTION;
    case SetPermissions:
      return SETPERMISSION_ACTION;
    case Append:
    case Flush:
      return APPEND_ACTION;
    case ReadFile:
      return READ_ACTION;
    case DeletePath:
      return DELETE_ACTION;
    case CreatePath:
      return CREATEFILE_ACTION;
    case Mkdir:
      return MKDIR_ACTION;
    default:
      throw new InvalidAbfsRestOperationException(
          new Exception("Unknown ABFS " + "Rest Opertation" + opType.name()));
    }
  }

}