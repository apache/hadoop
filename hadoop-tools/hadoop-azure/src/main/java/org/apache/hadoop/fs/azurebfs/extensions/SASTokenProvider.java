/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;

/**
 * Interface to support SAS authorization.
 */
@InterfaceAudience.LimitedPrivate("authorization-subsystems")
@InterfaceStability.Unstable
public interface SASTokenProvider {

  public static final String CONCAT_SOURCE_OPERATION = "concat-source";
  public static final String CONCAT_TARGET_OPERATION = "concat-target";
  public static final String CREATEFILE_OPERATION = "create";
  public static final String DELETE_OPERATION = "delete";
  public static final String EXECUTE_OPERATION = "execute";
  public static final String GETACL_OPERATION = "getaclstatus";
  public static final String GETFILESTATUS_OPERATION = "getfilestatus";
  public static final String LISTSTATUS_OPERATION = "liststatus";
  public static final String MKDIR_OPERATION = "mkdir";
  public static final String READ_OPERATION = "read";
  public static final String RENAME_SOURCE_OPERATION = "rename-source";
  public static final String RENAME_DESTINATION_OPERATION = "rename-destination";
  public static final String SETACL_OPERATION = "setacl";
  public static final String SETOWNER_OPERATION = "setowner";
  public static final String SETPERMISSION_OPERATION = "setpermission";
  public static final String APPEND_OPERATION = "write";

  /**
   * Initialize authorizer for Azure Blob File System.
   * @param configuration Configuration object
   * @param accountName Account Name
   * @throws IOException network problems or similar.
   */
  void initialize(Configuration configuration, String accountName)
      throws IOException;

  /**
   * Invokes the authorizer to obtain a SAS token.
   *
   * @param account the name of the storage account.
   * @param fileSystem the name of the fileSystem.
   * @param path the file or directory path.
   * @param operation the operation to be performed on the path.
   * @return a SAS token to perform the request operation.
   * @throws IOException if there is a network error.
   * @throws AccessControlException if access is denied.
   */
  String getSASToken(String account, String fileSystem, String path,
      String operation) throws IOException, AccessControlException;
}