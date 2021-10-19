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

  String CHECK_ACCESS_OPERATION = "check-access";
  String CREATE_DIRECTORY_OPERATION = "create-directory";
  String CREATE_FILE_OPERATION = "create-file";
  String DELETE_OPERATION = "delete";
  String DELETE_RECURSIVE_OPERATION = "delete-recursive";
  String GET_ACL_OPERATION = "get-acl";
  String GET_STATUS_OPERATION = "get-status";
  String GET_PROPERTIES_OPERATION = "get-properties";
  String LIST_OPERATION = "list";
  String READ_OPERATION = "read";
  String RENAME_SOURCE_OPERATION = "rename-source";
  String RENAME_DESTINATION_OPERATION = "rename-destination";
  String SET_ACL_OPERATION = "set-acl";
  String SET_OWNER_OPERATION = "set-owner";
  String SET_PERMISSION_OPERATION = "set-permission";
  String SET_PROPERTIES_OPERATION = "set-properties";
  String WRITE_OPERATION = "write";

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