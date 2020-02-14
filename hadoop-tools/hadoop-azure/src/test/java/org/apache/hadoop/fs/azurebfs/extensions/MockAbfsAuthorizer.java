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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizationException;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.*;

/**
 * A mock Azure Blob File System Authorization Implementation
 */
public class MockAbfsAuthorizer implements AbfsAuthorizer {
  public static final String TEST_READ_ONLY_FILE_0 = "readOnlyFile0";
  public static final String TEST_READ_ONLY_FILE_1 = "readOnlyFile1";
  public static final String TEST_READ_ONLY_FOLDER = "readOnlyFolder";
  public static final String TEST_WRITE_ONLY_FILE_0 = "writeOnlyFile0";
  public static final String TEST_WRITE_ONLY_FILE_1 = "writeOnlyFile1";
  public static final String TEST_WRITE_ONLY_FOLDER = "writeOnlyFolder";
  public static final String TEST_READ_WRITE_FILE_0 = "readWriteFile0";
  public static final String TEST_READ_WRITE_FILE_1 = "readWriteFile1";
  public static final String TEST_WRITE_THEN_READ_ONLY =
      "writeThenReadOnlyFile";
  private static final Set<String> apiAuthorizerActions = new HashSet<>();
  public String accountName;
  private Configuration conf;
  private Set<String> readOnlyPathsPrefixes = new HashSet<>();
  private Set<String> writeOnlyPathsPrefixes = new HashSet<>();
  private Set<String> readWritePathsPrefixes = new HashSet<>();
  private Map<String, WriteReadMode> writeReadModeMap = null;

  public MockAbfsAuthorizer(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void init() throws AbfsAuthorizationException {
    readOnlyPathsPrefixes.add(TEST_READ_ONLY_FILE_0);
    readOnlyPathsPrefixes.add(TEST_READ_ONLY_FILE_1);
    readOnlyPathsPrefixes.add(TEST_READ_ONLY_FOLDER);
    writeOnlyPathsPrefixes.add(TEST_WRITE_ONLY_FILE_0);
    writeOnlyPathsPrefixes.add(TEST_WRITE_ONLY_FILE_1);
    writeOnlyPathsPrefixes.add(TEST_WRITE_ONLY_FOLDER);
    readWritePathsPrefixes.add(TEST_READ_WRITE_FILE_0);
    readWritePathsPrefixes.add(TEST_READ_WRITE_FILE_1);

    apiAuthorizerActions.add(RENAME_SOURCE_ACTION);
    apiAuthorizerActions.add(RENAME_DESTINATION_ACTION);
    apiAuthorizerActions.add(CHECKACCESS_ACTION_PREFIX_ACTION);
    apiAuthorizerActions.add(LISTSTATUS_ACTION);
    apiAuthorizerActions.add(DELETE_ACTION);
    apiAuthorizerActions.add(CREATEFILE_ACTION);
    apiAuthorizerActions.add(MKDIR_ACTION);
    apiAuthorizerActions.add(GETACL_ACTION);
    apiAuthorizerActions.add(GETFILESTATUS_ACTION);
    apiAuthorizerActions.add(SETACL_ACTION);
    apiAuthorizerActions.add(SETOWNER_ACTION);
    apiAuthorizerActions.add(SETPERMISSION_ACTION);
    apiAuthorizerActions.add(APPEND_ACTION);
    apiAuthorizerActions.add(READ_ACTION);
    apiAuthorizerActions.add(EXECUTE_ACTION);
  }

  private AuthorizationResourceResult getAuthzResourceResultWithoutSAS(
      AuthorizationResource resource) {
    AuthorizationResourceResult resourceResult =
        new AuthorizationResourceResult(resource.getStorePathUri(),
            resource.getAuthorizerAction(), null);
    return resourceResult;
  }

  private AuthorizationResourceResult checkAndFetchAuthzResourceResult(
      AuthorizationResource resource) throws AbfsAuthorizationException {
    AuthorizationResourceResult resourceResult = null;

    for (String authorizerAction : apiAuthorizerActions) {
      resourceResult = checkAuthorizationForStoreAction(resource,
          authorizerAction);

      if (resourceResult != null) {
        return resourceResult;
      }
    }

    return null;
  }

  private AuthorizationResourceResult checkAuthorizationForStoreAction(
      AuthorizationResource resource, String storeAction)
      throws AbfsAuthorizationException {
    if (resource.getAuthorizerAction().equalsIgnoreCase(storeAction)) {
      String storePath = resource.getStorePathUri().getPath();
      int indexofLastDelimiter = storePath.lastIndexOf("/");
      String storeFileOrFolder = storePath.substring(indexofLastDelimiter + 1);
      String getStoreFileOrFolderPrefix = getStorePathPrefix(storeFileOrFolder);

      accessPermissions neededAccessPerm = getAccessPermission(storeAction);

      if ((neededAccessPerm == accessPermissions.Read) && (
          readOnlyPathsPrefixes.contains(getStoreFileOrFolderPrefix)
              || readWritePathsPrefixes.contains(getStoreFileOrFolderPrefix))) {
        // READ
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.Read)
          && storeFileOrFolder.startsWith(TEST_WRITE_THEN_READ_ONLY) && (
          writeReadModeMap.containsKey(storeFileOrFolder) && (
              writeReadModeMap.get(storeFileOrFolder)
                  == WriteReadMode.READ_MODE))) {
        // WRITE THEN READ FILE - Currently in read mode
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.Write) && (
          writeOnlyPathsPrefixes.contains(getStoreFileOrFolderPrefix)
              || readWritePathsPrefixes.contains(getStoreFileOrFolderPrefix))) {
        // WRITE
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.Write)
          && storeFileOrFolder.startsWith(TEST_WRITE_THEN_READ_ONLY) && (
          writeReadModeMap.containsKey(storeFileOrFolder) && (
              writeReadModeMap.get(storeFileOrFolder)
                  == WriteReadMode.WRITE_MODE))) {
        // WRITE THEN READ FILE - Currently in write mode
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.ReadWrite)
          && readWritePathsPrefixes.contains(getStoreFileOrFolderPrefix)) {
        return getAuthzResourceResultWithoutSAS(resource);
      }

      throw new AbfsAuthorizationException(
          "User is not authorized to perform" + " action");
    }

    return null;
  }

  private String getStorePathPrefix(String storeFileOrFolder) {
    if (storeFileOrFolder.startsWith(TEST_READ_ONLY_FILE_0)) {
      return TEST_READ_ONLY_FILE_0;
    }
    if (storeFileOrFolder.startsWith(TEST_READ_ONLY_FILE_1)) {
      return TEST_READ_ONLY_FILE_1;
    }
    if (storeFileOrFolder.startsWith(TEST_READ_ONLY_FOLDER)) {
      return TEST_READ_ONLY_FOLDER;
    }
    if (storeFileOrFolder.startsWith(TEST_WRITE_ONLY_FILE_0)) {
      return TEST_WRITE_ONLY_FILE_0;
    }
    if (storeFileOrFolder.startsWith(TEST_WRITE_ONLY_FILE_1)) {
      return TEST_WRITE_ONLY_FILE_1;
    }
    if (storeFileOrFolder.startsWith(TEST_WRITE_ONLY_FOLDER)) {
      return TEST_WRITE_ONLY_FOLDER;
    }
    if (storeFileOrFolder.startsWith(TEST_READ_WRITE_FILE_0)) {
      return TEST_READ_WRITE_FILE_0;
    }
    if (storeFileOrFolder.startsWith(TEST_READ_WRITE_FILE_1)) {
      return TEST_READ_WRITE_FILE_1;
    }

    return storeFileOrFolder;
  }

  private accessPermissions getAccessPermission(String storeAction) {

    if (storeAction.equalsIgnoreCase(RENAME_DESTINATION_ACTION) || storeAction
        .equalsIgnoreCase(RENAME_SOURCE_ACTION) || storeAction
        .equalsIgnoreCase(CREATEFILE_ACTION) || storeAction
        .equalsIgnoreCase(MKDIR_ACTION) || storeAction
        .equalsIgnoreCase(APPEND_ACTION) || storeAction
        .equalsIgnoreCase(SETOWNER_ACTION) || storeAction
        .equalsIgnoreCase(SETPERMISSION_ACTION) || storeAction
        .equalsIgnoreCase(DELETE_ACTION)) {
      return accessPermissions.Write;
    }

    if (storeAction.equalsIgnoreCase(GETACL_ACTION) || storeAction
        .equalsIgnoreCase(GETFILESTATUS_ACTION) || storeAction
        .equalsIgnoreCase(READ_ACTION) || storeAction
        .equalsIgnoreCase(LISTSTATUS_ACTION)) {
      return accessPermissions.Read;
    }

    if (storeAction.equalsIgnoreCase(SETACL_ACTION)) // Modify, RemoveAcl do
    // GetAclStatus first
    {
      return accessPermissions.ReadWrite;
    }

    return accessPermissions.None;
  }

  @Override
  public AuthType getAuthType() {
    return AuthType.None;
  }

  @Override
  public AuthorizationResult checkPrivileges(
      AuthorizationResource... authorizationResource)
      throws AbfsAuthorizationException {
    AuthorizationResourceResult[] authResourceResult =
        new AuthorizationResourceResult[authorizationResource.length];
    int index = -1;

    for (AuthorizationResource authzResource : authorizationResource) {
      authResourceResult[++index] = checkAndFetchAuthzResourceResult(
          authzResource);
    }

    AuthorizationResult result = new AuthorizationResult(true,
        authResourceResult);

    return result;
  }

  public void setwriteThenReadOnly(String storePath, WriteReadMode mode) {
    if (writeReadModeMap == null) {
      writeReadModeMap = new Hashtable<>();
    }
    writeReadModeMap.put(storePath, mode);
  }

  enum accessPermissions {
    Read, Write, Execute, ReadWrite, ReadExecute, ReadWriteExecute,
    SuperUserOrOwner, None
  }

  public enum WriteReadMode {
    WRITE_MODE, READ_MODE
  }
}
