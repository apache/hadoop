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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizationException;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.CHECKACCESS_ACTION_PREFIX_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.CREATEFILE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.DELETE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.EXECUTE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.GETACL_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.GETFILESTATUS_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.LISTSTATUS_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.MKDIR_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.READ_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.RENAME_DESTINATION_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.RENAME_SOURCE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.SETACL_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.SETOWNER_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.SETPERMISSION_ACTION;

/**
 * A mock Azure Blob File System Authorization Implementation
 */
public class MockAbfsAuthorizer implements AbfsAuthorizer {
  public String name1 = "defaultMockName1";
  public String name2 = "defaultMockName2";
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
  private static final Set<String> apiAuthorizerActions = new HashSet<String>();
  public String accountName;
  public String fileSystemName;
  private Configuration conf;
  private Set<String> readOnlyPaths = new HashSet<String>();
  private Set<String> writeOnlyPaths = new HashSet<String>();
  private Set<String> readWritePaths = new HashSet<String>();
  private int writeThenReadOnly = 0;
  public MockAbfsAuthorizer(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void init() throws AbfsAuthorizationException {
    readOnlyPaths.add(TEST_READ_ONLY_FILE_0);
    readOnlyPaths.add(TEST_READ_ONLY_FILE_1);
    readOnlyPaths.add(TEST_READ_ONLY_FOLDER);
    writeOnlyPaths.add(TEST_WRITE_ONLY_FILE_0);
    writeOnlyPaths.add(TEST_WRITE_ONLY_FILE_1);
    writeOnlyPaths.add(TEST_WRITE_ONLY_FOLDER);
    readWritePaths.add(TEST_READ_WRITE_FILE_0);
    readWritePaths.add(TEST_READ_WRITE_FILE_1);

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
        new AuthorizationResourceResult();
    resourceResult.authorizerAction = resource.authorizerAction;
    resourceResult.storePathUri = resource.storePathUri;
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
    if (resource.authorizerAction.equalsIgnoreCase(storeAction)) {
      String storePath = resource.storePathUri.getPath();
      int indexofLastDelimiter = storePath.lastIndexOf("/");
      String storeFileOrFolder = storePath.substring(indexofLastDelimiter + 1);
      accessPermissions neededAccessPerm =
          getAccessPermission(storeAction);

      if ((neededAccessPerm == accessPermissions.Read) && (
          readOnlyPaths.contains(storeFileOrFolder) || readWritePaths.contains(storeFileOrFolder))) {
        // READ
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.Read)
          && storeFileOrFolder.equals(TEST_WRITE_THEN_READ_ONLY)
          && (writeThenReadOnly == 1)) {
        // WRITE THEN READ
        //writeThenReadOnly = 0;
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.Write)
          && (writeOnlyPaths.contains(storeFileOrFolder) || readWritePaths.contains(storeFileOrFolder))) {
        // WRITE
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.Write)
          && storeFileOrFolder.equals(TEST_WRITE_THEN_READ_ONLY)
          && (writeThenReadOnly == 0)) {
        // WRITE THEN READ
        //writeThenReadOnly = 1;
        return getAuthzResourceResultWithoutSAS(resource);
      } else if ((neededAccessPerm == accessPermissions.ReadWrite)
          && readWritePaths.contains(storeFileOrFolder)) {
        return getAuthzResourceResultWithoutSAS(resource);
      }

      throw new AbfsAuthorizationException("User is not authorized to perform" + " action");
    }

    return null;
  }

  private accessPermissions getAccessPermission(String storeAction) {

    if (storeAction.equalsIgnoreCase(RENAME_DESTINATION_ACTION) ||
        storeAction.equalsIgnoreCase(RENAME_SOURCE_ACTION) ||
        storeAction.equalsIgnoreCase(CREATEFILE_ACTION) ||
        storeAction.equalsIgnoreCase(MKDIR_ACTION) ||
        storeAction.equalsIgnoreCase(APPEND_ACTION) ||
        storeAction.equalsIgnoreCase(SETOWNER_ACTION) ||
        storeAction.equalsIgnoreCase(SETPERMISSION_ACTION) ||
        storeAction.equalsIgnoreCase(DELETE_ACTION)) {
      return accessPermissions.Write;
    }

    if (storeAction.equalsIgnoreCase(GETACL_ACTION) ||
        storeAction.equalsIgnoreCase(GETFILESTATUS_ACTION) ||
        storeAction.equalsIgnoreCase(READ_ACTION) ||
        storeAction.equalsIgnoreCase(LISTSTATUS_ACTION)) {
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
    AuthorizationResult result = new AuthorizationResult();
    result.authResourceResult =
        new AuthorizationResourceResult[authorizationResource.length];
    int index = -1;

    for (AuthorizationResource authzResource : authorizationResource) {
      result.authResourceResult[++index] = checkAndFetchAuthzResourceResult(
          authzResource);
    }

    result.isAuthorized = true;

    return result;
  }

  enum accessPermissions {
    Read, Write, Execute, ReadWrite, ReadExecute, ReadWriteExecute,
    SuperUserOrOwner, None
  }

  //  @Override
  //  public boolean isAuthorized(FsAction action, Path... absolutePaths)
  //      throws AbfsAuthorizationException, IOException {
  //    Set<Path> paths = new HashSet<Path>();
  //    for (Path path : absolutePaths) {
  //      paths.add(new Path(path.getName()));
  //    }
  //
  //    if (action.equals(FsAction.READ) && Stream.concat(readOnlyPaths
  //    .stream(), readWritePaths.stream()).collect(Collectors.toSet())
  //    .containsAll(paths)) {
  //      return true;
  //    } else if (action.equals(FsAction.READ) && paths.contains(new Path
  //    (TEST_WRITE_THEN_READ_ONLY)) && writeThenReadOnly == 1) {
  //      return true;
  //    } else if (action.equals(FsAction.WRITE)
  //        && Stream.concat(writeOnlyPaths.stream(), readWritePaths.stream()
  //        ).collect(Collectors.toSet()).containsAll(paths)) {
  //      return true;
  //    } else if (action.equals(FsAction.WRITE) && paths.contains(new Path
  //    (TEST_WRITE_THEN_READ_ONLY)) && writeThenReadOnly == 0) {
  //      writeThenReadOnly = 1;
  //      return true;
  //    } else {
  //      return action.equals(FsAction.READ_WRITE) && readWritePaths
  //      .containsAll(paths);
  //    }
  //  }


  public void setwriteThenReadOnly(int writeThenReadOnly) {
    this.writeThenReadOnly = writeThenReadOnly;
  }
}
