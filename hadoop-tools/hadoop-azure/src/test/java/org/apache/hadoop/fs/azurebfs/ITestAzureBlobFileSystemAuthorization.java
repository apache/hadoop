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

package org.apache.hadoop.fs.azurebfs;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.*;
import org.apache.hadoop.fs.azurebfs.extensions.*;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizer.*;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Perform Authorization Check operation
 */
public class ITestAzureBlobFileSystemAuthorization
    extends AbstractAbfsIntegrationTest {

  private static final String TEST_READ_ONLY_FILE_PATH_PREFIX_0 =
      TEST_READ_ONLY_FILE_0;
  private static final String TEST_READ_ONLY_FILE_PATH_PREFIX_1 =
      TEST_READ_ONLY_FILE_1;
  private static final String TEST_READ_ONLY_FOLDER_PATH_PREFIX =
      TEST_READ_ONLY_FOLDER;
  private static final String TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 =
      TEST_WRITE_ONLY_FILE_0;
  private static final String TEST_WRITE_ONLY_FILE_PATH_PREFIX_1 =
      TEST_WRITE_ONLY_FILE_1;
  private static final String TEST_READ_WRITE_FILE_PATH_PREFIX_0 =
      TEST_READ_WRITE_FILE_0;
  private static final String TEST_READ_WRITE_FILE_PATH_PREFIX_1 =
      TEST_READ_WRITE_FILE_1;
  private static final String TEST_WRITE_ONLY_FOLDER_PATH_PREFIX =
      TEST_WRITE_ONLY_FOLDER;
  private static final String TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX =
      TEST_WRITE_THEN_READ_ONLY;
  private static final String TEST_AUTHZ_CLASS =
      "org.apache.hadoop.fs" + ".azurebfs.extensions.MockAbfsAuthorizer";
  private static final String TEST_USER = UUID.randomUUID().toString();
  private static final String TEST_GROUP = UUID.randomUUID().toString();
  private static final String BAR = UUID.randomUUID().toString();
  @Rule
  public TestName name = new TestName();

  public ITestAzureBlobFileSystemAuthorization() throws Exception {
  }

  @Override
  public void setup() throws Exception {
    boolean isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(isHNSEnabled == true);
    this.getConfiguration().setAbfsAuthorizerClass(TEST_AUTHZ_CLASS);
    loadAuthorizer();
    super.setup();
  }

  @Test
  public void testOpenFileWithInvalidPath() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(IllegalArgumentException.class, () -> {
      fs.open(new Path("")).close();
    });
  }

  @Test
  public void testOpenFileAuthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.Open, false,
        null, null);
  }

  @Test
  public void testOpenFileUnauthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.Open, true,
        null, null);
  }

  @Test
  public void testCreateFileAuthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.None, FileSystemOperations.CreatePath, false,
        null, null);
  }

  @Test
  public void testCreateFileUnauthorized() throws Exception {
    runTest(TEST_READ_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.None, FileSystemOperations.CreatePath, true, null,
        null);
  }

  @Test
  public void testAppendFileAuthorized() throws Exception {
    runTest(TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.AppendClose,
        false, null, null);
  }

  @Test
  public void testAppendFileUnauthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.AppendClose,
        true, null, null);
  }

  @Test
  public void testRenameSourceUnauthorized() throws Exception {
    runTest(TEST_READ_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.None, FileSystemOperations.RenamePath, true,
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(), null);
  }

  @Test
  public void testRenameDestUnauthorized() throws Exception {
    runTest(TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.None, FileSystemOperations.RenamePath, true,
        TEST_READ_ONLY_FILE_PATH_PREFIX_1 + name.getMethodName(), null);
  }

  @Test
  public void testDeleteFileAuthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.DeletePath,
        false, null, null);
  }

  @Test
  public void testDeleteFileUnauthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.DeletePath, true,
        null, null);
  }

  @Test
  public void testListStatusAuthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.ListPaths, false,
        null, null);
  }

  @Test
  public void testListStatusUnauthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.ListPaths, true,
        null, null);
  }

  @Test
  public void testMkDirsAuthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FOLDER_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.None, FileSystemOperations.Mkdir, false, null,
        null);
  }

  @Test
  public void testMkDirsUnauthorized() throws Exception {
    runTest(TEST_READ_ONLY_FOLDER_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.None, FileSystemOperations.Mkdir, true, null,
        null);
  }

  @Test
  public void testGetFileStatusAuthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.GetPathStatus,
        false, null, null);
  }

  @Test
  public void testGetFileStatusUnauthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.GetPathStatus,
        true, null, null);
  }

  @Test
  public void testSetOwnerAuthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.SetOwner, false,
        null, null);
  }

  @Test
  public void testSetOwnerUnauthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.SetOwner, true,
        null, null);
  }

  @Test
  public void testSetPermissionAuthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.SetPermissions,
        false, null, null);
  }

  @Test
  public void testSetPermissionUnauthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.SetPermissions,
        true, null, null);
  }

  @Test
  public void testModifyAclEntriesAuthorized() throws Exception {
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));

    runTest(TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.ModifyAclEntries,
        false, null, aclSpec);
  }

  @Test
  public void testModifyAclEntriesUnauthorized() throws Exception {
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));

    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.ModifyAclEntries,
        true, null, aclSpec);
  }

  @Test
  public void testRemoveAclEntriesAuthorized() throws Exception {
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));

    runTest(TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.RemoveAclEntries,
        false, null, aclSpec);
  }

  @Test
  public void testRemoveAclEntriesUnauthorized() throws Exception {
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));

    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.RemoveAclEntries,
        true, null, aclSpec);
  }

  @Test
  public void testRemoveDefaultAclAuthorized() throws Exception {
    runTest(TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.RemoveDefaultAcl,
        false, null, null);
  }

  @Test
  public void testRemoveDefaultAclUnauthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.RemoveDefaultAcl,
        true, null, null);
  }

  @Test
  public void testRemoveAclAuthorized() throws Exception {
    runTest(TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.RemoveAcl, false,
        null, null);
  }

  @Test
  public void testRemoveAclUnauthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.RemoveAcl, true,
        null, null);
  }

  @Test
  public void testSetAclAuthorized() throws Exception {
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));

    runTest(TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.SetAcl, false,
        null, aclSpec);
  }

  @Test
  public void testSetAclUnauthorized() throws Exception {
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));

    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.SetAcl, true,
        null, aclSpec);
  }

  @Test
  public void testGetAclStatusAuthorized() throws Exception {
    runTest(TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.GetAcl, false,
        null, null);
  }

  @Test
  public void testGetAclStatusUnauthorized() throws Exception {
    runTest(TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName(),
        FileSystemOperations.CreateClose, FileSystemOperations.GetAcl, true,
        null, null);
  }

  private void runTest(String reqFileName, FileSystemOperations initialOp,
      FileSystemOperations testOp, boolean expectAbfsAuthoirzationException,
      String renameDestFileName, List<AclEntry> aclSpec) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(reqFileName);
    Path renameDestPath = (renameDestFileName == null) ?
        null :
        new Path(renameDestFileName);
    boolean isWriteThenReadOnlyFile = (reqFileName
        .startsWith(TEST_WRITE_THEN_READ_ONLY));

    // Initial steps
    if (isWriteThenReadOnlyFile) {
      getMockAuthorizer(fs).setwriteThenReadOnly(reqFileName, WriteReadMode.WRITE_MODE);
    }

    if (initialOp != FileSystemOperations.None) {
      executeOp(fs, initialOp, testFilePath, renameDestPath, aclSpec);
    }

    if (isWriteThenReadOnlyFile) {
      getMockAuthorizer(fs).setwriteThenReadOnly(reqFileName, WriteReadMode.READ_MODE);
    }

    // Test Operation
    if (expectAbfsAuthoirzationException) {
      intercept(AbfsAuthorizationException.class, () -> {
        executeOp(fs, testOp, testFilePath, renameDestPath, aclSpec);
      });
    } else {
      executeOp(fs, testOp, testFilePath, renameDestPath, aclSpec);
    }
  }

  private void executeOp(AzureBlobFileSystem fs, FileSystemOperations op,
      Path reqPath, Path renameDestPath, List<AclEntry> aclSpec)
      throws IOException {
    switch (op) {
    case ListPaths:
      fs.listStatus(reqPath);
      break;
    case CreatePath:
      fs.create(reqPath);
      break;
    case RenamePath:
      fs.rename(reqPath, renameDestPath);
      break;
    case GetAcl:
      fs.getAclStatus(reqPath);
      break;
    case GetPathStatus:
      fs.getFileStatus(reqPath);
      break;
    case SetAcl:
      fs.setAcl(reqPath, aclSpec);
      break;
    case SetOwner:
      fs.setOwner(reqPath, TEST_USER, TEST_GROUP);
      break;
    case SetPermissions:
      fs.setPermission(reqPath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      break;
    case Append:
      fs.append(reqPath);
      break;
    case ReadFile:
      fs.open(reqPath);
      break;
    case AppendClose:
      fs.append(reqPath).close();
      break;
    case CreateClose:
      fs.create(reqPath).close();
      break;
    case Open:
      fs.open(reqPath);
      break;
    case DeletePath:
      fs.delete(reqPath, false);
      break;
    case Mkdir:
      fs.mkdirs(reqPath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      break;
    case RemoveAclEntries:
      fs.removeAclEntries(reqPath, aclSpec);
      break;
    case ModifyAclEntries:
      fs.modifyAclEntries(reqPath, aclSpec);
      break;
    case RemoveAcl:
      fs.removeAcl(reqPath);
      break;
    case RemoveDefaultAcl:
      fs.removeDefaultAcl(reqPath);
      break;
    default:
      throw new IllegalStateException("Unexpected value: " + op);
    }
  }

  private MockAbfsAuthorizer getMockAuthorizer(AzureBlobFileSystem fs)
      throws Exception {
    return ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer());
  }

  enum FileSystemOperations {
    None, ListPaths, CreatePath, RenamePath, GetAcl, GetPathStatus, SetAcl,
    SetOwner, SetPermissions, Append, ReadFile, DeletePath, Mkdir,
    RemoveAclEntries, RemoveDefaultAcl, RemoveAcl, ModifyAclEntries,
    AppendClose, CreateClose, Open
  }
}
