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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.MOCK_SASTOKENPROVIDER_FAIL_INIT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.MOCK_SASTOKENPROVIDER_RETURN_EMPTY_SAS_TOKEN;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Perform Authorization Check operation
 */
public class ITestAzureBlobFileSystemAuthorization extends AbstractAbfsIntegrationTest {

  private static final String TEST_AUTHZ_CLASS = "org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider";
  private static final String TEST_ERR_AUTHZ_CLASS = "org.apache.hadoop.fs.azurebfs.extensions.MockErrorSASTokenProvider";
  private static final String TEST_USER = UUID.randomUUID().toString();
  private static final String TEST_GROUP = UUID.randomUUID().toString();
  private static final String BAR = UUID.randomUUID().toString();

  public ITestAzureBlobFileSystemAuthorization() throws Exception {
    // The mock SAS token provider relies on the account key to generate SAS.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    boolean isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(isHNSEnabled);
    loadConfiguredFileSystem();
    this.getConfiguration().set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_AUTHZ_CLASS);
    this.getConfiguration().set(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
    super.setup();
  }

  @Test
  public void testSASTokenProviderInitializeException() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    final AzureBlobFileSystem testFs = new AzureBlobFileSystem();
    Configuration testConfig = this.getConfiguration().getRawConfiguration();
    testConfig.set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_ERR_AUTHZ_CLASS);
    testConfig.set(MOCK_SASTOKENPROVIDER_FAIL_INIT, "true");

    intercept(TokenAccessProviderException.class,
        ()-> {
          testFs.initialize(fs.getUri(), this.getConfiguration().getRawConfiguration());
        });
  }

  @Test
  public void testSASTokenProviderEmptySASToken() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    final AzureBlobFileSystem testFs = new AzureBlobFileSystem();
    Configuration testConfig = this.getConfiguration().getRawConfiguration();
    testConfig.set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_ERR_AUTHZ_CLASS);
    testConfig.set(MOCK_SASTOKENPROVIDER_RETURN_EMPTY_SAS_TOKEN, "true");

    testFs.initialize(fs.getUri(),
        this.getConfiguration().getRawConfiguration());
    intercept(SASTokenProviderException.class,
        () -> {
          testFs.create(new org.apache.hadoop.fs.Path("/testFile"));
        });
  }

  @Test
  public void testSASTokenProviderNullSASToken() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    final AzureBlobFileSystem testFs = new AzureBlobFileSystem();
    Configuration testConfig = this.getConfiguration().getRawConfiguration();
    testConfig.set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_ERR_AUTHZ_CLASS);

    testFs.initialize(fs.getUri(), this.getConfiguration().getRawConfiguration());
    intercept(SASTokenProviderException.class,
        ()-> {
          testFs.create(new org.apache.hadoop.fs.Path("/testFile"));
        });
  }

  @Test
  public void testOpenFileWithInvalidPath() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(IllegalArgumentException.class,
        ()-> {
          fs.open(new Path("")).close();
    });
  }

  @Test
  public void testOpenFileAuthorized() throws Exception {
    runTest(FileSystemOperations.Open, false);
  }

  @Test
  public void testOpenFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.Open, true);
  }

  @Test
  public void testCreateFileAuthorized() throws Exception {
    runTest(FileSystemOperations.CreatePath, false);
  }

  @Test
  public void testCreateFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.CreatePath, true);
  }

  @Test
  public void testAppendFileAuthorized() throws Exception {
    runTest(FileSystemOperations.Append, false);
  }

  @Test
  public void testAppendFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.Append, true);
  }

  @Test
  public void testRenameAuthorized() throws Exception {
    runTest(FileSystemOperations.RenamePath, false);
  }

  @Test
  public void testRenameUnauthorized() throws Exception {
    runTest(FileSystemOperations.RenamePath, true);
  }

  @Test
  public void testDeleteFileAuthorized() throws Exception {
    runTest(FileSystemOperations.DeletePath, false);
  }

  @Test
  public void testDeleteFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.DeletePath, true);
  }

  @Test
  public void testListStatusAuthorized() throws Exception {
    runTest(FileSystemOperations.ListPaths, false);
  }

  @Test
  public void testListStatusUnauthorized() throws Exception {
    runTest(FileSystemOperations.ListPaths, true);
  }

  @Test
  public void testMkDirsAuthorized() throws Exception {
    runTest(FileSystemOperations.Mkdir, false);
  }

  @Test
  public void testMkDirsUnauthorized() throws Exception {
    runTest(FileSystemOperations.Mkdir, true);
  }

  @Test
  public void testGetFileStatusAuthorized() throws Exception {
    runTest(FileSystemOperations.GetPathStatus, false);
  }

  @Test
  public void testGetFileStatusUnauthorized() throws Exception {
    runTest(FileSystemOperations.GetPathStatus, true);
  }

  @Test
  public void testSetOwnerUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.SetOwner, true);
  }

  @Test
  public void testSetPermissionUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.SetPermissions, true);
  }

  @Test
  public void testModifyAclEntriesUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.ModifyAclEntries, true);
  }

  @Test
  public void testRemoveAclEntriesUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.RemoveAclEntries, true);
  }

  @Test
  public void testRemoveDefaultAclUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.RemoveDefaultAcl, true);
  }

  @Test
  public void testRemoveAclUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.RemoveAcl, true);
  }

  @Test
  public void testSetAclUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.SetAcl, true);
  }

  @Test
  public void testGetAclStatusAuthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.GetAcl, false);
  }

  @Test
  public void testGetAclStatusUnauthorized() throws Exception {
    Assume.assumeTrue(this.getFileSystem().getIsNamespaceEnabled());
    runTest(FileSystemOperations.GetAcl, true);
  }


  private void runTest(FileSystemOperations testOp,
      boolean expectAbfsAuthorizationException) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path reqPath = new Path("requestPath"
        + UUID.randomUUID().toString()
        + (expectAbfsAuthorizationException ? "unauthorized":""));

    getMockSASTokenProvider(fs).setSkipAuthorizationForTestSetup(true);
    if ((testOp != FileSystemOperations.CreatePath)
        && (testOp != FileSystemOperations.Mkdir)) {
      fs.create(reqPath).close();
      fs.getFileStatus(reqPath);
    }
    getMockSASTokenProvider(fs).setSkipAuthorizationForTestSetup(false);

    // Test Operation
    if (expectAbfsAuthorizationException) {
      intercept(SASTokenProviderException.class, () -> {
        executeOp(reqPath, fs, testOp);
      });
    } else {
      executeOp(reqPath, fs, testOp);
    }
  }

  private void executeOp(Path reqPath, AzureBlobFileSystem fs,
      FileSystemOperations op) throws IOException, IOException {


    switch (op) {
    case ListPaths:
      fs.listStatus(reqPath);
      break;
    case CreatePath:
      fs.create(reqPath);
      break;
    case RenamePath:
      fs.rename(reqPath,
          new Path("renameDest" + UUID.randomUUID().toString()));
      break;
    case GetAcl:
      fs.getAclStatus(reqPath);
      break;
    case GetPathStatus:
      fs.getFileStatus(reqPath);
      break;
    case SetAcl:
      fs.setAcl(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
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
      fs.removeAclEntries(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
      break;
    case ModifyAclEntries:
      fs.modifyAclEntries(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
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

  private MockSASTokenProvider getMockSASTokenProvider(AzureBlobFileSystem fs)
      throws Exception {
    return ((MockSASTokenProvider) fs.getAbfsStore().getClient().getSasTokenProvider());
  }

  enum FileSystemOperations {
    None, ListPaths, CreatePath, RenamePath, GetAcl, GetPathStatus, SetAcl,
    SetOwner, SetPermissions, Append, ReadFile, DeletePath, Mkdir,
    RemoveAclEntries, RemoveDefaultAcl, RemoveAcl, ModifyAclEntries,
    Open
  }
}
