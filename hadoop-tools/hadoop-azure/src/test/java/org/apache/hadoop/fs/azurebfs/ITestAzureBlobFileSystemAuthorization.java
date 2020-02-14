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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.*;
import org.apache.hadoop.fs.azurebfs.extensions.*;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.hadoop.fs.Path;
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
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());
    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();
    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    fs.open(testFilePath).close();
  }

  @Test
  public void testOpenFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());
    fs.create(testFilePath).close();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.open(testFilePath).close();
    });
  }

  @Test
  public void testCreateFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());
    fs.create(testFilePath).close();
  }

  @Test
  public void testCreateFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_READ_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());
    intercept(AbfsAuthorizationException.class, () -> {
      fs.create(testFilePath).close();
    });
  }

  @Test
  public void testAppendFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());
    fs.create(testFilePath).close();
    fs.append(testFilePath).close();
  }

  @Test
  public void testAppendFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.append(testFilePath).close();
    });
  }

  @Test
  public void testRenameSourceUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testSrcFilePath = new Path(
        TEST_READ_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());
    Path testDestFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());

    intercept(AbfsAuthorizationException.class, () -> {
      fs.rename(testSrcFilePath, testDestFilePath);
    });
  }

  @Test
  public void testRenameDestUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testSrcFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());
    Path testDestFilePath = new Path(
        TEST_READ_ONLY_FILE_PATH_PREFIX_1 + name.getMethodName());

    intercept(AbfsAuthorizationException.class, () -> {
      fs.rename(testSrcFilePath, testDestFilePath);
    });
  }

  @Test
  public void testDeleteFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());
    fs.create(testFilePath).close();
    fs.delete(testFilePath, false);
  }

  @Test
  public void testDeleteFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.delete(testFilePath, false);
    });
  }

  @Test
  public void testListStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    fs.listStatus(testFilePath);
  }

  @Test
  public void testListStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.listStatus(testFilePath);
    });
  }

  @Test
  public void testMkDirsAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = new Path(
        TEST_WRITE_ONLY_FOLDER_PATH_PREFIX + name.getMethodName());

    fs.mkdirs(testFolderPath,
        new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
  }

  @Test
  public void testMkDirsUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = new Path(
        TEST_READ_ONLY_FOLDER_PATH_PREFIX + name.getMethodName());

    intercept(AbfsAuthorizationException.class, () -> {
      fs.mkdirs(testFolderPath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    });
  }

  @Test
  public void testGetFileStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    fs.getFileStatus(testFilePath);
  }

  @Test
  public void testGetFileStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());
    fs.create(testFilePath).close();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.getFileStatus(testFilePath);
    });
  }

  @Test
  public void testSetOwnerAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());
    fs.create(testFilePath).close();
    fs.setOwner(testFilePath, TEST_USER, TEST_GROUP);
  }

  @Test
  public void testSetOwnerUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.setOwner(testFilePath, TEST_USER, TEST_GROUP);
    });
  }

  @Test
  public void testSetPermissionAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    fs.setPermission(testFilePath,
        new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
  }

  @Test
  public void testSetPermissionUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.setPermission(testFilePath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    });
  }

  @Test
  public void testModifyAclEntriesAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.modifyAclEntries(testFilePath, aclSpec);
  }

  @Test
  public void testModifyAclEntriesUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.modifyAclEntries(testFilePath, aclSpec);
    });
  }

  @Test
  public void testRemoveAclEntriesAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.removeAclEntries(testFilePath, aclSpec);
  }

  @Test
  public void testRemoveAclEntriesUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.removeAclEntries(testFilePath, aclSpec);
    });
  }

  @Test
  public void testRemoveDefaultAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    fs.removeDefaultAcl(testFilePath);
  }

  @Test
  public void testRemoveDefaultAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.removeDefaultAcl(testFilePath);
    });
  }

  @Test
  public void testRemoveAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    fs.removeAcl(testFilePath);
  }

  @Test
  public void testRemoveAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.removeAcl(testFilePath);
    });
  }

  @Test
  public void testSetAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_READ_WRITE_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.setAcl(testFilePath, aclSpec);
  }

  @Test
  public void testSetAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.setAcl(testFilePath, aclSpec);
    });
  }

  @Test
  public void testGetAclStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_THEN_READ_ONLY_PATH_PREFIX + name.getMethodName());

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.WRITE_MODE);
    fs.create(testFilePath).close();

    getMockAuthorizer(fs)
        .setwriteThenReadOnly(testFilePath.getName(), WriteReadMode.READ_MODE);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.getAclStatus(testFilePath);
  }

  @Test
  public void testGetAclStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = new Path(
        TEST_WRITE_ONLY_FILE_PATH_PREFIX_0 + name.getMethodName());

    fs.create(testFilePath).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.getAclStatus(testFilePath);
    });
  }

  private MockAbfsAuthorizer getMockAuthorizer(AzureBlobFileSystem fs)
      throws Exception {
    return ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer());
  }
}
