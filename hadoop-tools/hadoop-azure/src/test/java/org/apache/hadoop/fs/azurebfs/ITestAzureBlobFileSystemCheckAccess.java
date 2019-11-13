/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CHECK_ACCESS;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;

public class ITestAzureBlobFileSystemCheckAccess
    extends AbstractAbfsIntegrationTest {

  private static final String TEST_FOLDER_PATH = "CheckAccessTestFolder";
  private final FileSystem superUserFs;
  private final FileSystem testUserFs;

  public ITestAzureBlobFileSystemCheckAccess() throws Exception {
    super();
    super.setup();
    this.superUserFs = getFileSystem();
    this.testUserFs = getTestUserFs();
  }

  private FileSystem getTestUserFs() throws Exception {
    String orgClientId = getConfiguration().get(FS_AZURE_BLOB_FS_CLIENT_ID);
    String orgClientSecret = getConfiguration()
        .get(FS_AZURE_BLOB_FS_CLIENT_SECRET);
    Boolean orgCreateFileSystemDurungInit = getConfiguration()
        .getBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_ID,
        getConfiguration().get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID));
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_SECRET, getConfiguration()
        .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET));
    getRawConfiguration()
        .setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
            false);
    FileSystem fs = FileSystem.newInstance(getRawConfiguration());
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_ID, orgClientId);
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_SECRET, orgClientSecret);
    getRawConfiguration()
        .setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
            orgCreateFileSystemDurungInit);
    return fs;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckAccessWithNullPath() throws IOException {

    superUserFs.access(null, FsAction.READ);
  }

  @Test(expected = NullPointerException.class)
  public void testCheckAccessForFileWithNullFsAction() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("test0.txt",
        FsAction.ALL);
    //  NPE when trying to convert null FsAction enum
    superUserFs.access(testFilePath, null);
  }

  @Test
  public void testWhenCheckAccessConfigIsOff() throws Exception {
    Assume.assumeTrue(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT + " is false",
        getConfiguration()
            .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false));
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ENABLE_CHECK_ACCESS, false);
    FileSystem fs = FileSystem.newInstance(conf);
    Path testFilePath = setupTestDirectoryAndUserAccess("/test1.txt",
        FsAction.NONE);
    fs.access(testFilePath, FsAction.EXECUTE);
    fs.access(testFilePath, FsAction.READ);
    fs.access(testFilePath, FsAction.WRITE);
    fs.access(testFilePath, FsAction.READ_EXECUTE);
    fs.access(testFilePath, FsAction.WRITE_EXECUTE);
    fs.access(testFilePath, FsAction.READ_WRITE);
    fs.access(testFilePath, FsAction.ALL);
    testFilePath = setupTestDirectoryAndUserAccess("/test1.txt", FsAction.ALL);
    fs.access(testFilePath, FsAction.EXECUTE);
    fs.access(testFilePath, FsAction.READ);
    fs.access(testFilePath, FsAction.WRITE);
    fs.access(testFilePath, FsAction.READ_EXECUTE);
    fs.access(testFilePath, FsAction.WRITE_EXECUTE);
    fs.access(testFilePath, FsAction.READ_WRITE);
    fs.access(testFilePath, FsAction.ALL);
    fs.access(testFilePath, null);
  }

  @Test
  public void testFsActionNONE() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("/test2.txt",
        FsAction.NONE);
    assertFalse(isAccessible(testFilePath, FsAction.EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.ALL));
  }

  @Test
  public void testFsActionEXECUTE() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("/test3.txt",
        FsAction.EXECUTE);
    assertTrue(isAccessible(testFilePath, FsAction.EXECUTE));

    assertFalse(isAccessible(testFilePath, FsAction.READ));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.ALL));
  }

  @Test
  public void testFsActionREAD() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("/test4.txt",
        FsAction.READ);
    assertTrue(isAccessible(testFilePath, FsAction.READ));

    assertFalse(isAccessible(testFilePath, FsAction.EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.ALL));
  }

  @Test
  public void testFsActionWRITE() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("/test5.txt",
        FsAction.WRITE);
    assertTrue(isAccessible(testFilePath, FsAction.WRITE));

    assertFalse(isAccessible(testFilePath, FsAction.EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ));
    assertFalse(isAccessible(testFilePath, FsAction.READ_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.ALL));
  }

  @Test
  public void testFsActionREAD_EXECUTE() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("/test6.txt",
        FsAction.READ_EXECUTE);
    assertTrue(isAccessible(testFilePath, FsAction.EXECUTE));
    assertTrue(isAccessible(testFilePath, FsAction.READ));
    assertTrue(isAccessible(testFilePath, FsAction.READ_EXECUTE));

    assertFalse(isAccessible(testFilePath, FsAction.WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.WRITE_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.ALL));
  }

  @Test
  public void testFsActionWRITE_EXECUTE() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("/test7.txt",
        FsAction.WRITE_EXECUTE);
    assertTrue(isAccessible(testFilePath, FsAction.EXECUTE));
    assertTrue(isAccessible(testFilePath, FsAction.WRITE));
    assertTrue(isAccessible(testFilePath, FsAction.WRITE_EXECUTE));

    assertFalse(isAccessible(testFilePath, FsAction.READ));
    assertFalse(isAccessible(testFilePath, FsAction.READ_EXECUTE));
    assertFalse(isAccessible(testFilePath, FsAction.READ_WRITE));
    assertFalse(isAccessible(testFilePath, FsAction.ALL));
  }

  @Test
  public void testFsActionALL() throws Exception {
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    Path testFilePath = setupTestDirectoryAndUserAccess("/test8.txt",
        FsAction.ALL);
    assertTrue(isAccessible(testFilePath, FsAction.EXECUTE));
    assertTrue(isAccessible(testFilePath, FsAction.WRITE));
    assertTrue(isAccessible(testFilePath, FsAction.WRITE_EXECUTE));
    assertTrue(isAccessible(testFilePath, FsAction.READ));
    assertTrue(isAccessible(testFilePath, FsAction.READ_EXECUTE));
    assertTrue(isAccessible(testFilePath, FsAction.READ_WRITE));
    assertTrue(isAccessible(testFilePath, FsAction.ALL));
  }

  private void setExecuteAccessForParentDirs(Path dir) throws IOException {
    String testUser = getConfiguration()
        .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID);
    dir = dir.getParent();
    while (dir != null) {
      modifyAcl(dir, testUser, FsAction.EXECUTE);
      dir = dir.getParent();
    }
  }

  private void modifyAcl(Path file, String uid, FsAction fsAction)
      throws IOException {
    List<AclEntry> aclSpec = Lists.newArrayList(new AclEntry[] {
        AclTestHelpers.aclEntry(AclEntryScope.ACCESS, AclEntryType.USER, uid,
            fsAction)});
    this.superUserFs.modifyAclEntries(file, aclSpec);
  }

  private Path setupTestDirectoryAndUserAccess(String testFileName,
      FsAction fsAction) throws Exception {
    Path file = new Path(TEST_FOLDER_PATH + testFileName);
    file = this.superUserFs.makeQualified(file);
    this.superUserFs.delete(file, true);
    this.superUserFs.create(file);
    modifyAcl(file,
        getConfiguration().get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID),
        fsAction);

    setExecuteAccessForParentDirs(file);
    return file;
  }

  private boolean isAccessible(Path path, FsAction fsAction)
      throws IOException {
    boolean isAccessible = true;
    try {
      this.testUserFs.access(path, fsAction);
    } catch (AccessControlException e) {
      isAccessible = false;
    } catch (IOException e) {
      throw e;
    }
    return isAccessible;
  }
}