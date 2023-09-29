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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;

/**
 * Test FileStatus.
 */
public class ITestAzureBlobFileSystemFileStatus extends
    AbstractAbfsIntegrationTest {
  private static final String DEFAULT_FILE_PERMISSION_VALUE = "640";
  private static final String DEFAULT_DIR_PERMISSION_VALUE = "750";
  private static final String DEFAULT_UMASK_VALUE = "027";
  private static final String FULL_PERMISSION = "777";

  private static final String TEST_FILE = "testFile";
  private static final String TEST_FOLDER = "testDir";

  public ITestAzureBlobFileSystemFileStatus() throws Exception {
    super();
  }

  @Test
  public void testEnsureStatusWorksForRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    Path root = new Path("/");
    FileStatus[] rootls = fs.listStatus(root);
    assertEquals("root listing", 0, rootls.length);
  }

  @Test
  public void testFileStatusPermissionsAndOwnerAndGroup() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, DEFAULT_UMASK_VALUE);
    Path testFile = path(TEST_FILE);
    touch(testFile);
    validateStatus(fs, testFile, false);
  }

  private FileStatus validateStatus(final AzureBlobFileSystem fs, final Path name, final boolean isDir)
      throws IOException {
    FileStatus fileStatus = fs.getFileStatus(name);

    String errorInStatus = "error in " + fileStatus + " from " + fs;

    if (!getIsNamespaceEnabled(fs)) {
      assertEquals(errorInStatus + ": owner",
              fs.getOwnerUser(), fileStatus.getOwner());
      assertEquals(errorInStatus + ": group",
              fs.getOwnerUserPrimaryGroup(), fileStatus.getGroup());
      assertEquals(new FsPermission(FULL_PERMISSION), fileStatus.getPermission());
    } else {
      // When running with namespace enabled account,
      // the owner and group info retrieved from server will be digit ids.
      // hence skip the owner and group validation
      if (isDir) {
        assertEquals(errorInStatus + ": permission",
                new FsPermission(DEFAULT_DIR_PERMISSION_VALUE), fileStatus.getPermission());
        assertTrue(errorInStatus + "not a directory", fileStatus.isDirectory());
      } else {
        assertEquals(errorInStatus + ": permission",
                new FsPermission(DEFAULT_FILE_PERMISSION_VALUE), fileStatus.getPermission());
        assertTrue(errorInStatus + "not a file", fileStatus.isFile());
      }
    }

    return fileStatus;
  }

  @Test
  public void testFolderStatusPermissionsAndOwnerAndGroup() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, DEFAULT_UMASK_VALUE);
    Path testFolder = path(TEST_FOLDER);
    fs.mkdirs(testFolder);

    validateStatus(fs, testFolder, true);
  }

  @Test
  public void testAbfsPathWithHost() throws IOException {
    AzureBlobFileSystem fs = this.getFileSystem();
    Path pathWithHost1 = new Path("abfs://mycluster/abfs/file1.txt");
    Path pathwithouthost1 = new Path("/abfs/file1.txt");

    Path pathWithHost2 = new Path("abfs://mycluster/abfs/file2.txt");
    Path pathwithouthost2 = new Path("/abfs/file2.txt");

    // verify compatibility of this path format
    fs.create(pathWithHost1).close();
    assertPathExists(fs, "This path should exist", pathwithouthost1);

    fs.create(pathwithouthost2).close();
    assertPathExists(fs, "This path should exist", pathWithHost2);

    // verify get
    FileStatus fileStatus1 = fs.getFileStatus(pathWithHost1);
    assertEquals(pathwithouthost1.getName(), fileStatus1.getPath().getName());

    FileStatus fileStatus2 = fs.getFileStatus(pathwithouthost2);
    assertEquals(pathWithHost2.getName(), fileStatus2.getPath().getName());
  }

  @Test
  public void testLastModifiedTime() throws IOException {
    AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = path("childfile1.txt");
    long createStartTime = System.currentTimeMillis();
    long minCreateStartTime = (createStartTime / 1000) * 1000 - 1;
    //  Dividing and multiplying by 1000 to make last 3 digits 0.
    //  It is observed that modification time is returned with last 3
    //  digits 0 always.
    fs.create(testFilePath).close();
    long createEndTime = System.currentTimeMillis();
    FileStatus fStat = fs.getFileStatus(testFilePath);
    long lastModifiedTime = fStat.getModificationTime();
    assertTrue("lastModifiedTime should be after minCreateStartTime",
        minCreateStartTime < lastModifiedTime);
    assertTrue("lastModifiedTime should be before createEndTime",
        createEndTime > lastModifiedTime);
  }

  @Test
  public void testFileStatusOnRoot() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();

    // Assert that passing relative root path works
    Path testPath = new Path("/");
    validateStatus(fs, testPath, true);

    // Assert that passing absolute root path works
    String testPathStr = makeQualified(testPath).toString();
    validateStatus(fs, new Path(testPathStr), true);

    // Assert that passing absolute root path without "/" works
    testPathStr = testPathStr.substring(0, testPathStr.length() - 1);
    validateStatus(fs, new Path(testPathStr), true);

  }
}
