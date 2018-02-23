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

package org.apache.hadoop.fs.azure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;
import org.apache.hadoop.fs.azure.integration.AzureTestUtils;

import static org.junit.Assume.assumeNotNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Run the {@link FileSystemContractBaseTest} test suite against azure
 * storage, after switching the FS using page blobs everywhere.
 */
public class ITestNativeAzureFileSystemContractPageBlobLive extends
    FileSystemContractBaseTest {
  private AzureBlobStorageTestAccount testAccount;
  private Path basePath;
  @Rule
  public TestName methodName = new TestName();

  private void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  private AzureBlobStorageTestAccount createTestAccount()
      throws Exception {
    Configuration conf = new Configuration();

    // Configure the page blob directories key so every file created is a page blob.
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, "/");

    // Configure the atomic rename directories key so every folder will have
    // atomic rename applied.
    conf.set(AzureNativeFileSystemStore.KEY_ATOMIC_RENAME_DIRECTORIES, "/");
    return AzureBlobStorageTestAccount.create(conf);
  }

  @Before
  public void setUp() throws Exception {
    testAccount = createTestAccount();
    assumeNotNull(testAccount);
    fs = testAccount.getFileSystem();
    basePath = AzureTestUtils.pathForTests(fs, "filesystemcontractpageblob");
  }

  @Override
  public void tearDown() throws Exception {
    testAccount = AzureTestUtils.cleanup(testAccount);
    fs = null;
  }

  protected int getGlobalTimeout() {
    return AzureTestConstants.AZURE_TEST_TIMEOUT;
  }

  @Override
  public Path getTestBaseDir() {
    return basePath;
  }

  /**
   * The following tests are failing on Azure and the Azure 
   * file system code needs to be modified to make them pass.
   * A separate work item has been opened for this.
   */
  @Ignore
  @Test
  public void testMoveFileUnderParent() throws Throwable {
  }

  @Ignore
  @Test
  public void testRenameFileToSelf() throws Throwable {
  }
  
  @Ignore
  @Test
  public void testRenameChildDirForbidden() throws Exception {
  }
  
  @Ignore
  @Test
  public void testMoveDirUnderParent() throws Throwable {
  }
  
  @Ignore
  @Test
  public void testRenameDirToSelf() throws Throwable {
  }
}
