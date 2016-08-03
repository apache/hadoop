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

import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;


public class TestFileSystemOperationExceptionHandling
    extends AbstractWasbTestBase {

  private FSDataInputStream inputStream = null;

  private static Path testPath = new Path("testfile.dat");

  private static Path testFolderPath = new Path("testfolder");

  /*
   * Helper method that creates a InputStream to validate exceptions
   * for various scenarios
   */
  private void setupInputStreamToTest(AzureBlobStorageTestAccount testAccount)
      throws Exception {

    FileSystem fs = testAccount.getFileSystem();

    // Step 1: Create a file and write dummy data.
    Path testFilePath1 = new Path("test1.dat");
    Path testFilePath2 = new Path("test2.dat");
    FSDataOutputStream outputStream = fs.create(testFilePath1);
    String testString = "This is a test string";
    outputStream.write(testString.getBytes());
    outputStream.close();

    // Step 2: Open a read stream on the file.
    inputStream = fs.open(testFilePath1);

    // Step 3: Rename the file
    fs.rename(testFilePath1, testFilePath2);
  }

  /*
   * Tests a basic single threaded read scenario for Page blobs.
   */
  @Test(expected=FileNotFoundException.class)
  public void testSingleThreadedPageBlobReadScenario() throws Throwable {
    AzureBlobStorageTestAccount testAccount = ExceptionHandlingTestHelper.getPageBlobTestStorageAccount();
    setupInputStreamToTest(testAccount);
    byte[] readBuffer = new byte[512];
    inputStream.read(readBuffer);
  }

  /*
   * Tests a basic single threaded seek scenario for Page blobs.
   */
  @Test(expected=FileNotFoundException.class)
  public void testSingleThreadedPageBlobSeekScenario() throws Throwable {
    AzureBlobStorageTestAccount testAccount = ExceptionHandlingTestHelper.getPageBlobTestStorageAccount();
    setupInputStreamToTest(testAccount);
    inputStream.seek(5);
  }

  /*
   * Test a basic single thread seek scenario for Block blobs.
   */
  @Test(expected=FileNotFoundException.class)
  public void testSingleThreadBlockBlobSeekScenario() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    setupInputStreamToTest(testAccount);
    inputStream.seek(5);
  }

  /*
   * Tests a basic single threaded read scenario for Block blobs.
   */
  @Test(expected=FileNotFoundException.class)
  public void testSingledThreadBlockBlobReadScenario() throws Throwable{
    AzureBlobStorageTestAccount testAccount = createTestAccount();
    setupInputStreamToTest(testAccount);
    byte[] readBuffer = new byte[512];
    inputStream.read(readBuffer);
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic single threaded setPermission scenario
   */
  public void testSingleThreadedBlockBlobSetPermissionScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(), testPath);
    fs.delete(testPath, true);
    fs.setPermission(testPath, new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic single threaded setPermission scenario
   */
  public void testSingleThreadedPageBlobSetPermissionScenario() throws Throwable {
    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testPath);
    fs.delete(testPath, true);
    fs.setOwner(testPath, "testowner", "testgroup");
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic single threaded setPermission scenario
   */
  public void testSingleThreadedBlockBlobSetOwnerScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(), testPath);
    fs.delete(testPath, true);
    fs.setOwner(testPath, "testowner", "testgroup");
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic single threaded setPermission scenario
   */
  public void testSingleThreadedPageBlobSetOwnerScenario() throws Throwable {
    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testPath);
    fs.delete(testPath, true);
    fs.setPermission(testPath, new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Test basic single threaded listStatus scenario
   */
  public void testSingleThreadedBlockBlobListStatusScenario() throws Throwable {
    ExceptionHandlingTestHelper.createTestFolder(createTestAccount(), testFolderPath);
    fs.delete(testFolderPath, true);
    fs.listStatus(testFolderPath);
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Test basica single threaded listStatus scenario
   */
  public void testSingleThreadedPageBlobListStatusScenario() throws Throwable {
    ExceptionHandlingTestHelper.createTestFolder(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testFolderPath);
    fs.delete(testFolderPath, true);
    fs.listStatus(testFolderPath);
  }

  @Test
  /*
   * Test basic single threaded listStatus scenario
   */
  public void testSingleThreadedBlockBlobRenameScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(),
        testPath);
    Path dstPath = new Path("dstFile.dat");
    fs.delete(testPath, true);
    boolean renameResult = fs.rename(testPath, dstPath);
    Assert.assertFalse(renameResult);
  }

  @Test
  /*
   * Test basic single threaded listStatus scenario
   */
  public void testSingleThreadedPageBlobRenameScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testPath);
    Path dstPath = new Path("dstFile.dat");
    fs.delete(testPath, true);
    boolean renameResult = fs.rename(testPath, dstPath);
    Assert.assertFalse(renameResult);
  }

  @Test
  /*
   * Test basic single threaded listStatus scenario
   */
  public void testSingleThreadedBlockBlobDeleteScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(),
        testPath);
    fs.delete(testPath, true);
    boolean deleteResult = fs.delete(testPath, true);
    Assert.assertFalse(deleteResult);
  }

  @Test
  /*
   * Test basic single threaded listStatus scenario
   */
  public void testSingleThreadedPageBlobDeleteScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testPath);
    fs.delete(testPath, true);
    boolean deleteResult = fs.delete(testPath, true);
    Assert.assertFalse(deleteResult);
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Test basic single threaded listStatus scenario
   */
  public void testSingleThreadedBlockBlobOpenScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(),
        testPath);
    fs.delete(testPath, true);
    inputStream = fs.open(testPath);
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Test basic single threaded listStatus scenario
   */
  public void testSingleThreadedPageBlobOpenScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testPath);
    fs.delete(testPath, true);
    inputStream = fs.open(testPath);
  }

  @After
  public void tearDown() throws Exception {
    if (inputStream != null) {
      inputStream.close();
    }

    if (fs != null && fs.exists(testPath)) {
      fs.delete(testPath, true);
    }
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }
}
