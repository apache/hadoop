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

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azure.ExceptionHandlingTestHelper.*;

/**
 * Multithreaded operations on FS, verify failures are as expected.
 */
public class ITestFileSystemOperationsExceptionHandlingMultiThreaded
    extends AbstractWasbTestBase {

  FSDataInputStream inputStream = null;

  private Path testPath;
  private Path testFolderPath;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    testPath = path("testfile.dat");
    testFolderPath = path("testfolder");
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  @Override
  public void tearDown() throws Exception {

    IOUtils.closeStream(inputStream);
    ContractTestUtils.rm(fs, testPath, true, false);
    ContractTestUtils.rm(fs, testFolderPath, true, false);
    super.tearDown();
  }

  /**
   * Helper method to creates an input stream to test various scenarios.
   */
  private void getInputStreamToTest(FileSystem fs, Path testPath)
      throws Throwable {

    FSDataOutputStream outputStream = fs.create(testPath);
    String testString = "This is a test string";
    outputStream.write(testString.getBytes());
    outputStream.close();

    inputStream = fs.open(testPath);
  }

  /**
   * Test to validate correct exception is thrown for Multithreaded read
   * scenario for block blobs.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedBlockBlobReadScenario() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    Path base = methodPath();
    Path testFilePath1 = new Path(base, "test1.dat");
    Path renamePath = new Path(base, "test2.dat");
    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(
        new RenameThread(fs, testFilePath1, renamePath));
    renameThread.start();

    renameThread.join();

    byte[] readBuffer = new byte[512];
    inputStream.read(readBuffer);
  }

  /**
   * Test to validate correct exception is thrown for Multithreaded seek
   * scenario for block blobs.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadBlockBlobSeekScenario() throws Throwable {

/*
    AzureBlobStorageTestAccount testAccount = createTestAccount();
    fs = testAccount.getFileSystem();
*/
    Path base = methodPath();
    Path testFilePath1 = new Path(base, "test1.dat");
    Path renamePath = new Path(base, "test2.dat");

    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(
        new RenameThread(fs, testFilePath1, renamePath));
    renameThread.start();

    renameThread.join();

    inputStream.seek(5);
    inputStream.read();
  }

  /**
   * Tests basic multi threaded setPermission scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedPageBlobSetPermissionScenario()
      throws Throwable {
    createEmptyFile(
        getPageBlobTestStorageAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setPermission(testPath,
          new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
    }
    fs.setPermission(testPath,
        new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
  }

  /**
   * Tests basic multi threaded setPermission scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedBlockBlobSetPermissionScenario()
      throws Throwable {
    createEmptyFile(createTestAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setPermission(testPath,
          new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
    }
    fs.setPermission(testPath,
        new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
  }

  /**
   * Tests basic multi threaded setPermission scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedPageBlobOpenScenario() throws Throwable {

    createEmptyFile(createTestAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      inputStream = fs.open(testPath);
      inputStream.close();
    }

    inputStream = fs.open(testPath);
    inputStream.close();
  }

  /**
   * Tests basic multi threaded setPermission scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedBlockBlobOpenScenario() throws Throwable {

    createEmptyFile(
        getPageBlobTestStorageAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();

    while (t.isAlive()) {
      inputStream = fs.open(testPath);
      inputStream.close();
    }
    inputStream = fs.open(testPath);
    inputStream.close();
  }

  /**
   * Tests basic multi threaded setOwner scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedBlockBlobSetOwnerScenario() throws Throwable {

    createEmptyFile(createTestAccount(), testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setOwner(testPath, "testowner", "testgroup");
    }
    fs.setOwner(testPath, "testowner", "testgroup");
  }

  /**
   * Tests basic multi threaded setOwner scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedPageBlobSetOwnerScenario() throws Throwable {
    createEmptyFile(
        getPageBlobTestStorageAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setOwner(testPath, "testowner", "testgroup");
    }
    fs.setOwner(testPath, "testowner", "testgroup");
  }

  /**
   * Tests basic multi threaded listStatus scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedBlockBlobListStatusScenario() throws Throwable {

    createTestFolder(createTestAccount(),
        testFolderPath);
    Thread t = new Thread(new DeleteThread(fs, testFolderPath));
    t.start();
    while (t.isAlive()) {
      fs.listStatus(testFolderPath);
    }
    fs.listStatus(testFolderPath);
  }

  /**
   * Tests basic multi threaded listStatus scenario.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedPageBlobListStatusScenario() throws Throwable {

    createTestFolder(
        getPageBlobTestStorageAccount(),
        testFolderPath);
    Thread t = new Thread(new DeleteThread(fs, testFolderPath));
    t.start();
    while (t.isAlive()) {
      fs.listStatus(testFolderPath);
    }
    fs.listStatus(testFolderPath);
  }

  /**
   * Test to validate correct exception is thrown for Multithreaded read
   * scenario for page blobs.
   */
  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedPageBlobReadScenario() throws Throwable {

    bindToTestAccount(getPageBlobTestStorageAccount());
    Path base = methodPath();
    Path testFilePath1 = new Path(base, "test1.dat");
    Path renamePath = new Path(base, "test2.dat");

    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(
        new RenameThread(fs, testFilePath1, renamePath));
    renameThread.start();

    renameThread.join();
    byte[] readBuffer = new byte[512];
    inputStream.read(readBuffer);
  }

  /**
   * Test to validate correct exception is thrown for Multithreaded seek
   * scenario for page blobs.
   */

  @Test(expected = FileNotFoundException.class)
  public void testMultiThreadedPageBlobSeekScenario() throws Throwable {

    bindToTestAccount(getPageBlobTestStorageAccount());

    Path base = methodPath();
    Path testFilePath1 = new Path(base, "test1.dat");
    Path renamePath = new Path(base, "test2.dat");

    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(
        new RenameThread(fs, testFilePath1, renamePath));
    renameThread.start();

    renameThread.join();
    inputStream.seek(5);
  }


  /**
   * Helper thread that just renames the test file.
   */
  private static class RenameThread implements Runnable {

    private final FileSystem fs;
    private final Path testPath;
    private final Path renamePath;

    RenameThread(FileSystem fs,
        Path testPath,
        Path renamePath) {
      this.fs = fs;
      this.testPath = testPath;
      this.renamePath = renamePath;
    }

    @Override
    public void run() {
      try {
        fs.rename(testPath, renamePath);
      } catch (Exception e) {
        // Swallowing the exception as the
        // correctness of the test is controlled
        // by the other thread
      }
    }
  }

  private static class DeleteThread implements Runnable {
    private final FileSystem fs;
    private final Path testPath;

    DeleteThread(FileSystem fs, Path testPath) {
      this.fs = fs;
      this.testPath = testPath;
    }

    @Override
    public void run() {
      try {
        fs.delete(testPath, true);
      } catch (Exception e) {
        // Swallowing the exception as the
        // correctness of the test is controlled
        // by the other thread
      }
    }
  }
}
