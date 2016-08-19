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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Test;

public class TestFileSystemOperationsExceptionHandlingMultiThreaded
    extends AbstractWasbTestBase {

  FSDataInputStream inputStream = null;

  private static Path testPath = new Path("testfile.dat");
  private static Path testFolderPath = new Path("testfolder");


  /*
   * Helper method to creates an input stream to test various scenarios.
   */
  private void getInputStreamToTest(FileSystem fs, Path testPath) throws Throwable {

    FSDataOutputStream outputStream = fs.create(testPath);
    String testString = "This is a test string";
    outputStream.write(testString.getBytes());
    outputStream.close();

    inputStream = fs.open(testPath);
  }

  /*
   * Test to validate correct exception is thrown for Multithreaded read
   * scenario for block blobs
   */
  @Test(expected=FileNotFoundException.class)
  public void testMultiThreadedBlockBlobReadScenario() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    fs = testAccount.getFileSystem();
    Path testFilePath1 = new Path("test1.dat");

    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(new RenameThread(fs, testFilePath1));
    renameThread.start();

    renameThread.join();

    byte[] readBuffer = new byte[512];
    inputStream.read(readBuffer);
  }

  /*
   * Test to validate correct exception is thrown for Multithreaded seek
   * scenario for block blobs
   */

  @Test(expected=FileNotFoundException.class)
  public void testMultiThreadBlockBlobSeekScenario() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    fs = testAccount.getFileSystem();
    Path testFilePath1 = new Path("test1.dat");

    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(new RenameThread(fs, testFilePath1));
    renameThread.start();

    renameThread.join();

    inputStream.seek(5);
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded setPermission scenario
   */
  public void testMultiThreadedPageBlobSetPermissionScenario() throws Throwable {
    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setPermission(testPath, new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
    }
    fs.setPermission(testPath, new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded setPermission scenario
   */
  public void testMultiThreadedBlockBlobSetPermissionScenario() throws Throwable {
    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setPermission(testPath, new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
    }
    fs.setPermission(testPath, new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded setPermission scenario
   */
  public void testMultiThreadedPageBlobOpenScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(),
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

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded setPermission scenario
   */
  public void testMultiThreadedBlockBlobOpenScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
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

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded setOwner scenario
   */
  public void testMultiThreadedBlockBlobSetOwnerScenario() throws Throwable {

    ExceptionHandlingTestHelper.createEmptyFile(createTestAccount(), testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setOwner(testPath, "testowner", "testgroup");
    }
    fs.setOwner(testPath, "testowner", "testgroup");
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded setOwner scenario
   */
  public void testMultiThreadedPageBlobSetOwnerScenario() throws Throwable {
    ExceptionHandlingTestHelper.createEmptyFile(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testPath);
    Thread t = new Thread(new DeleteThread(fs, testPath));
    t.start();
    while (t.isAlive()) {
      fs.setOwner(testPath, "testowner", "testgroup");
    }
    fs.setOwner(testPath, "testowner", "testgroup");
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded listStatus scenario
   */
  public void testMultiThreadedBlockBlobListStatusScenario() throws Throwable {

    ExceptionHandlingTestHelper.createTestFolder(createTestAccount(), testFolderPath);
    Thread t = new Thread(new DeleteThread(fs, testFolderPath));
    t.start();
    while (t.isAlive()) {
      fs.listStatus(testFolderPath);
    }
    fs.listStatus(testFolderPath);
  }

  @Test(expected=FileNotFoundException.class)
  /*
   * Tests basic multi threaded listStatus scenario
   */
  public void testMultiThreadedPageBlobListStatusScenario() throws Throwable {

    ExceptionHandlingTestHelper.createTestFolder(ExceptionHandlingTestHelper.getPageBlobTestStorageAccount(),
        testFolderPath);
    Thread t = new Thread(new DeleteThread(fs, testFolderPath));
    t.start();
    while (t.isAlive()) {
      fs.listStatus(testFolderPath);
    }
    fs.listStatus(testFolderPath);
  }

  /*
   * Test to validate correct exception is thrown for Multithreaded read
   * scenario for page blobs
   */

  @Test(expected=FileNotFoundException.class)
  public void testMultiThreadedPageBlobReadScenario() throws Throwable {

    AzureBlobStorageTestAccount testAccount = ExceptionHandlingTestHelper.getPageBlobTestStorageAccount();
    fs = testAccount.getFileSystem();
    Path testFilePath1 = new Path("test1.dat");

    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(new RenameThread(fs, testFilePath1));
    renameThread.start();

    renameThread.join();
    byte[] readBuffer = new byte[512];
    inputStream.read(readBuffer);
  }

  /*
   * Test to validate correct exception is thrown for Multithreaded seek
   * scenario for page blobs
   */

  @Test(expected=FileNotFoundException.class)
  public void testMultiThreadedPageBlobSeekScenario() throws Throwable {

    AzureBlobStorageTestAccount testAccount = ExceptionHandlingTestHelper.getPageBlobTestStorageAccount();
    fs = testAccount.getFileSystem();
    Path testFilePath1 = new Path("test1.dat");

    getInputStreamToTest(fs, testFilePath1);
    Thread renameThread = new Thread(new RenameThread(fs, testFilePath1));
    renameThread.start();

    renameThread.join();
    inputStream.seek(5);
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
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
}

/*
 * Helper thread that just renames the test file.
 */
class RenameThread implements Runnable {

  private FileSystem fs;
  private Path testPath;
  private Path renamePath = new Path("test2.dat");

  public RenameThread(FileSystem fs, Path testPath) {
    this.fs = fs;
    this.testPath = testPath;
  }

  @Override
  public void run(){
    try {
      fs.rename(testPath, renamePath);
    }catch (Exception e) {
      // Swallowing the exception as the
      // correctness of the test is controlled
      // by the other thread
    }
  }
}

class DeleteThread implements Runnable {
  private FileSystem fs;
  private Path testPath;

  public DeleteThread(FileSystem fs, Path testPath) {
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
