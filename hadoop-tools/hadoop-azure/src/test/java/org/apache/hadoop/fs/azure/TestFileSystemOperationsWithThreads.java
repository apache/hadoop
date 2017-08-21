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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem.FolderRenamePending;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests the Native Azure file system (WASB) using parallel threads for rename and delete operations.
 */
public class TestFileSystemOperationsWithThreads extends AbstractWasbTestBase {

  private final int renameThreads = 10;
  private final int deleteThreads = 20;
  private int iterations = 1;
  private LogCapturer logs = null;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = fs.getConf();

    // By default enable parallel threads for rename and delete operations.
    // Also enable flat listing of blobs for these operations.
    conf.setInt(NativeAzureFileSystem.AZURE_RENAME_THREADS, renameThreads);
    conf.setInt(NativeAzureFileSystem.AZURE_DELETE_THREADS, deleteThreads);
    conf.setBoolean(AzureNativeFileSystemStore.KEY_ENABLE_FLAT_LISTING, true);

    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    // Capture logs
    logs = LogCapturer.captureLogs(new Log4JLogger(org.apache.log4j.Logger
        .getRootLogger()));
  }

  /*
   * Helper method to create sub directory and different types of files
   * for multiple iterations.
   */
  private void createFolder(FileSystem fs, String root) throws Exception {
    fs.mkdirs(new Path(root));
    for (int i = 0; i < this.iterations; i++) {
      fs.mkdirs(new Path(root + "/" + i));
      fs.createNewFile(new Path(root + "/" + i + "/fileToRename"));
      fs.createNewFile(new Path(root + "/" + i + "/file/to/rename"));
      fs.createNewFile(new Path(root + "/" + i + "/file+to%rename"));
      fs.createNewFile(new Path(root + "/fileToRename" + i));
    }
  }

  /*
   * Helper method to do rename operation and validate all files in source folder
   * doesn't exists and similar files exists in new folder.
   */
  private void validateRenameFolder(FileSystem fs, String source, String dest) throws Exception {
    // Create source folder with files.
    createFolder(fs, source);
    Path sourceFolder = new Path(source);
    Path destFolder = new Path(dest);

    // rename operation
    assertTrue(fs.rename(sourceFolder, destFolder));
    assertTrue(fs.exists(destFolder));

    for (int i = 0; i < this.iterations; i++) {
      // Check destination folder and files exists.
      assertTrue(fs.exists(new Path(dest + "/" + i)));
      assertTrue(fs.exists(new Path(dest + "/" + i + "/fileToRename")));
      assertTrue(fs.exists(new Path(dest + "/" + i + "/file/to/rename")));
      assertTrue(fs.exists(new Path(dest + "/" + i + "/file+to%rename")));
      assertTrue(fs.exists(new Path(dest + "/fileToRename" + i)));

      // Check source folder and files doesn't exists.
      assertFalse(fs.exists(new Path(source + "/" + i)));
      assertFalse(fs.exists(new Path(source + "/" + i + "/fileToRename")));
      assertFalse(fs.exists(new Path(source + "/" + i + "/file/to/rename")));
      assertFalse(fs.exists(new Path(source + "/" + i + "/file+to%rename")));
      assertFalse(fs.exists(new Path(source + "/fileToRename" + i)));
    }
  }

  /*
   * Test case for rename operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testRenameSmallFolderWithThreads() throws Exception {

    validateRenameFolder(fs, "root", "rootnew");

    // With single iteration, we would have created 7 blobs.
    int expectedThreadsCreated = Math.min(7, renameThreads);

    // Validate from logs that threads are created.
    String content = logs.getOutput();
    assertInLog(content, "ms with threads: " + expectedThreadsCreated);

    // Validate thread executions
    for (int i = 0; i < expectedThreadsCreated; i++) {
      assertInLog(content,
          "AzureBlobRenameThread-" + Thread.currentThread().getName() + "-" + i);
    }

    // Also ensure that we haven't spawned extra threads.
    if (expectedThreadsCreated < renameThreads) {
      for (int i = expectedThreadsCreated; i < renameThreads; i++) {
        assertNotInLog(content,
            "AzureBlobRenameThread-" + Thread.currentThread().getName() + "-" + i);
      }
    }
  }

  /*
   * Test case for rename operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testRenameLargeFolderWithThreads() throws Exception {

    // Populate source folder with large number of files and directories.
    this.iterations = 10;
    validateRenameFolder(fs, "root", "rootnew");

    // Validate from logs that threads are created.
    String content = logs.getOutput();
    assertInLog(content, "ms with threads: " + renameThreads);

    // Validate thread executions
    for (int i = 0; i < renameThreads; i++) {
      assertInLog(content,
          "AzureBlobRenameThread-" + Thread.currentThread().getName() + "-" + i);
    }
  }

  /*
   * Test case for rename operation with threads disabled and flat listing enabled.
   */
  @Test
  public void testRenameLargeFolderDisableThreads() throws Exception {
    Configuration conf = fs.getConf();

    // Number of threads set to 0 or 1 disables threads.
    conf.setInt(NativeAzureFileSystem.AZURE_RENAME_THREADS, 0);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    // Populate source folder with large number of files and directories.
    this.iterations = 10;
    validateRenameFolder(fs, "root", "rootnew");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content,
        "Disabling threads for Rename operation as thread count 0");

    // Validate no thread executions
    for (int i = 0; i < renameThreads; i++) {
      String term = "AzureBlobRenameThread-"
          + Thread.currentThread().getName()
          + "-" + i;
      assertNotInLog(content, term);
    }
  }

  /**
   * Assert that a log contains the given term.
   * @param content log output
   * @param term search term
   */
  protected void assertInLog(String content, String term) {
    assertTrue("Empty log", !content.isEmpty());
    if (!content.contains(term)) {
      String message = "No " + term + " found in logs";
      LOG.error(message);
      System.err.println(content);
      fail(message);
    }
  }

  /**
   * Assert that a log does not contain the given term.
   * @param content log output
   * @param term search term
   */
  protected void assertNotInLog(String content, String term) {
    assertTrue("Empty log", !content.isEmpty());
    if (content.contains(term)) {
      String message = term + " found in logs";
      LOG.error(message);
      System.err.println(content);
      fail(message);
    }
  }

  /*
   * Test case for rename operation with threads and flat listing disabled.
   */
  @Test
  public void testRenameSmallFolderDisableThreadsDisableFlatListing() throws Exception {
    Configuration conf = fs.getConf();
    conf = fs.getConf();

    // Number of threads set to 0 or 1 disables threads.
    conf.setInt(NativeAzureFileSystem.AZURE_RENAME_THREADS, 1);
    conf.setBoolean(AzureNativeFileSystemStore.KEY_ENABLE_FLAT_LISTING, false);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    validateRenameFolder(fs, "root", "rootnew");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content,
        "Disabling threads for Rename operation as thread count 1");

    // Validate no thread executions
    for (int i = 0; i < renameThreads; i++) {
      assertNotInLog(content,
          "AzureBlobRenameThread-" + Thread.currentThread().getName() + "-" + i);
    }
  }

  /*
   * Helper method to do delete operation and validate all files in source folder
   * doesn't exists after delete operation.
   */
  private void validateDeleteFolder(FileSystem fs, String source)  throws Exception {
    // Create folder with files.
    createFolder(fs, "root");
    Path sourceFolder = new Path(source);

    // Delete operation
    assertTrue(fs.delete(sourceFolder, true));
    assertFalse(fs.exists(sourceFolder));

    for (int i = 0; i < this.iterations; i++) {
      // check that source folder and files doesn't exists
      assertFalse(fs.exists(new Path(source + "/" + i)));
      assertFalse(fs.exists(new Path(source + "/" + i + "/fileToRename")));
      assertFalse(fs.exists(new Path(source + "/" + i + "/file/to/rename")));
      assertFalse(fs.exists(new Path(source + "/" + i + "/file+to%rename")));
      assertFalse(fs.exists(new Path(source + "/fileToRename" + i)));
    }
  }

  /*
   * Test case for delete operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testDeleteSmallFolderWithThreads() throws Exception {

    validateDeleteFolder(fs, "root");

    // With single iteration, we would have created 7 blobs.
    int expectedThreadsCreated = Math.min(7, deleteThreads);

    // Validate from logs that threads are enabled.
    String content = logs.getOutput();
    assertInLog(content, "ms with threads: " + expectedThreadsCreated);

    // Validate thread executions
    for (int i = 0; i < expectedThreadsCreated; i++) {
      assertInLog(content,
          "AzureBlobDeleteThread-" + Thread.currentThread().getName() + "-" + i);
    }

    // Also ensure that we haven't spawned extra threads.
    if (expectedThreadsCreated < deleteThreads) {
      for (int i = expectedThreadsCreated; i < deleteThreads; i++) {
        assertNotInLog(content,
            "AzureBlobDeleteThread-" + Thread.currentThread().getName() + "-" + i);
      }
    }
  }

  /*
   * Test case for delete operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testDeleteLargeFolderWithThreads() throws Exception {
    // Populate source folder with large number of files and directories.
    this.iterations = 10;
    validateDeleteFolder(fs, "root");

    // Validate from logs that threads are enabled.
    String content = logs.getOutput();
    assertInLog(content, "ms with threads: " + deleteThreads);

    // Validate thread executions
    for (int i = 0; i < deleteThreads; i++) {
      assertInLog(content,
          "AzureBlobDeleteThread-" + Thread.currentThread().getName() + "-" + i);
    }
  }

  /*
   * Test case for delete operation with threads disabled and flat listing enabled.
   */
  @Test
  public void testDeleteLargeFolderDisableThreads() throws Exception {
    Configuration conf = fs.getConf();
    conf.setInt(NativeAzureFileSystem.AZURE_DELETE_THREADS, 0);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    // Populate source folder with large number of files and directories.
    this.iterations = 10;
    validateDeleteFolder(fs, "root");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content,
        "Disabling threads for Delete operation as thread count 0");

    // Validate no thread executions
    for (int i = 0; i < deleteThreads; i++) {
      assertNotInLog(content,
          "AzureBlobDeleteThread-" + Thread.currentThread().getName() + "-" + i);
    }
  }

  /*
   * Test case for rename operation with threads and flat listing disabled.
   */
  @Test
  public void testDeleteSmallFolderDisableThreadsDisableFlatListing() throws Exception {
    Configuration conf = fs.getConf();

    // Number of threads set to 0 or 1 disables threads.
    conf.setInt(NativeAzureFileSystem.AZURE_DELETE_THREADS, 1);
    conf.setBoolean(AzureNativeFileSystemStore.KEY_ENABLE_FLAT_LISTING, false);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    validateDeleteFolder(fs, "root");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content,
        "Disabling threads for Delete operation as thread count 1");

    // Validate no thread executions
    for (int i = 0; i < deleteThreads; i++) {
      assertNotInLog(content,
          "AzureBlobDeleteThread-" + Thread.currentThread().getName() + "-" + i);
    }
  }

  /*
   * Test case for delete operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testDeleteThreadPoolExceptionFailure() throws Exception {

    // Spy azure file system object and raise exception for new thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));

    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        mockFs.getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
            path, NativeAzureFileSystem.AZURE_DELETE_THREADS));
    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenThrow(new Exception());

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.when(mockFs.getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
        path, NativeAzureFileSystem.AZURE_DELETE_THREADS)).thenReturn(mockThreadPoolExecutor);

    validateDeleteFolder(mockFs, "root");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content, "Failed to create thread pool with threads");
    assertInLog(content, "Serializing the Delete operation");
  }

  /*
   * Test case for delete operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testDeleteThreadPoolExecuteFailure() throws Exception {

    // Mock thread pool executor to throw exception for all requests.
    ThreadPoolExecutor mockThreadExecutor = Mockito.mock(ThreadPoolExecutor.class);
    Mockito.doThrow(new RejectedExecutionException()).when(mockThreadExecutor).execute(Mockito.any(Runnable.class));

    // Spy azure file system object and return mocked thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));

    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        mockFs.getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
            path, NativeAzureFileSystem.AZURE_DELETE_THREADS));
    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenReturn(mockThreadExecutor);

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.when(mockFs.getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
        path, NativeAzureFileSystem.AZURE_DELETE_THREADS)).thenReturn(mockThreadPoolExecutor);

    validateDeleteFolder(mockFs, "root");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content,
        "Rejected execution of thread for Delete operation on blob");
    assertInLog(content, "Serializing the Delete operation");
  }

  /*
   * Test case for delete operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testDeleteThreadPoolExecuteSingleThreadFailure() throws Exception {

    // Spy azure file system object and return mocked thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    // Spy a thread pool executor and link it to azure file system object.
    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));
    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        mockFs.getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
            path, NativeAzureFileSystem.AZURE_DELETE_THREADS));

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.when(mockFs.getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
        path, NativeAzureFileSystem.AZURE_DELETE_THREADS)).thenReturn(mockThreadPoolExecutor);

    // Create a thread executor and link it to mocked thread pool executor object.
    ThreadPoolExecutor mockThreadExecutor = Mockito.spy(mockThreadPoolExecutor.getThreadPool(7));
    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenReturn(mockThreadExecutor);

    // Mock thread executor to throw exception for all requests.
    Mockito.doCallRealMethod().doThrow(new RejectedExecutionException()).when(mockThreadExecutor).execute(Mockito.any(Runnable.class));

    validateDeleteFolder(mockFs, "root");

    // Validate from logs that threads are enabled and unused threads.
    String content = logs.getOutput();
    assertInLog(content,
        "Using thread pool for Delete operation with threads 7");
    assertInLog(content,
        "6 threads not used for Delete operation on blob");
  }

  /*
   * Test case for delete operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testDeleteThreadPoolTerminationFailure() throws Exception {

    // Spy azure file system object and return mocked thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    // Spy a thread pool executor and link it to azure file system object.
    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));
    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        ((NativeAzureFileSystem) fs).getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
            path, NativeAzureFileSystem.AZURE_DELETE_THREADS));

    // Create a thread executor and link it to mocked thread pool executor object.
    // Mock thread executor to throw exception for terminating threads.
    ThreadPoolExecutor mockThreadExecutor = Mockito.mock(ThreadPoolExecutor.class);
    Mockito.doNothing().when(mockThreadExecutor).execute(Mockito.any(Runnable.class));
    Mockito.when(mockThreadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)).thenThrow(new InterruptedException());

    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenReturn(mockThreadExecutor);

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.when(mockFs.getThreadPoolExecutor(deleteThreads, "AzureBlobDeleteThread", "Delete",
        path, NativeAzureFileSystem.AZURE_DELETE_THREADS)).thenReturn(mockThreadPoolExecutor);

    createFolder(mockFs, "root");
    Path sourceFolder = new Path("root");
    boolean exception = false;
    try {
      mockFs.delete(sourceFolder, true);
    } catch (IOException e){
      exception = true;
    }

    assertTrue(exception);
    assertTrue(mockFs.exists(sourceFolder));

    // Validate from logs that threads are enabled and delete operation is failed.
    String content = logs.getOutput();
    assertInLog(content,
        "Using thread pool for Delete operation with threads");
    assertInLog(content, "Threads got interrupted Delete blob operation");
    assertInLog(content,
        "Delete failed as operation on subfolders and files failed.");
  }

  /*
   * Validate that when a directory is deleted recursively, the operation succeeds
   * even if a child directory delete fails because the directory does not exist.
   * This can happen if a child directory is deleted by an external agent while
   * the parent is in progress of being deleted recursively.
   */
  @Test
  public void testRecursiveDirectoryDeleteWhenChildDirectoryDeleted()
      throws Exception {
    testRecusiveDirectoryDelete(true);
  }

  /*
   * Validate that when a directory is deleted recursively, the operation succeeds
   * even if a file delete fails because it does not exist.
   * This can happen if a file is deleted by an external agent while
   * the parent directory is in progress of being deleted.
   */
  @Test
  public void testRecursiveDirectoryDeleteWhenDeletingChildFileReturnsFalse()
      throws Exception {
    testRecusiveDirectoryDelete(false);
  }

  private void testRecusiveDirectoryDelete(boolean useDir) throws Exception {
    String childPathToBeDeletedByExternalAgent = (useDir)
        ? "root/0"
        : "root/0/fileToRename";
    // Spy azure file system object and return false for deleting one file
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);
    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path(
        childPathToBeDeletedByExternalAgent)));

    Answer<Boolean> answer = new Answer<Boolean>() {
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        String path = (String) invocation.getArguments()[0];
        boolean isDir = (boolean) invocation.getArguments()[1];
        boolean realResult = fs.deleteFile(path, isDir);
        assertTrue(realResult);
        boolean fakeResult = false;
        return fakeResult;
      }
    };

    Mockito.when(mockFs.deleteFile(path, useDir)).thenAnswer(answer);

    createFolder(mockFs, "root");
    Path sourceFolder = new Path("root");

    assertTrue(mockFs.delete(sourceFolder, true));
    assertFalse(mockFs.exists(sourceFolder));

    // Validate from logs that threads are enabled, that a child directory was
    // deleted by an external caller, and the parent delete operation still
    // succeeds.
    String content = logs.getOutput();
    assertInLog(content,
        "Using thread pool for Delete operation with threads");
    assertInLog(content, String.format("Attempt to delete non-existent %s %s",
        useDir ? "directory" : "file", path));
  }

  /*
   * Test case for delete operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testDeleteSingleDeleteException() throws Exception {

    // Spy azure file system object and raise exception for deleting one file
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);
    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root/0")));
    Mockito.doThrow(new IOException()).when(mockFs).deleteFile(path, true);

    createFolder(mockFs, "root");
    Path sourceFolder = new Path("root");

    boolean exception = false;
    try {
      mockFs.delete(sourceFolder, true);
    } catch (IOException e){
      exception = true;
    }

    assertTrue(exception);
    assertTrue(mockFs.exists(sourceFolder));

    // Validate from logs that threads are enabled and delete operation failed.
    String content = logs.getOutput();
    assertInLog(content,
        "Using thread pool for Delete operation with threads");
    assertInLog(content,
        "Encountered Exception for Delete operation for file " + path);
    assertInLog(content,
        "Terminating execution of Delete operation now as some other thread already got exception or operation failed");
  }

  /*
   * Test case for rename operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testRenameThreadPoolExceptionFailure() throws Exception {

    // Spy azure file system object and raise exception for new thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));
    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        ((NativeAzureFileSystem) fs).getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
            path, NativeAzureFileSystem.AZURE_RENAME_THREADS));
    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenThrow(new Exception());

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.doReturn(mockThreadPoolExecutor).when(mockFs).getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
        path, NativeAzureFileSystem.AZURE_RENAME_THREADS);

    validateRenameFolder(mockFs, "root", "rootnew");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content, "Failed to create thread pool with threads");
    assertInLog(content, "Serializing the Rename operation");
  }

  /*
   * Test case for rename operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testRenameThreadPoolExecuteFailure() throws Exception {

    // Mock thread pool executor to throw exception for all requests.
    ThreadPoolExecutor mockThreadExecutor = Mockito.mock(ThreadPoolExecutor.class);
    Mockito.doThrow(new RejectedExecutionException()).when(mockThreadExecutor).execute(Mockito.any(Runnable.class));

    // Spy azure file system object and return mocked thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));
    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        mockFs.getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
            path, NativeAzureFileSystem.AZURE_RENAME_THREADS));
    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenReturn(mockThreadExecutor);

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.when(mockFs.getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
        path, NativeAzureFileSystem.AZURE_RENAME_THREADS)).thenReturn(mockThreadPoolExecutor);

    validateRenameFolder(mockFs, "root", "rootnew");

    // Validate from logs that threads are disabled.
    String content = logs.getOutput();
    assertInLog(content,
        "Rejected execution of thread for Rename operation on blob");
    assertInLog(content, "Serializing the Rename operation");
  }

  /*
   * Test case for rename operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testRenameThreadPoolExecuteSingleThreadFailure() throws Exception {

    // Spy azure file system object and return mocked thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    // Spy a thread pool executor and link it to azure file system object.
    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));
    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        mockFs.getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
            path, NativeAzureFileSystem.AZURE_RENAME_THREADS));

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.when(mockFs.getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
        path, NativeAzureFileSystem.AZURE_RENAME_THREADS)).thenReturn(mockThreadPoolExecutor);

    // Create a thread executor and link it to mocked thread pool executor object.
    ThreadPoolExecutor mockThreadExecutor = Mockito.spy(mockThreadPoolExecutor.getThreadPool(7));
    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenReturn(mockThreadExecutor);

    // Mock thread executor to throw exception for all requests.
    Mockito.doCallRealMethod().doThrow(new RejectedExecutionException()).when(mockThreadExecutor).execute(Mockito.any(Runnable.class));

    validateRenameFolder(mockFs, "root", "rootnew");

    // Validate from logs that threads are enabled and unused threads exists.
    String content = logs.getOutput();
    assertInLog(content,
        "Using thread pool for Rename operation with threads 7");
    assertInLog(content,
        "6 threads not used for Rename operation on blob");
  }

  /*
   * Test case for rename operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testRenameThreadPoolTerminationFailure() throws Exception {

    // Spy azure file system object and return mocked thread pool
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    // Spy a thread pool executor and link it to azure file system object.
    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root")));
    AzureFileSystemThreadPoolExecutor mockThreadPoolExecutor = Mockito.spy(
        mockFs.getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
            path, NativeAzureFileSystem.AZURE_RENAME_THREADS));

    // With single iteration, we would have created 7 blobs resulting 7 threads.
    Mockito.when(mockFs.getThreadPoolExecutor(renameThreads, "AzureBlobRenameThread", "Rename",
        path, NativeAzureFileSystem.AZURE_RENAME_THREADS)).thenReturn(mockThreadPoolExecutor);

    // Mock thread executor to throw exception for all requests.
    ThreadPoolExecutor mockThreadExecutor = Mockito.mock(ThreadPoolExecutor.class);
    Mockito.doNothing().when(mockThreadExecutor).execute(Mockito.any(Runnable.class));
    Mockito.when(mockThreadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)).thenThrow(new InterruptedException());
    Mockito.when(mockThreadPoolExecutor.getThreadPool(7)).thenReturn(mockThreadExecutor);


    createFolder(mockFs, "root");
    Path sourceFolder = new Path("root");
    Path destFolder = new Path("rootnew");
    boolean exception = false;
    try {
      mockFs.rename(sourceFolder, destFolder);
    } catch (IOException e){
      exception = true;
    }

    assertTrue(exception);
    assertTrue(mockFs.exists(sourceFolder));

    // Validate from logs that threads are enabled and rename operation is failed.
    String content = logs.getOutput();
    assertInLog(content,
        "Using thread pool for Rename operation with threads");
    assertInLog(content, "Threads got interrupted Rename blob operation");
    assertInLog(content,
        "Rename failed as operation on subfolders and files failed.");
  }

  /*
   * Test case for rename operation with multiple threads and flat listing enabled.
   */
  @Test
  public void testRenameSingleRenameException() throws Exception {

    // Spy azure file system object and raise exception for deleting one file
    Path sourceFolder = new Path("root");
    Path destFolder = new Path("rootnew");

    // Spy azure file system object and populate rename pending spy object.
    NativeAzureFileSystem mockFs = Mockito.spy((NativeAzureFileSystem) fs);

    // Populate data now only such that rename pending spy object would see this data.
    createFolder(mockFs, "root");

    String srcKey = mockFs.pathToKey(mockFs.makeAbsolute(sourceFolder));
    String dstKey = mockFs.pathToKey(mockFs.makeAbsolute(destFolder));

    FolderRenamePending mockRenameFs = Mockito.spy(mockFs.prepareAtomicFolderRename(srcKey, dstKey));
    Mockito.when(mockFs.prepareAtomicFolderRename(srcKey, dstKey)).thenReturn(mockRenameFs);
    String path = mockFs.pathToKey(mockFs.makeAbsolute(new Path("root/0")));
    Mockito.doThrow(new IOException()).when(mockRenameFs).renameFile(Mockito.any(FileMetadata.class));

    boolean exception = false;
    try {
      mockFs.rename(sourceFolder, destFolder);
    } catch (IOException e){
      exception = true;
    }

    assertTrue(exception);
    assertTrue(mockFs.exists(sourceFolder));

    // Validate from logs that threads are enabled and delete operation failed.
    String content = logs.getOutput();
    assertInLog(content,
        "Using thread pool for Rename operation with threads");
    assertInLog(content,
        "Encountered Exception for Rename operation for file " + path);
    assertInLog(content,
        "Terminating execution of Rename operation now as some other thread already got exception or operation failed");
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

}
