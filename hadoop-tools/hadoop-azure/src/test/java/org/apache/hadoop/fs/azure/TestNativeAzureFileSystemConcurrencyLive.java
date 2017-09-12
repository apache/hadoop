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


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/***
 * Test class to hold all Live Azure storage concurrency tests.
 */
public class TestNativeAzureFileSystemConcurrencyLive
    extends AbstractWasbTestBase {

  private static final int THREAD_COUNT = 102;
  private static final int TEST_EXECUTION_TIMEOUT = 5000;
  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  /**
   * Validate contract for FileSystem.create when overwrite is true and there
   * are concurrent callers of FileSystem.delete.  An existing file should be
   * overwritten, even if the original destination exists but is deleted by an
   * external agent during the create operation.
   */
  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testConcurrentCreateDeleteFile() throws Exception {
    Path testFile = new Path("test.dat");

    List<CreateFileTask> tasks = new ArrayList<>(THREAD_COUNT);

    for (int i = 0; i < THREAD_COUNT; i++) {
      tasks.add(new CreateFileTask(fs, testFile));
    }

    ExecutorService es = null;

    try {
      es = Executors.newFixedThreadPool(THREAD_COUNT);

      List<Future<Void>> futures = es.invokeAll(tasks);

      for (Future<Void> future : futures) {
        Assert.assertTrue(future.isDone());

        // we are using Callable<V>, so if an exception
        // occurred during the operation, it will be thrown
        // when we call get
        Assert.assertEquals(null, future.get());
      }
    } finally {
      if (es != null) {
        es.shutdownNow();
      }
    }
  }

  /**
   * Validate contract for FileSystem.delete when invoked concurrently.
   * One of the threads should successfully delete the file and return true;
   * all other threads should return false.
   */
  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testConcurrentDeleteFile() throws Exception {
    Path testFile = new Path("test.dat");
    fs.create(testFile).close();

    List<DeleteFileTask> tasks = new ArrayList<>(THREAD_COUNT);

    for (int i = 0; i < THREAD_COUNT; i++) {
      tasks.add(new DeleteFileTask(fs, testFile));
    }

    ExecutorService es = null;
    try {
      es = Executors.newFixedThreadPool(THREAD_COUNT);

      List<Future<Boolean>> futures = es.invokeAll(tasks);

      int successCount = 0;
      for (Future<Boolean> future : futures) {
        Assert.assertTrue(future.isDone());

        // we are using Callable<V>, so if an exception
        // occurred during the operation, it will be thrown
        // when we call get
        Boolean success = future.get();
        if (success) {
          successCount++;
        }
      }

      Assert.assertEquals(
          "Exactly one delete operation should return true.",
          1,
          successCount);
    } finally {
      if (es != null) {
        es.shutdownNow();
      }
    }
  }
}

abstract class FileSystemTask<V> implements Callable<V> {
  private final FileSystem fileSystem;
  private final Path path;

  protected FileSystem getFileSystem() {
    return this.fileSystem;
  }

  protected Path getFilePath() {
    return this.path;
  }

  FileSystemTask(FileSystem fs, Path p) {
    this.fileSystem = fs;
    this.path = p;
  }

  public abstract V call() throws Exception;
}

class DeleteFileTask extends FileSystemTask<Boolean> {

  DeleteFileTask(FileSystem fs, Path p) {
    super(fs, p);
  }

  @Override
  public Boolean call() throws Exception {
    return this.getFileSystem().delete(this.getFilePath(), false);
  }
}

class CreateFileTask extends FileSystemTask<Void> {
  CreateFileTask(FileSystem fs, Path p) {
    super(fs, p);
  }

  public Void call() throws Exception {
    FileSystem fs = getFileSystem();
    Path p = getFilePath();

    // Create an empty file and close the stream.
    FSDataOutputStream stream = fs.create(p, true);
    stream.close();

    // Delete the file.  We don't care if delete returns true or false.
    // We just want to ensure the file does not exist.
    this.getFileSystem().delete(this.getFilePath(), false);

    return null;
  }
}