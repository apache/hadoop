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


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/***
 * Test class to hold all Live Azure storage concurrency tests.
 */
public class TestNativeAzureFileSystemConcurrencyLive
    extends AbstractWasbTestBase {

  private static final int TEST_COUNT = 102;
  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  /**
   * Test multi-threaded deletes in WASB. Expected behavior is one of the thread
   * should be to successfully delete the file and return true and all other
   * threads need to return false.
   */
  @Test
  public void testMultiThreadedDeletes() throws Exception {
    Path testFile = new Path("test.dat");
    fs.create(testFile).close();

    int threadCount = TEST_COUNT;
    DeleteHelperThread[] helperThreads = new DeleteHelperThread[threadCount];

    for (int i = 0; i < threadCount; i++) {
      helperThreads[i] = new DeleteHelperThread(fs, testFile);
    }

    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threadCount; i++) {
      threads[i] = new Thread(helperThreads[i]);
      threads[i].start();
    }

    for (int i = 0; i < threadCount; i++) {
      threads[i].join();
    }

    boolean deleteSuccess = false;

    for (int i = 0; i < threadCount; i++) {

      Assert.assertFalse("child thread has exception : " + helperThreads[i].getException(),
          helperThreads[i].getExceptionEncounteredFlag());

      if (deleteSuccess) {
        Assert.assertFalse("More than one thread delete() retuhelperThreads[i].getDeleteSuccess()",
            helperThreads[i].getExceptionEncounteredFlag());
      } else {
        deleteSuccess = helperThreads[i].getDeleteSuccess();
      }
    }

    Assert.assertTrue("No successfull delete found", deleteSuccess);
  }
}

class DeleteHelperThread implements Runnable {

  private FileSystem fs;
  private Path p;
  private boolean deleteSuccess;
  private boolean exceptionEncountered;
  private Exception ex;

  public DeleteHelperThread(FileSystem fs, Path p) {
    this.fs = fs;
    this.p = p;
  }

  public void run() {
    try {
      deleteSuccess = fs.delete(p, false);
    } catch (Exception ioEx) {
      exceptionEncountered = true;
      this.ex = ioEx;
    }
  }

  public boolean getDeleteSuccess() {
    return deleteSuccess;
  }

  public boolean getExceptionEncounteredFlag() {
    return exceptionEncountered;
  }

  public Exception getException() {
    return ex;
  }
}