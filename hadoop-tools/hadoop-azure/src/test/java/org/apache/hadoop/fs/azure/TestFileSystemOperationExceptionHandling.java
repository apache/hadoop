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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;


public class TestFileSystemOperationExceptionHandling extends
  NativeAzureFileSystemBaseTest {

  FSDataInputStream inputStream = null;
  /*
   * Helper method to create a PageBlob test storage account.
   */
  private AzureBlobStorageTestAccount getPageBlobTestStorageAccount()
      throws Exception {

    Configuration conf = new Configuration();

    // Configure the page blob directories key so every file created is a page blob.
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, "/");

    // Configure the atomic rename directories key so every folder will have
    // atomic rename applied.
    conf.set(AzureNativeFileSystemStore.KEY_ATOMIC_RENAME_DIRECTORIES, "/");
    return AzureBlobStorageTestAccount.create(conf);
  }


  /*
   * Helper method that creates a InputStream to validate exceptions
   * for various scenarios
   */
  private void setupInputStreamToTest(AzureBlobStorageTestAccount testAccount)
      throws Exception {

    fs = testAccount.getFileSystem();

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
    AzureBlobStorageTestAccount testAccount = getPageBlobTestStorageAccount();
    setupInputStreamToTest(testAccount);
    byte[] readBuffer = new byte[512];
    inputStream.read(readBuffer);
  }

  /*
   * Tests a basic single threaded seek scenario for Page blobs.
   */
  @Test(expected=FileNotFoundException.class)
  public void testSingleThreadedPageBlobSeekScenario() throws Throwable {
    AzureBlobStorageTestAccount testAccount = getPageBlobTestStorageAccount();
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

  @After
  public void tearDown() throws Exception {
    if (inputStream != null) {
      inputStream.close();
    }
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }
}
