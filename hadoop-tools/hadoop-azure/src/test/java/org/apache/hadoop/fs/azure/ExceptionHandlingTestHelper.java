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
import org.apache.hadoop.conf.Configuration;

public class ExceptionHandlingTestHelper {

  /*
   * Helper method to create a PageBlob test storage account.
   */
  public static AzureBlobStorageTestAccount getPageBlobTestStorageAccount()
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
   * Helper method to create an empty file
   */
  public static void createEmptyFile(AzureBlobStorageTestAccount testAccount, Path testPath) throws Exception {
    FileSystem fs = testAccount.getFileSystem();
    FSDataOutputStream inputStream = fs.create(testPath);
    inputStream.close();
  }

  /*
   * Helper method to create an folder and files inside it.
   */
  public static void createTestFolder(AzureBlobStorageTestAccount testAccount, Path testFolderPath) throws Exception {
    FileSystem fs = testAccount.getFileSystem();
    fs.mkdirs(testFolderPath);
    String testFolderFilePathBase = "test";

    for (int i = 0; i < 10; i++) {
      Path p = new Path(testFolderPath.toString() + "/" + testFolderFilePathBase + i + ".dat");
      fs.create(p).close();
    }
  }
}