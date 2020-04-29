/*
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

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.junit.Assume.assumeNotNull;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.cleanupTestAccount;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.readStringFromFile;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.writeStringToFile;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
/**
 * Because FileSystem.Statistics is per FileSystem, so statistics can not be ran in
 * parallel, hence in this test file, force them to run in sequential.
 */
public class ITestNativeFileSystemStatistics extends AbstractWasbTestWithTimeout{

  @Test
  public void test_001_NativeAzureFileSystemMocked() throws Exception {
    AzureBlobStorageTestAccount testAccount = AzureBlobStorageTestAccount.createMock();
    assumeNotNull(testAccount);
    testStatisticsWithAccount(testAccount);
  }

  @Test
  public void test_002_NativeAzureFileSystemPageBlobLive() throws Exception {
    Configuration conf = new Configuration();
    // Configure the page blob directories key so every file created is a page blob.
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, "/");

    // Configure the atomic rename directories key so every folder will have
    // atomic rename applied.
    conf.set(AzureNativeFileSystemStore.KEY_ATOMIC_RENAME_DIRECTORIES, "/");
    AzureBlobStorageTestAccount testAccount =  AzureBlobStorageTestAccount.create(conf);
    assumeNotNull(testAccount);
    testStatisticsWithAccount(testAccount);
  }

  @Test
  public void test_003_NativeAzureFileSystem() throws Exception {
    AzureBlobStorageTestAccount testAccount = AzureBlobStorageTestAccount.create();
    assumeNotNull(testAccount);
    testStatisticsWithAccount(testAccount);
  }

  private void testStatisticsWithAccount(AzureBlobStorageTestAccount testAccount) throws Exception {
    assumeNotNull(testAccount);
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    testStatistics(fs);
    cleanupTestAccount(testAccount);
  }

  /**
   * When tests are ran in parallel, this tests will fail because
   * FileSystem.Statistics is per FileSystem class.
   */
  @SuppressWarnings("deprecation")
  private void testStatistics(NativeAzureFileSystem fs) throws Exception {
    FileSystem.clearStatistics();
    FileSystem.Statistics stats = FileSystem.getStatistics("wasb",
            NativeAzureFileSystem.class);
    assertEquals(0, stats.getBytesRead());
    assertEquals(0, stats.getBytesWritten());
    Path newFile = new Path("testStats");
    writeStringToFile(fs, newFile, "12345678");
    assertEquals(8, stats.getBytesWritten());
    assertEquals(0, stats.getBytesRead());
    String readBack = readStringFromFile(fs, newFile);
    assertEquals("12345678", readBack);
    assertEquals(8, stats.getBytesRead());
    assertEquals(8, stats.getBytesWritten());
    assertTrue(fs.delete(newFile, true));
    assertEquals(8, stats.getBytesRead());
    assertEquals(8, stats.getBytesWritten());
  }
}
