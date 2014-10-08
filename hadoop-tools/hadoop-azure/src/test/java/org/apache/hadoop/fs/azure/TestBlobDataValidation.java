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

import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.KEY_CHECK_BLOCK_MD5;
import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.KEY_STORE_BLOB_MD5;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.TestHookOperationContext;
import org.junit.After;
import org.junit.Test;

import com.microsoft.windowsazure.storage.Constants;
import com.microsoft.windowsazure.storage.OperationContext;
import com.microsoft.windowsazure.storage.ResponseReceivedEvent;
import com.microsoft.windowsazure.storage.StorageErrorCodeStrings;
import com.microsoft.windowsazure.storage.StorageEvent;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.BlockEntry;
import com.microsoft.windowsazure.storage.blob.BlockSearchMode;
import com.microsoft.windowsazure.storage.blob.CloudBlockBlob;
import com.microsoft.windowsazure.storage.core.Base64;

/**
 * Test that we do proper data integrity validation with MD5 checks as
 * configured.
 */
public class TestBlobDataValidation {
  private AzureBlobStorageTestAccount testAccount;

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  /**
   * Test that by default we don't store the blob-level MD5.
   */
  @Test
  public void testBlobMd5StoreOffByDefault() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    testStoreBlobMd5(false);
  }

  /**
   * Test that we get blob-level MD5 storage and validation if we specify that
   * in the configuration.
   */
  @Test
  public void testStoreBlobMd5() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(KEY_STORE_BLOB_MD5, true);
    testAccount = AzureBlobStorageTestAccount.create(conf);
    testStoreBlobMd5(true);
  }

  private void testStoreBlobMd5(boolean expectMd5Stored) throws Exception {
    assumeNotNull(testAccount);
    // Write a test file.
    String testFileKey = "testFile";
    Path testFilePath = new Path("/" + testFileKey);
    OutputStream outStream = testAccount.getFileSystem().create(testFilePath);
    outStream.write(new byte[] { 5, 15 });
    outStream.close();

    // Check that we stored/didn't store the MD5 field as configured.
    CloudBlockBlob blob = testAccount.getBlobReference(testFileKey);
    blob.downloadAttributes();
    String obtainedMd5 = blob.getProperties().getContentMD5();
    if (expectMd5Stored) {
      assertNotNull(obtainedMd5);
    } else {
      assertNull("Expected no MD5, found: " + obtainedMd5, obtainedMd5);
    }

    // Mess with the content so it doesn't match the MD5.
    String newBlockId = Base64.encode(new byte[] { 55, 44, 33, 22 });
    blob.uploadBlock(newBlockId,
        new ByteArrayInputStream(new byte[] { 6, 45 }), 2);
    blob.commitBlockList(Arrays.asList(new BlockEntry[] { new BlockEntry(
        newBlockId, BlockSearchMode.UNCOMMITTED) }));

    // Now read back the content. If we stored the MD5 for the blob content
    // we should get a data corruption error.
    InputStream inStream = testAccount.getFileSystem().open(testFilePath);
    try {
      byte[] inBuf = new byte[100];
      while (inStream.read(inBuf) > 0){
        //nothing;
      }
      inStream.close();
      if (expectMd5Stored) {
        fail("Should've thrown because of data corruption.");
      }
    } catch (IOException ex) {
      if (!expectMd5Stored) {
        throw ex;
      }
      StorageException cause = (StorageException)ex.getCause();
      assertNotNull(cause);
      assertTrue("Unexpected cause: " + cause,
          cause.getErrorCode().equals(StorageErrorCodeStrings.INVALID_MD5));
    }
  }

  /**
   * Test that by default we check block-level MD5.
   */
  @Test
  public void testCheckBlockMd5() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    testCheckBlockMd5(true);
  }

  /**
   * Test that we don't check block-level MD5 if we specify that in the
   * configuration.
   */
  @Test
  public void testDontCheckBlockMd5() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(KEY_CHECK_BLOCK_MD5, false);
    testAccount = AzureBlobStorageTestAccount.create(conf);
    testCheckBlockMd5(false);
  }

  /**
   * Connection inspector to check that MD5 fields for content is set/not set as
   * expected.
   */
  private static class ContentMD5Checker extends
      StorageEvent<ResponseReceivedEvent> {
    private final boolean expectMd5;

    public ContentMD5Checker(boolean expectMd5) {
      this.expectMd5 = expectMd5;
    }

    @Override
    public void eventOccurred(ResponseReceivedEvent eventArg) {
      HttpURLConnection connection = (HttpURLConnection) eventArg
          .getConnectionObject();
      if (isGetRange(connection)) {
        checkObtainedMd5(connection
            .getHeaderField(Constants.HeaderConstants.CONTENT_MD5));
      } else if (isPutBlock(connection)) {
        checkObtainedMd5(connection
            .getRequestProperty(Constants.HeaderConstants.CONTENT_MD5));
      }
    }

    private void checkObtainedMd5(String obtainedMd5) {
      if (expectMd5) {
        assertNotNull(obtainedMd5);
      } else {
        assertNull("Expected no MD5, found: " + obtainedMd5, obtainedMd5);
      }
    }

    private static boolean isPutBlock(HttpURLConnection connection) {
      return connection.getRequestMethod().equals("PUT")
          && connection.getURL().getQuery().contains("blockid");
    }

    private static boolean isGetRange(HttpURLConnection connection) {
      return connection.getRequestMethod().equals("GET")
          && connection
              .getHeaderField(Constants.HeaderConstants.STORAGE_RANGE_HEADER) != null;
    }
  }

  private void testCheckBlockMd5(final boolean expectMd5Checked)
      throws Exception {
    assumeNotNull(testAccount);
    Path testFilePath = new Path("/testFile");

    // Add a hook to check that for GET/PUT requests we set/don't set
    // the block-level MD5 field as configured. I tried to do clever
    // testing by also messing with the raw data to see if we actually
    // validate the data as expected, but the HttpURLConnection wasn't
    // pluggable enough for me to do that.
    testAccount.getFileSystem().getStore()
    .addTestHookToOperationContext(new TestHookOperationContext() {
    @Override
          public OperationContext modifyOperationContext(
              OperationContext original) {
      original.getResponseReceivedEventHandler().addListener(
          new ContentMD5Checker(expectMd5Checked));
      return original;
          }
        });

    OutputStream outStream = testAccount.getFileSystem().create(testFilePath);
    outStream.write(new byte[] { 5, 15 });
    outStream.close();

    InputStream inStream = testAccount.getFileSystem().open(testFilePath);
    byte[] inBuf = new byte[100];
    while (inStream.read(inBuf) > 0){
      //nothing;
    }
    inStream.close();
  }
}
