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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for the upload, buffering and flush logic in WASB.
 */
public class TestNativeAzureFileSystemUploadLogic {
  private AzureBlobStorageTestAccount testAccount;

  // Just an arbitrary number so that the values I write have a predictable
  // pattern: 0, 1, 2, .. , 45, 46, 0, 1, 2, ...
  static final int byteValuePeriod = 47;

  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createMock();
  }

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  /**
   * Various scenarios to test in how often we flush data while uploading.
   */
  private enum FlushFrequencyVariation {
    /**
     * Flush before even a single in-memory buffer is full.
     */
    BeforeSingleBufferFull,
    /**
     * Flush after a single in-memory buffer is full.
     */
    AfterSingleBufferFull,
    /**
     * Flush after all the in-memory buffers got full and were
     * automatically flushed to the backing store.
     */
    AfterAllRingBufferFull,
  }

  /**
   * Tests that we upload consistently if we flush after every little
   * bit of data.
   */
  @Test
  @Ignore /* flush() no longer does anything. @@TODO: implement a force-flush and reinstate this test */
  public void testConsistencyAfterSmallFlushes() throws Exception {
    testConsistencyAfterManyFlushes(FlushFrequencyVariation.BeforeSingleBufferFull);
  }

  /**
   * Tests that we upload consistently if we flush after every medium-sized
   * bit of data.
   */
  @Test
  @Ignore /* flush() no longer does anything. @@TODO: implement a force-flush and reinstate this test */
  public void testConsistencyAfterMediumFlushes() throws Exception {
    testConsistencyAfterManyFlushes(FlushFrequencyVariation.AfterSingleBufferFull);
  }

  /**
   * Tests that we upload consistently if we flush after every large chunk
   * of data.
   */
  @Test
  @Ignore /* flush() no longer does anything. @@TODO: implement a force-flush and reinstate this test */
  public void testConsistencyAfterLargeFlushes() throws Exception {
    testConsistencyAfterManyFlushes(FlushFrequencyVariation.AfterAllRingBufferFull);
  }

  /**
   * Makes sure the data in the given input is what I'd expect.
   * @param inStream The input stream.
   * @param expectedSize The expected size of the data in there.
   */
  private void assertDataInStream(InputStream inStream, int expectedSize)
      throws Exception {
    int byteRead;
    int countBytes = 0;
    while ((byteRead = inStream.read()) != -1) {
      assertEquals(countBytes % byteValuePeriod, byteRead);
      countBytes++;
    }
    assertEquals(expectedSize, countBytes);
  }

  /**
   * Checks that the data in the given file is what I'd expect.
   * @param file The file to check.
   * @param expectedSize The expected size of the data in there.
   */
  private void assertDataInFile(Path file, int expectedSize) throws Exception {
    InputStream inStream = testAccount.getFileSystem().open(file);
    assertDataInStream(inStream, expectedSize);
    inStream.close();
  }

  /**
   * Checks that the data in the current temporary upload blob
   * is what I'd expect.
   * @param expectedSize The expected size of the data in there.
   */
  private void assertDataInTempBlob(int expectedSize) throws Exception {
    // Look for the temporary upload blob in the backing store.
    InMemoryBlockBlobStore backingStore =
        testAccount.getMockStorage().getBackingStore();
    String tempKey = null;
    for (String key : backingStore.getKeys()) {
      if (key.contains(NativeAzureFileSystem.AZURE_TEMP_FOLDER)) {
        // Assume this is the one we're looking for.
        tempKey = key;
        break;
      }
    }
    assertNotNull(tempKey);
    InputStream inStream = new ByteArrayInputStream(backingStore.getContent(tempKey));
    assertDataInStream(inStream, expectedSize);
    inStream.close();
  }

  /**
   * Tests the given scenario for uploading a file while flushing
   * periodically and making sure the data is always consistent
   * with what I'd expect.
   * @param variation The variation/scenario to test.
   */
  private void testConsistencyAfterManyFlushes(FlushFrequencyVariation variation)
      throws Exception {
    Path uploadedFile = new Path("/uploadedFile");
    OutputStream outStream = testAccount.getFileSystem().create(uploadedFile);
    final int totalSize = 9123;
    int flushPeriod;
    switch (variation) {
      case BeforeSingleBufferFull: flushPeriod = 300; break;
      case AfterSingleBufferFull: flushPeriod = 600; break;
      case AfterAllRingBufferFull: flushPeriod = 1600; break;
      default:
        throw new IllegalArgumentException("Unknown variation: " + variation);
    }
    for (int i = 0; i < totalSize; i++) {
      outStream.write(i % byteValuePeriod);
      if ((i + 1) % flushPeriod == 0) {
        outStream.flush();
        assertDataInTempBlob(i + 1);
      }
    }
    outStream.close();
    assertDataInFile(uploadedFile, totalSize);
  }
}
