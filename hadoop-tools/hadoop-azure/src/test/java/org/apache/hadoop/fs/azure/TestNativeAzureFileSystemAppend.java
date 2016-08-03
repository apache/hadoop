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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNativeAzureFileSystemAppend extends AbstractWasbTestBase {

  private static final String TEST_FILE = "test.dat";
  private static final Path TEST_PATH = new Path(TEST_FILE);

  private AzureBlobStorageTestAccount testAccount = null;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    testAccount = createTestAccount();
    fs = testAccount.getFileSystem();
    Configuration conf = fs.getConf();
    conf.setBoolean(NativeAzureFileSystem.APPEND_SUPPORT_ENABLE_PROPERTY_NAME, true);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);
  }

  /*
   * Helper method that creates test data of size provided by the
   * "size" parameter.
   */
  private static byte[] getTestData(int size) {
    byte[] testData = new byte[size];
    System.arraycopy(RandomStringUtils.randomAlphabetic(size).getBytes(), 0, testData, 0, size);
    return testData;
  }

  // Helper method to create file and write fileSize bytes of data on it.
  private byte[] createBaseFileWithData(int fileSize, Path testPath) throws Throwable {

    FSDataOutputStream createStream = null;
    try {
      createStream = fs.create(testPath);
      byte[] fileData = null;

      if (fileSize != 0) {
        fileData = getTestData(fileSize);
        createStream.write(fileData);
      }
      return fileData;
    } finally {
      if (createStream != null) {
        createStream.close();
      }
    }
  }

  /*
   * Helper method to verify a file data equal to "dataLength" parameter
   */
  private boolean verifyFileData(int dataLength, byte[] testData, int testDataIndex,
      FSDataInputStream srcStream) {

    try {

      byte[] fileBuffer = new byte[dataLength];
      byte[] testDataBuffer = new byte[dataLength];

      int fileBytesRead = srcStream.read(fileBuffer);

      if (fileBytesRead < dataLength) {
        return false;
      }

      System.arraycopy(testData, testDataIndex, testDataBuffer, 0, dataLength);

      if (!Arrays.equals(fileBuffer, testDataBuffer)) {
        return false;
      }

      return true;

    } catch (Exception ex) {
      return false;
    }

  }

  /*
   * Helper method to verify Append on a testFile.
   */
  private boolean verifyAppend(byte[] testData, Path testFile) {

    FSDataInputStream srcStream = null;
    try {

      srcStream = fs.open(testFile);
      int baseBufferSize = 2048;
      int testDataSize = testData.length;
      int testDataIndex = 0;

      while (testDataSize > baseBufferSize) {

        if (!verifyFileData(baseBufferSize, testData, testDataIndex, srcStream)) {
          return false;
        }
        testDataIndex += baseBufferSize;
        testDataSize -= baseBufferSize;
      }

      if (!verifyFileData(testDataSize, testData, testDataIndex, srcStream)) {
        return false;
      }

      return true;
    } catch(Exception ex) {
      return false;
    } finally {
      if (srcStream != null) {
        try {
          srcStream.close();
        } catch(IOException ioe) {
          // Swallowing
        }
      }
    }
  }

  /*
   * Test case to verify if an append on small size data works. This tests
   * append E2E
   */
  @Test
  public void testSingleAppend() throws Throwable{

    FSDataOutputStream appendStream = null;
    try {
      int baseDataSize = 50;
      byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, TEST_PATH);

      int appendDataSize = 20;
      byte[] appendDataBuffer = getTestData(appendDataSize);
      appendStream = fs.append(TEST_PATH, 10);
      appendStream.write(appendDataBuffer);
      appendStream.close();
      byte[] testData = new byte[baseDataSize + appendDataSize];
      System.arraycopy(baseDataBuffer, 0, testData, 0, baseDataSize);
      System.arraycopy(appendDataBuffer, 0, testData, baseDataSize, appendDataSize);

      Assert.assertTrue(verifyAppend(testData, TEST_PATH));
    } finally {
      if (appendStream != null) {
        appendStream.close();
      }
    }
  }

  /*
   * Test case to verify append to an empty file.
   */
  @Test
  public void testSingleAppendOnEmptyFile() throws Throwable {

    FSDataOutputStream appendStream = null;

    try {
      createBaseFileWithData(0, TEST_PATH);

      int appendDataSize = 20;
      byte[] appendDataBuffer = getTestData(appendDataSize);
      appendStream = fs.append(TEST_PATH, 10);
      appendStream.write(appendDataBuffer);
      appendStream.close();

      Assert.assertTrue(verifyAppend(appendDataBuffer, TEST_PATH));
    } finally {
      if (appendStream != null) {
        appendStream.close();
      }
    }
  }

  /*
   * Test to verify that we can open only one Append stream on a File.
   */
  @Test
  public void testSingleAppenderScenario() throws Throwable {

    FSDataOutputStream appendStream1 = null;
    FSDataOutputStream appendStream2 = null;
    IOException ioe = null;
    try {
      createBaseFileWithData(0, TEST_PATH);
      appendStream1 = fs.append(TEST_PATH, 10);
      boolean encounteredException = false;
      try {
        appendStream2 = fs.append(TEST_PATH, 10);
      } catch(IOException ex) {
        encounteredException = true;
        ioe = ex;
      }

      appendStream1.close();

      Assert.assertTrue(encounteredException);
      GenericTestUtils.assertExceptionContains("Unable to set Append lease on the Blob", ioe);
    } finally {
      if (appendStream1 != null) {
        appendStream1.close();
      }

      if (appendStream2 != null) {
        appendStream2.close();
      }
    }
  }

  /*
   * Tests to verify multiple appends on a Blob.
   */
  @Test
  public void testMultipleAppends() throws Throwable {

    int baseDataSize = 50;
    byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, TEST_PATH);

    int appendDataSize = 100;
    int targetAppendCount = 50;
    byte[] testData = new byte[baseDataSize + (appendDataSize*targetAppendCount)];
    int testDataIndex = 0;
    System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
    testDataIndex += baseDataSize;

    int appendCount = 0;

    FSDataOutputStream appendStream = null;

    try {
      while (appendCount < targetAppendCount) {

        byte[] appendDataBuffer = getTestData(appendDataSize);
        appendStream = fs.append(TEST_PATH, 30);
        appendStream.write(appendDataBuffer);
        appendStream.close();

        System.arraycopy(appendDataBuffer, 0, testData, testDataIndex, appendDataSize);
        testDataIndex += appendDataSize;
        appendCount++;
      }

      Assert.assertTrue(verifyAppend(testData, TEST_PATH));

    } finally {
      if (appendStream != null) {
        appendStream.close();
      }
    }
  }

  /*
   * Test to verify we multiple appends on the same stream.
   */
  @Test
  public void testMultipleAppendsOnSameStream() throws Throwable {

    int baseDataSize = 50;
    byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, TEST_PATH);
    int appendDataSize = 100;
    int targetAppendCount = 50;
    byte[] testData = new byte[baseDataSize + (appendDataSize*targetAppendCount)];
    int testDataIndex = 0;
    System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
    testDataIndex += baseDataSize;
    int appendCount = 0;

    FSDataOutputStream appendStream = null;

    try {

      while (appendCount < targetAppendCount) {

        appendStream = fs.append(TEST_PATH, 50);

        int singleAppendChunkSize = 20;
        int appendRunSize = 0;
        while (appendRunSize < appendDataSize) {

          byte[] appendDataBuffer = getTestData(singleAppendChunkSize);
          appendStream.write(appendDataBuffer);
          System.arraycopy(appendDataBuffer, 0, testData,
              testDataIndex + appendRunSize, singleAppendChunkSize);

          appendRunSize += singleAppendChunkSize;
        }

        appendStream.close();
        testDataIndex += appendDataSize;
        appendCount++;
      }

      Assert.assertTrue(verifyAppend(testData, TEST_PATH));
    } finally {
      if (appendStream != null) {
        appendStream.close();
      }
    }
  }

  @Test(expected=UnsupportedOperationException.class)
  /*
   * Test to verify the behavior when Append Support configuration flag is set to false
   */
  public void testFalseConfigurationFlagBehavior() throws Throwable {

    fs = testAccount.getFileSystem();
    Configuration conf = fs.getConf();
    conf.setBoolean(NativeAzureFileSystem.APPEND_SUPPORT_ENABLE_PROPERTY_NAME, false);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    FSDataOutputStream appendStream = null;

    try {
      createBaseFileWithData(0, TEST_PATH);
      appendStream = fs.append(TEST_PATH, 10);
    } finally {
      if (appendStream != null) {
        appendStream.close();
      }
    }
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }
}
