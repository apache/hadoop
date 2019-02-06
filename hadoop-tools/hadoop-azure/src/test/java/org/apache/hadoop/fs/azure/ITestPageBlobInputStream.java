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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.Callable;

import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test semantics of the page blob input stream
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class ITestPageBlobInputStream extends AbstractWasbTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestPageBlobInputStream.class);
  private static final int KILOBYTE = 1024;
  private static final int MEGABYTE = KILOBYTE * KILOBYTE;
  private static final int TEST_FILE_SIZE = 6 * MEGABYTE;
  private static final Path TEST_FILE_PATH = new Path(
      "TestPageBlobInputStream.txt");

  private long testFileLength;

  /**
   * Long test timeout.
   */
  @Rule
  public Timeout testTimeout = new Timeout(10 * 60 * 1000);
  private FileStatus testFileStatus;
  private Path hugefile;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTestAccount();

    hugefile = fs.makeQualified(TEST_FILE_PATH);
    try {
      testFileStatus = fs.getFileStatus(TEST_FILE_PATH);
      testFileLength = testFileStatus.getLen();
    } catch (FileNotFoundException e) {
      // file doesn't exist
      testFileLength = 0;
    }
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();

    // Configure the page blob directories key so every file created is a page blob.
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, "/");

    return AzureBlobStorageTestAccount.create(
        "testpageblobinputstream",
        EnumSet.of(AzureBlobStorageTestAccount.CreateOptions.CreateContainer),
        conf,
        true);
  }

  /**
   * Create a test file by repeating the characters in the alphabet.
   * @throws IOException
   */
  private void createTestFileAndSetLength() throws IOException {
    // To reduce test run time, the test file can be reused.
    if (fs.exists(TEST_FILE_PATH)) {
      testFileStatus = fs.getFileStatus(TEST_FILE_PATH);
      testFileLength = testFileStatus.getLen();
      LOG.info("Reusing test file: {}", testFileStatus);
      return;
    }

    byte[] buffer = new byte[256];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) i;
    }

    LOG.info("Creating test file {} of size: {}", TEST_FILE_PATH,
        TEST_FILE_SIZE);

    try(FSDataOutputStream outputStream = fs.create(TEST_FILE_PATH)) {
      int bytesWritten = 0;
      while (bytesWritten < TEST_FILE_SIZE) {
        outputStream.write(buffer);
        bytesWritten += buffer.length;
      }
      LOG.info("Closing stream {}", outputStream);
      outputStream.close();
    }
    testFileLength = fs.getFileStatus(TEST_FILE_PATH).getLen();
  }

  void assumeHugeFileExists() throws IOException {
    ContractTestUtils.assertPathExists(fs, "huge file not created", hugefile);
    FileStatus status = fs.getFileStatus(hugefile);
    ContractTestUtils.assertIsFile(hugefile, status);
    assertTrue("File " + hugefile + " is empty", status.getLen() > 0);
  }

  @Test
  public void test_0100_CreateHugeFile() throws IOException {
    createTestFileAndSetLength();
  }

  @Test
  public void test_0200_BasicReadTest() throws Exception {
    assumeHugeFileExists();

    try (
        FSDataInputStream inputStream = fs.open(TEST_FILE_PATH);
    ) {
      byte[] buffer = new byte[3 * MEGABYTE];

      // v1 forward seek and read a kilobyte into first kilobyte of buffer
      long position = 5 * MEGABYTE;
      inputStream.seek(position);
      int numBytesRead = inputStream.read(buffer, 0, KILOBYTE);
      assertEquals(KILOBYTE, numBytesRead);

      byte[] expected = new byte[3 * MEGABYTE];

      for (int i = 0; i < KILOBYTE; i++) {
        expected[i] = (byte) ((i + position) % 256);
      }

      assertArrayEquals(expected, buffer);

      int len = MEGABYTE;
      int offset = buffer.length - len;

      // v1 reverse seek and read a megabyte into last megabyte of buffer
      position = 3 * MEGABYTE;
      inputStream.seek(position);
      numBytesRead = inputStream.read(buffer, offset, len);
      assertEquals(len, numBytesRead);

      for (int i = offset; i < offset + len; i++) {
        expected[i] = (byte) ((i + position) % 256);
      }

      assertArrayEquals(expected, buffer);
    }
  }

  @Test
  public void test_0201_RandomReadTest() throws Exception {
    assumeHugeFileExists();

    try (
        FSDataInputStream inputStream = fs.open(TEST_FILE_PATH);
    ) {
      final int bufferSize = 4 * KILOBYTE;
      byte[] buffer = new byte[bufferSize];
      long position = 0;

      verifyConsistentReads(inputStream, buffer, position);

      inputStream.seek(0);

      verifyConsistentReads(inputStream, buffer, position);

      int seekPosition = 2 * KILOBYTE;
      inputStream.seek(seekPosition);
      position = seekPosition;
      verifyConsistentReads(inputStream, buffer, position);

      inputStream.seek(0);
      position = 0;
      verifyConsistentReads(inputStream, buffer, position);

      seekPosition = 5 * KILOBYTE;
      inputStream.seek(seekPosition);
      position = seekPosition;
      verifyConsistentReads(inputStream, buffer, position);

      seekPosition = 10 * KILOBYTE;
      inputStream.seek(seekPosition);
      position = seekPosition;
      verifyConsistentReads(inputStream, buffer, position);

      seekPosition = 4100 * KILOBYTE;
      inputStream.seek(seekPosition);
      position = seekPosition;
      verifyConsistentReads(inputStream, buffer, position);

      for (int i = 4 * 1024 * 1023; i < 5000; i++) {
        seekPosition = i;
        inputStream.seek(seekPosition);
        position = seekPosition;
        verifyConsistentReads(inputStream, buffer, position);
      }

      inputStream.seek(0);
      position = 0;
      buffer = new byte[1];

      for (int i = 0; i < 5000; i++) {
        assertEquals(1, inputStream.skip(1));
        position++;
        verifyConsistentReads(inputStream, buffer, position);
        position++;
      }
    }
  }

  private void verifyConsistentReads(FSDataInputStream inputStream,
                                     byte[] buffer,
                                     long position) throws IOException {
    int size = buffer.length;
    final int numBytesRead = inputStream.read(buffer, 0, size);
    assertEquals("Bytes read from stream", size, numBytesRead);

    byte[] expected = new byte[size];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) ((position + i) % 256);
    }

    assertArrayEquals("Mismatch", expected, buffer);
  }

  /**
   * Validates the implementation of InputStream.markSupported.
   * @throws IOException
   */
  @Test
  public void test_0301_MarkSupported() throws IOException {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      assertTrue("mark is not supported", inputStream.markSupported());
    }
  }

  /**
   * Validates the implementation of InputStream.mark and reset
   * for version 1 of the block blob input stream.
   * @throws Exception
   */
  @Test
  public void test_0303_MarkAndResetV1() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      inputStream.mark(KILOBYTE - 1);

      byte[] buffer = new byte[KILOBYTE];
      int bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);

      inputStream.reset();
      assertEquals("rest -> pos 0", 0, inputStream.getPos());

      inputStream.mark(8 * KILOBYTE - 1);

      buffer = new byte[8 * KILOBYTE];
      bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);

      intercept(IOException.class,
          "Resetting to invalid mark",
          new Callable<FSDataInputStream>() {
            @Override
            public FSDataInputStream call() throws Exception {
              inputStream.reset();
              return inputStream;
            }
          }
      );
    }
  }

  /**
   * Validates the implementation of Seekable.seekToNewSource, which should
   * return false for version 1 of the block blob input stream.
   * @throws IOException
   */
  @Test
  public void test_0305_SeekToNewSourceV1() throws IOException {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      assertFalse(inputStream.seekToNewSource(0));
    }
  }

  /**
   * Validates the implementation of InputStream.skip and ensures there is no
   * network I/O for version 1 of the block blob input stream.
   * @throws Exception
   */
  @Test
  public void test_0307_SkipBounds() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      long skipped = inputStream.skip(-1);
      assertEquals(0, skipped);

      skipped = inputStream.skip(0);
      assertEquals(0, skipped);

      assertTrue(testFileLength > 0);

      skipped = inputStream.skip(testFileLength);
      assertEquals(testFileLength, skipped);

      intercept(EOFException.class,
          new Callable<Long>() {
            @Override
            public Long call() throws Exception {
              return inputStream.skip(1);
            }
          }
      );
    }
  }

  /**
   * Validates the implementation of Seekable.seek and ensures there is no
   * network I/O for forward seek.
   * @throws Exception
   */
  @Test
  public void test_0309_SeekBounds() throws Exception {
    assumeHugeFileExists();
    try (
        FSDataInputStream inputStream = fs.open(TEST_FILE_PATH);
    ) {
      inputStream.seek(0);
      assertEquals(0, inputStream.getPos());

      intercept(EOFException.class,
          FSExceptionMessages.NEGATIVE_SEEK,
          new Callable<FSDataInputStream>() {
            @Override
            public FSDataInputStream call() throws Exception {
              inputStream.seek(-1);
              return inputStream;
            }
          }
      );

      assertTrue("Test file length only " + testFileLength, testFileLength > 0);
      inputStream.seek(testFileLength);
      assertEquals(testFileLength, inputStream.getPos());

      intercept(EOFException.class,
          FSExceptionMessages.CANNOT_SEEK_PAST_EOF,
          new Callable<FSDataInputStream>() {
            @Override
            public FSDataInputStream call() throws Exception {
              inputStream.seek(testFileLength + 1);
              return inputStream;
            }
          }
      );
    }
  }

  /**
   * Validates the implementation of Seekable.seek, Seekable.getPos,
   * and InputStream.available.
   * @throws Exception
   */
  @Test
  public void test_0311_SeekAndAvailableAndPosition() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      byte[] expected1 = {0, 1, 2};
      byte[] expected2 = {3, 4, 5};
      byte[] expected3 = {1, 2, 3};
      byte[] expected4 = {6, 7, 8};
      byte[] buffer = new byte[3];

      int bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected1, buffer);
      assertEquals(buffer.length, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());

      bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected2, buffer);
      assertEquals(2 * buffer.length, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());

      // reverse seek
      int seekPos = 0;
      inputStream.seek(seekPos);

      bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected1, buffer);
      assertEquals(buffer.length + seekPos, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());

      // reverse seek
      seekPos = 1;
      inputStream.seek(seekPos);

      bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected3, buffer);
      assertEquals(buffer.length + seekPos, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());

      // forward seek
      seekPos = 6;
      inputStream.seek(seekPos);

      bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected4, buffer);
      assertEquals(buffer.length + seekPos, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());
    }
  }

  /**
   * Validates the implementation of InputStream.skip, Seekable.getPos,
   * and InputStream.available.
   * @throws IOException
   */
  @Test
  public void test_0313_SkipAndAvailableAndPosition() throws IOException {
    assumeHugeFileExists();
    try (
        FSDataInputStream inputStream = fs.open(TEST_FILE_PATH);
    ) {
      byte[] expected1 = {0, 1, 2};
      byte[] expected2 = {3, 4, 5};
      byte[] expected3 = {1, 2, 3};
      byte[] expected4 = {6, 7, 8};
      assertEquals(testFileLength, inputStream.available());
      assertEquals(0, inputStream.getPos());

      int n = 3;
      long skipped = inputStream.skip(n);

      assertEquals(skipped, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());
      assertEquals(skipped, n);

      byte[] buffer = new byte[3];
      int bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected2, buffer);
      assertEquals(buffer.length + skipped, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());

      // does skip still work after seek?
      int seekPos = 1;
      inputStream.seek(seekPos);

      bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected3, buffer);
      assertEquals(buffer.length + seekPos, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());

      long currentPosition = inputStream.getPos();
      n = 2;
      skipped = inputStream.skip(n);

      assertEquals(currentPosition + skipped, inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());
      assertEquals(skipped, n);

      bytesRead = inputStream.read(buffer);
      assertEquals(buffer.length, bytesRead);
      assertArrayEquals(expected4, buffer);
      assertEquals(buffer.length + skipped + currentPosition,
          inputStream.getPos());
      assertEquals(testFileLength - inputStream.getPos(),
          inputStream.available());
    }
  }

  @Test
  public void test_999_DeleteHugeFiles() throws IOException {
    fs.delete(TEST_FILE_PATH, false);
  }

}
