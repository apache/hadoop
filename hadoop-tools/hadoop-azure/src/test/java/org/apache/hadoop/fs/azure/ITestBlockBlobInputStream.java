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
import java.util.Random;
import java.util.concurrent.Callable;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.integration.AbstractAzureScaleTest;
import org.apache.hadoop.fs.azure.integration.AzureTestUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils.NanoTimer;

import static org.junit.Assume.assumeNotNull;

import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test semantics and performance of the original block blob input stream
 * (KEY_INPUT_STREAM_VERSION=1) and the new
 * <code>BlockBlobInputStream</code> (KEY_INPUT_STREAM_VERSION=2).
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class ITestBlockBlobInputStream extends AbstractAzureScaleTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestBlockBlobInputStream.class);
  private static final int KILOBYTE = 1024;
  private static final int MEGABYTE = KILOBYTE * KILOBYTE;
  private static final int TEST_FILE_SIZE = 6 * MEGABYTE;
  private static final Path TEST_FILE_PATH = new Path(
      "TestBlockBlobInputStream.txt");

  private AzureBlobStorageTestAccount accountUsingInputStreamV1;
  private AzureBlobStorageTestAccount accountUsingInputStreamV2;
  private long testFileLength;



  private FileStatus testFileStatus;
  private Path hugefile;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = new Configuration();
    conf.setInt(AzureNativeFileSystemStore.KEY_INPUT_STREAM_VERSION, 1);

    accountUsingInputStreamV1 = AzureBlobStorageTestAccount.create(
        "testblockblobinputstream",
        EnumSet.of(AzureBlobStorageTestAccount.CreateOptions.CreateContainer),
        conf,
        true);

    accountUsingInputStreamV2 = AzureBlobStorageTestAccount.create(
        "testblockblobinputstream",
        EnumSet.noneOf(AzureBlobStorageTestAccount.CreateOptions.class),
        null,
        true);

    assumeNotNull(accountUsingInputStreamV1);
    assumeNotNull(accountUsingInputStreamV2);
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
    conf.setInt(AzureNativeFileSystemStore.KEY_INPUT_STREAM_VERSION, 1);

    accountUsingInputStreamV1 = AzureBlobStorageTestAccount.create(
        "testblockblobinputstream",
        EnumSet.of(AzureBlobStorageTestAccount.CreateOptions.CreateContainer),
        conf,
        true);

    accountUsingInputStreamV2 = AzureBlobStorageTestAccount.create(
        "testblockblobinputstream",
        EnumSet.noneOf(AzureBlobStorageTestAccount.CreateOptions.class),
        null,
        true);

    assumeNotNull(accountUsingInputStreamV1);
    assumeNotNull(accountUsingInputStreamV2);
    return accountUsingInputStreamV1;
  }

  /**
   * Create a test file by repeating the characters in the alphabet.
   * @throws IOException
   */
  private void createTestFileAndSetLength() throws IOException {
    FileSystem fs = accountUsingInputStreamV1.getFileSystem();

    // To reduce test run time, the test file can be reused.
    if (fs.exists(TEST_FILE_PATH)) {
      testFileStatus = fs.getFileStatus(TEST_FILE_PATH);
      testFileLength = testFileStatus.getLen();
      LOG.info("Reusing test file: {}", testFileStatus);
      return;
    }

    int sizeOfAlphabet = ('z' - 'a' + 1);
    byte[] buffer = new byte[26 * KILOBYTE];
    char character = 'a';
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) character;
      character = (character == 'z') ? 'a' : (char) ((int) character + 1);
    }

    LOG.info("Creating test file {} of size: {}", TEST_FILE_PATH,
        TEST_FILE_SIZE);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

    try(FSDataOutputStream outputStream = fs.create(TEST_FILE_PATH)) {
      int bytesWritten = 0;
      while (bytesWritten < TEST_FILE_SIZE) {
        outputStream.write(buffer);
        bytesWritten += buffer.length;
      }
      LOG.info("Closing stream {}", outputStream);
      ContractTestUtils.NanoTimer closeTimer
          = new ContractTestUtils.NanoTimer();
      outputStream.close();
      closeTimer.end("time to close() output stream");
    }
    timer.end("time to write %d KB", TEST_FILE_SIZE / 1024);
    testFileLength = fs.getFileStatus(TEST_FILE_PATH).getLen();
  }

  void assumeHugeFileExists() throws IOException {
    ContractTestUtils.assertPathExists(fs, "huge file not created", hugefile);
    FileStatus status = fs.getFileStatus(hugefile);
    ContractTestUtils.assertIsFile(hugefile, status);
    assertTrue("File " + hugefile + " is empty", status.getLen() > 0);
  }

  /**
   * Calculate megabits per second from the specified values for bytes and
   * milliseconds.
   * @param bytes The number of bytes.
   * @param milliseconds The number of milliseconds.
   * @return The number of megabits per second.
   */
  private static double toMbps(long bytes, long milliseconds) {
    return bytes / 1000.0 * 8 / milliseconds;
  }

  @Test
  public void test_0100_CreateHugeFile() throws IOException {
    createTestFileAndSetLength();
  }

  @Test
  public void test_0200_BasicReadTest() throws Exception {
    assumeHugeFileExists();

    try (
        FSDataInputStream inputStreamV1
            = accountUsingInputStreamV1.getFileSystem().open(TEST_FILE_PATH);

        FSDataInputStream inputStreamV2
            = accountUsingInputStreamV2.getFileSystem().open(TEST_FILE_PATH);
    ) {
      byte[] bufferV1 = new byte[3 * MEGABYTE];
      byte[] bufferV2 = new byte[bufferV1.length];

      // v1 forward seek and read a kilobyte into first kilobyte of bufferV1
      inputStreamV1.seek(5 * MEGABYTE);
      int numBytesReadV1 = inputStreamV1.read(bufferV1, 0, KILOBYTE);
      assertEquals(KILOBYTE, numBytesReadV1);

      // v2 forward seek and read a kilobyte into first kilobyte of bufferV2
      inputStreamV2.seek(5 * MEGABYTE);
      int numBytesReadV2 = inputStreamV2.read(bufferV2, 0, KILOBYTE);
      assertEquals(KILOBYTE, numBytesReadV2);

      assertArrayEquals(bufferV1, bufferV2);

      int len = MEGABYTE;
      int offset = bufferV1.length - len;

      // v1 reverse seek and read a megabyte into last megabyte of bufferV1
      inputStreamV1.seek(3 * MEGABYTE);
      numBytesReadV1 = inputStreamV1.read(bufferV1, offset, len);
      assertEquals(len, numBytesReadV1);

      // v2 reverse seek and read a megabyte into last megabyte of bufferV2
      inputStreamV2.seek(3 * MEGABYTE);
      numBytesReadV2 = inputStreamV2.read(bufferV2, offset, len);
      assertEquals(len, numBytesReadV2);

      assertArrayEquals(bufferV1, bufferV2);
    }
  }

  @Test
  public void test_0201_RandomReadTest() throws Exception {
    assumeHugeFileExists();

    try (
        FSDataInputStream inputStreamV1
            = accountUsingInputStreamV1.getFileSystem().open(TEST_FILE_PATH);

        FSDataInputStream inputStreamV2
            = accountUsingInputStreamV2.getFileSystem().open(TEST_FILE_PATH);
    ) {
      final int bufferSize = 4 * KILOBYTE;
      byte[] bufferV1 = new byte[bufferSize];
      byte[] bufferV2 = new byte[bufferV1.length];

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      inputStreamV1.seek(0);
      inputStreamV2.seek(0);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      int seekPosition = 2 * KILOBYTE;
      inputStreamV1.seek(seekPosition);
      inputStreamV2.seek(seekPosition);

      inputStreamV1.seek(0);
      inputStreamV2.seek(0);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      seekPosition = 5 * KILOBYTE;
      inputStreamV1.seek(seekPosition);
      inputStreamV2.seek(seekPosition);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      seekPosition = 10 * KILOBYTE;
      inputStreamV1.seek(seekPosition);
      inputStreamV2.seek(seekPosition);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      seekPosition = 4100 * KILOBYTE;
      inputStreamV1.seek(seekPosition);
      inputStreamV2.seek(seekPosition);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);
    }
  }

  private void verifyConsistentReads(FSDataInputStream inputStreamV1,
      FSDataInputStream inputStreamV2,
      byte[] bufferV1,
      byte[] bufferV2) throws IOException {
    int size = bufferV1.length;
    final int numBytesReadV1 = inputStreamV1.read(bufferV1, 0, size);
    assertEquals("Bytes read from V1 stream", size, numBytesReadV1);

    final int numBytesReadV2 = inputStreamV2.read(bufferV2, 0, size);
    assertEquals("Bytes read from V2 stream", size, numBytesReadV2);

    assertArrayEquals("Mismatch in read data", bufferV1, bufferV2);
  }

  /**
   * Validates the implementation of InputStream.markSupported.
   * @throws IOException
   */
  @Test
  public void test_0301_MarkSupportedV1() throws IOException {
    validateMarkSupported(accountUsingInputStreamV1.getFileSystem());
  }

  /**
   * Validates the implementation of InputStream.markSupported.
   * @throws IOException
   */
  @Test
  public void test_0302_MarkSupportedV2() throws IOException {
    validateMarkSupported(accountUsingInputStreamV1.getFileSystem());
  }

  private void validateMarkSupported(FileSystem fs) throws IOException {
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
    validateMarkAndReset(accountUsingInputStreamV1.getFileSystem());
  }

  /**
   * Validates the implementation of InputStream.mark and reset
   * for version 2 of the block blob input stream.
   * @throws Exception
   */
  @Test
  public void test_0304_MarkAndResetV2() throws Exception {
    validateMarkAndReset(accountUsingInputStreamV2.getFileSystem());
  }

  private void validateMarkAndReset(FileSystem fs) throws Exception {
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
    validateSeekToNewSource(accountUsingInputStreamV1.getFileSystem());
  }

  /**
   * Validates the implementation of Seekable.seekToNewSource, which should
   * return false for version 2 of the block blob input stream.
   * @throws IOException
   */
  @Test
  public void test_0306_SeekToNewSourceV2() throws IOException {
    validateSeekToNewSource(accountUsingInputStreamV2.getFileSystem());
  }

  private void validateSeekToNewSource(FileSystem fs) throws IOException {
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
  public void test_0307_SkipBoundsV1() throws Exception {
    validateSkipBounds(accountUsingInputStreamV1.getFileSystem());
  }

  /**
   * Validates the implementation of InputStream.skip and ensures there is no
   * network I/O for version 2 of the block blob input stream.
   * @throws Exception
   */
  @Test
  public void test_0308_SkipBoundsV2() throws Exception {
    validateSkipBounds(accountUsingInputStreamV2.getFileSystem());
  }

  private void validateSkipBounds(FileSystem fs) throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      NanoTimer timer = new NanoTimer();

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
      long elapsedTimeMs = timer.elapsedTimeMs();
      assertTrue(
          String.format(
              "There should not be any network I/O (elapsedTimeMs=%1$d).",
              elapsedTimeMs),
          elapsedTimeMs < 20);
    }
  }

  /**
   * Validates the implementation of Seekable.seek and ensures there is no
   * network I/O for forward seek.
   * @throws Exception
   */
  @Test
  public void test_0309_SeekBoundsV1() throws Exception {
    validateSeekBounds(accountUsingInputStreamV1.getFileSystem());
  }

  /**
   * Validates the implementation of Seekable.seek and ensures there is no
   * network I/O for forward seek.
   * @throws Exception
   */
  @Test
  public void test_0310_SeekBoundsV2() throws Exception {
    validateSeekBounds(accountUsingInputStreamV2.getFileSystem());
  }

  private void validateSeekBounds(FileSystem fs) throws Exception {
    assumeHugeFileExists();
    try (
        FSDataInputStream inputStream = fs.open(TEST_FILE_PATH);
    ) {
      NanoTimer timer = new NanoTimer();

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

      long elapsedTimeMs = timer.elapsedTimeMs();
      assertTrue(
          String.format(
              "There should not be any network I/O (elapsedTimeMs=%1$d).",
              elapsedTimeMs),
          elapsedTimeMs < 20);
    }
  }

  /**
   * Validates the implementation of Seekable.seek, Seekable.getPos,
   * and InputStream.available.
   * @throws Exception
   */
  @Test
  public void test_0311_SeekAndAvailableAndPositionV1() throws Exception {
    validateSeekAndAvailableAndPosition(
        accountUsingInputStreamV1.getFileSystem());
  }

  /**
   * Validates the implementation of Seekable.seek, Seekable.getPos,
   * and InputStream.available.
   * @throws Exception
   */
  @Test
  public void test_0312_SeekAndAvailableAndPositionV2() throws Exception {
    validateSeekAndAvailableAndPosition(
        accountUsingInputStreamV2.getFileSystem());
  }

  private void validateSeekAndAvailableAndPosition(FileSystem fs)
      throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      byte[] expected1 = {(byte) 'a', (byte) 'b', (byte) 'c'};
      byte[] expected2 = {(byte) 'd', (byte) 'e', (byte) 'f'};
      byte[] expected3 = {(byte) 'b', (byte) 'c', (byte) 'd'};
      byte[] expected4 = {(byte) 'g', (byte) 'h', (byte) 'i'};
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
  public void test_0313_SkipAndAvailableAndPositionV1() throws IOException {
    validateSkipAndAvailableAndPosition(
        accountUsingInputStreamV1.getFileSystem());
  }

  /**
   * Validates the implementation of InputStream.skip, Seekable.getPos,
   * and InputStream.available.
   * @throws IOException
   */
  @Test
  public void test_0314_SkipAndAvailableAndPositionV2() throws IOException {
    validateSkipAndAvailableAndPosition(
        accountUsingInputStreamV1.getFileSystem());
  }

  private void validateSkipAndAvailableAndPosition(FileSystem fs)
      throws IOException {
    assumeHugeFileExists();
    try (
        FSDataInputStream inputStream = fs.open(TEST_FILE_PATH);
    ) {
      byte[] expected1 = {(byte) 'a', (byte) 'b', (byte) 'c'};
      byte[] expected2 = {(byte) 'd', (byte) 'e', (byte) 'f'};
      byte[] expected3 = {(byte) 'b', (byte) 'c', (byte) 'd'};
      byte[] expected4 = {(byte) 'g', (byte) 'h', (byte) 'i'};

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

  /**
   * Ensures parity in the performance of sequential read for
   * version 1 and version 2 of the block blob input stream.
   * @throws IOException
   */
  @Test
  public void test_0315_SequentialReadPerformance() throws IOException {
    assumeHugeFileExists();
    final int maxAttempts = 10;
    final double maxAcceptableRatio = 1.01;
    double v1ElapsedMs = 0, v2ElapsedMs = 0;
    double ratio = Double.MAX_VALUE;
    for (int i = 0; i < maxAttempts && ratio >= maxAcceptableRatio; i++) {
      v1ElapsedMs = sequentialRead(1,
          accountUsingInputStreamV1.getFileSystem(), false);
      v2ElapsedMs = sequentialRead(2,
          accountUsingInputStreamV2.getFileSystem(), false);
      ratio = v2ElapsedMs / v1ElapsedMs;
      LOG.info(String.format(
          "v1ElapsedMs=%1$d, v2ElapsedMs=%2$d, ratio=%3$.2f",
          (long) v1ElapsedMs,
          (long) v2ElapsedMs,
          ratio));
    }
    assertTrue(String.format(
        "Performance of version 2 is not acceptable: v1ElapsedMs=%1$d,"
            + " v2ElapsedMs=%2$d, ratio=%3$.2f",
        (long) v1ElapsedMs,
        (long) v2ElapsedMs,
        ratio),
        ratio < maxAcceptableRatio);
  }

  /**
   * Ensures parity in the performance of sequential read after reverse seek for
   * version 2 of the block blob input stream.
   * @throws IOException
   */
  @Test
  public void test_0316_SequentialReadAfterReverseSeekPerformanceV2()
      throws IOException {
    assumeHugeFileExists();
    final int maxAttempts = 10;
    final double maxAcceptableRatio = 1.01;
    double beforeSeekElapsedMs = 0, afterSeekElapsedMs = 0;
    double ratio = Double.MAX_VALUE;
    for (int i = 0; i < maxAttempts && ratio >= maxAcceptableRatio; i++) {
      beforeSeekElapsedMs = sequentialRead(2,
          accountUsingInputStreamV2.getFileSystem(), false);
      afterSeekElapsedMs = sequentialRead(2,
          accountUsingInputStreamV2.getFileSystem(), true);
      ratio = afterSeekElapsedMs / beforeSeekElapsedMs;
      LOG.info(String.format(
          "beforeSeekElapsedMs=%1$d, afterSeekElapsedMs=%2$d, ratio=%3$.2f",
          (long) beforeSeekElapsedMs,
          (long) afterSeekElapsedMs,
          ratio));
    }
    assertTrue(String.format(
        "Performance of version 2 after reverse seek is not acceptable:"
            + " beforeSeekElapsedMs=%1$d, afterSeekElapsedMs=%2$d,"
            + " ratio=%3$.2f",
        (long) beforeSeekElapsedMs,
        (long) afterSeekElapsedMs,
        ratio),
        ratio < maxAcceptableRatio);
  }

  private long sequentialRead(int version,
      FileSystem fs,
      boolean afterReverseSeek) throws IOException {
    byte[] buffer = new byte[16 * KILOBYTE];
    long totalBytesRead = 0;
    long bytesRead = 0;

    try(FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      if (afterReverseSeek) {
        while (bytesRead > 0 && totalBytesRead < 4 * MEGABYTE) {
          bytesRead = inputStream.read(buffer);
          totalBytesRead += bytesRead;
        }
        totalBytesRead = 0;
        inputStream.seek(0);
      }

      NanoTimer timer = new NanoTimer();
      while ((bytesRead = inputStream.read(buffer)) > 0) {
        totalBytesRead += bytesRead;
      }
      long elapsedTimeMs = timer.elapsedTimeMs();

      LOG.info(String.format(
          "v%1$d: bytesRead=%2$d, elapsedMs=%3$d, Mbps=%4$.2f,"
              + " afterReverseSeek=%5$s",
          version,
          totalBytesRead,
          elapsedTimeMs,
          toMbps(totalBytesRead, elapsedTimeMs),
          afterReverseSeek));

      assertEquals(testFileLength, totalBytesRead);
      inputStream.close();
      return elapsedTimeMs;
    }
  }

  @Test
  public void test_0317_RandomReadPerformance() throws IOException {
    assumeHugeFileExists();
    final int maxAttempts = 10;
    final double maxAcceptableRatio = 0.10;
    double v1ElapsedMs = 0, v2ElapsedMs = 0;
    double ratio = Double.MAX_VALUE;
    for (int i = 0; i < maxAttempts && ratio >= maxAcceptableRatio; i++) {
      v1ElapsedMs = randomRead(1,
          accountUsingInputStreamV1.getFileSystem());
      v2ElapsedMs = randomRead(2,
          accountUsingInputStreamV2.getFileSystem());
      ratio = v2ElapsedMs / v1ElapsedMs;
      LOG.info(String.format(
          "v1ElapsedMs=%1$d, v2ElapsedMs=%2$d, ratio=%3$.2f",
          (long) v1ElapsedMs,
          (long) v2ElapsedMs,
          ratio));
    }
    assertTrue(String.format(
        "Performance of version 2 is not acceptable: v1ElapsedMs=%1$d,"
            + " v2ElapsedMs=%2$d, ratio=%3$.2f",
        (long) v1ElapsedMs,
        (long) v2ElapsedMs,
        ratio),
        ratio < maxAcceptableRatio);
  }

  private long randomRead(int version, FileSystem fs) throws IOException {
    assumeHugeFileExists();
    final int minBytesToRead = 2 * MEGABYTE;
    Random random = new Random();
    byte[] buffer = new byte[8 * KILOBYTE];
    long totalBytesRead = 0;
    long bytesRead = 0;
    try(FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      NanoTimer timer = new NanoTimer();

      do {
        bytesRead = inputStream.read(buffer);
        totalBytesRead += bytesRead;
        inputStream.seek(random.nextInt(
            (int) (testFileLength - buffer.length)));
      } while (bytesRead > 0 && totalBytesRead < minBytesToRead);

      long elapsedTimeMs = timer.elapsedTimeMs();

      inputStream.close();

      LOG.info(String.format(
          "v%1$d: totalBytesRead=%2$d, elapsedTimeMs=%3$d, Mbps=%4$.2f",
          version,
          totalBytesRead,
          elapsedTimeMs,
          toMbps(totalBytesRead, elapsedTimeMs)));

      assertTrue(minBytesToRead <= totalBytesRead);

      return elapsedTimeMs;
    }
  }

  @Test
  public void test_999_DeleteHugeFiles() throws IOException {
    try {
      NanoTimer timer = new NanoTimer();
      NativeAzureFileSystem fs = getFileSystem();
      fs.delete(TEST_FILE_PATH, false);
      timer.end("time to delete %s", TEST_FILE_PATH);
    } finally {
      // clean up the test account
      AzureTestUtils.cleanupTestAccount(accountUsingInputStreamV1);
    }
  }

}
