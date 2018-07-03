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
package org.apache.hadoop.fs.azurebfs;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertArrayEquals;


/**
 * Test random read operation.
 */
public class ITestAzureBlobFileSystemRandomRead extends DependencyInjectedTest {
  private static final int KILOBYTE = 1024;
  private static final int MEGABYTE = KILOBYTE * KILOBYTE;
  private static final long TEST_FILE_SIZE = 8 * MEGABYTE;
  private static final int MAX_ELAPSEDTIMEMS = 20;
  private static final int SEQUENTIAL_READ_BUFFER_SIZE = 16 * KILOBYTE;
  private static final int CREATE_BUFFER_SIZE = 26 * KILOBYTE;

  private static final int SEEK_POSITION_ONE = 2* KILOBYTE;
  private static final int SEEK_POSITION_TWO = 5 * KILOBYTE;
  private static final int SEEK_POSITION_THREE = 10 * KILOBYTE;
  private static final int SEEK_POSITION_FOUR = 4100 * KILOBYTE;

  private static final Path TEST_FILE_PATH = new Path(
            "/TestRandomRead.txt");
  private static final String WASB = "WASB";
  private static final String ABFS = "ABFS";
  private static long testFileLength = 0;

  public ITestAzureBlobFileSystemRandomRead() throws Exception {
    super();
  }

  @Test
  public void testBasicRead() throws Exception {
    assumeHugeFileExists();

    try (FSDataInputStream inputStream = this.getFileSystem().open(TEST_FILE_PATH)) {
      byte[] buffer = new byte[3 * MEGABYTE];

      // forward seek and read a kilobyte into first kilobyte of bufferV2
      inputStream.seek(5 * MEGABYTE);
      int numBytesRead = inputStream.read(buffer, 0, KILOBYTE);
      assertEquals(KILOBYTE, numBytesRead);

      int len = MEGABYTE;
      int offset = buffer.length - len;

      // reverse seek and read a megabyte into last megabyte of bufferV1
      inputStream.seek(3 * MEGABYTE);
      numBytesRead = inputStream.read(buffer, offset, len);
      assertEquals(len, numBytesRead);
    }
  }

  /**
   * Validates the implementation of random read in ABFS
   * @throws IOException
   */
  @Test
  public void testRandomRead() throws Exception {
    assumeHugeFileExists();
    try (
            FSDataInputStream inputStreamV1
                    = this.getFileSystem().open(TEST_FILE_PATH);
            FSDataInputStream inputStreamV2
                    = this.getWasbFileSystem().open(TEST_FILE_PATH);
    ) {
      final int bufferSize = 4 * KILOBYTE;
      byte[] bufferV1 = new byte[bufferSize];
      byte[] bufferV2 = new byte[bufferV1.length];

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      inputStreamV1.seek(0);
      inputStreamV2.seek(0);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      inputStreamV1.seek(SEEK_POSITION_ONE);
      inputStreamV2.seek(SEEK_POSITION_ONE);

      inputStreamV1.seek(0);
      inputStreamV2.seek(0);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      inputStreamV1.seek(SEEK_POSITION_TWO);
      inputStreamV2.seek(SEEK_POSITION_TWO);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      inputStreamV1.seek(SEEK_POSITION_THREE);
      inputStreamV2.seek(SEEK_POSITION_THREE);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);

      inputStreamV1.seek(SEEK_POSITION_FOUR);
      inputStreamV2.seek(SEEK_POSITION_FOUR);

      verifyConsistentReads(inputStreamV1, inputStreamV2, bufferV1, bufferV2);
    }
  }

  /**
   * Validates the implementation of Seekable.seekToNewSource
   * @throws IOException
   */
  @Test
  public void testSeekToNewSource() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = this.getFileSystem().open(TEST_FILE_PATH)) {
      assertFalse(inputStream.seekToNewSource(0));
    }
  }

  /**
   * Validates the implementation of InputStream.skip and ensures there is no
   * network I/O for AbfsInputStream
   * @throws Exception
   */
  @Test
  public void testSkipBounds() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = this.getFileSystem().open(TEST_FILE_PATH)) {
      ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

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
              elapsedTimeMs < MAX_ELAPSEDTIMEMS);
    }
  }

  /**
   * Validates the implementation of Seekable.seek and ensures there is no
   * network I/O for forward seek.
   * @throws Exception
   */
  @Test
  public void testValidateSeekBounds() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = this.getFileSystem().open(TEST_FILE_PATH)) {
      ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

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
              elapsedTimeMs < MAX_ELAPSEDTIMEMS);
    }
  }

  /**
   * Validates the implementation of Seekable.seek, Seekable.getPos,
   * and InputStream.available.
   * @throws Exception
   */
  @Test
  public void testSeekAndAvailableAndPosition() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = this.getFileSystem().open(TEST_FILE_PATH)) {
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
  public void testSkipAndAvailableAndPosition() throws Exception {
    assumeHugeFileExists();
    try (FSDataInputStream inputStream = this.getFileSystem().open(TEST_FILE_PATH)) {
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
   * Ensures parity in the performance of sequential read after reverse seek for
   * abfs of the AbfsInputStream.
   * @throws IOException
   */
  @Test
  public void testSequentialReadAfterReverseSeekPerformance()
          throws Exception {
    assumeHugeFileExists();
    final int maxAttempts = 10;
    final double maxAcceptableRatio = 1.01;
    double beforeSeekElapsedMs = 0, afterSeekElapsedMs = 0;
    double ratio = Double.MAX_VALUE;
    for (int i = 0; i < maxAttempts && ratio >= maxAcceptableRatio; i++) {
      beforeSeekElapsedMs = sequentialRead(ABFS,
              this.getFileSystem(), false);
      afterSeekElapsedMs = sequentialRead(ABFS,
              this.getFileSystem(), true);
      ratio = afterSeekElapsedMs / beforeSeekElapsedMs;
      System.out.println((String.format(
              "beforeSeekElapsedMs=%1$d, afterSeekElapsedMs=%2$d, ratio=%3$.2f",
              (long) beforeSeekElapsedMs,
              (long) afterSeekElapsedMs,
              ratio)));
    }
    assertTrue(String.format(
            "Performance of ABFS stream after reverse seek is not acceptable:"
                    + " beforeSeekElapsedMs=%1$d, afterSeekElapsedMs=%2$d,"
                    + " ratio=%3$.2f",
            (long) beforeSeekElapsedMs,
            (long) afterSeekElapsedMs,
            ratio),
            ratio < maxAcceptableRatio);
  }

  @Test
  public void testRandomReadPerformance() throws Exception {
    createTestFile();
    assumeHugeFileExists();

    final AzureBlobFileSystem abFs = this.getFileSystem();
    final NativeAzureFileSystem wasbFs = this.getWasbFileSystem();

    final int maxAttempts = 10;
    final double maxAcceptableRatio = 1.025;
    double v1ElapsedMs = 0, v2ElapsedMs = 0;
    double ratio = Double.MAX_VALUE;
    for (int i = 0; i < maxAttempts && ratio >= maxAcceptableRatio; i++) {
      v1ElapsedMs = randomRead(1, wasbFs);
      v2ElapsedMs = randomRead(2, abFs);

      ratio = v2ElapsedMs / v1ElapsedMs;

      System.out.println(String.format(
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


  private long sequentialRead(String version,
                              FileSystem fs,
                              boolean afterReverseSeek) throws IOException {
    byte[] buffer = new byte[SEQUENTIAL_READ_BUFFER_SIZE];
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

      ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
      while ((bytesRead = inputStream.read(buffer)) > 0) {
        totalBytesRead += bytesRead;
      }
      long elapsedTimeMs = timer.elapsedTimeMs();

      System.out.println(String.format(
              "v%1$s: bytesRead=%2$d, elapsedMs=%3$d, Mbps=%4$.2f,"
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

  private long randomRead(int version, FileSystem fs) throws Exception {
    assumeHugeFileExists();
    final long minBytesToRead = 2 * MEGABYTE;
    Random random = new Random();
    byte[] buffer = new byte[8 * KILOBYTE];
    long totalBytesRead = 0;
    long bytesRead = 0;
    try(FSDataInputStream inputStream = fs.open(TEST_FILE_PATH)) {
      ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
      do {
        bytesRead = inputStream.read(buffer);
        totalBytesRead += bytesRead;
        inputStream.seek(random.nextInt(
                (int) (TEST_FILE_SIZE - buffer.length)));
        } while (bytesRead > 0 && totalBytesRead < minBytesToRead);
      long elapsedTimeMs = timer.elapsedTimeMs();
      inputStream.close();
      System.out.println(String.format(
              "v%1$d: totalBytesRead=%2$d, elapsedTimeMs=%3$d, Mbps=%4$.2f",
              version,
              totalBytesRead,
              elapsedTimeMs,
              toMbps(totalBytesRead, elapsedTimeMs)));
      assertTrue(minBytesToRead <= totalBytesRead);
      return elapsedTimeMs;
    }
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

  private void createTestFile() throws Exception {
    FileSystem fs = this.getWasbFileSystem();

    if (fs.exists(TEST_FILE_PATH)) {
      FileStatus status = fs.getFileStatus(TEST_FILE_PATH);
      if (status.getLen() >= TEST_FILE_SIZE) {
        return;
      }
    }

    byte[] buffer = new byte[CREATE_BUFFER_SIZE];
    char character = 'a';
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) character;
      character = (character == 'z') ? 'a' : (char) ((int) character + 1);
    }

    System.out.println(String.format("Creating test file %s of size: %d ", TEST_FILE_PATH, TEST_FILE_SIZE));
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

    try (FSDataOutputStream outputStream = fs.create(TEST_FILE_PATH)) {
      int bytesWritten = 0;
      while (bytesWritten < TEST_FILE_SIZE) {
        outputStream.write(buffer);
        bytesWritten += buffer.length;
      }
      System.out.println(String.format("Closing stream %s", outputStream));
      ContractTestUtils.NanoTimer closeTimer
              = new ContractTestUtils.NanoTimer();
      outputStream.close();
      closeTimer.end("time to close() output stream");
    }
    timer.end("time to write %d KB", TEST_FILE_SIZE / 1024);
    testFileLength = fs.getFileStatus(TEST_FILE_PATH).getLen();

  }

  private void assumeHugeFileExists() throws Exception{
    createTestFile();
    FileSystem fs = this.getFileSystem();
    ContractTestUtils.assertPathExists(this.getFileSystem(), "huge file not created", TEST_FILE_PATH);
    FileStatus status = fs.getFileStatus(TEST_FILE_PATH);
    ContractTestUtils.assertIsFile(TEST_FILE_PATH, status);
    assertTrue("File " + TEST_FILE_PATH + " is empty", status.getLen() > 0);
  }

  private void verifyConsistentReads(FSDataInputStream inputStreamV1,
                                     FSDataInputStream inputStreamV2,
                                     byte[] bufferV1,
                                     byte[] bufferV2) throws IOException {
    int size = bufferV1.length;
    final int numBytesReadV1 = inputStreamV1.read(bufferV1, 0, size);
    assertEquals("Bytes read from wasb stream", size, numBytesReadV1);

    final int numBytesReadV2 = inputStreamV2.read(bufferV2, 0, size);
    assertEquals("Bytes read from abfs stream", size, numBytesReadV2);

    assertArrayEquals("Mismatch in read data", bufferV1, bufferV2);
  }

}