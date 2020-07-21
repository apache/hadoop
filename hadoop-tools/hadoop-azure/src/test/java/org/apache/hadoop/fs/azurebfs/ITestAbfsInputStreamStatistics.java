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

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.io.IOUtils;

public class ITestAbfsInputStreamStatistics
    extends AbstractAbfsIntegrationTest {
  private static final int OPERATIONS = 10;
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsInputStreamStatistics.class);
  private static final int ONE_MB = 1024 * 1024;
  private static final int ONE_KB = 1024;
  private static final int CUSTOM_BLOCK_BUFFER_SIZE = 4 * 1024;
  private static final int CUSTOM_READ_AHEAD_BUFFER_SIZE = 8 * CUSTOM_BLOCK_BUFFER_SIZE;
  private static final int THREAD_SLEEP_10_SECONDS = 10;
  private static final int TIMEOUT_30_SECONDS = 30000;
  private byte[] defBuffer = new byte[ONE_MB];

  public ITestAbfsInputStreamStatistics() throws Exception {
  }

  /**
   * Test to check the initial values of the AbfsInputStream statistics.
   */
  @Test
  public void testInitValues() throws IOException {
    describe("Testing the initial values of AbfsInputStream Statistics");

    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfss = fs.getAbfsStore();
    Path initValuesPath = path(getMethodName());
    AbfsOutputStream outputStream = null;
    AbfsInputStream inputStream = null;

    try {

      outputStream = createAbfsOutputStreamWithFlushEnabled(fs, initValuesPath);
      inputStream = abfss.openFileForRead(initValuesPath, fs.getFsStatistics());

      AbfsInputStreamStatisticsImpl stats =
          (AbfsInputStreamStatisticsImpl) inputStream.getStreamStatistics();

      checkInitValue(stats.getSeekOperations(), "seekOps");
      checkInitValue(stats.getForwardSeekOperations(), "forwardSeekOps");
      checkInitValue(stats.getBackwardSeekOperations(), "backwardSeekOps");
      checkInitValue(stats.getBytesRead(), "bytesRead");
      checkInitValue(stats.getBytesSkippedOnSeek(), "bytesSkippedOnSeek");
      checkInitValue(stats.getBytesBackwardsOnSeek(), "bytesBackwardsOnSeek");
      checkInitValue(stats.getSeekInBuffer(), "seekInBuffer");
      checkInitValue(stats.getReadOperations(), "readOps");
      checkInitValue(stats.getBytesReadFromBuffer(), "bytesReadFromBuffer");
      checkInitValue(stats.getRemoteReadOperations(), "remoteReadOps");
      checkInitValue(stats.getReadAheadBytesRead(), "readAheadBytesRead");
      checkInitValue(stats.getRemoteBytesRead(), "readAheadRemoteBytesRead");

    } finally {
      IOUtils.cleanupWithLogger(LOG, outputStream, inputStream);
    }
  }

  /**
   * Test to check statistics from seek operation in AbfsInputStream.
   */
  @Test
  public void testSeekStatistics() throws IOException {
    describe("Testing the values of statistics from seek operations in "
        + "AbfsInputStream");

    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfss = fs.getAbfsStore();
    Path seekStatPath = path(getMethodName());

    AbfsOutputStream out = null;
    AbfsInputStream in = null;

    try {
      out = createAbfsOutputStreamWithFlushEnabled(fs, seekStatPath);

      //Writing a default buffer in a file.
      out.write(defBuffer);
      out.hflush();
      in = abfss.openFileForRead(seekStatPath, fs.getFsStatistics());

      /*
       * Writing 1MB buffer to the file, this would make the fCursor(Current
       * position of cursor) to the end of file.
       */
      int result = in.read(defBuffer, 0, ONE_MB);
      LOG.info("Result of read : {}", result);

      /*
       * Seeking to start of file and then back to end would result in a
       * backward and a forward seek respectively 10 times.
       */
      for (int i = 0; i < OPERATIONS; i++) {
        in.seek(0);
        in.seek(ONE_MB);
      }

      AbfsInputStreamStatisticsImpl stats =
          (AbfsInputStreamStatisticsImpl) in.getStreamStatistics();

      LOG.info("STATISTICS: {}", stats.toString());

      /*
       * seekOps - Since we are doing backward and forward seek OPERATIONS
       * times, total seeks would be 2 * OPERATIONS.
       *
       * backwardSeekOps - Since we are doing a backward seek inside a loop
       * for OPERATION times, total backward seeks would be OPERATIONS.
       *
       * forwardSeekOps - Since we are doing a forward seek inside a loop
       * for OPERATION times, total forward seeks would be OPERATIONS.
       *
       * bytesBackwardsOnSeek - Since we are doing backward seeks from end of
       * file in a ONE_MB file each time, this would mean the bytes from
       * backward seek would be OPERATIONS * ONE_MB. Since this is backward
       * seek this value is expected be to be negative.
       *
       * bytesSkippedOnSeek - Since, we move from start to end in seek, but
       * our fCursor(position of cursor) always remain at end of file, this
       * would mean no bytes were skipped on seek. Since, all forward seeks
       * are in buffer.
       *
       * seekInBuffer - Since all seeks were in buffer, the seekInBuffer
       * would be equal to 2 * OPERATIONS.
       *
       */
      assertEquals("Mismatch in seekOps value", 2 * OPERATIONS,
          stats.getSeekOperations());
      assertEquals("Mismatch in backwardSeekOps value", OPERATIONS,
          stats.getBackwardSeekOperations());
      assertEquals("Mismatch in forwardSeekOps value", OPERATIONS,
          stats.getForwardSeekOperations());
      assertEquals("Mismatch in bytesBackwardsOnSeek value",
          -1 * OPERATIONS * ONE_MB, stats.getBytesBackwardsOnSeek());
      assertEquals("Mismatch in bytesSkippedOnSeek value",
          0, stats.getBytesSkippedOnSeek());
      assertEquals("Mismatch in seekInBuffer value", 2 * OPERATIONS,
          stats.getSeekInBuffer());

      in.close();
      // Verifying whether stats are readable after stream is closed.
      LOG.info("STATISTICS after closing: {}", stats.toString());
    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }
  }

  /**
   * Test to check statistics value from read operation in AbfsInputStream.
   */
  @Test
  public void testReadStatistics() throws IOException {
    describe("Testing the values of statistics from read operation in "
        + "AbfsInputStream");

    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfss = fs.getAbfsStore();
    Path readStatPath = path(getMethodName());

    AbfsOutputStream out = null;
    AbfsInputStream in = null;

    try {
      out = createAbfsOutputStreamWithFlushEnabled(fs, readStatPath);

      /*
       * Writing 1MB buffer to the file.
       */
      out.write(defBuffer);
      out.hflush();
      in = abfss.openFileForRead(readStatPath, fs.getFsStatistics());

      /*
       * Doing file read 10 times.
       */
      for (int i = 0; i < OPERATIONS; i++) {
        in.read();
      }

      AbfsInputStreamStatisticsImpl stats =
          (AbfsInputStreamStatisticsImpl) in.getStreamStatistics();

      LOG.info("STATISTICS: {}", stats.toString());

      /*
       * bytesRead - Since each time a single byte is read, total
       * bytes read would be equal to OPERATIONS.
       *
       * readOps - Since each time read operation is performed OPERATIONS
       * times, total number of read operations would be equal to OPERATIONS.
       *
       * remoteReadOps - Only a single remote read operation is done. Hence,
       * total remote read ops is 1.
       *
       */
      assertEquals("Mismatch in bytesRead value", OPERATIONS,
          stats.getBytesRead());
      assertEquals("Mismatch in readOps value", OPERATIONS,
          stats.getReadOperations());
      assertEquals("Mismatch in remoteReadOps value", 1,
          stats.getRemoteReadOperations());

      in.close();
      // Verifying if stats are still readable after stream is closed.
      LOG.info("STATISTICS after closing: {}", stats.toString());
    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }
  }

  /**
   * Testing AbfsInputStream works with null Statistics.
   */
  @Test
  public void testWithNullStreamStatistics() throws IOException {
    describe("Testing AbfsInputStream operations with statistics as null");

    AzureBlobFileSystem fs = getFileSystem();
    Path nullStatFilePath = path(getMethodName());
    byte[] oneKbBuff = new byte[ONE_KB];

    // Creating an AbfsInputStreamContext instance with null StreamStatistics.
    AbfsInputStreamContext abfsInputStreamContext =
        new AbfsInputStreamContext(
            getConfiguration().getSasTokenRenewPeriodForStreamsInSeconds())
            .withReadBufferSize(getConfiguration().getReadBufferSize())
            .withReadAheadQueueDepth(getConfiguration().getReadAheadQueueDepth())
            .withStreamStatistics(null)
            .build();

    AbfsOutputStream out = null;
    AbfsInputStream in = null;

    try {
      out = createAbfsOutputStreamWithFlushEnabled(fs, nullStatFilePath);

      // Writing a 1KB buffer in the file.
      out.write(oneKbBuff);
      out.hflush();

      // AbfsRestOperation Instance required for eTag.
      AbfsRestOperation abfsRestOperation =
          fs.getAbfsClient().getPathStatus(nullStatFilePath.toUri().getPath(), false);

      // AbfsInputStream with no StreamStatistics.
      in = new AbfsInputStream(fs.getAbfsClient(), null,
          nullStatFilePath.toUri().getPath(), ONE_KB,
          abfsInputStreamContext,
          abfsRestOperation.getResult().getResponseHeader("ETag"));

      // Verifying that AbfsInputStream Operations works with null statistics.
      assertNotEquals("AbfsInputStream read() with null statistics should "
          + "work", -1, in.read());
      in.seek(ONE_KB);

      // Verifying toString() with no StreamStatistics.
      LOG.info("AbfsInputStream: {}", in.toString());
    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }
  }

  /**
   * Testing readAhead counters in AbfsInputStream with 30 seconds timeout.
   */
  @Test(timeout = TIMEOUT_30_SECONDS)
  public void testReadAheadCounters() throws IOException, InterruptedException {
    describe("Test to check correct values for readAhead counters in "
        + "AbfsInputStream");

    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfss = fs.getAbfsStore();
    Path readAheadCountersPath = path(getMethodName());

    /*
     * Setting the block size for readAhead as 4KB.
     */
    abfss.getAbfsConfiguration().setReadBufferSize(CUSTOM_BLOCK_BUFFER_SIZE);

    AbfsOutputStream out = null;
    AbfsInputStream in = null;

    try {

      /*
       * Creating a file of 1MB size.
       */
      out = createAbfsOutputStreamWithFlushEnabled(fs, readAheadCountersPath);
      out.write(defBuffer);
      out.close();

      in = abfss.openFileForRead(readAheadCountersPath, fs.getFsStatistics());

      /*
       * Reading 1KB after each i * KB positions. Hence the reads are from 0
       * to 1KB, 1KB to 2KB, and so on.. for 5 operations.
       */
      for (int i = 0; i < 5; i++) {
        in.seek(ONE_KB * i);
        in.read(defBuffer, ONE_KB * i, ONE_KB);
      }
      AbfsInputStreamStatisticsImpl stats =
          (AbfsInputStreamStatisticsImpl) in.getStreamStatistics();

      /*
       * Since, readAhead is done in background threads. Sometimes, the
       * threads aren't finished in the background and could result in
       * inaccurate results. So, we wait till we have the accurate values
       * with a limit of 30 seconds as that's when the test times out.
       *
       */
      while (stats.getRemoteBytesRead() < CUSTOM_READ_AHEAD_BUFFER_SIZE
          || stats.getReadAheadBytesRead() < CUSTOM_BLOCK_BUFFER_SIZE) {
        Thread.sleep(THREAD_SLEEP_10_SECONDS);
      }

      /*
       * Verifying the counter values of readAheadBytesRead and remoteBytesRead.
       *
       * readAheadBytesRead : Since, we read 1KBs 5 times, that means we go
       * from 0 to 5KB in the file. The bufferSize is set to 4KB, and since
       * we have 8 blocks of readAhead buffer. We would have 8 blocks of 4KB
       * buffer. Our read is till 5KB, hence readAhead would ideally read 2
       * blocks of 4KB which is equal to 8KB. But, sometimes to get more than
       * one block from readAhead buffer we might have to wait for background
       * threads to fill the buffer and hence we might do remote read which
       * would be faster. Therefore, readAheadBytesRead would be equal to or
       * greater than 4KB.
       *
       * remoteBytesRead : Since, the bufferSize is set to 4KB and the number
       * of blocks or readAheadQueueDepth is equal to 8. We would read 8 * 4
       * KB buffer on the first read, which is equal to 32KB. But, if we are not
       * able to read some bytes that were in the buffer after doing
       * readAhead, we might use remote read again. Thus, the bytes read
       * remotely could also be greater than 32Kb.
       *
       */
      Assertions.assertThat(stats.getReadAheadBytesRead()).describedAs(
          "Mismatch in readAheadBytesRead counter value")
          .isGreaterThanOrEqualTo(CUSTOM_BLOCK_BUFFER_SIZE);

      Assertions.assertThat(stats.getRemoteBytesRead()).describedAs(
          "Mismatch in remoteBytesRead counter value")
          .isGreaterThanOrEqualTo(CUSTOM_READ_AHEAD_BUFFER_SIZE);

    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }
  }

  /**
   * Method to assert the initial values of the statistics.
   *
   * @param actualValue the actual value of the statistics.
   * @param statistic   the name of operation or statistic being asserted.
   */
  private void checkInitValue(long actualValue, String statistic) {
    assertEquals("Mismatch in " + statistic + " value", 0, actualValue);
  }
}
