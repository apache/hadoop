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
   * Method to assert the initial values of the statistics.
   *
   * @param actualValue the actual value of the statistics.
   * @param statistic   the name of operation or statistic being asserted.
   */
  private void checkInitValue(long actualValue, String statistic) {
    assertEquals("Mismatch in " + statistic + " value", 0, actualValue);
  }
}
