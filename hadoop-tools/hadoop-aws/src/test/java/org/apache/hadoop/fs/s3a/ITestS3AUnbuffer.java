/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.io.IOUtils;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_BYTES_READ_CLOSE;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_TOTAL_BYTES;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatisticsSource;

/**
 * Integration test for calling
 * {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer} on {@link S3AInputStream}.
 * Validates that the object has been closed using the
 * {@link S3AInputStream#isObjectStreamOpen()} method. Unlike the
 * {@link org.apache.hadoop.fs.contract.s3a.ITestS3AContractUnbuffer} tests,
 * these tests leverage the fact that isObjectStreamOpen exposes if the
 * underlying stream has been closed or not.
 */
public class ITestS3AUnbuffer extends AbstractS3ATestBase {

  public static final int FILE_LENGTH = 16;

  private Path dest;

  @Override
  public void setup() throws Exception {
    super.setup();
    dest = path("ITestS3AUnbuffer");
    describe("ITestS3AUnbuffer");

    byte[] data = ContractTestUtils.dataset(FILE_LENGTH, 'a', 26);
    ContractTestUtils.writeDataset(getFileSystem(), dest, data, data.length,
            16, true);
  }

  @Test
  public void testUnbuffer() throws IOException {
    describe("testUnbuffer");

    IOStatisticsSnapshot iostats = new IOStatisticsSnapshot();
    // Open file, read half the data, and then call unbuffer
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      assertTrue(inputStream.getWrappedStream() instanceof S3AInputStream);
      int bytesToRead = 8;
      readAndAssertBytesRead(inputStream, bytesToRead);
      assertTrue(isObjectStreamOpen(inputStream));
      assertTrue("No IOstatistics from " + inputStream,
          iostats.aggregate(inputStream.getIOStatistics()));
      verifyStatisticCounterValue(iostats,
          StreamStatisticNames.STREAM_READ_BYTES,
          bytesToRead);
      verifyStatisticCounterValue(iostats,
          StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
          1);

      // do the unbuffering
      inputStream.unbuffer();

      // audit the updated statistics
      IOStatistics st2 = inputStream.getIOStatistics();

      // the unbuffered operation must be tracked
      verifyStatisticCounterValue(st2,
          StreamStatisticNames.STREAM_READ_UNBUFFERED,
          1);

      // all other counter values consistent.
      verifyStatisticCounterValue(st2,
          StreamStatisticNames.STREAM_READ_BYTES,
          bytesToRead);
      verifyStatisticCounterValue(st2,
          StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
          1);

      // Check the the wrapped stream is closed
      assertFalse(isObjectStreamOpen(inputStream));
    }
  }

  /**
   * Test that calling {@link S3AInputStream#unbuffer()} merges a stream's
   * {@code InputStreamStatistics}
   * into the {@link S3AFileSystem}'s {@link S3AInstrumentation} instance.
   */
  @Test
  public void testUnbufferStreamStatistics() throws IOException {
    describe("testUnbufferStreamStatistics");

    // Validate bytesRead is updated correctly
    S3AFileSystem fs = getFileSystem();
    S3ATestUtils.MetricDiff bytesRead = new S3ATestUtils.MetricDiff(
        fs, STREAM_READ_BYTES);
    S3ATestUtils.MetricDiff totalBytesRead = new S3ATestUtils.MetricDiff(
        fs, STREAM_READ_TOTAL_BYTES);
    S3ATestUtils.MetricDiff bytesReadInClose = new S3ATestUtils.MetricDiff(
        fs, STREAM_READ_BYTES_READ_CLOSE);

    // Open file, read half the data, and then call unbuffer
    FSDataInputStream inputStream = null;
    int firstBytesToRead = 8;

    int secondBytesToRead = 1;
    long expectedFinalBytesRead;
    long expectedTotalBytesRead;

    Object streamStatsStr;
    try {
      inputStream = fs.open(dest);
      streamStatsStr = demandStringifyIOStatisticsSource(inputStream);

      LOG.info("initial stream statistics {}", streamStatsStr);
      readAndAssertBytesRead(inputStream, firstBytesToRead);
      LOG.info("stream statistics after read {}", streamStatsStr);
      inputStream.unbuffer();

      // Validate that calling unbuffer updates the input stream statistics
      bytesRead.assertDiffEquals(firstBytesToRead);
      final long bytesInUnbuffer = bytesReadInClose.diff();
      totalBytesRead.assertDiffEquals(firstBytesToRead + bytesInUnbuffer);

      // Validate that calling unbuffer twice in a row updates the statistics
      // correctly
      bytesReadInClose.reset();
      bytesRead.reset();
      readAndAssertBytesRead(inputStream, secondBytesToRead);
      inputStream.unbuffer();
      LOG.info("stream statistics after second read {}", streamStatsStr);
      bytesRead.assertDiffEquals(secondBytesToRead);
      final long bytesInClose = bytesReadInClose.diff();
      expectedFinalBytesRead = firstBytesToRead + secondBytesToRead;
      expectedTotalBytesRead = expectedFinalBytesRead
          + bytesInUnbuffer + bytesInClose;

      totalBytesRead.assertDiffEquals(expectedTotalBytesRead);
    } finally {
      LOG.info("Closing stream");
      IOUtils.closeStream(inputStream);
    }
    LOG.info("stream statistics after close {}", streamStatsStr);

    // Validate that closing the file does not further change the statistics
    totalBytesRead.assertDiffEquals(expectedTotalBytesRead);

    // Validate that the input stream stats are correct when the file is closed
    S3AInputStreamStatistics streamStatistics = ((S3AInputStream) inputStream
        .getWrappedStream())
        .getS3AStreamStatistics();
    Assertions.assertThat(streamStatistics)
        .describedAs("Stream statistics %s", streamStatistics)
        .hasFieldOrPropertyWithValue("bytesRead",
            expectedFinalBytesRead)
        .hasFieldOrPropertyWithValue("totalBytesRead", expectedTotalBytesRead);
    assertEquals("S3AInputStream statistics were not updated properly in "
        + streamStatsStr,
        expectedFinalBytesRead,
            streamStatistics.getBytesRead());
  }

  private boolean isObjectStreamOpen(FSDataInputStream inputStream) {
    return ((S3AInputStream) inputStream.getWrappedStream()).isObjectStreamOpen();
  }

  /**
   * Read the specified number of bytes from the given
   * {@link FSDataInputStream} and assert that
   * {@link FSDataInputStream#read(byte[])} read the specified number of bytes.
   */
  private static void readAndAssertBytesRead(FSDataInputStream inputStream,
                                        int bytesToRead) throws IOException {
    assertEquals("S3AInputStream#read did not read the correct number of " +
                    "bytes", bytesToRead,
            inputStream.read(new byte[bytesToRead]));
  }
}
