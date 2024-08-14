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

package org.apache.hadoop.fs.s3a.prefetch;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;

import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPrefetchOption;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMaximum;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticGaugeValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsContext.getCurrentIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCK_CACHE_ENABLED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCK_PREFETCH_ENABLED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS;
import static org.apache.hadoop.test.Sizes.S_16K;
import static org.apache.hadoop.test.Sizes.S_1K;
import static org.apache.hadoop.test.Sizes.S_4K;

/**
 * Test the prefetching input stream, validates that the
 * {@link S3AInMemoryInputStream} is working as expected.
 */
public class ITestInMemoryInputStream extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestInMemoryInputStream.class);

  // Size should be < block size so S3AInMemoryInputStream is used
  private static final int SMALL_FILE_SIZE = S_16K;

  /**
   * Thread level IOStatistics; reset in setup().
   */
  private IOStatisticsContext ioStatisticsContext;

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_ENABLED_KEY);
    setPrefetchOption(conf, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    ioStatisticsContext = getCurrentIOStatisticsContext();
    ioStatisticsContext.reset();
  }

  private void printStreamStatistics(final FSDataInputStream in) {
    LOG.info("Stream statistics\n{}",
        ioStatisticsToPrettyString(in.getIOStatistics()));
  }

  @Test
  public void testRandomReadSmallFile() throws Throwable {
    describe("random read on a small file, uses S3AInMemoryInputStream");

    byte[] data = ContractTestUtils.dataset(SMALL_FILE_SIZE, 'a', 26);
    Path smallFile = path("randomReadSmallFile");
    ContractTestUtils.writeDataset(getFileSystem(), smallFile, data, data.length, 16, true);

    int expectedReadBytes = 0;
    try (FSDataInputStream in = getFileSystem().open(smallFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[SMALL_FILE_SIZE];

      in.read(buffer, 0, S_4K);
      expectedReadBytes += S_4K;
      verifyStatisticCounterValue(ioStats, STREAM_READ_BYTES,
          expectedReadBytes);

      in.seek(S_1K * 12);
      in.read(buffer, 0, S_4K);
      expectedReadBytes += S_4K;

      verifyStatisticCounterValue(ioStats, STREAM_READ_BYTES,
          expectedReadBytes);
      printStreamStatistics(in);

      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, 1);
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS, 0);
      // the whole file is loaded
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, SMALL_FILE_SIZE);
      // there is no prefetching
      verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCK_PREFETCH_ENABLED, 0);
      // there is no caching
      verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCK_CACHE_ENABLED, 0);
      // no prefetch ops, so no action_executor_acquired
      assertThatStatisticMaximum(ioStats,
          ACTION_EXECUTOR_ACQUIRED + SUFFIX_MAX).isEqualTo(-1);

      // now read offset 0 again and again, expect no new costs
      in.readFully(0, buffer);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);
      expectedReadBytes += buffer.length;

      verifyStatisticCounterValue(ioStats, STREAM_READ_BYTES,
          expectedReadBytes);
      // unbuffer
      in.unbuffer();
      LOG.info("unbuffered {}", in);
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);

      printStreamStatistics(in);

      in.readFully(0, buffer);
      verifyStatisticCounterValue(ioStats, STREAM_READ_FULLY_OPERATIONS, 2);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 2);
      expectedReadBytes += buffer.length;

      verifyStatisticCounterValue(ioStats, STREAM_READ_BYTES,
          expectedReadBytes);
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, SMALL_FILE_SIZE);

    }

    // thread IOStats are updated in close
    final IOStatistics threadIOStats = ioStatisticsContext.getIOStatistics();
    verifyStatisticCounterValue(threadIOStats,
        ACTION_HTTP_GET_REQUEST, 2);
    verifyStatisticGaugeValue(threadIOStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
    verifyStatisticCounterValue(threadIOStats, STREAM_READ_BYTES, expectedReadBytes);
  }

  @Test
  public void testStatusProbesAfterClosingStream() throws Throwable {
    describe("When the underlying input stream is closed, the prefetch input stream"
        + " should still support some status probes");

    byte[] data = ContractTestUtils.dataset(SMALL_FILE_SIZE, 'a', 26);
    Path smallFile = methodPath();
    ContractTestUtils.writeDataset(getFileSystem(), smallFile, data, data.length, 16, true);


    try (FSDataInputStream in = getFileSystem().open(smallFile)) {
      byte[] buffer = new byte[SMALL_FILE_SIZE];
      in.read(buffer, 0, S_4K);
      in.seek(S_1K * 12);
      in.read(buffer, 0, S_4K);

      long pos = in.getPos();
      IOStatistics ioStats = in.getIOStatistics();
      S3AInputStreamStatistics inputStreamStatistics =
          ((S3APrefetchingInputStream) (in.getWrappedStream())).getS3AStreamStatistics();

      assertNotNull("Prefetching input IO stats should not be null", ioStats);
      assertNotNull("Prefetching input stream stats should not be null", inputStreamStatistics);
      assertNotEquals("Position retrieved from prefetching input stream should be greater than 0",
          0, pos);

      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);


      // close the stream and still use it.
      in.close();

      // status probes after closing the input stream
      long newPos = in.getPos();
      IOStatistics newIoStats = in.getIOStatistics();
      S3AInputStreamStatistics newInputStreamStatistics =
          ((S3APrefetchingInputStream) (in.getWrappedStream())).getS3AStreamStatistics();

      assertNotNull("Prefetching input IO stats should not be null", newIoStats);
      assertNotNull("Prefetching input stream stats should not be null", newInputStreamStatistics);
      assertNotEquals("Position retrieved from prefetching input stream should be greater than 0",
          0, newPos);

      // compare status probes after closing of the stream with status probes done before
      // closing the stream
      assertEquals("Position retrieved through stream before and after closing should match", pos,
          newPos);
      assertEquals("IO stats retrieved through stream before and after closing should match",
          ioStats, newIoStats);
      assertEquals("Stream stats retrieved through stream before and after closing should match",
          inputStreamStatistics, newInputStreamStatistics);

      Assertions.assertThat(in.seekToNewSource(10))
          .describedAs("seekToNewSource() not supported with prefetch: %s", in)
          .isFalse();
    }

  }

}
