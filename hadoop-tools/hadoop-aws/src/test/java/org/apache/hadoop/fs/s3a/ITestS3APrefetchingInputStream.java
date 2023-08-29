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

package org.apache.hadoop.fs.s3a;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.s3a.prefetch.S3APrefetchingInputStream;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMaximum;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticGaugeValue;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS;

/**
 * Test the prefetching input stream, validates that the underlying S3ACachingInputStream and
 * S3AInMemoryInputStream are working as expected.
 */
public class ITestS3APrefetchingInputStream extends AbstractS3ACostTest {

  public ITestS3APrefetchingInputStream() {
    super(true);
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3APrefetchingInputStream.class);

  private static final int S_500 = 512;
  private static final int S_1K = S_500 * 2;
  private static final int S_1M = S_1K * S_1K;
  private int numBlocks;

  // Size should be > block size so S3ACachingInputStream is used
  private long largeFileSize;

  // Size should be < block size so S3AInMemoryInputStream is used
  private static final int SMALL_FILE_SIZE = S_1K * 9;

  private static final int TIMEOUT_MILLIS = 5000;
  private static final int INTERVAL_MILLIS = 500;
  private static final int BLOCK_SIZE = S_1K * 10;

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_ENABLED_KEY);
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_BLOCK_SIZE_KEY);
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
    conf.setInt(PREFETCH_BLOCK_SIZE_KEY, BLOCK_SIZE);
    return conf;
  }

  private void createLargeFile() throws Exception {
    byte[] data = ContractTestUtils.dataset(S_1K * 72, 'x', 26);
    Path largeFile = methodPath();
    FileSystem largeFileFS = getFileSystem();
    ContractTestUtils.writeDataset(getFileSystem(), largeFile, data, data.length, 16, true);
    FileStatus fileStatus = largeFileFS.getFileStatus(largeFile);
    largeFileSize = fileStatus.getLen();
    numBlocks = calculateNumBlocks(largeFileSize, BLOCK_SIZE);
  }

  private static int calculateNumBlocks(long largeFileSize, int blockSize) {
    if (largeFileSize == 0) {
      return 0;
    } else {
      return ((int) (largeFileSize / blockSize)) + (largeFileSize % blockSize > 0 ? 1 : 0);
    }
  }

  @Test
  public void testReadLargeFileFully() throws Throwable {
    describe("read a large file fully, uses S3ACachingInputStream");
    skipIfClientSideEncryption();
    IOStatistics ioStats;
    createLargeFile();

    try (FSDataInputStream in = getFileSystem().open(methodPath())) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[S_1M * 10];
      long bytesRead = 0;

      while (bytesRead < largeFileSize) {
        in.readFully(buffer, 0, (int) Math.min(buffer.length, largeFileSize - bytesRead));
        bytesRead += buffer.length;
        // Blocks are fully read, no blocks should be cached
        verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE,
            0);
      }

      // Assert that first block is read synchronously, following blocks are prefetched
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS,
          numBlocks - 1);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, numBlocks);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, numBlocks);
    }
    // Verify that once stream is closed, all memory is freed
    verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
    assertThatStatisticMaximum(ioStats,
        ACTION_EXECUTOR_ACQUIRED + SUFFIX_MAX).isGreaterThan(0);
  }

  @Test
  public void testReadLargeFileFullyLazySeek() throws Throwable {
    describe("read a large file using readFully(position,buffer,offset,length),"
        + " uses S3ACachingInputStream");
    skipIfClientSideEncryption();
    IOStatistics ioStats;
    createLargeFile();

    try (FSDataInputStream in = getFileSystem().open(methodPath())) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[S_1M * 10];
      long bytesRead = 0;

      while (bytesRead < largeFileSize) {
        in.readFully(bytesRead, buffer, 0, (int) Math.min(buffer.length,
            largeFileSize - bytesRead));
        bytesRead += buffer.length;
        // Blocks are fully read, no blocks should be cached
        verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE,
                0);
      }

      // Assert that first block is read synchronously, following blocks are prefetched
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS,
              numBlocks - 1);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, numBlocks);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, numBlocks);
    }
    // Verify that once stream is closed, all memory is freed
    verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
  }

  @Test
  public void testRandomReadLargeFile() throws Throwable {
    describe("random read on a large file, uses S3ACachingInputStream");
    skipIfClientSideEncryption();
    IOStatistics ioStats;
    createLargeFile();

    try (FSDataInputStream in = getFileSystem().open(methodPath())) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[BLOCK_SIZE];

      // Don't read block 0 completely so it gets cached on read after seek
      in.read(buffer, 0, BLOCK_SIZE - S_500 * 10);

      // Seek to block 2 and read all of it
      in.seek(BLOCK_SIZE * 2);
      in.read(buffer, 0, BLOCK_SIZE);

      // Seek to block 4 but don't read: noop.
      in.seek(BLOCK_SIZE * 4);

      // Backwards seek, will use cached block 0
      in.seek(S_500 * 5);
      in.read();

      // Expected to get block 0 (partially read), 1 (prefetch), 2 (fully read), 3 (prefetch)
      // Blocks 0, 1, 3 were not fully read, so remain in the file cache
      LambdaTestUtils.eventually(TIMEOUT_MILLIS, INTERVAL_MILLIS, () -> {
        LOG.info("IO stats: {}", ioStats);
        verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 4);
        verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, 4);
        verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS, 2);
        verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE, 3);
      });
    }
    LambdaTestUtils.eventually(TIMEOUT_MILLIS, INTERVAL_MILLIS, () -> {
      LOG.info("IO stats: {}", ioStats);
      verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE, 0);
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
    });
  }

  @Test
  public void testRandomReadSmallFile() throws Throwable {
    describe("random read on a small file, uses S3AInMemoryInputStream");

    byte[] data = ContractTestUtils.dataset(SMALL_FILE_SIZE, 'a', 26);
    Path smallFile = path("randomReadSmallFile");
    ContractTestUtils.writeDataset(getFileSystem(), smallFile, data, data.length, 16, true);

    try (FSDataInputStream in = getFileSystem().open(smallFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[SMALL_FILE_SIZE];

      in.read(buffer, 0, S_1K * 2);
      in.seek(S_1K * 7);
      in.read(buffer, 0, S_1K * 2);

      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, 1);
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS, 0);
      // The buffer pool is not used
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
      // no prefetch ops, so no action_executor_acquired
      assertThatStatisticMaximum(ioStats,
          ACTION_EXECUTOR_ACQUIRED + SUFFIX_MAX).isEqualTo(-1);
    }
  }

  @Test
  public void testStatusProbesAfterClosingStream() throws Throwable {
    describe("When the underlying input stream is closed, the prefetch input stream"
        + " should still support some status probes");

    byte[] data = ContractTestUtils.dataset(SMALL_FILE_SIZE, 'a', 26);
    Path smallFile = methodPath();
    ContractTestUtils.writeDataset(getFileSystem(), smallFile, data, data.length, 16, true);

    FSDataInputStream in = getFileSystem().open(smallFile);

    byte[] buffer = new byte[SMALL_FILE_SIZE];
    in.read(buffer, 0, S_1K * 2);
    in.seek(S_1K * 7);
    in.read(buffer, 0, S_1K * 2);

    long pos = in.getPos();
    IOStatistics ioStats = in.getIOStatistics();
    S3AInputStreamStatistics inputStreamStatistics =
        ((S3APrefetchingInputStream) (in.getWrappedStream())).getS3AStreamStatistics();

    assertNotNull("Prefetching input IO stats should not be null", ioStats);
    assertNotNull("Prefetching input stream stats should not be null", inputStreamStatistics);
    assertNotEquals("Position retrieved from prefetching input stream should be greater than 0", 0,
        pos);

    in.close();

    // status probes after closing the input stream
    long newPos = in.getPos();
    IOStatistics newIoStats = in.getIOStatistics();
    S3AInputStreamStatistics newInputStreamStatistics =
        ((S3APrefetchingInputStream) (in.getWrappedStream())).getS3AStreamStatistics();

    assertNotNull("Prefetching input IO stats should not be null", newIoStats);
    assertNotNull("Prefetching input stream stats should not be null", newInputStreamStatistics);
    assertNotEquals("Position retrieved from prefetching input stream should be greater than 0", 0,
        newPos);

    // compare status probes after closing of the stream with status probes done before
    // closing the stream
    assertEquals("Position retrieved through stream before and after closing should match", pos,
        newPos);
    assertEquals("IO stats retrieved through stream before and after closing should match", ioStats,
        newIoStats);
    assertEquals("Stream stats retrieved through stream before and after closing should match",
        inputStreamStatistics, newInputStreamStatistics);

    assertFalse("seekToNewSource() not supported with prefetch", in.seekToNewSource(10));
  }

}
