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

import java.net.URI;

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
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticGaugeValue;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

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

  private static final int S_1K = 1024;
  private static final int S_1M = S_1K * S_1K;
  // Path for file which should have length > block size so S3ACachingInputStream is used
  private Path largeFile;
  private FileSystem largeFileFS;
  private int numBlocks;
  private int blockSize;
  private long largeFileSize;
  // Size should be < block size so S3AInMemoryInputStream is used
  private static final int SMALL_FILE_SIZE = S_1K * 16;


  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_ENABLED_KEY);
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
    return conf;
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    cleanupWithLogger(LOG, largeFileFS);
    largeFileFS = null;
  }

  private void openFS() throws Exception {
    Configuration conf = getConfiguration();

    largeFile = new Path(DEFAULT_CSVTEST_FILE);
    blockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    largeFileFS = new S3AFileSystem();
    largeFileFS.initialize(new URI(DEFAULT_CSVTEST_FILE), getConfiguration());
    FileStatus fileStatus = largeFileFS.getFileStatus(largeFile);
    largeFileSize = fileStatus.getLen();
    numBlocks = calculateNumBlocks(largeFileSize, blockSize);
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
    IOStatistics ioStats;
    openFS();

    try (FSDataInputStream in = largeFileFS.open(largeFile)) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[S_1M * 10];
      long bytesRead = 0;

      while (bytesRead < largeFileSize) {
        in.readFully(buffer, 0, (int) Math.min(buffer.length, largeFileSize - bytesRead));
        bytesRead += buffer.length;
        // Blocks are fully read, no blocks should be cached
        verifyStatisticGaugeValue(ioStats, StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE,
            0);
      }

      // Assert that first block is read synchronously, following blocks are prefetched
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS,
          numBlocks - 1);
      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, numBlocks);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, numBlocks);
    }
    // Verify that once stream is closed, all memory is freed
    verifyStatisticGaugeValue(ioStats, StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
  }

  @Test
  public void testRandomReadLargeFile() throws Throwable {
    describe("random read on a large file, uses S3ACachingInputStream");
    IOStatistics ioStats;
    openFS();

    try (FSDataInputStream in = largeFileFS.open(largeFile)) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[blockSize];

      // Don't read the block completely so it gets cached on seek
      in.read(buffer, 0, blockSize - S_1K * 10);
      in.seek(blockSize + S_1K * 10);
      // Backwards seek, will use cached block
      in.seek(S_1K * 5);
      in.read();

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, 2);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, 2);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS, 1);
      // block 0 is cached when we seek to block 1, block 1 is cached as it is being prefetched
      // when we seek out of block 0, see cancelPrefetches()
      verifyStatisticGaugeValue(ioStats, StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE, 2);
    }
    verifyStatisticGaugeValue(ioStats, StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE, 0);
    verifyStatisticGaugeValue(ioStats, StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
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

      in.read(buffer, 0, S_1K * 4);
      in.seek(S_1K * 12);
      in.read(buffer, 0, S_1K * 4);

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, 1);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, 1);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS, 0);
      // The buffer pool is not used
      verifyStatisticGaugeValue(ioStats, StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
    }
  }

}
