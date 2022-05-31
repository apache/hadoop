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

import java.io.IOException;

import org.junit.Test;

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

/**
 * Test the prefetching input stream, validates that the underlying S3CachingInputStream and
 * S3InMemoryInputStream are working as expected.
 */
public class ITestS3PrefetchingInputStream extends AbstractS3ACostTest {

  public ITestS3PrefetchingInputStream() {
    super(true);
  }

  private static final int _1K = 1024;
  // Path for file which should have length > block size so S3CachingInputStream is used
  private Path largeFile;
  private FileSystem fs;
  private int numBlocks;
  private int blockSize;
  private long largeFileSize;
  // Size should be < block size so S3InMemoryInputStream is used
  private static final int SMALL_FILE_SIZE = _1K * 16;


  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_ENABLED_KEY);
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
    return conf;
  }


  private void openFS() throws IOException {
    Configuration conf = getConfiguration();

    largeFile = new Path(DEFAULT_CSVTEST_FILE);
    blockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    fs = largeFile.getFileSystem(getConfiguration());
    FileStatus fileStatus = fs.getFileStatus(largeFile);
    largeFileSize = fileStatus.getLen();
    calculateNumBlocks();
  }

  private void calculateNumBlocks() {
    numBlocks = (largeFileSize == 0) ?
        0 :
        ((int) (largeFileSize / blockSize)) + (largeFileSize % blockSize > 0 ? 1 : 0);
  }

  @Test
  public void testReadLargeFileFully() throws Throwable {
    describe("read a large file fully, uses S3CachingInputStream");
    openFS();

    try (FSDataInputStream in = fs.open(largeFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[(int) largeFileSize];

      in.read(buffer, 0, (int) largeFileSize);

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, numBlocks);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, numBlocks);
    }
  }

  @Test
  public void testRandomReadLargeFile() throws Throwable {
    describe("random read on a large file, uses S3CachingInputStream");
    openFS();

    try (FSDataInputStream in = fs.open(largeFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[blockSize];

      // Don't read the block completely so it gets cached on seek
      in.read(buffer, 0, blockSize - _1K * 10);
      in.seek(blockSize + _1K * 10);
      // Backwards seek, will use cached block
      in.seek(_1K * 5);
      in.read();

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, 2);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, 2);
    }
  }

  @Test
  public void testRandomReadSmallFile() throws Throwable {
    describe("random read on a small file, uses S3InMemoryInputStream");

    byte[] data = ContractTestUtils.dataset(SMALL_FILE_SIZE, 'a', 26);
    Path smallFile = path("randomReadSmallFile");
    ContractTestUtils.writeDataset(getFileSystem(), smallFile, data, data.length, 16, true);

    try (FSDataInputStream in = getFileSystem().open(smallFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[SMALL_FILE_SIZE];

      in.read(buffer, 0, _1K * 4);
      in.seek(_1K * 12);
      in.read(buffer, 0, _1K * 4);

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, 1);
      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, 1);
    }

  }
}