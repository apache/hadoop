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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_COUNT_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticGauge;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticGaugeValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsContext.getCurrentIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCK_CACHE_ENABLED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCK_PREFETCH_ENABLED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_REMOTE_BLOCK_READ;
import static org.apache.hadoop.test.Sizes.S_1K;

/**
 * Test the prefetching input stream, validates that the underlying S3ACachingInputStream and
 * S3AInMemoryInputStream are working as expected.
 */
public class ITestS3APrefetchingLargeFiles extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3APrefetchingLargeFiles.class);

  private static final int S_500 = 512;

  private int numBlocks;

  // Size should be > block size so S3ACachingInputStream is used
  private long largeFileSize;

  private static final int TIMEOUT_MILLIS = 5000;
  private static final int INTERVAL_MILLIS = 500;

  /**
   * Size of blocks for this test {@value}.
   */
  private static final int BLOCK_SIZE = S_1K * 10;

  /**
   * Size of the large file {@value}.
   */
  public static final int LARGE_FILE_SIZE = S_1K * 72;

  /**
   * large file dataset.
   */
  private static final byte[] DATASET = ContractTestUtils.dataset(LARGE_FILE_SIZE, 'a', 26);


  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private File tmpFileDir;

  /**
   * Thread level IOStatistics; reset in setup().
   */
  private IOStatisticsContext ioStatisticsContext;

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        BUFFER_DIR,
        PREFETCH_ENABLED_KEY,
        PREFETCH_BLOCK_COUNT_KEY,
        PREFETCH_BLOCK_SIZE_KEY);
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
    conf.setInt(PREFETCH_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(PREFETCH_BLOCK_COUNT_KEY, 2);
    conf.set(BUFFER_DIR, tmpFileDir.getAbsolutePath());
    return conf;
  }

  @Override
  public void setup() throws Exception {
    tmpFileDir = tempFolder.newFolder("cache");
    super.setup();

    ioStatisticsContext = getCurrentIOStatisticsContext();
    ioStatisticsContext.reset();
    nameThread();
    skipIfClientSideEncryption();
  }

  /**
   * Create a large file from the method path.
   *
   * @return the path.
   */
  private Path createLargeFile() throws Exception {
    Path largeFile = methodPath();
    FileSystem largeFileFS = getFileSystem();
    ContractTestUtils.writeDataset(getFileSystem(), largeFile, DATASET, DATASET.length, 16, true);
    FileStatus fileStatus = largeFileFS.getFileStatus(largeFile);
    largeFileSize = fileStatus.getLen();
    numBlocks = calculateNumBlocks(largeFileSize, BLOCK_SIZE);
    return largeFile;
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
    final Path path = createLargeFile();

    try (FSDataInputStream in = getFileSystem().open(path)) {
      ioStats = in.getIOStatistics();
      assertPrefetchingAndCachingEnabled(ioStats);

      byte[] buffer = new byte[S_1K];
      long bytesRead = 0;

      while (bytesRead < largeFileSize) {
        in.readFully(buffer, 0, (int) Math.min(buffer.length, largeFileSize - bytesRead));
        bytesRead += buffer.length;
        // Blocks are fully read, no blocks should be cached
        verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE,
            0);
      }
      printStreamStatistics(in);

      // Assert that first block is read synchronously, following blocks are prefetched
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS,
          numBlocks - 1);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, numBlocks);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, numBlocks);

      in.unbuffer();
      // Verify that once stream is closed, all memory is freed
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
      assertPrefetchingAndCachingEnabled(ioStats);
    }
    // Verify that once stream is closed, all memory is freed
    verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
  }

  /**
   * Assert that prefetching and caching enabled -determined by examining gauges.
   * @param ioStats iostatistics
   */
  private static void assertPrefetchingAndCachingEnabled(final IOStatistics ioStats) {
    // prefetching is on
    verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCK_PREFETCH_ENABLED, 1);
    // there is caching
    verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCK_CACHE_ENABLED, 1);
  }

  private void printStreamStatistics(final FSDataInputStream in) {
    LOG.info("Stream statistics\n{}",
        ioStatisticsToPrettyString(in.getIOStatistics()));
  }

  @Test
  public void testReadLargeFileFullyLazySeek() throws Throwable {
    describe("read a large file using readFully(position,buffer,offset,length),"
        + " uses S3ACachingInputStream");
    IOStatistics ioStats;
    final Path path = createLargeFile();

    try (FSDataInputStream in = getFileSystem().open(path)) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[S_1K];
      long bytesRead = 0;

      while (bytesRead < largeFileSize) {
        in.readFully(bytesRead, buffer, 0, (int) Math.min(buffer.length,
            largeFileSize - bytesRead));
        bytesRead += buffer.length;
        // Blocks are fully read, no blocks should be cached
        verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE,
            0);
      }
      printStreamStatistics(in);

      // Assert that first block is read synchronously, following blocks are prefetched
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS,
          numBlocks - 1);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, numBlocks);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, numBlocks);
    }
    // Verify that once stream is closed, all memory is freed
    verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);

    // thread IOStats are updated in close
    final IOStatistics threadIOStats = ioStatisticsContext.getIOStatistics();
    verifyStatisticCounterValue(threadIOStats,
        STREAM_READ_REMOTE_BLOCK_READ, numBlocks);
    verifyStatisticGaugeValue(threadIOStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
  }

  /**
   * Random read of a large file, with a small block size
   * so as to reduce wait time for read completion.
   * @throws Throwable
   */
  @Test
  public void testRandomReadLargeFile() throws Throwable {
    describe("random read on a large file, uses S3ACachingInputStream");
    IOStatistics ioStats;
    final Path path = createLargeFile();

    describe("Commencing Read Sequence");
    try (FSDataInputStream in = getFileSystem()
        .openFile(path)
        .opt(FS_OPTION_OPENFILE_READ_POLICY, "random")
        .build().get()) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[BLOCK_SIZE];

      // there is no prefetch of block 0
      awaitCachedBlocks(ioStats, 0);

      // Don't read block 0 completely so it gets cached on read after seek
      in.read(buffer, 0, BLOCK_SIZE - S_1K);
      awaitCachedBlocks(ioStats, 1);

      // Seek to block 2 and read all of it
      in.seek(BLOCK_SIZE * 2);
      in.read(buffer, 0, BLOCK_SIZE);

      // so no increment in cache block count
      awaitCachedBlocks(ioStats, 1);

      // Seek to block 4 but don't read: noop.
      in.seek(BLOCK_SIZE * 4);
      awaitCachedBlocks(ioStats, 1);

      // Backwards seek, will use cached block 0
      in.seek(S_500 * 5);
      in.read();

      AtomicLong iterations = new AtomicLong();

      /*
      these tests are really brittle to prefetching, timeouts and threading.
       */
      // Expected to get block 0 (partially read), 1 (prefetch), 2 (fully read), 3 (prefetch)
      // Blocks 0, 1, 3 were not fully read, so remain in the file cache
      LambdaTestUtils.eventually(TIMEOUT_MILLIS, INTERVAL_MILLIS, () -> {
        LOG.info("Attempt {}: {}", iterations.incrementAndGet(),
            ioStatisticsToPrettyString(ioStats));
        verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 4);
        verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, 4);
      });
      printStreamStatistics(in);
    }
    awaitCachedBlocks(ioStats, 0);
    LambdaTestUtils.eventually(TIMEOUT_MILLIS, INTERVAL_MILLIS, () -> {
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
    });

  }

  /**
   * Await the cached block count to match the expected value.
   * @param ioStats live statistics
   * @param count count of blocks
   * @throws Exception failure
   */
  public void awaitCachedBlocks(final IOStatistics ioStats, final long count) throws Exception {
    LambdaTestUtils.eventually(TIMEOUT_MILLIS, INTERVAL_MILLIS, () -> {
      verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE, count);
      verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
    });
  }
  /**
   * Test to verify the existence of the cache file.
   * Tries to perform inputStream read and seek ops to make the prefetching take place and
   * asserts whether file with .bin suffix is present. It also verifies certain file stats.
   */
  @Test
  public void testCacheFileExistence() throws Throwable {
    describe("Verify that FS cache files exist on local FS");

    int prefetchBlockSize = BLOCK_SIZE;
    final Path path = createLargeFile();
    LOG.info("Opening file {}", path);

    try (FSDataInputStream in = getFileSystem().open(path)) {
      byte[] buffer = new byte[prefetchBlockSize];
      // caching is enabled
      final IOStatistics ioStats = in.getIOStatistics();
      assertThatStatisticGauge(ioStats, STREAM_READ_BLOCK_CACHE_ENABLED)
          .isEqualTo(1);

      // read less than the block size
      LOG.info("reading first data block");
      in.readFully(buffer, 0, prefetchBlockSize - S_1K);
      LOG.info("stream stats after a read {}", ioStatisticsToPrettyString(ioStats));

      // there's been at least one fetch so far; others may be in progress.
      assertThatStatisticCounter(ioStats, ACTION_HTTP_GET_REQUEST)
          .isGreaterThanOrEqualTo(1);


      //assertCacheFileExists();

      LOG.info("reading rest of file");

      in.readFully(prefetchBlockSize * 2, buffer, 0, prefetchBlockSize);

      LOG.info("stream stats after reading whole file {}", ioStatisticsToPrettyString(ioStats));

      // and at least one block is in cache
      assertThatStatisticGauge(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE)
          .isGreaterThanOrEqualTo(1);

      // a file which must be under the tempt dir
      assertCacheFileExists();
    }
  }

  /**
   * Scan the temp dir and asserdt that there are cache files.
   */
  private void assertCacheFileExists() throws IOException {
    final Configuration conf = getFileSystem().getConf();
    Assertions.assertThat(tmpFileDir.isDirectory())
        .describedAs("The dir to keep cache files must exist %s", tmpFileDir);
    File[] tmpFiles = tmpFileDir.listFiles();
    Assertions.assertThat(tmpFiles)
        .describedAs("No cache files found under " + tmpFileDir)
        .isNotNull()
        .hasSizeGreaterThanOrEqualTo(1);

    for (File tmpFile : tmpFiles) {
      Path path = new Path(tmpFile.getAbsolutePath());
      try (FileSystem localFs = FileSystem.getLocal(conf)) {
        FileStatus stat = localFs.getFileStatus(path);
        ContractTestUtils.assertIsFile(path, stat);
        Assertions.assertThat(stat.getLen())
            .describedAs("%s: File length not matching with prefetchBlockSize", path)
            .isEqualTo(BLOCK_SIZE);

        assertEquals("User permissions should be RW", FsAction.READ_WRITE,
            stat.getPermission().getUserAction());
        assertEquals("Group permissions should be NONE", FsAction.NONE,
            stat.getPermission().getGroupAction());
        assertEquals("Other permissions should be NONE", FsAction.NONE,
            stat.getPermission().getOtherAction());
      }
    }
  }



}
