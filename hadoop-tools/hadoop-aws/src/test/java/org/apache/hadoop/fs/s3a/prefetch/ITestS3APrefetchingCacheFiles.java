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

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;

import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPrefetchOption;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticGauge;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticGaugeValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsContext.getCurrentIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCK_CACHE_ENABLED;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.test.Sizes.S_1K;

/**
 * Test the cache file behaviour with prefetching input stream.
 * TODO: remove this once the test in ITestS3APrefetchingLargeFiles is good.
 */
public class ITestS3APrefetchingCacheFiles extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3APrefetchingCacheFiles.class);
  private static final int BLOCK_SIZE = S_1K * 10;

  private Path testFile;
  private S3AFileSystem testFileSystem;
  private int prefetchBlockSize;
  private Configuration conf;

  /**
   * Thread level IOStatistics; reset in setup().
   */
  private IOStatisticsContext ioStatisticsContext;

  private File tmpFileDir;


  @Before
  public void setUp() throws Exception {
    super.setup();
    conf = createConfiguration();
    tmpFileDir = File.createTempFile("ITestS3APrefetchingCacheFiles", "");
    tmpFileDir.delete();
    tmpFileDir.mkdirs();

    conf.set(BUFFER_DIR, tmpFileDir.getAbsolutePath());
    String testFileUri = S3ATestUtils.getCSVTestFile(conf);

    testFile = new Path(testFileUri);
    testFileSystem = (S3AFileSystem) FileSystem.get(testFile.toUri(), conf);

    prefetchBlockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    final FileStatus testFileStatus = testFileSystem.getFileStatus(testFile);
    Assumptions.assumeThat(testFileStatus.getLen())
        .describedAs("Test file %s is smaller than required size %d",
            testFileStatus, prefetchBlockSize * 4)
        .isGreaterThan(prefetchBlockSize);

    ioStatisticsContext = getCurrentIOStatisticsContext();
    ioStatisticsContext.reset();
    nameThread();
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    setPrefetchOption(conf, true);
    disableFilesystemCaching(conf);
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_BLOCK_SIZE_KEY);
    conf.setInt(PREFETCH_BLOCK_SIZE_KEY, BLOCK_SIZE);
    return conf;
  }

  @Override
  public synchronized void teardown() throws Exception {
    super.teardown();
    tmpFileDir.delete();
    File[] tmpFiles = tmpFileDir.listFiles();
    if (tmpFiles != null) {
      for (File filePath : tmpFiles) {
        String path = filePath.getPath();
        filePath.delete();
      }
    }
    cleanupWithLogger(LOG, testFileSystem);
  }

  /**
   * Test to verify the existence of the cache file.
   * Tries to perform inputStream read and seek ops to make the prefetching take place and
   * asserts whether file with .bin suffix is present. It also verifies certain file stats.
   */
  @Test
  public void testCacheFileExistence() throws Throwable {
    describe("Verify that FS cache files exist on local FS");
    skipIfClientSideEncryption();

    try (FSDataInputStream in = testFileSystem.open(testFile)) {
      byte[] buffer = new byte[prefetchBlockSize];
      // caching is enabled
      final IOStatistics ioStats = in.getIOStatistics();
      assertThatStatisticGauge(ioStats, STREAM_READ_BLOCK_CACHE_ENABLED)
          .isEqualTo(1);

      // read less than the block size
      LOG.info("reading first data block");
      in.readFully(buffer, 0, prefetchBlockSize - 10240);
      LOG.info("stream stats after a read {}", ioStatisticsToPrettyString(ioStats));

      // there's been one fetch so far
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);

      // a file is cached
      verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE, 1);

      assertCacheFileExists();

      in.readFully(prefetchBlockSize * 2, buffer, 0, prefetchBlockSize);

      assertCacheFileExists();
    }
  }

  private void assertCacheFileExists() throws IOException {
    final Configuration conf = testFileSystem.getConf();
    Assertions.assertThat(tmpFileDir.isDirectory())
        .describedAs("The dir to keep cache files must exist %s", tmpFileDir);
    File[] tmpFiles = tmpFileDir.listFiles();
    Assertions.assertThat(tmpFiles)
        .describedAs("No cache files found under " + tmpFileDir)
        .isNotNull()
        .hasSizeGreaterThanOrEqualTo( 1);

    for (File tmpFile : tmpFiles) {
      Path path = new Path(tmpFile.getAbsolutePath());
      try (FileSystem localFs = FileSystem.getLocal(conf)) {
        FileStatus stat = localFs.getFileStatus(path);
        ContractTestUtils.assertIsFile(path, stat);
        assertEquals("File length not matching with prefetchBlockSize", prefetchBlockSize,
            stat.getLen());
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
