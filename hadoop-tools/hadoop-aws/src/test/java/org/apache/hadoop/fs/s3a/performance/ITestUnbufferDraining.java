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

package org.apache.hadoop.fs.s3a.performance;

import java.io.IOException;
import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.ASYNC_DRAIN_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_VALIDATION;
import static org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disablePrefetching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.ConfigurationHelper.setDurationAsSeconds;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ABORTED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_UNBUFFERED;

/**
 * Test stream unbuffer performance/behavior with stream draining
 * and aborting.
 */
public class ITestUnbufferDraining extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestUnbufferDraining.class);

  /**
   * Readahead range to use, sets drain threshold too.
   */
  public static final int READAHEAD = 1000;

  /**
   * How big a file to create?
   */
  public static final int FILE_SIZE = 50_000;

  /**
   * Number of attempts to unbuffer on each stream.
   */
  public static final int ATTEMPTS = 10;

  /**
   * Should checksums be enabled?
   */
  public static final boolean CHECKSUMS = false;

  /**
   * Test FS with a tiny connection pool and
   * no recovery.
   */
  private FileSystem brittleFS;

  @Override
  public Configuration createConfiguration() {
    Configuration conf = disablePrefetching(super.createConfiguration());
    removeBaseAndBucketOverrides(conf,
        ASYNC_DRAIN_THRESHOLD,
        CHECKSUM_VALIDATION,
        ESTABLISH_TIMEOUT,
        INPUT_FADVISE,
        MAX_ERROR_RETRIES,
        MAXIMUM_CONNECTIONS,
        READAHEAD_RANGE,
        REQUEST_TIMEOUT,
        RETRY_LIMIT,
        SOCKET_TIMEOUT);
    conf.setBoolean(CHECKSUM_VALIDATION, CHECKSUMS);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    // now create a new FS with minimal http capacity and recovery
    // a separate one is used to avoid test teardown suffering
    // from the lack of http connections and short timeouts.
    try {
      // allow small durations.
      AWSClientConfig.setMinimumOperationDuration(Duration.ZERO);
      Configuration conf = getConfiguration();
      // kick off async drain for any data
      conf.setInt(ASYNC_DRAIN_THRESHOLD, 1);
      conf.setInt(MAXIMUM_CONNECTIONS, 2);
      conf.setInt(MAX_ERROR_RETRIES, 1);
      conf.setInt(READAHEAD_RANGE, READAHEAD);
      conf.setInt(RETRY_LIMIT, 1);
      conf.setBoolean(CHECKSUM_VALIDATION, CHECKSUMS);
      setDurationAsSeconds(conf, ESTABLISH_TIMEOUT,
          Duration.ofSeconds(1));

      brittleFS = FileSystem.newInstance(getFileSystem().getUri(), conf);
    } finally {
      AWSClientConfig.resetMinimumOperationDuration();
    }
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    FileSystem bfs = getBrittleFS();
    FILESYSTEM_IOSTATS.aggregate(retrieveIOStatistics(bfs));
    IOUtils.cleanupWithLogger(LOG, bfs);
  }

  public FileSystem getBrittleFS() {
    return brittleFS;
  }

  /**
   * Test stream close performance/behavior with stream draining
   * and unbuffer.
   */
  @Test
  public void testUnbufferDraining() throws Throwable {

    describe("unbuffer draining");
    FileStatus st = createTestFile();

    IOStatistics brittleStats = retrieveIOStatistics(getBrittleFS());
    long originalUnbuffered = lookupCounter(brittleStats,
        STREAM_READ_UNBUFFERED);

    int offset = FILE_SIZE - READAHEAD + 1;
    try (FSDataInputStream in = getBrittleFS().openFile(st.getPath())
        .withFileStatus(st)
        .mustLong(ASYNC_DRAIN_THRESHOLD, 1)
        .build().get()) {
      describe("Initiating unbuffer with async drain\n");
      for (int i = 0; i < ATTEMPTS; i++) {
        describe("Starting read/unbuffer #%d", i);
        in.seek(offset);
        in.read();
        in.unbuffer();
      }
      // verify the policy switched.
      assertReadPolicy(in, S3AInputPolicy.Random);
      // assert that the statistics are as expected
      IOStatistics stats = in.getIOStatistics();
      verifyStatisticCounterValue(stats,
          STREAM_READ_UNBUFFERED,
          ATTEMPTS);
      verifyStatisticCounterValue(stats,
          STREAM_READ_ABORTED,
          0);
      // there's always a policy of 1, so
      // this value must be 1 + 1
      verifyStatisticCounterValue(stats,
          STREAM_READ_SEEK_POLICY_CHANGED,
          2);
    }
    // filesystem statistic propagation
    verifyStatisticCounterValue(brittleStats,
        STREAM_READ_UNBUFFERED,
        ATTEMPTS + originalUnbuffered);
  }

  /**
   * Lookup a counter, returning 0 if it is not defined.
   * @param statistics stats to probe
   * @param key counter key
   * @return the value or 0
   */
  private static long lookupCounter(
      final IOStatistics statistics,
      final String key) {
    Long counter = statistics.counters().get(key);
    return counter == null ? 0 : counter;
  }

  /**
   * Assert that the read policy is as expected.
   * @param in input stream
   * @param policy read policy.
   */
  private static void assertReadPolicy(final FSDataInputStream in,
      final S3AInputPolicy policy) {
    S3AInputStream inner = getS3AInputStream(in);
    Assertions.assertThat(inner.getInputPolicy())
        .describedAs("input policy of %s", inner)
        .isEqualTo(policy);
  }

  /**
   * Extract the inner stream from an FSDataInputStream.
   * Because prefetching is disabled, this is always an S3AInputStream.
   * @param in input stream
   * @return the inner stream cast to an S3AInputStream.
   */
  private static S3AInputStream getS3AInputStream(final FSDataInputStream in) {
    return (S3AInputStream) in.getWrappedStream();
  }

  /**
   * Test stream close performance/behavior with unbuffer
   * aborting rather than draining.
   */
  @Test
  public void testUnbufferAborting() throws Throwable {

    describe("unbuffer aborting");
    FileStatus st = createTestFile();
    IOStatistics brittleStats = retrieveIOStatistics(getBrittleFS());
    long originalUnbuffered =
        lookupCounter(brittleStats, STREAM_READ_UNBUFFERED);
    long originalAborted =
        lookupCounter(brittleStats, STREAM_READ_ABORTED);

    // open the file at the beginning with a whole file read policy,
    // so even with s3a switching to random on unbuffer,
    // this always does a full GET
    // also provide a floating point string for the threshold, to
    // verify it is safely parsed
    try (FSDataInputStream in = getBrittleFS().openFile(st.getPath())
        .withFileStatus(st)
        .must(ASYNC_DRAIN_THRESHOLD, "1.0")
        .must(FS_OPTION_OPENFILE_READ_POLICY,
            FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE)
        .build().get()) {
      assertReadPolicy(in, S3AInputPolicy.Sequential);

      describe("Initiating unbuffer with async drain\n");
      for (int i = 0; i < ATTEMPTS; i++) {
        describe("Starting read/unbuffer #%d", i);
        in.read();
        in.unbuffer();
        // because the read policy is sequential, it doesn't change
        assertReadPolicy(in, S3AInputPolicy.Sequential);
      }

      // assert that the statistics are as expected
      IOStatistics stats = in.getIOStatistics();
      verifyStatisticCounterValue(stats,
          STREAM_READ_UNBUFFERED,
          ATTEMPTS);
      verifyStatisticCounterValue(stats,
          STREAM_READ_ABORTED,
          ATTEMPTS);
      // there's always a policy of 1.
      verifyStatisticCounterValue(stats,
          STREAM_READ_SEEK_POLICY_CHANGED,
          1);
    }
    // look at FS statistics
    verifyStatisticCounterValue(brittleStats,
        STREAM_READ_UNBUFFERED,
        ATTEMPTS + originalUnbuffered);
    verifyStatisticCounterValue(brittleStats,
        STREAM_READ_ABORTED,
        ATTEMPTS + originalAborted);
  }

  private FileStatus createTestFile() throws IOException {
    byte[] data = dataset(FILE_SIZE, '0', 10);
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    ContractTestUtils.createFile(fs, path, true, data);
    return fs.getFileStatus(path);
  }


}
