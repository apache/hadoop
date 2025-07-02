/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;
import org.assertj.core.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStream;
import org.apache.hadoop.fs.statistics.IOStatistics;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.getExternalData;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;

import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ANALYTICS_GET_REQUESTS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ANALYTICS_HEAD_REQUESTS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ANALYTICS_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.ANALYTICS_STREAM_FACTORY_CLOSED;
import static org.apache.hadoop.io.Sizes.S_1K;
import static org.apache.hadoop.io.Sizes.S_1M;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests integration of the
 * <a href="https://github.com/awslabs/analytics-accelerator-s3">analytics accelerator library</a>
 *
 * Certain tests in this class rely on reading local parquet files stored in resources.
 * These files are copied from local to S3 and then read via the analytics stream.
 * This is done to ensure AAL can read the parquet format, and handles exceptions from malformed
 * parquet files.
 *
 */
public class ITestS3AAnalyticsAcceleratorStreamReading extends AbstractS3ATestBase {

  private static final String PHYSICAL_IO_PREFIX = "physicalio";

  private Path externalTestFile;

  @Before
  public void setUp() throws Exception {
    super.setup();
    skipIfClientSideEncryption();
    externalTestFile = getExternalData(getConfiguration());
  }

  @Override
  public Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    enableAnalyticsAccelerator(configuration);
    return configuration;
  }

  @Test
  public void testConnectorFrameWorkIntegration() throws Throwable {
    describe("Verify S3 connector framework integration");

    S3AFileSystem fs =
        (S3AFileSystem) FileSystem.get(externalTestFile.toUri(), getConfiguration());
    byte[] buffer = new byte[500];
    IOStatistics ioStats;

    try (FSDataInputStream inputStream =
        fs.openFile(externalTestFile)
            .must(FS_OPTION_OPENFILE_READ_POLICY, FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE)
            .build().get()) {
      ioStats = inputStream.getIOStatistics();
      inputStream.seek(5);
      inputStream.read(buffer, 0, 500);

      final InputStream wrappedStream = inputStream.getWrappedStream();
      ObjectInputStream objectInputStream = (ObjectInputStream) wrappedStream;

      Assertions.assertThat(objectInputStream.streamType()).isEqualTo(InputStreamType.Analytics);
      Assertions.assertThat(objectInputStream.getInputPolicy())
          .isEqualTo(S3AInputPolicy.Sequential);

      verifyStatisticCounterValue(ioStats, STREAM_READ_BYTES, 500);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPERATIONS, 1);

      long streamBytesRead = objectInputStream.getS3AStreamStatistics().getBytesRead();
      Assertions.assertThat(streamBytesRead).as("Stream statistics should track bytes read").isEqualTo(500);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
    fs.close();
    verifyStatisticCounterValue(fs.getIOStatistics(), ANALYTICS_STREAM_FACTORY_CLOSED, 1);
  }

  @Test
  public void testMalformedParquetFooter() throws IOException {
    describe("Reading a malformed parquet file should not throw an exception");

    // File with malformed footer take from
    // https://github.com/apache/parquet-testing/blob/master/bad_data/PARQUET-1481.parquet.
    // This test ensures AAL does not throw exceptions if footer parsing fails.
    // It will only emit a WARN log, "Unable to parse parquet footer for
    // test/malformedFooter.parquet, parquet prefetch optimisations will be disabled for this key."
    Path dest = path("malformed_footer.parquet");

    File file = new File("src/test/resources/malformed_footer.parquet");

    Path sourcePath = new Path(file.toURI().getPath());
    getFileSystem().copyFromLocalFile(false, true, sourcePath, dest);

    byte[] buffer = new byte[500];
    IOStatistics ioStats;
    int bytesRead;

    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.seek(5);
      bytesRead = inputStream.read(buffer, 0, 500);

      ObjectInputStream objectInputStream = (ObjectInputStream) inputStream.getWrappedStream();
      long streamBytesRead = objectInputStream.getS3AStreamStatistics().getBytesRead();
      Assertions.assertThat(streamBytesRead).as("Stream statistics should track bytes read").isEqualTo(bytesRead);

    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_GET_REQUESTS, 1);
    // S3A passes in the meta data on file open, we expect AAL to make no HEAD requests
    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_HEAD_REQUESTS, 0);
  }

  /**
   * This test reads a multi-row group parquet file. Each parquet consists of at least one
   * row group, which contains the column data for a subset of rows. A single parquet file
   * can contain multiple row groups, this allows for further parallelisation, as each row group
   * can be processed independently.
   */
  @Test
  public void testMultiRowGroupParquet() throws Throwable {
    describe("A parquet file is read successfully");

    Path dest = path("multi_row_group.parquet");

    File file = new File("src/test/resources/multi_row_group.parquet");
    Path sourcePath = new Path(file.toURI().getPath());
    getFileSystem().copyFromLocalFile(false, true, sourcePath, dest);

    FileStatus fileStatus = getFileSystem().getFileStatus(dest);

    byte[] buffer = new byte[3000];
    IOStatistics ioStats;

    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.readFully(buffer, 0, (int) fileStatus.getLen());
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_GET_REQUESTS, 1);
    try (FSDataInputStream inputStream = getFileSystem().openFile(dest)
        .must(FS_OPTION_OPENFILE_READ_POLICY, FS_OPTION_OPENFILE_READ_POLICY_PARQUET)
        .build().get()) {
      ioStats = inputStream.getIOStatistics();
      inputStream.readFully(buffer, 0, (int) fileStatus.getLen());

      verifyStatisticCounterValue(ioStats, STREAM_READ_BYTES, (int) fileStatus.getLen());
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPERATIONS, 1);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
    // S3A passes in the meta-data(content length) on file open, we expect AAL to make no HEAD requests
    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_HEAD_REQUESTS, 0);
  }

  @Test
  public void testInvalidConfigurationThrows() throws Exception {
    describe("Verify S3 connector framework throws with invalid configuration");

    Configuration conf = new Configuration(getConfiguration());
    removeBaseAndBucketOverrides(conf);
    //Disable Sequential Prefetching
    conf.setInt(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
        "." + PHYSICAL_IO_PREFIX + ".blobstore.capacity", -1);

    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(conf, ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX);

    intercept(IllegalArgumentException.class,
        () -> S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration));
  }

  /**
   *
   * TXT files are classified as SEQUENTIAL format and use SequentialPrefetcher(requests the entire 10MB file)
   * RangeOptimiser splits ranges larger than maxRangeSizeBytes (8MB) using partSizeBytes (8MB)
   * The 10MB range gets split into: [0-8MB) and [8MB-10MB)
   * Each split range becomes a separate Block, resulting in 2 GET requests:
   */
  @Test
  public void testLargeFileMultipleGets() throws Throwable {
    describe("Large file should trigger multiple GET requests");

    Path dest = path("large-test-file.txt");
    byte[] data = dataset(10 * S_1M, 256, 255);
    writeDataset(getFileSystem(), dest, data, 10 * S_1M, 1024, true);

    byte[] buffer = new byte[S_1M * 10];
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      IOStatistics ioStats = inputStream.getIOStatistics();
      inputStream.readFully(buffer);

      verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_GET_REQUESTS, 2);
      // Because S3A passes in the meta-data(content length) on file open, we expect AAL to make no HEAD requests
      verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_HEAD_REQUESTS, 0);
    }
  }

  @Test
  public void testSmallFileSingleGet() throws Throwable {
    describe("Small file should trigger only one GET request");

    Path dest = path("small-test-file.txt");
    byte[] data = dataset(S_1M, 256, 255);
    writeDataset(getFileSystem(), dest, data, S_1M, 1024, true);

    byte[] buffer = new byte[S_1M];
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      IOStatistics ioStats = inputStream.getIOStatistics();
      inputStream.readFully(buffer);

      verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_GET_REQUESTS, 1);
      // Because S3A passes in the meta-data(content length) on file open, we expect AAL to make no HEAD requests
      verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_HEAD_REQUESTS, 0);
    }
  }


  @Test
  public void testRandomSeekPatternGets() throws Throwable {
    describe("Random seek pattern should optimize GET requests");

    Path dest = path("seek-test.txt");
    byte[] data = dataset(5 * S_1M, 256, 255);
    writeDataset(getFileSystem(), dest, data, 5 * S_1M, 1024, true);

    byte[] buffer = new byte[S_1M];
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      IOStatistics ioStats = inputStream.getIOStatistics();

      inputStream.read(buffer);
      inputStream.seek(2 * S_1M);
      inputStream.read(new byte[512 * S_1K]);
      inputStream.seek(3 * S_1M);
      inputStream.read(new byte[512 * S_1K]);

      verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_GET_REQUESTS, 1);
      verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_HEAD_REQUESTS, 0);
    }
  }


  @Test
  public void testSequentialStreamsNoDuplicateGets() throws Throwable {
    describe("Sequential streams reading same object should not duplicate GETs");

    Path dest = path("sequential-test.txt");
    byte[] data = dataset(S_1M, 256, 255);
    writeDataset(getFileSystem(), dest, data, S_1M, 1024, true);

    byte[] buffer = new byte[1024];
    try (FSDataInputStream stream1 = getFileSystem().open(dest);
         FSDataInputStream stream2 = getFileSystem().open(dest)) {

      stream1.read(buffer);
      stream2.read(buffer);

      IOStatistics stats1 = stream1.getIOStatistics();
      IOStatistics stats2 = stream2.getIOStatistics();

      verifyStatisticCounterValue(stats1, STREAM_READ_ANALYTICS_GET_REQUESTS, 1);
      verifyStatisticCounterValue(stats2, STREAM_READ_ANALYTICS_GET_REQUESTS, 0);
    }
  }
}
