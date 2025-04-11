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
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.fs.s3a.Constants.ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.getExternalData;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ANALYTICS_OPENED;
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
  private static final String LOGICAL_IO_PREFIX = "logicalio";


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
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
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

    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.seek(5);
      inputStream.read(buffer, 0, 500);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
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

    try (FSDataInputStream inputStream = getFileSystem().openFile(dest)
        .must(FS_OPTION_OPENFILE_READ_POLICY, FS_OPTION_OPENFILE_READ_POLICY_PARQUET)
        .build().get()) {
      ioStats = inputStream.getIOStatistics();
      inputStream.readFully(buffer, 0, (int) fileStatus.getLen());
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
  }

  @Test
  public void testConnectorFrameworkConfigurable() {
    describe("Verify S3 connector framework reads configuration");

    Configuration conf = new Configuration(getConfiguration());

    //Disable Predictive Prefetching
    conf.set(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
        "." + LOGICAL_IO_PREFIX + ".prefetching.mode", "all");

    //Set Blobstore Capacity
    conf.setInt(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
        "." + PHYSICAL_IO_PREFIX + ".blobstore.capacity", 1);

    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(conf, ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX);

    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration);

    Assertions.assertThat(configuration.getLogicalIOConfiguration().getPrefetchingMode())
            .as("AnalyticsStream configuration is not set to expected value")
            .isSameAs(PrefetchMode.ALL);

    Assertions.assertThat(configuration.getPhysicalIOConfiguration().getBlobStoreCapacity())
            .as("AnalyticsStream configuration is not set to expected value")
            .isEqualTo(1);
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

}
