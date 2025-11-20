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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import static org.apache.hadoop.fs.audit.AuditStatisticNames.AUDIT_REQUEST_EXECUTION;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.getExternalData;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_HEAD_REQUEST;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.ANALYTICS_STREAM_FACTORY_CLOSED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ANALYTICS_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_CACHE_HIT;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PARQUET_FOOTER_PARSING_FAILED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCHED_BYTES;
import static org.apache.hadoop.io.Sizes.S_1K;
import static org.apache.hadoop.io.Sizes.S_1M;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

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

  @BeforeEach
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

    final long initialAuditCount = fs.getIOStatistics().counters()
            .getOrDefault(AUDIT_REQUEST_EXECUTION, 0L);

   long fileLength = fs.getFileStatus(externalTestFile).getLen();

   // Head request for the file length.
    verifyStatisticCounterValue(fs.getIOStatistics(), AUDIT_REQUEST_EXECUTION,
            initialAuditCount + 1);

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
      Assertions.assertThat(streamBytesRead).as("Stream statistics should track bytes read")
              .isEqualTo(500);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);

    // Since policy is WHOLE_FILE, the whole file starts getting prefetched as soon as the stream to it is opened.
    // So prefetched bytes is fileLen - 5
    verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCHED_BYTES, fileLength - 5);

    fs.close();
    verifyStatisticCounterValue(fs.getIOStatistics(), ANALYTICS_STREAM_FACTORY_CLOSED, 1);

    // Expect 4 audited requests. One HEAD, and 3 GETs. The 3 GETs are because the read policy is WHOLE_FILE,
    // in which case, AAL will start prefetching till EoF on file open in 8MB chunks. The file read here
    // s3://noaa-cors-pds/raw/2023/017/ohfh/OHFH017d.23_.gz, has a size of ~21MB, resulting in 3 GETS:
    // [0-8388607, 8388608-16777215, 16777216-21511173].
    verifyStatisticCounterValue(fs.getIOStatistics(), AUDIT_REQUEST_EXECUTION,
            initialAuditCount + 1 + 4);
  }

  @Test
  public void testSequentialPrefetching() throws IOException {

    Configuration conf = getConfiguration();

    // AAL uses a caffeine cache, and expires any prefetched data for a key 1s after it was last accessed by default.
    // While this works well when running on EC2, for local testing, it can take more than 1s to download large chunks
    // of data. Set this value to higher for testing to prevent early cache evictions.
    conf.setInt(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
            "."  + AAL_CACHE_TIMEOUT, 10000);

    S3AFileSystem fs =
            (S3AFileSystem) FileSystem.get(externalTestFile.toUri(), getConfiguration());
    byte[] buffer = new byte[10 * ONE_MB];
    IOStatistics ioStats;

    long fileLength = fs.getFileStatus(externalTestFile).getLen();

    // Here we read through the 21MB external test file, but do not pass in the WHOLE_FILE policy. Instead, we rely
    // on AAL detecting a sequential pattern being read, and then prefetching bytes in a geometrical progression.
    // AAL's sequential prefetching starts prefetching in increments 4MB, 8MB, 16MB etc. depending on how many
    // sequential reads happen.
    try (FSDataInputStream inputStream = fs.open(externalTestFile)) {
      ioStats = inputStream.getIOStatistics();

      inputStream.readFully(buffer, 0, ONE_MB);
      // The first sequential read, so prefetch the next 4MB.
      inputStream.readFully(buffer,   0, ONE_MB);

      // Since ONE_MB was requested by the reader, the prefetched bytes are 3MB.
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCHED_BYTES, 3 * ONE_MB);

      // These next two reads are within the last prefetched bytes, so no further bytes are prefetched.
      inputStream.readFully(buffer, 0, 2 *  ONE_MB);
      inputStream.readFully(buffer, 0, ONE_MB);
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCHED_BYTES, 3 * ONE_MB);
      // Two cache hits, as the previous two reads were already prefetched.
      verifyStatisticCounterValue(ioStats, STREAM_READ_CACHE_HIT, 2);

      // Another sequential read, GP will now prefetch the next 8MB of data.
      inputStream.readFully(buffer, 0, ONE_MB);
      // Cache hit is still 2, as the previous read required a new GET request as it was outside the previously fetched
      // 4MB.
      verifyStatisticCounterValue(ioStats, STREAM_READ_CACHE_HIT, 2);
      // A total of 10MB is prefetched - 3MB and then 7MB.
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCHED_BYTES, 10 * ONE_MB);
      long bytesRemainingForPrefetch = fileLength - (inputStream.getPos() + 10 * ONE_MB);
      inputStream.readFully(buffer, 0, 10 * ONE_MB);


      // Though the next GP should prefetch 16MB, since the file is ~23MB, only the bytes till EoF are prefetched.
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCHED_BYTES,
              10 * ONE_MB + bytesRemainingForPrefetch);
      inputStream.readFully(buffer, 0, 3 * ONE_MB);
      verifyStatisticCounterValue(ioStats, STREAM_READ_CACHE_HIT, 3);
    }

    // verify all AAL stats are passed to the FS.
    verifyStatisticCounterValue(fs.getIOStatistics(), STREAM_READ_CACHE_HIT, 3);
    verifyStatisticCounterValue(fs.getIOStatistics(), STREAM_READ_PARQUET_FOOTER_PARSING_FAILED, 0);
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

    long fileLength = getFileSystem().getFileStatus(dest).getLen();

    byte[] buffer = new byte[500];
    IOStatistics ioStats;
    int bytesRead;

    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.seek(5);
      bytesRead = inputStream.read(buffer, 0, 500);

      ObjectInputStream objectInputStream = (ObjectInputStream) inputStream.getWrappedStream();
      long streamBytesRead = objectInputStream.getS3AStreamStatistics().getBytesRead();
      Assertions.assertThat(streamBytesRead).as("Stream statistics should track bytes read")
              .isEqualTo(bytesRead);

    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
    verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);
    verifyStatisticCounterValue(ioStats, ACTION_HTTP_HEAD_REQUEST, 0);
    // This file has a content length of 451. Since it's a parquet file, AAL will prefetch the footer bytes (last 32KB),
    // as soon as the file is opened, but because the file is < 32KB, the whole file is prefetched.
    verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCHED_BYTES, fileLength);
    
    // Open a stream to the object twice, verifying that data is cached, and streams to the same object, do not
    // prefetch the same data twice.
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.seek(5);
      inputStream.read(buffer, 0, 500);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
    verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 0);
    verifyStatisticCounterValue(ioStats, ACTION_HTTP_HEAD_REQUEST, 0);
    // No data is prefetched, as it already exists in the cache from the previous factory.
    verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCHED_BYTES, 0);
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

    final int size = 3000;
    byte[] buffer = new byte[size];
    int readLimit = Math.min(size, (int) fileStatus.getLen());

    IOStatistics ioStats;

    final IOStatistics fsIostats = getFileSystem().getIOStatistics();
    final long initialAuditCount = fsIostats.counters()
        .getOrDefault(AUDIT_REQUEST_EXECUTION, 0L);

    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.readFully(buffer, 0, readLimit);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
    verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);

    // S3A makes a HEAD request on the stream open(), and then AAL makes a GET request to get the object, total audit
    // operations = 10.
    long currentAuditCount = initialAuditCount + 2;
    verifyStatisticCounterValue(getFileSystem().getIOStatistics(),
            AUDIT_REQUEST_EXECUTION, currentAuditCount);

    try (FSDataInputStream inputStream = getFileSystem().openFile(dest)
        .withFileStatus(fileStatus)
        .must(FS_OPTION_OPENFILE_READ_POLICY, FS_OPTION_OPENFILE_READ_POLICY_PARQUET)
        .build().get()) {
      ioStats = inputStream.getIOStatistics();
      inputStream.readFully(buffer, 0, readLimit);

      verifyStatisticCounterValue(ioStats, STREAM_READ_BYTES, (int) fileStatus.getLen());
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPERATIONS, 1);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);

    // S3A passes in the meta-data(content length) on file open,
    // we expect AAL to make no HEAD requests
    verifyStatisticCounterValue(ioStats, ACTION_HTTP_HEAD_REQUEST, 0);
    verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 0);
  }

  @Test
  public void testInvalidConfigurationThrows() throws Exception {
    describe("Verify S3 connector framework throws with invalid configuration");

    Configuration conf = new Configuration(getConfiguration());
    removeBaseAndBucketOverrides(conf);
    //Disable Sequential Prefetching
    conf.setInt(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
        "." + PHYSICAL_IO_PREFIX + ".cache.timeout", -1);

    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(conf, ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX);

    intercept(IllegalArgumentException.class,
        () -> S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration));
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

      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, 1);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_HEAD_REQUEST, 0);
    }

    // We did 3 reads, and all of them were served from the cache
    verifyStatisticCounterValue(getFileSystem().getIOStatistics(), STREAM_READ_CACHE_HIT, 3);
  }


  @Test
  public void testSequentialStreamsNoDuplicateGets() throws Throwable {
    describe("Sequential streams reading same object should not duplicate GETs");

    Path dest = path("sequential-test.txt");
    int fileLen = S_1M;

    byte[] data = dataset(fileLen, 256, 255);
    writeDataset(getFileSystem(), dest, data, fileLen, 1024, true);

    byte[] buffer = new byte[ONE_MB];
    try (FSDataInputStream stream1 = getFileSystem().open(dest);
         FSDataInputStream stream2 = getFileSystem().open(dest)) {

      stream1.read(buffer, 0, 2 * ONE_KB);
      stream2.read(buffer);
      stream1.read(buffer, 0, 10 * ONE_KB);

      IOStatistics stats1 = stream1.getIOStatistics();
      IOStatistics stats2 = stream2.getIOStatistics();

      verifyStatisticCounterValue(stats1, ACTION_HTTP_GET_REQUEST, 1);
      verifyStatisticCounterValue(stats2, ACTION_HTTP_HEAD_REQUEST, 0);

      // Since it's a small file (ALL will prefetch the whole file for size < 8MB), the whole file is prefetched
      // on the first read.
      verifyStatisticCounterValue(stats1, STREAM_READ_PREFETCHED_BYTES, fileLen);

      // The second stream will not prefetch any bytes, as they have already been prefetched by stream 1.
      verifyStatisticCounterValue(stats2, STREAM_READ_PREFETCHED_BYTES, 0);
    }

    // verify value is passed up to the FS
    verifyStatisticCounterValue(getFileSystem().getIOStatistics(),
            STREAM_READ_PREFETCHED_BYTES, fileLen);

    // We did 3 reads, all of them were served from the small object cache. In this case, the whole object was
    // downloaded as soon as the stream to it was opened.
    verifyStatisticCounterValue(getFileSystem().getIOStatistics(), STREAM_READ_CACHE_HIT, 3);
  }
}
