
/**
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
package org.apache.hadoop.fs.azurebfs;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_INFO;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_ACCOUNT_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_URI;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.enums.FileType.NON_PARQUET;
import static org.apache.hadoop.fs.azurebfs.enums.FileType.PARQUET;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_FILE_LENGTH;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_READ_LEN_REQUESTED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_SIZE_READ_BY_FIRST_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.TOTAL_FILES;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_FIRST_OFFSET_DIFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_SECOND_OFFSET_DIFF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;

import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.fs.azurebfs.services.AbfsReadFooterMetrics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;

public class ITestAbfsReadFooterMetrics extends AbstractAbfsScaleTest {

  public ITestAbfsReadFooterMetrics() throws Exception {
    checkPrerequisites();
  }

  private void checkPrerequisites(){
    checkIfConfigIsSet(FS_AZURE_METRIC_ACCOUNT_NAME);
    checkIfConfigIsSet(FS_AZURE_METRIC_ACCOUNT_KEY);
    checkIfConfigIsSet(FS_AZURE_METRIC_URI);
  }

  private void checkIfConfigIsSet(String configKey){
    AbfsConfiguration conf = getConfiguration();
    String value = conf.get(configKey);
    Assume.assumeTrue(configKey + " config is mandatory for the test to run",
        value != null && value.trim().length() > 1);
  }

  private static final String TEST_PATH = "/testfile";
  private static final String SLEEP_PERIOD = "90000";

  /**
   * Integration test for reading footer metrics with both Parquet and non-Parquet reads.
   */
  @Test
  public void testReadFooterMetricsWithParquetAndNonParquet() throws Exception {
    testReadWriteAndSeek(8 * ONE_MB, DEFAULT_READ_BUFFER_SIZE, ONE_KB, 4 * ONE_KB);
  }

  /**
   * Configures the AzureBlobFileSystem with the given buffer size.
   *
   * @param bufferSize Buffer size to set for write and read operations.
   * @return AbfsConfiguration used for configuration.
   */
  private Configuration getConfiguration(int bufferSize) {
    final Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_METRIC_FORMAT, String.valueOf(MetricFormat.INTERNAL_FOOTER_METRIC_FORMAT));
    configuration.setInt(AZURE_READ_BUFFER_SIZE, bufferSize);
    configuration.setInt(AZURE_WRITE_BUFFER_SIZE, bufferSize);
    return configuration;
  }

  /**
   * Writes data to the specified file path in the AzureBlobFileSystem.
   *
   * @param fs      AzureBlobFileSystem instance.
   * @param testPath Path to the file.
   * @param data    Data to write to the file.
   */
  private void writeDataToFile(AzureBlobFileSystem fs, Path testPath, byte[] data) throws IOException {
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(data);
    } finally {
      stream.close();
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, stream);
  }

  /**
   * Asserts that the actual metrics obtained from the AzureBlobFileSystem match the expected metrics string.
   *
   * @param fs               AzureBlobFileSystem instance.
   * @param expectedMetrics  Expected metrics string.
   */
  private void assertMetricsEquality(AzureBlobFileSystem fs, String expectedMetrics) {
    AbfsReadFooterMetrics actualMetrics = fs.getAbfsClient().getAbfsCounters().getAbfsReadFooterMetrics();
    assertNotNull("AbfsReadFooterMetrics is null", actualMetrics);
    assertEquals("The computed metrics differs from the actual metrics", expectedMetrics, actualMetrics.toString());
  }

  /**
   * Test for reading footer metrics with a non-Parquet file.
   */
  @Test
  public void testReadFooterMetrics() throws Exception {
    // Initialize AzureBlobFileSystem and set buffer size for configuration.
    int bufferSize = MIN_BUFFER_SIZE;
    Configuration configuration = getConfiguration(bufferSize);
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();

    // Generate random data to write to the test file.
    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    // Set up the test file path.
    Path testPath = path(TEST_PATH);

    // Write random data to the test file.
    writeDataToFile(fs, testPath, b);

    // Initialize a buffer for reading data.
    final byte[] readBuffer = new byte[2 * bufferSize];
    int result;

    // Initialize statistics source for logging.
    IOStatisticsSource statisticsSource = null;

    try (FSDataInputStream inputStream = fs.open(testPath)) {
      // Register a listener for tracing header validation.
      statisticsSource = inputStream;
      ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
              new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
                      fs.getFileSystemId(), FSOperationType.READ, true, 0,
                      ((AbfsInputStream) inputStream.getWrappedStream())
                              .getStreamID()));

      // Perform the first read operation with seek.
      inputStream.seek(bufferSize);
      result = inputStream.read(readBuffer, bufferSize, bufferSize);
      assertNotEquals(-1, result);

      // To test tracingHeader for case with bypassReadAhead == true
      inputStream.seek(0);
      byte[] temp = new byte[5];
      int t = inputStream.read(temp, 0, 1);

      // Seek back to the beginning and perform another read operation.
      inputStream.seek(0);
      result = inputStream.read(readBuffer, 0, bufferSize);
    }

    // Log IO statistics at the INFO level.
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG,
            IOSTATISTICS_LOGGING_LEVEL_INFO, statisticsSource);

    // Ensure data is read successfully and matches the written data.
    assertNotEquals("data read in final read()", -1, result);
    assertArrayEquals(readBuffer, b);

    // Get non-Parquet metrics and assert metrics equality.
    AbfsReadFooterMetrics nonParquetMetrics = getNonParquetMetrics();
    String metrics = nonParquetMetrics.toString();
    assertMetricsEquality(fs, metrics);

    // Close the AzureBlobFileSystem.
    fs.close();
  }

  /**
   * Generates and returns an instance of AbfsReadFooterMetrics for non-Parquet files.
   */
  private AbfsReadFooterMetrics getNonParquetMetrics() {
    AbfsReadFooterMetrics nonParquetMetrics = new AbfsReadFooterMetrics();
    nonParquetMetrics.addMeanMetricValue(NON_PARQUET, AVG_FILE_LENGTH, Long.parseLong("32768"));
    nonParquetMetrics.addMeanMetricValue(NON_PARQUET, AVG_READ_LEN_REQUESTED, Long.parseLong("10923"));
    nonParquetMetrics.addMeanMetricValue(NON_PARQUET, AVG_SIZE_READ_BY_FIRST_READ, Long.parseLong("16384"));
    nonParquetMetrics.addMeanMetricValue(NON_PARQUET, AVG_OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ, 1);
    nonParquetMetrics.addMeanMetricValue(NON_PARQUET, AVG_FIRST_OFFSET_DIFF, Long.parseLong("16384"));
    nonParquetMetrics.addMeanMetricValue(NON_PARQUET, AVG_SECOND_OFFSET_DIFF, Long.parseLong("16384"));
    nonParquetMetrics.incrementMetricValue(NON_PARQUET, TOTAL_FILES);
    return nonParquetMetrics;
  }

  /**
   * Generates and returns an instance of AbfsReadFooterMetrics for parquet files.
   */
  private AbfsReadFooterMetrics getParquetMetrics() {
    AbfsReadFooterMetrics parquetMetrics = new AbfsReadFooterMetrics();
    parquetMetrics.addMeanMetricValue(PARQUET, AVG_FILE_LENGTH, Long.parseLong("8388608"));
    parquetMetrics.addMeanMetricValue(PARQUET, AVG_READ_LEN_REQUESTED, Long.parseLong("2560"));
    parquetMetrics.addMeanMetricValue(PARQUET, AVG_SIZE_READ_BY_FIRST_READ, Long.parseLong("1024"));
    parquetMetrics.addMeanMetricValue(PARQUET, AVG_OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ, Long.parseLong("4096"));
    parquetMetrics.incrementMetricValue(PARQUET, TOTAL_FILES);
    return parquetMetrics;
  }

  /**
   * Test for reading, writing, and seeking with footer metrics.
   *
   * This method performs the integration test for reading, writing, and seeking operations
   * with footer metrics. It creates an AzureBlobFileSystem, configures it, writes random data
   * to a test file, performs read and seek operations, and checks the footer metrics for both
   * Parquet and non-Parquet scenarios.
   *
   * @param fileSize Size of the test file.
   * @param bufferSize Size of the buffer used for read and write operations.
   * @param seek1 The position to seek to in the test file.
   * @param seek2 Additional position to seek to in the test file (if not 0).
   */
  private void testReadWriteAndSeek(int fileSize, int bufferSize, Integer seek1, Integer seek2) throws Exception {
    // Create an AzureBlobFileSystem instance.
    Configuration configuration = getConfiguration(bufferSize);
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();

    // Generate random data to write to the test file.
    final byte[] b = new byte[fileSize];
    new Random().nextBytes(b);

    // Define the path for the test file.
    Path testPath = path("/testfile");

    // Write the random data to the test file.
    writeDataToFile(fs, testPath, b);

    // Initialize a buffer for reading.
    final byte[] readBuffer = new byte[fileSize];

    // Initialize a source for IO statistics.
    IOStatisticsSource statisticsSource = null;

    // Open an input stream for the test file.
    FSDataInputStream inputStream = fs.open(testPath);
    statisticsSource = inputStream;

    // Register a listener for tracing headers.
    ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
            new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
                    fs.getFileSystemId(), FSOperationType.READ, true, 0,
                    ((AbfsInputStream) inputStream.getWrappedStream())
                            .getStreamID()));

    // Seek to the specified position in the test file and read data.
    inputStream.seek(fileSize - seek1);
    inputStream.read(readBuffer, 0, seek1);

    // If seek2 is non-zero, perform an additional seek and read.
    if (seek2 != 0) {
      inputStream.seek(fileSize - seek1 - seek2);
      inputStream.read(readBuffer, 0, seek2);
    }

    // Close the input stream.
    inputStream.close();

    // Set a new buffer size for read and write operations.
    int bufferSize1 = MIN_BUFFER_SIZE;
    abfsConfiguration.setWriteBufferSize(bufferSize1);
    abfsConfiguration.setReadBufferSize(bufferSize1);

    // Generate new random data for a second test file.
    final byte[] b1 = new byte[2 * bufferSize1];
    new Random().nextBytes(b1);

    // Define the path for the second test file.
    Path testPath1 = path("/testfile1");

    // Write the new random data to the second test file.
    writeDataToFile(fs, testPath1, b1);

    // Initialize a buffer for reading from the second test file.
    final byte[] readBuffer1 = new byte[2 * bufferSize1];

    // Open an input stream for the second test file.
    FSDataInputStream inputStream1 = fs.open(testPath1);
    statisticsSource = inputStream1;

    // Register a listener for tracing headers.
    ((AbfsInputStream) inputStream1.getWrappedStream()).registerListener(
            new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
                    fs.getFileSystemId(), FSOperationType.READ, true, 0,
                    ((AbfsInputStream) inputStream1.getWrappedStream())
                            .getStreamID()));

    // Seek to a position in the second test file and read data.
    inputStream1.seek(bufferSize1);
    inputStream1.read(readBuffer1, bufferSize1, bufferSize1);

    // To test tracingHeader for case with bypassReadAhead == true.
    inputStream1.seek(0);
    byte[] temp = new byte[5];
    int t = inputStream1.read(temp, 0, 1);

    // Seek to the beginning of the second test file and read data.
    inputStream1.seek(0);
    inputStream1.read(readBuffer1, 0, bufferSize1);

    // Close the input stream for the second test file.
    inputStream1.close();

    // Get footer metrics for both Parquet and non-Parquet scenarios.
    AbfsReadFooterMetrics parquetMetrics = getParquetMetrics();
    AbfsReadFooterMetrics nonParquetMetrics = getNonParquetMetrics();

    // Concatenate and assert the metrics equality.
    String metrics = parquetMetrics.toString();
    metrics += nonParquetMetrics.toString();
    assertMetricsEquality(fs, metrics);

    // Close the AzureBlobFileSystem instance.
    fs.close();
  }

  /**
   * Test for reading footer metrics with an idle period.
   *
   * This method tests reading footer metrics with an idle period. It creates an AzureBlobFileSystem,
   * configures it, writes random data to a test file, performs read operations, introduces an idle
   * period, and checks the footer metrics for non-Parquet scenarios.
   *
   */
  @Test
  public void testMetricWithIdlePeriod() throws Exception {
    // Set the buffer size for the test.
    int bufferSize = MIN_BUFFER_SIZE;
    Configuration configuration = getConfiguration(bufferSize);
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();

    // Generate random data to write to the test file.
    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    // Define the path for the test file.
    Path testPath = path(TEST_PATH);

    // Write the random data to the test file.
    writeDataToFile(fs, testPath, b);

    // Initialize a buffer for reading.
    final byte[] readBuffer = new byte[2 * bufferSize];

    // Initialize a source for IO statistics.
    IOStatisticsSource statisticsSource = null;

    // Open an input stream for the test file.
    try (FSDataInputStream inputStream = fs.open(testPath)) {
      // Register a listener for tracing headers.
      ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
              new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
                      fs.getFileSystemId(), FSOperationType.READ, true, 0,
                      ((AbfsInputStream) inputStream.getWrappedStream())
                              .getStreamID()));

      // Seek to the specified position in the test file and read data.
      inputStream.seek(bufferSize);
      inputStream.read(readBuffer, bufferSize, bufferSize);

      // Introduce an idle period by sleeping.
      int sleepPeriod = Integer.parseInt(SLEEP_PERIOD);
      Thread.sleep(sleepPeriod);

      // To test tracingHeader for case with bypassReadAhead == true.
      inputStream.seek(0);
      byte[] temp = new byte[5];
      int t = inputStream.read(temp, 0, 1);

      // Seek to the beginning of the test file and read data.
      inputStream.seek(0);
      inputStream.read(readBuffer, 0, bufferSize);

      // Get and assert the footer metrics for non-Parquet scenarios.
      AbfsReadFooterMetrics nonParquetMetrics = getNonParquetMetrics();
      String metrics = nonParquetMetrics.toString();
      assertMetricsEquality(fs, metrics);

      // Introduce an additional idle period by sleeping.
      Thread.sleep(sleepPeriod);
    }
  }
}
