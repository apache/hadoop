
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
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_TRACINGMETRICHEADER_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;

import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.junit.Test;

import java.util.Random;
import org.apache.hadoop.fs.azurebfs.services.AbfsReadFooterMetrics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;

public class ITestAbfsReadFooterMetrics extends AbstractAbfsScaleTest {

  public ITestAbfsReadFooterMetrics() throws Exception {
  }

  private static final String TEST_PATH = "/testfile";

  @Test
  public void testReadFooterMetricsWithParquetAndNonParquet() throws Exception {
    testReadWriteAndSeek(8 * ONE_MB, DEFAULT_READ_BUFFER_SIZE, ONE_KB, 4 * ONE_KB);
  }

  @Test
  public void testReadFooterMetrics() throws Exception {
    int bufferSize = MIN_BUFFER_SIZE;
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();
    abfsConfiguration.set(FS_AZURE_METRIC_FORMAT, String.valueOf(MetricFormat.INTERNAL_FOOTER_METRIC_FORMAT));
    abfsConfiguration.set(FS_AZURE_TRACINGMETRICHEADER_FORMAT,
        String.valueOf(TracingHeaderFormat.INTERNAL_FOOTER_METRIC_FORMAT));
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally {
      stream.close();
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG,
        IOSTATISTICS_LOGGING_LEVEL_INFO, stream);

    final byte[] readBuffer = new byte[2 * bufferSize];
    int result;
    IOStatisticsSource statisticsSource = null;
    try (FSDataInputStream inputStream = fs.open(testPath)) {
      statisticsSource = inputStream;
      ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
          new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
              fs.getFileSystemId(), FSOperationType.READ, true, 0,
              ((AbfsInputStream) inputStream.getWrappedStream())
                  .getStreamID()));
      inputStream.seek(bufferSize);
      result = inputStream.read(readBuffer, bufferSize, bufferSize);
      assertNotEquals(-1, result);

      //to test tracingHeader for case with bypassReadAhead == true
      inputStream.seek(0);
      byte[] temp = new byte[5];
      int t = inputStream.read(temp, 0, 1);

      inputStream.seek(0);
      result = inputStream.read(readBuffer, 0, bufferSize);
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG,
        IOSTATISTICS_LOGGING_LEVEL_INFO, statisticsSource);

    assertNotEquals("data read in final read()", -1, result);
    assertArrayEquals(readBuffer, b);
    AbfsReadFooterMetrics abfsReadFooterMetrics = fs.getAbfsClient()
        .getAbfsCounters()
        .getAbfsReadFooterMetrics();
    String readFooterMetric = "";
    if (abfsReadFooterMetrics != null) {
      readFooterMetric = abfsReadFooterMetrics.toString();
    }
    assertEquals(
        "$NonParquet:$FR=16384.000_16384.000$SR=1.000_16384.000$FL=32768.000$RL=16384.000",
        readFooterMetric);
  }

  private void testReadWriteAndSeek(int fileSize, int bufferSize, Integer seek1, Integer seek2) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();
    abfsConfiguration.set(FS_AZURE_TRACINGMETRICHEADER_FORMAT,
        String.valueOf(TracingHeaderFormat.INTERNAL_FOOTER_METRIC_FORMAT));
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[fileSize];
    new Random().nextBytes(b);

    Path testPath = path("/testfile");
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally {
      stream.close();
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG,
        IOSTATISTICS_LOGGING_LEVEL_INFO, stream);

    final byte[] readBuffer = new byte[fileSize];
    IOStatisticsSource statisticsSource = null;
    FSDataInputStream inputStream = fs.open(testPath);
    statisticsSource = inputStream;
    ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
        new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.READ, true, 0,
            ((AbfsInputStream) inputStream.getWrappedStream())
                .getStreamID()));
    inputStream.seek(fileSize - seek1);
    inputStream.read(readBuffer, 0, seek1);

    if (seek2 != 0) {
      inputStream.seek(fileSize - seek1 - seek2);
      inputStream.read(readBuffer, 0, seek2);
    }
    inputStream.close();

    int bufferSize1 = MIN_BUFFER_SIZE;
    abfsConfiguration.setWriteBufferSize(bufferSize1);
    abfsConfiguration.setReadBufferSize(bufferSize1);

    final byte[] b1 = new byte[2 * bufferSize1];
    new Random().nextBytes(b1);
    Path testPath1 = path("/testfile1");
    FSDataOutputStream stream1 = fs.create(testPath1);
    try {
      stream1.write(b1);
    } finally {
      stream1.close();
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG,
        IOSTATISTICS_LOGGING_LEVEL_INFO, stream);

    final byte[] readBuffer1 = new byte[2 * bufferSize1];

    FSDataInputStream inputStream1 = fs.open(testPath1);
    statisticsSource = inputStream1;
    ((AbfsInputStream) inputStream1.getWrappedStream()).registerListener(
        new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.READ, true, 0,
            ((AbfsInputStream) inputStream1.getWrappedStream())
                .getStreamID()));
    inputStream1.seek(bufferSize1);
    inputStream1.read(readBuffer1, bufferSize1, bufferSize1);

    //to test tracingHeader for case with bypassReadAhead == true
    inputStream1.seek(0);
    byte[] temp = new byte[5];
    int t = inputStream1.read(temp, 0, 1);

    inputStream1.seek(0);
    inputStream1.read(readBuffer1, 0, bufferSize1);
    inputStream1.close();
    AbfsReadFooterMetrics abfsReadFooterMetrics = fs.getAbfsClient()
        .getAbfsCounters()
        .getAbfsReadFooterMetrics();
    String readFooterMetric = "";
    if (abfsReadFooterMetrics != null) {
      readFooterMetric = abfsReadFooterMetrics.toString();
    }
    assertEquals("$Parquet:$FR=1024.000$SR=4096.000$FL=8388608.000$RL=0.000$NonParquet:$FR=16384.000_16384.000$SR=1.000_16384.000$FL=32768.000$RL=16384.000",
        readFooterMetric);
    try {
      fs.close();
    } catch (Exception e) {
    }
  }

  @Test
  public void testMetricWithIdlePeriod() throws Exception {
    int bufferSize = MIN_BUFFER_SIZE;
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();
    abfsConfiguration.set(FS_AZURE_TRACINGMETRICHEADER_FORMAT,
        String.valueOf(TracingHeaderFormat.INTERNAL_FOOTER_METRIC_FORMAT));
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally {
      stream.close();
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG,
        IOSTATISTICS_LOGGING_LEVEL_INFO, stream);

    final byte[] readBuffer = new byte[2 * bufferSize];
    IOStatisticsSource statisticsSource = null;
    try (FSDataInputStream inputStream = fs.open(testPath)) {
      statisticsSource = inputStream;
      ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
          new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
              fs.getFileSystemId(), FSOperationType.READ, true, 0,
              ((AbfsInputStream) inputStream.getWrappedStream())
                  .getStreamID()));
      inputStream.seek(bufferSize);
      inputStream.read(readBuffer, bufferSize, bufferSize);
      int sleepPeriod = 90000;
      Thread.sleep(sleepPeriod);

      //to test tracingHeader for case with bypassReadAhead == true
      inputStream.seek(0);
      byte[] temp = new byte[5];
      int t = inputStream.read(temp, 0, 1);

      inputStream.seek(0);
      inputStream.read(readBuffer, 0, bufferSize);

      AbfsReadFooterMetrics abfsReadFooterMetrics = fs.getAbfsClient()
          .getAbfsCounters()
          .getAbfsReadFooterMetrics();
      String readFooterMetric = "";
      if (abfsReadFooterMetrics != null) {
        readFooterMetric = abfsReadFooterMetrics.toString();
      }
      assertEquals("$NonParquet:$FR=16384.000_16384.000$SR=1.000_16384.000$FL=32768.000$RL=16384.000", readFooterMetric);
      Thread.sleep(sleepPeriod);
    }
  }
}
