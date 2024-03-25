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

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_INFO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtLevel;

/**
 * Test read, write and seek.
 * Uses package-private methods in AbfsConfiguration, which is why it is in
 * this package.
 */
@RunWith(Parameterized.class)
public class ITestAbfsReadWriteAndSeek extends AbstractAbfsScaleTest {
  private static final String TEST_PATH = "/testfile";

  /**
   * Parameterize on read buffer size and readahead.
   * For test performance, a full x*y test matrix is not used.
   * @return the test parameters
   */
  @Parameterized.Parameters(name = "Size={0}-readahead={1}")
  public static Iterable<Object[]> sizes() {
    return Arrays.asList(new Object[][]{{MIN_BUFFER_SIZE, true},
        {DEFAULT_READ_BUFFER_SIZE, false},
        {DEFAULT_READ_BUFFER_SIZE, true},
        {APPENDBLOB_MAX_WRITE_BUFFER_SIZE, false},
        {MAX_BUFFER_SIZE, true}});
  }

  private final int size;
  private final boolean readaheadEnabled;

  public ITestAbfsReadWriteAndSeek(final int size,
      final boolean readaheadEnabled) throws Exception {
    this.size = size;
    this.readaheadEnabled = readaheadEnabled;
  }

  @Test
  public void testReadAndWriteWithDifferentBufferSizesAndSeek() throws Exception {
    testReadWriteAndSeek(size);
  }

  private void testReadWriteAndSeek(int bufferSize) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);
    abfsConfiguration.setReadAheadEnabled(readaheadEnabled);
    abfsConfiguration.setOptimizeFooterRead(false);

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally{
    stream.close();
    }
    logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, stream);

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
    logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, statisticsSource);

    assertNotEquals("data read in final read()", -1, result);
    assertArrayEquals(readBuffer, b);
  }

  @Test
  public void testReadAheadRequestID() throws java.io.IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    int bufferSize = MIN_BUFFER_SIZE;
    abfsConfiguration.setReadBufferSize(bufferSize);
    abfsConfiguration.setReadAheadEnabled(readaheadEnabled);

    final byte[] b = new byte[bufferSize * 10];
    new Random().nextBytes(b);
    Path testPath = path(TEST_PATH);
    try (FSDataOutputStream stream = fs.create(testPath)) {
      ((AbfsOutputStream) stream.getWrappedStream()).registerListener(
          new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
              fs.getFileSystemId(), FSOperationType.WRITE, false, 0,
              ((AbfsOutputStream) stream.getWrappedStream())
                  .getStreamID()));
      stream.write(b);
      logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, stream);
    }


    final byte[] readBuffer = new byte[4 * bufferSize];
    int result;
    fs.registerListener(
        new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.OPEN, false, 0));
    try (FSDataInputStream inputStream = fs.open(testPath)) {
      ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
          new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
              fs.getFileSystemId(), FSOperationType.READ, false, 0,
              ((AbfsInputStream) inputStream.getWrappedStream())
                  .getStreamID()));
      result = inputStream.read(readBuffer, 0, bufferSize*4);
      logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, inputStream);
    }
    fs.registerListener(null);
  }
}
