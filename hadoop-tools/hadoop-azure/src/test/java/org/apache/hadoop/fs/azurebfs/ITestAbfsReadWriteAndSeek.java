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

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsInputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FASTPATH_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

/**
 * Test read, write and seek.
 * Uses package-private methods in AbfsConfiguration, which is why it is in
 * this package.
 */
@RunWith(Parameterized.class)
public class ITestAbfsReadWriteAndSeek extends AbstractAbfsScaleTest {
  private static final String TEST_PATH = "/testfile";

  @Parameterized.Parameters(name = "Size={0}")
  public static Iterable<Object[]> sizes() {
    return Arrays.asList(new Object[][]{{MIN_BUFFER_SIZE},
        {DEFAULT_READ_BUFFER_SIZE},
        {APPENDBLOB_MAX_WRITE_BUFFER_SIZE},
        {17 * ONE_MB}});
        //{MAX_BUFFER_SIZE}}); - To be reenabled by https://issues.apache.org/jira/browse/HADOOP-17852
  }

  private final int size;

  public ITestAbfsReadWriteAndSeek(final int size) throws Exception {
    this.size = size;
  }

  @Test
  public void testMockFastpathReadWriteWithDiffBuffSizesAndSeek() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testReadWriteAndSeek(size, true);
  }

  @Test
  public void testReadWriteWithDiffBuffSizesAndSeek() throws Exception {
    testReadWriteAndSeek(size, false);
  }

  private void testReadWriteAndSeek(int bufferSize, boolean isMockFastpathTest) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);
    try (FSDataOutputStream stream = fs.create(testPath)) {
      stream.write(b);
    }
    if (isMockFastpathTest) {
      MockFastpathConnection.registerAppend(b.length, testPath.getName(), b, 0, b.length);
    }

    final byte[] readBuffer = new byte[2 * bufferSize];
    int result = -1;
    boolean isUnsuppFastpathBuffSize = (
        (bufferSize != DEFAULT_FASTPATH_READ_BUFFER_SIZE)
            && (fs.getAbfsStore().getAbfsConfiguration().isFastpathEnabled()
            || isMockFastpathTest));
    try (FSDataInputStream inputStream = isMockFastpathTest
        ? openMockAbfsInputStream(fs, fs.open(testPath))
        : fs.open(testPath)) {
      ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
          new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
              fs.getFileSystemId(), FSOperationType.READ, true, 0,
              ((AbfsInputStream) inputStream.getWrappedStream())
                  .getStreamID()));
      if (isUnsuppFastpathBuffSize) {
        Assertions.assertThat(TestAbfsInputStream.isFastpathEnabled(
            (AbfsInputStream) (inputStream.getWrappedStream())))
            .describedAs("Fastpath session should be disabled "
                + "automatically for unsupported buffer size")
            .isFalse();
      }

      inputStream.seek(bufferSize);
      result = inputStream.read(readBuffer, bufferSize, bufferSize);
      assertNotEquals(-1, result);
      inputStream.seek(0);
      result = inputStream.read(readBuffer, 0, bufferSize);
    }

    assertNotEquals("data read in final read()", -1, result);
    assertArrayEquals(readBuffer, b);
    if (isMockFastpathTest) {
      MockFastpathConnection.unregisterAppend(testPath.getName());
    }
  }

  @Test
  public void testReadAheadRequestID() throws java.io.IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    int bufferSize = MIN_BUFFER_SIZE;
    abfsConfiguration.setReadBufferSize(bufferSize);

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
    }
    fs.registerListener(null);
  }
}
