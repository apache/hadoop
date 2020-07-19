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

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.SCALETEST_READ_WRITE_SEEK_MAX_SIZE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.SCALETEST_READ_WRITE_SEEK_TIMEOUT_IN_MINS;
import static org.apache.hadoop.fs.contract.ContractTestUtils.bandwidth;
import static org.apache.hadoop.fs.contract.ContractTestUtils.toHuman;

/**
 * Test read, write and seek.
 * Uses package-private methods in AbfsConfiguration, which is why it is in
 * this package.
 */
@RunWith(Parameterized.class)
public class ITestAbfsReadWriteAndSeek extends AbstractAbfsScaleTest {
  private static final Path TEST_PATH = new Path("/testfile");
  private static final int SIXTY = 60;

  @Parameterized.Parameters(name = "Size={0}")
  public static Iterable<Object[]> sizes() {

    return Arrays.asList(new Object[][]{{MIN_BUFFER_SIZE},
        {DEFAULT_READ_BUFFER_SIZE},
        {APPENDBLOB_MAX_WRITE_BUFFER_SIZE},
        {getMaxSize()}});
  }

  private final int size;

  public ITestAbfsReadWriteAndSeek(final int size) throws Exception {
    this.size = size;
  }

  private static int getInt(String key) {
    String timeoutStr = System.getProperty(key);
    int timeout;
    try {
      return Integer.parseInt(timeoutStr);
    } catch (NumberFormatException nfe) {
      return -1;
    }
  }

  private static int getMaxSize() {
    int maxSize = getInt(SCALETEST_READ_WRITE_SEEK_MAX_SIZE_MB);
    if (maxSize < 0) {
      maxSize = MAX_BUFFER_SIZE;
    } else {
      maxSize *= ONE_MB;
    }
    return maxSize;
  }

  @Override
  protected int getTestTimeoutMillis() {
    int timeout = getInt(SCALETEST_READ_WRITE_SEEK_TIMEOUT_IN_MINS);
    if (timeout < 0) {
      timeout = super.getTestTimeoutMillis();
    } else {
      timeout *= SIXTY * 1000;
    }
    return timeout;
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


    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try (FSDataOutputStream stream = fs.create(TEST_PATH)) {
      stream.write(b);
    }
    int sizeInKb = 2 * bufferSize / ONE_KB;
    timer.end("Time to write file of %d KB ", sizeInKb);
    LOG.info("Time per KB to write = {} nS",
        toHuman(timer.nanosPerOperation(sizeInKb)));
    bandwidth(timer, 2 * bufferSize);

    final byte[] readBuffer = new byte[2 * bufferSize];
    int result;
    try (FSDataInputStream inputStream = fs.open(TEST_PATH)) {
      inputStream.seek(bufferSize);
      timer.reset();
      result = inputStream.read(readBuffer, bufferSize, bufferSize);
      sizeInKb = bufferSize / ONE_KB;
      timer.end("Time to read file of %d KB ", sizeInKb);
      LOG.info("Time per KB to read = {} nS",
          toHuman(timer.nanosPerOperation(sizeInKb)));
      bandwidth(timer, bufferSize);
      assertNotEquals(-1, result);
      inputStream.seek(0);
      timer.reset();
      result = inputStream.read(readBuffer, 0, bufferSize);
      sizeInKb = bufferSize / ONE_KB;
      timer.end("Time to read file of %d KB ", sizeInKb);
      LOG.info("Time per KB to read = {} nS",
          toHuman(timer.nanosPerOperation(sizeInKb)));
      bandwidth(timer, bufferSize);
    }
    assertNotEquals("data read in final read()", -1, result);
    assertArrayEquals(readBuffer, b);
  }
}
