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
package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.assertj.core.api.Assertions;

public class ITestAbfsPositionedRead extends AbstractAbfsIntegrationTest {

  private static final int TEST_FILE_DATA_SIZE = 100;

  @Rule
  public TestName methodName = new TestName();

  public ITestAbfsPositionedRead() throws Exception {
  }

  @Test
  public void testPositionedRead() throws IOException {
    describe("Testing positioned reads in AbfsInputStream");
    Path dest = path(methodName.getMethodName());

    byte[] data = ContractTestUtils.dataset(TEST_FILE_DATA_SIZE, 'a', 'z');
    ContractTestUtils.writeDataset(getFileSystem(), dest, data, data.length,
        TEST_FILE_DATA_SIZE, true);
    int bytesToRead = 10;
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      assertTrue(
          "unexpected stream type "
              + inputStream.getWrappedStream().getClass().getSimpleName(),
          inputStream.getWrappedStream() instanceof AbfsInputStream);
      byte[] readBuffer = new byte[bytesToRead];
      int readPos = 0;
      Assertions
          .assertThat(inputStream.read(readPos, readBuffer, 0, bytesToRead))
          .describedAs(
              "AbfsInputStream pread did not read the correct number of bytes")
          .isEqualTo(bytesToRead);
      Assertions.assertThat(readBuffer)
          .describedAs("AbfsInputStream pread did not read correct data")
          .containsExactly(
              Arrays.copyOfRange(data, readPos, readPos + bytesToRead));
      // Read only 10 bytes from offset 0. But by default it will do the seek
      // and read where the entire 100 bytes get read into the
      // AbfsInputStream buffer.
      Assertions
          .assertThat(Arrays.copyOfRange(
              ((AbfsInputStream) inputStream.getWrappedStream()).getBuffer(), 0,
              TEST_FILE_DATA_SIZE))
          .describedAs(
              "AbfsInputStream pread did not read more data into its buffer")
          .containsExactly(data);
      // Check statistics
      assertStatistics(inputStream.getIOStatistics(), bytesToRead, 1, 1,
          TEST_FILE_DATA_SIZE);

      readPos = 50;
      Assertions
          .assertThat(inputStream.read(readPos, readBuffer, 0, bytesToRead))
          .describedAs(
              "AbfsInputStream pread did not read the correct number of bytes")
          .isEqualTo(bytesToRead);
      Assertions.assertThat(readBuffer)
          .describedAs("AbfsInputStream pread did not read correct data")
          .containsExactly(
              Arrays.copyOfRange(data, readPos, readPos + bytesToRead));
      // Check statistics
      assertStatistics(inputStream.getIOStatistics(), 2 * bytesToRead, 2, 1,
          TEST_FILE_DATA_SIZE);
      // Did positioned read from pos 0 and then 50 but the stream pos should
      // remain at 0.
      Assertions.assertThat(inputStream.getPos())
          .describedAs("AbfsInputStream positioned reads moved stream position")
          .isEqualTo(0);
    }
  }

  private void assertStatistics(IOStatistics ioStatistics,
      long expectedBytesRead, long expectedReadOps, long expectedRemoteReadOps,
      long expectedRemoteReadBytes) {
    Assertions
        .assertThat(ioStatistics.counters()
            .get(StreamStatisticNames.STREAM_READ_BYTES).longValue())
        .describedAs("Mismatch in bytesRead statistics")
        .isEqualTo(expectedBytesRead);
    Assertions
        .assertThat(ioStatistics.counters()
            .get(StreamStatisticNames.STREAM_READ_OPERATIONS).longValue())
        .describedAs("Mismatch in readOps statistics")
        .isEqualTo(expectedReadOps);
    Assertions
        .assertThat(ioStatistics.counters()
            .get(StreamStatisticNames.REMOTE_READ_OP).longValue())
        .describedAs("Mismatch in remoteReadOps statistics")
        .isEqualTo(expectedRemoteReadOps);
    Assertions
        .assertThat(ioStatistics.counters()
            .get(StreamStatisticNames.REMOTE_BYTES_READ).longValue())
        .describedAs("Mismatch in remoteReadBytes statistics")
        .isEqualTo(expectedRemoteReadBytes);
  }

  @Test
  public void testPositionedReadWithBufferedReadDisabled() throws IOException {
    describe("Testing positioned reads in AbfsInputStream with BufferedReadDisabled");
    Path dest = path(methodName.getMethodName());
    byte[] data = ContractTestUtils.dataset(TEST_FILE_DATA_SIZE, 'a', 'z');
    ContractTestUtils.writeDataset(getFileSystem(), dest, data, data.length,
        TEST_FILE_DATA_SIZE, true);
    FutureDataInputStreamBuilder builder = getFileSystem().openFile(dest);
    builder.opt(ConfigurationKeys.FS_AZURE_BUFFERED_PREAD_DISABLE, true);
    FSDataInputStream inputStream = null;
    try {
      inputStream = builder.build().get();
    } catch (IllegalArgumentException | UnsupportedOperationException
        | InterruptedException | ExecutionException e) {
      throw new IOException(
          "Exception opening " + dest + " with FutureDataInputStreamBuilder",
          e);
    }
    assertNotNull("Null InputStream over " + dest, inputStream);
    int bytesToRead = 10;
    try {
      AbfsInputStream abfsIs = (AbfsInputStream) inputStream.getWrappedStream();
      byte[] readBuffer = new byte[bytesToRead];
      int readPos = 10;
      Assertions
          .assertThat(inputStream.read(readPos, readBuffer, 0, bytesToRead))
          .describedAs(
              "AbfsInputStream pread did not read the correct number of bytes")
          .isEqualTo(bytesToRead);
      Assertions.assertThat(readBuffer)
          .describedAs("AbfsInputStream pread did not read correct data")
          .containsExactly(
              Arrays.copyOfRange(data, readPos, readPos + bytesToRead));
      // Read only 10 bytes from offset 10. This time, as buffered pread is
      // disabled, it will only read the exact bytes as requested and no data
      // will get read into the AbfsInputStream#buffer. Infact the buffer won't
      // even get initialized.
      assertNull("AbfsInputStream pread caused the internal buffer creation",
          abfsIs.getBuffer());
      // Check statistics
      assertStatistics(inputStream.getIOStatistics(), bytesToRead, 1, 1,
          bytesToRead);
      readPos = 40;
      Assertions
          .assertThat(inputStream.read(readPos, readBuffer, 0, bytesToRead))
          .describedAs(
              "AbfsInputStream pread did not read the correct number of bytes")
          .isEqualTo(bytesToRead);
      Assertions.assertThat(readBuffer)
          .describedAs("AbfsInputStream pread did not read correct data")
          .containsExactly(
              Arrays.copyOfRange(data, readPos, readPos + bytesToRead));
      assertStatistics(inputStream.getIOStatistics(), 2 * bytesToRead, 2, 2,
          2 * bytesToRead);
      // Now make a seek and read so that internal buffer gets created
      inputStream.seek(0);
      Assertions.assertThat(inputStream.read(readBuffer)).describedAs(
          "AbfsInputStream seek+read did not read the correct number of bytes")
          .isEqualTo(bytesToRead);
      // This read would have fetched all 100 bytes into internal buffer.
      Assertions
          .assertThat(Arrays.copyOfRange(
              ((AbfsInputStream) inputStream.getWrappedStream()).getBuffer(), 0,
              TEST_FILE_DATA_SIZE))
          .describedAs(
              "AbfsInputStream seek+read did not read more data into its buffer")
          .containsExactly(data);
      assertStatistics(inputStream.getIOStatistics(), 3 * bytesToRead, 3, 3,
          TEST_FILE_DATA_SIZE + 2 * bytesToRead);
      resetBuffer(abfsIs.getBuffer());
      // Now again do pos read and make sure not any extra data being fetched.
      readPos = 0;
      Assertions
          .assertThat(inputStream.read(readPos, readBuffer, 0, bytesToRead))
          .describedAs(
              "AbfsInputStream pread did not read the correct number of bytes")
          .isEqualTo(bytesToRead);
      Assertions.assertThat(readBuffer)
          .describedAs("AbfsInputStream pread did not read correct data")
          .containsExactly(
              Arrays.copyOfRange(data, readPos, readPos + bytesToRead));
      Assertions
          .assertThat(Arrays.copyOfRange(
              ((AbfsInputStream) inputStream.getWrappedStream()).getBuffer(), 0,
              TEST_FILE_DATA_SIZE))
          .describedAs(
              "AbfsInputStream pread read more data into its buffer than expected")
          .doesNotContain(data);
      assertStatistics(inputStream.getIOStatistics(), 4 * bytesToRead, 4, 4,
          TEST_FILE_DATA_SIZE + 3 * bytesToRead);
    } finally {
      inputStream.close();
    }
  }

  private void resetBuffer(byte[] buf) {
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0;
    }
  }
}
