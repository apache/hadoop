/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;

import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Statistic.STREAM_SEEK_BYTES_READ;

/**
 * Integration test for calling
 * {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer} on {@link S3AInputStream}.
 * Validates that the object has been closed using the
 * {@link S3AInputStream#isObjectStreamOpen()} method. Unlike the
 * {@link org.apache.hadoop.fs.contract.s3a.ITestS3AContractUnbuffer} tests,
 * these tests leverage the fact that isObjectStreamOpen exposes if the
 * underlying stream has been closed or not.
 */
public class ITestS3AUnbuffer extends AbstractS3ATestBase {

  private Path dest;

  @Override
  public void setup() throws Exception {
    super.setup();
    dest = path("ITestS3AUnbuffer");
    describe("ITestS3AUnbuffer");

    byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    ContractTestUtils.writeDataset(getFileSystem(), dest, data, data.length,
            16, true);
  }

  @Test
  public void testUnbuffer() throws IOException {
    describe("testUnbuffer");

    // Open file, read half the data, and then call unbuffer
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      assertTrue(inputStream.getWrappedStream() instanceof S3AInputStream);
      readAndAssertBytesRead(inputStream, 8);
      assertTrue(isObjectStreamOpen(inputStream));
      inputStream.unbuffer();

      // Check the the wrapped stream is closed
      assertFalse(isObjectStreamOpen(inputStream));
    }
  }

  /**
   * Test that calling {@link S3AInputStream#unbuffer()} merges a stream's
   * {@link org.apache.hadoop.fs.s3a.S3AInstrumentation.InputStreamStatistics}
   * into the {@link S3AFileSystem}'s {@link S3AInstrumentation} instance.
   */
  @Test
  public void testUnbufferStreamStatistics() throws IOException {
    describe("testUnbufferStreamStatistics");

    // Validate bytesRead is updated correctly
    S3ATestUtils.MetricDiff bytesRead = new S3ATestUtils.MetricDiff(
            getFileSystem(), STREAM_SEEK_BYTES_READ);

    // Open file, read half the data, and then call unbuffer
    FSDataInputStream inputStream = null;
    try {
      inputStream = getFileSystem().open(dest);

      readAndAssertBytesRead(inputStream, 8);
      inputStream.unbuffer();

      // Validate that calling unbuffer updates the input stream statistics
      bytesRead.assertDiffEquals(8);

      // Validate that calling unbuffer twice in a row updates the statistics
      // correctly
      readAndAssertBytesRead(inputStream, 4);
      inputStream.unbuffer();
      bytesRead.assertDiffEquals(12);
    } finally {
      IOUtils.closeStream(inputStream);
    }

    // Validate that closing the file does not further change the statistics
    bytesRead.assertDiffEquals(12);

    // Validate that the input stream stats are correct when the file is closed
    assertEquals("S3AInputStream statistics were not updated properly", 12,
            ((S3AInputStream) inputStream.getWrappedStream())
                    .getS3AStreamStatistics().bytesRead);
  }

  private boolean isObjectStreamOpen(FSDataInputStream inputStream) {
    return ((S3AInputStream) inputStream.getWrappedStream()).isObjectStreamOpen();
  }

  /**
   * Read the specified number of bytes from the given
   * {@link FSDataInputStream} and assert that
   * {@link FSDataInputStream#read(byte[])} read the specified number of bytes.
   */
  private static void readAndAssertBytesRead(FSDataInputStream inputStream,
                                        int bytesToRead) throws IOException {
    assertEquals("S3AInputStream#read did not read the correct number of " +
                    "bytes", bytesToRead,
            inputStream.read(new byte[bytesToRead]));
  }
}
