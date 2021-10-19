/*
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

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.contract.ContractTestUtils;

/**
 * Integration test for calling
 * {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer} on {@link AbfsInputStream}.
 * Validates that the underlying stream's buffer is null.
 */
public class ITestAbfsUnbuffer extends AbstractAbfsIntegrationTest {

  private Path dest;

  public ITestAbfsUnbuffer() throws Exception {
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    dest = path("ITestAbfsUnbuffer");

    byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    ContractTestUtils.writeDataset(getFileSystem(), dest, data, data.length,
            16, true);
  }

  @Test
  public void testUnbuffer() throws IOException {
    // Open file, read half the data, and then call unbuffer
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      assertTrue("unexpected stream type "
              + inputStream.getWrappedStream().getClass().getSimpleName(),
              inputStream.getWrappedStream() instanceof AbfsInputStream);
      readAndAssertBytesRead(inputStream, 8);
      assertFalse("AbfsInputStream buffer should not be null",
              isBufferNull(inputStream));
      inputStream.unbuffer();

      // Check the the underlying buffer is null
      assertTrue("AbfsInputStream buffer should be null",
              isBufferNull(inputStream));
    }
  }

  private boolean isBufferNull(FSDataInputStream inputStream) {
    return ((AbfsInputStream) inputStream.getWrappedStream()).getBuffer() == null;
  }

  /**
   * Read the specified number of bytes from the given
   * {@link FSDataInputStream} and assert that
   * {@link FSDataInputStream#read(byte[])} read the specified number of bytes.
   */
  private static void readAndAssertBytesRead(FSDataInputStream inputStream,
                                             int bytesToRead) throws IOException {
    assertEquals("AbfsInputStream#read did not read the correct number of "
            + "bytes", bytesToRead, inputStream.read(new byte[bytesToRead]));
  }
}
