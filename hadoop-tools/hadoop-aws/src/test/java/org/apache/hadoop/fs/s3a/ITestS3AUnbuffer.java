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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import org.junit.Test;

import java.io.IOException;

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

  @Test
  public void testUnbuffer() throws IOException {
    // Setup test file
    Path dest = path("testUnbuffer");
    describe("testUnbuffer");
    try (FSDataOutputStream outputStream = getFileSystem().create(dest, true)) {
      byte[] data = ContractTestUtils.dataset(16, 'a', 26);
      outputStream.write(data);
    }

    // Open file, read half the data, and then call unbuffer
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      assertTrue(inputStream.getWrappedStream() instanceof S3AInputStream);
      assertEquals(8, inputStream.read(new byte[8]));
      assertTrue(isObjectStreamOpen(inputStream));
      inputStream.unbuffer();

      // Check the the wrapped stream is closed
      assertFalse(isObjectStreamOpen(inputStream));
    }
  }

  private boolean isObjectStreamOpen(FSDataInputStream inputStream) {
    return ((S3AInputStream) inputStream.getWrappedStream()).isObjectStreamOpen();
  }
}
