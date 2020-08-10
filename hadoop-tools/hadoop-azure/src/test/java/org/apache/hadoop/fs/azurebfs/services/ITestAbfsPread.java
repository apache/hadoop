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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.bouncycastle.util.Arrays;
import org.junit.Test;

public class ITestAbfsPread extends AbstractAbfsIntegrationTest {

  public ITestAbfsPread() throws Exception {
  }

  @Test
  public void testPread() throws IOException {
    describe("Testing preads in AbfsInputStream");
    Path dest = path("ITestAbfsPread");

    int dataSize = 100;
    byte[] data = ContractTestUtils.dataset(dataSize, 'a', 26);
    ContractTestUtils.writeDataset(getFileSystem(), dest, data, data.length, dataSize, true);
    int bytesToRead = 10;
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      assertTrue(
          "unexpected stream type " + inputStream.getWrappedStream().getClass().getSimpleName(),
          inputStream.getWrappedStream() instanceof AbfsInputStream);
      byte[] readBuffer = new byte[bytesToRead];
      int pos = 0;
      assertEquals("AbfsInputStream#read did not read the correct number of bytes",
          bytesToRead, inputStream.read(pos, readBuffer, 0, bytesToRead));
      assertTrue("AbfsInputStream#read did not read the correct bytes",
          Arrays.areEqual(Arrays.copyOfRange(data, pos, pos + bytesToRead), readBuffer));
      // Read only 10 bytes from offset 0. But by default it will do the seek and read where the
      // entire 100 bytes get read into the AbfsInputStream buffer.
      assertArrayEquals("AbfsInputStream#read did not read more data into its buffer", data,
          Arrays.copyOfRange(((AbfsInputStream) inputStream.getWrappedStream()).getBuffer(), 0,
              dataSize));
    }
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      AbfsInputStream abfsIs = (AbfsInputStream) inputStream.getWrappedStream();
      abfsIs.setEnablePread(true);
      byte[] readBuffer = new byte[bytesToRead];
      int pos = 10;
      assertEquals("AbfsInputStream#read did not read the correct number of bytes",
          bytesToRead, inputStream.read(pos, readBuffer, 0, bytesToRead));
      assertTrue("AbfsInputStream#read did not read the correct bytes",
          Arrays.areEqual(Arrays.copyOfRange(data, pos, pos + bytesToRead), readBuffer));
      // Read only 10 bytes from offset 10. This time, as pread is enabled, it will only read the
      // exact bytes as requested and no data will get read into the AbfsInputStream#buffer. Infact
      // the buffer won't even get initialized.
      assertNull("AbfsInputStream pread caused the internal buffer creation", abfsIs.getBuffer());
      // Now make a seek and read so that internal buffer gets created
      inputStream.seek(0);
      inputStream.read(readBuffer);
      // This read would have fetched all 100 bytes into internal buffer.
      assertArrayEquals("AbfsInputStream#read did not read more data into its buffer", data,
          Arrays.copyOfRange(((AbfsInputStream) inputStream.getWrappedStream()).getBuffer(), 0,
              dataSize));
      // Now again do pos read and make sure not any extra data being fetched.
      resetBuffer(abfsIs.getBuffer());
      pos = 0;
      assertEquals("AbfsInputStream#read did not read the correct number of bytes", bytesToRead,
          inputStream.read(pos, readBuffer, 0, bytesToRead));
      assertTrue("AbfsInputStream#read did not read the correct bytes",
          Arrays.areEqual(Arrays.copyOfRange(data, pos, pos + bytesToRead), readBuffer));
      assertFalse("AbfsInputStream#read read more data into its buffer than expected",
          Arrays.areEqual(data, Arrays.copyOfRange(abfsIs.getBuffer(), 0, dataSize)));
    }
  }

  private void resetBuffer(byte[] buf) {
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0;
    }
  }
}
