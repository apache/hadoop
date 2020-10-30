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

import java.util.Random;

import org.junit.Test;
import org.assertj.core.api.Assertions;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.services.AbfsInputStream.SLURP_FILE_SIZE;

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {

  public ITestAbfsInputStream() throws Exception {
  }

  @Test
  public void testSmallFilesAreReadCompletely() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = 1; i <= 4; i++) {
      final int readBufferSize = i * ONE_KB;
      final int fileSize = i * ONE_MB;
      fs.getAbfsStore().getAbfsConfiguration()
          .setReadBufferSize(readBufferSize);
      final Path testFilePath = path(methodName.getMethodName() + i);
      final byte[] iBuffer = getRandomBytesArray(fileSize);
      try (FSDataOutputStream oStream = fs.create(testFilePath)) {
        oStream.write(iBuffer);
        oStream.flush();
      }
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        byte[] b = new byte[ONE_KB];
        iStream.read(0, b, 0, ONE_KB);
        AbfsInputStream abfsIStream = (AbfsInputStream) iStream
            .getWrappedStream();
        Assertions.assertThat(abfsIStream.getBuffer().length).describedAs(
            "Entire file should have been read as the file size is "
                + "less than SLURP_FILE_SIZE (" + SLURP_FILE_SIZE + ")")
            .isEqualTo(fileSize);
      }
    }
  }

  @Test
  public void testBigFilesAreNotReadCompletely() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = 5; i <= 8; i++) {
      final int readBufferSize = i * ONE_KB;
      final int fileSize = i * ONE_MB;
      fs.getAbfsStore().getAbfsConfiguration()
          .setReadBufferSize(readBufferSize);
      final Path testFilePath = path(methodName.getMethodName() + i);
      final byte[] iBuffer = getRandomBytesArray(fileSize);
      try (FSDataOutputStream oStream = fs.create(testFilePath)) {
        oStream.write(iBuffer);
        oStream.flush();
      }
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        byte[] b = new byte[ONE_KB];
        iStream.read(0, b, 0, ONE_KB);
        AbfsInputStream abfsIStream = (AbfsInputStream) iStream
            .getWrappedStream();
        Assertions.assertThat(abfsIStream.getBuffer().length)
            .describedAs("It is expected to read only first "
                + "AZURE_READ_BUFFER_SIZE ("+ readBufferSize + ") bytes")
            .isEqualTo(readBufferSize);
      }
    }
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }
}
