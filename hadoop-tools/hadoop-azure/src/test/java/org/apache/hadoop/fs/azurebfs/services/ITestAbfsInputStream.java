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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamTestUtils.HUNDRED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {

  private final AbfsInputStreamTestUtils abfsInputStreamTestUtils;
  public ITestAbfsInputStream() throws Exception {
    this.abfsInputStreamTestUtils = new AbfsInputStreamTestUtils(this);
  }

  @Test
  public void testWithNoOptimization() throws Exception {
    for (int i = 2; i <= 7; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(false, false, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
      Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(fs, fileName, fileContent);
      testWithNoOptimization(fs, testFilePath, HUNDRED, fileContent);
    }
  }

  protected void testWithNoOptimization(final FileSystem fs,
      final Path testFilePath, final int seekPos, final byte[] fileContent)
      throws IOException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      long totalBytesRead = 0;
      int length = HUNDRED * HUNDRED;
      do {
        byte[] buffer = new byte[length];
        int bytesRead = iStream.read(buffer, 0, length);
        totalBytesRead += bytesRead;
        if ((totalBytesRead + seekPos) >= fileContent.length) {
          length = (fileContent.length - seekPos) % length;
        }
        assertEquals(length, bytesRead);
        abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent,
            (int) (seekPos + totalBytesRead - length), length, buffer, testFilePath);

        assertTrue(abfsInputStream.getFCursor() >= seekPos + totalBytesRead);
        assertTrue(abfsInputStream.getFCursorAfterLastRead() >= seekPos + totalBytesRead);
        assertTrue(abfsInputStream.getBCursor() >= totalBytesRead % abfsInputStream.getBufferSize());
        assertTrue(abfsInputStream.getLimit() >= totalBytesRead % abfsInputStream.getBufferSize());
      } while (totalBytesRead + seekPos < fileContent.length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testExceptionInOptimization() throws Exception {
    for (int i = 2; i <= 7; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, true, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
      Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(fs, fileName, fileContent);
      testExceptionInOptimization(fs, testFilePath, fileSize - HUNDRED,
          fileSize / 4, fileContent);
    }
  }

  /**
   * Testing the back reference being passed down to AbfsInputStream.
   */
  @Test
  public void testAzureBlobFileSystemBackReferenceInInputStream()
      throws IOException {
    Path path = path(getMethodName());
    // Create a file then open it to verify if this input stream contains any
    // back reference.
    try (FSDataOutputStream out = getFileSystem().create(path);
        FSDataInputStream in = getFileSystem().open(path)) {
      AbfsInputStream abfsInputStream = (AbfsInputStream) in.getWrappedStream();

      Assertions.assertThat(abfsInputStream.getFsBackRef().isNull())
          .describedAs("BackReference in input stream should not be null")
          .isFalse();
    }
  }

  private void testExceptionInOptimization(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException {

    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      doThrow(new IOException())
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.verifyAbfsInputStreamBaseStateBeforeSeek(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      long actualLength = length;
      if (seekPos + length > fileContent.length) {
        long delta = seekPos + length - fileContent.length;
        actualLength = length - delta;
      }
      assertEquals(bytesRead, actualLength);
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos,
          (int) actualLength, buffer, testFilePath);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(fileContent.length, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(actualLength, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= actualLength);
    } finally {
      iStream.close();
    }
  }

  private AzureBlobFileSystem getFileSystem(boolean optimizeFooterRead,
      boolean readSmallFileCompletely, int fileSize) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration()
        .setOptimizeFooterRead(optimizeFooterRead);
    getAbfsStore(fs).getAbfsConfiguration()
        .setIsChecksumValidationEnabled(true);
    if (fileSize <= getAbfsStore(fs).getAbfsConfiguration()
        .getReadBufferSize()) {
      getAbfsStore(fs).getAbfsConfiguration()
          .setReadSmallFilesCompletely(readSmallFileCompletely);
    }
    return fs;
  }
}
