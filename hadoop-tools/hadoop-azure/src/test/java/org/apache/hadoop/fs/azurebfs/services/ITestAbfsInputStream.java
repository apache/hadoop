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
import java.lang.reflect.Field;
import java.util.Random;

import org.junit.Test;
import org.assertj.core.api.Assertions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {

  public ITestAbfsInputStream() throws Exception {
  }

  @Test
  public void testSmallFilesAreReadCompletelyByDefault() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final int bufferSize = 4 * ONE_MB;
    fs.getAbfsStore().getAbfsConfiguration().setReadBufferSize(bufferSize);
    for (int i = 1; i <= 4; i++) {
      final int fileSize = i * ONE_MB;
      final Path testFilePath = createFileOfSize(fs,
          methodName.getMethodName() + i, fileSize);
      final long bytesRead = readOneKBAndGetFcursor(fs, testFilePath);
      assertFileReadCompletely(bytesRead,fileSize,bufferSize);
    }
  }

  @Test
  public void testSmallFilesAreReadCompletelyWithConfigOn() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final int bufferSize = 4 * ONE_MB;
    fs.getAbfsStore().getAbfsConfiguration().setReadBufferSize(bufferSize);
    fs.getAbfsStore().getAbfsConfiguration().setReadSmallFilesCompletely(true);
    for (int i = 1; i <= 4; i++) {
      final int fileSize = i * ONE_MB;
      final Path testFilePath = createFileOfSize(fs,
          methodName.getMethodName() + i, fileSize);
      final long bytesRead = readOneKBAndGetFcursor(fs, testFilePath);
      assertFileReadCompletely(bytesRead,fileSize,bufferSize);
    }
  }

  @Test
  public void testBigFilesAreNotReadCompletelyWithConfigOn() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.getAbfsStore().getAbfsConfiguration().setReadBufferSize(4 * ONE_MB);
    fs.getAbfsStore().getAbfsConfiguration().setReadSmallFilesCompletely(true);
    for (int i = 5; i <= 8; i++) {
      final Path testFilePath = createFileOfSize(fs,
          methodName.getMethodName() + i, i * ONE_MB);
      final long bytesRead = readOneKBAndGetFcursor(fs, testFilePath);
      assertFileNotReadCompletely(bytesRead, ONE_KB);
    }
  }

  @Test
  public void testSmallFilesAreReadNotCompletelyWithConfigOff() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final int bufferSize = 4 * ONE_MB;
    fs.getAbfsStore().getAbfsConfiguration().setReadBufferSize(bufferSize);
    fs.getAbfsStore().getAbfsConfiguration().setReadSmallFilesCompletely(false);
    for (int i = 1; i <= 4; i++) {
      final int fileSize = i * ONE_MB;
      final Path testFilePath = createFileOfSize(fs,
          methodName.getMethodName() + i, fileSize);
      final long bytesRead = readOneKBAndGetFcursor(fs, testFilePath);
      assertFileNotReadCompletely(bytesRead, ONE_KB);
    }
  }

  @Test
  public void testBigFilesAreNotReadCompletelyWithConfigOff() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.getAbfsStore().getAbfsConfiguration().setReadBufferSize(4 * ONE_MB);
    fs.getAbfsStore().getAbfsConfiguration().setReadSmallFilesCompletely(false);
    for (int i = 5; i <= 8; i++) {
      final Path testFilePath = createFileOfSize(fs,
          methodName.getMethodName() + i, i * ONE_MB);
      final long bytesRead = readOneKBAndGetFcursor(fs, testFilePath);
      assertFileNotReadCompletely(bytesRead, ONE_KB);
    }
  }

  private void assertFileReadCompletely(final long bytesRead,
      final int fileSize, final int bufferSize){
    Assertions.assertThat(bytesRead)
        .describedAs("Entire file should have been read as the file size ("
            +fileSize+") <= the buffer size (" + bufferSize + ")")
        .isEqualTo(fileSize);
  }

  private void assertFileNotReadCompletely(final long bytesRead,
      final int readLength){
    Assertions.assertThat(bytesRead)
        .describedAs("It is expected to read only the requested length ("
            + readLength + ") bytes")
        .isEqualTo(readLength);
  }

  private Path createFileOfSize(final FileSystem fs, final String fileName,
      final int fileSize) throws IOException {
    final Path testFilePath = path(fileName);
    final byte[] iBuffer = getRandomBytesArray(fileSize);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(iBuffer);
      oStream.flush();
    }
    return testFilePath;
  }

  private long readOneKBAndGetFcursor(final FileSystem fs,
      final Path testFilePath)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      AbfsInputStream abfsIStream = (AbfsInputStream) iStream
          .getWrappedStream();
      setIStreamForNonSequentialReadCondition(abfsIStream);
      final int length = ONE_KB;
      byte[] b = new byte[length];
      iStream.read(0, b, 0, length);
      return abfsIStream.getFcursor();
    }
  }

  private void setIStreamForNonSequentialReadCondition(
      AbfsInputStream abfsIStream)
      throws IllegalAccessException, NoSuchFieldException {
    Field fCursorAfterLastReadField = abfsIStream.getClass()
        .getDeclaredField("fCursorAfterLastRead");
    fCursorAfterLastReadField.setAccessible(true);
    fCursorAfterLastReadField.setLong(abfsIStream, -2L);
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }
}
