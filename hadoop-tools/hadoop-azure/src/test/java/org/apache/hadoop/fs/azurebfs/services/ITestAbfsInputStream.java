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
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {

  protected static final int HUNDRED = 100;

  public ITestAbfsInputStream() throws Exception {
  }

  @Test
  public void testWithNoOptimization() throws Exception {
    for (int i = 2; i <= 7; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(false, false, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
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
      seek(iStream, seekPos);
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
        assertContentReadCorrectly(fileContent,
            (int) (seekPos + totalBytesRead - length), length, buffer);

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
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      testExceptionInOptimization(fs, testFilePath, fileSize - HUNDRED,
          fileSize / 4, fileContent);
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
          .readRemote(anyLong(), any(), anyInt(), anyInt());

      iStream = new FSDataInputStream(abfsInputStream);
      verifyBeforeSeek(abfsInputStream);
      seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      long actualLength = length;
      if (seekPos + length > fileContent.length) {
        long delta = seekPos + length - fileContent.length;
        actualLength = length - delta;
      }
      assertEquals(bytesRead, actualLength);
      assertContentReadCorrectly(fileContent, seekPos, (int) actualLength, buffer);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(fileContent.length, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(actualLength, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= actualLength);
    } finally {
      iStream.close();
    }
  }

  protected AzureBlobFileSystem getFileSystem(boolean readSmallFilesCompletely)
      throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration()
        .setReadSmallFilesCompletely(readSmallFilesCompletely);
    return fs;
  }

  private AzureBlobFileSystem getFileSystem(boolean optimizeFooterRead,
      boolean readSmallFileCompletely, int fileSize) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration()
        .setOptimizeFooterRead(optimizeFooterRead);
    if (fileSize <= getAbfsStore(fs).getAbfsConfiguration()
        .getReadBufferSize()) {
      getAbfsStore(fs).getAbfsConfiguration()
          .setReadSmallFilesCompletely(readSmallFileCompletely);
    }
    return fs;
  }

  protected byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  protected Path createFileWithContent(FileSystem fs, String fileName,
      byte[] fileContent) throws IOException {
    Path testFilePath = path(fileName);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testFilePath;
  }

  protected AzureBlobFileSystemStore getAbfsStore(FileSystem fs)
      throws NoSuchFieldException, IllegalAccessException {
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
    Field abfsStoreField = AzureBlobFileSystem.class
        .getDeclaredField("abfsStore");
    abfsStoreField.setAccessible(true);
    return (AzureBlobFileSystemStore) abfsStoreField.get(abfs);
  }

  protected Map<String, Long> getInstrumentationMap(FileSystem fs)
      throws NoSuchFieldException, IllegalAccessException {
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
    Field abfsCountersField = AzureBlobFileSystem.class
        .getDeclaredField("abfsCounters");
    abfsCountersField.setAccessible(true);
    AbfsCounters abfsCounters = (AbfsCounters) abfsCountersField.get(abfs);
    return abfsCounters.toMap();
  }

  protected void assertContentReadCorrectly(byte[] actualFileContent, int from,
      int len, byte[] contentRead) {
    for (int i = 0; i < len; i++) {
      assertEquals(contentRead[i], actualFileContent[i + from]);
    }
  }

  protected void assertBuffersAreNotEqual(byte[] actualContent,
      byte[] contentRead, AbfsConfiguration conf) {
    assertBufferEquality(actualContent, contentRead, conf, false);
  }

  protected void assertBuffersAreEqual(byte[] actualContent, byte[] contentRead,
      AbfsConfiguration conf) {
    assertBufferEquality(actualContent, contentRead, conf, true);
  }

  private void assertBufferEquality(byte[] actualContent, byte[] contentRead,
      AbfsConfiguration conf, boolean assertEqual) {
    int bufferSize = conf.getReadBufferSize();
    int actualContentSize = actualContent.length;
    int n = (actualContentSize < bufferSize) ? actualContentSize : bufferSize;
    int matches = 0;
    for (int i = 0; i < n; i++) {
      if (actualContent[i] == contentRead[i]) {
        matches++;
      }
    }
    if (assertEqual) {
      assertEquals(n, matches);
    } else {
      assertNotEquals(n, matches);
    }
  }

  protected void seek(FSDataInputStream iStream, long seekPos)
      throws IOException {
    AbfsInputStream abfsInputStream = (AbfsInputStream) iStream.getWrappedStream();
    verifyBeforeSeek(abfsInputStream);
    iStream.seek(seekPos);
    verifyAfterSeek(abfsInputStream, seekPos);
  }

  private void verifyBeforeSeek(AbfsInputStream abfsInputStream){
    assertEquals(0, abfsInputStream.getFCursor());
    assertEquals(-1, abfsInputStream.getFCursorAfterLastRead());
    assertEquals(0, abfsInputStream.getLimit());
    assertEquals(0, abfsInputStream.getBCursor());
  }

  private void verifyAfterSeek(AbfsInputStream abfsInputStream, long seekPos){
    assertEquals(seekPos, abfsInputStream.getFCursor());
    assertEquals(-1, abfsInputStream.getFCursorAfterLastRead());
    assertEquals(0, abfsInputStream.getLimit());
    assertEquals(0, abfsInputStream.getBCursor());
  }
}
