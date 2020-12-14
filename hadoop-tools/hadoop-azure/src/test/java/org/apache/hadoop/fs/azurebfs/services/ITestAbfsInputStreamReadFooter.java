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

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static java.lang.Math.max;
import static java.lang.Math.min;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsInputStreamReadFooter
    extends AbstractAbfsIntegrationTest {

  private static final int TEN = 10;
  private static final int TWENTY = 20;

  public ITestAbfsInputStreamReadFooter() throws Exception {
  }

  @Test
  public void testOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    testNumBackendCalls(true);
  }

  @Test
  public void testMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    testNumBackendCalls(false);
  }

  private void testNumBackendCalls(boolean optimizeFooterRead)
      throws Exception {
    for (int i = 1; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(optimizeFooterRead,
          fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = AbfsInputStream.FOOTER_SIZE;
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        byte[] buffer = new byte[length];

        Map<String, Long> metricMap = getInstrumentationMap(fs);
        long requestsMadeBeforeTest = metricMap
            .get(CONNECTIONS_MADE.getStatName());

        iStream.seek(fileSize - 8);
        iStream.read(buffer, 0, length);

        iStream.seek(fileSize - (TEN * ONE_KB));
        iStream.read(buffer, 0, length);

        iStream.seek(fileSize - (TWENTY * ONE_KB));
        iStream.read(buffer, 0, length);

        metricMap = getInstrumentationMap(fs);
        long requestsMadeAfterTest = metricMap
            .get(CONNECTIONS_MADE.getStatName());

        if (optimizeFooterRead) {
          assertEquals(1, requestsMadeAfterTest - requestsMadeBeforeTest);
        } else {
          assertEquals(3, requestsMadeAfterTest - requestsMadeBeforeTest);
        }
      }
    }
  }

  private Map<String, Long> getInstrumentationMap(FileSystem fs)
      throws NoSuchFieldException, IllegalAccessException {
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
    Field abfsCountersField = AzureBlobFileSystem.class
        .getDeclaredField("abfsCounters");
    abfsCountersField.setAccessible(true);
    AbfsCounters abfsCounters = (AbfsCounters) abfsCountersField.get(abfs);
    return abfsCounters.toMap();
  }

  @Test
  public void testSeekToBeginAndReadWithConfTrue() throws Exception {
    testSeekAndReadWithConf(true, SeekTo.BEGIN);
  }

  @Test
  public void testSeekToBeginAndReadWithConfFalse() throws Exception {
    testSeekAndReadWithConf(false, SeekTo.BEGIN);
  }

  @Test
  public void testSeekToBeforeFooterAndReadWithConfTrue() throws Exception {
    testSeekAndReadWithConf(true, SeekTo.BEFORE_FOOTER_START);
  }

  @Test
  public void testSeekToBeforeFooterAndReadWithConfFalse() throws Exception {
    testSeekAndReadWithConf(false, SeekTo.BEFORE_FOOTER_START);
  }

  @Test
  public void testSeekToFooterAndReadWithConfTrue() throws Exception {
    testSeekAndReadWithConf(true, SeekTo.AT_FOOTER_START);
  }

  @Test
  public void testSeekToFooterAndReadWithConfFalse() throws Exception {
    testSeekAndReadWithConf(false, SeekTo.AT_FOOTER_START);
  }

  @Test
  public void testSeekToAfterFooterAndReadWithConfTrue() throws Exception {
    testSeekAndReadWithConf(true, SeekTo.AFTER_FOOTER_START);
  }

  @Test
  public void testSeekToToAfterFooterAndReadWithConfFalse() throws Exception {
    testSeekAndReadWithConf(false, SeekTo.AFTER_FOOTER_START);
  }

  private void testSeekAndReadWithConf(boolean optimizeFooterRead,
      SeekTo seekTo) throws Exception {
    for (int i = 2; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(optimizeFooterRead,
          fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      seekReadAndTest(fs, testFilePath, seekPos(seekTo, fileSize), 100,
          fileContent);
    }
  }

  private int seekPos(SeekTo seekTo, int fileSize) {
    if (seekTo == SeekTo.BEGIN) {
      return 0;
    }
    if (seekTo == SeekTo.BEFORE_FOOTER_START) {
      return fileSize - AbfsInputStream.FOOTER_SIZE - 1;
    }
    if (seekTo == SeekTo.AT_FOOTER_START) {
      return fileSize - AbfsInputStream.FOOTER_SIZE;
    }
    //seekTo == SeekTo.AFTER_FOOTER_START
    return fileSize - AbfsInputStream.FOOTER_SIZE + 1;
  }

  private AzureBlobFileSystem getFileSystem(boolean optimizeFooterRead,
      int fileSze) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration()
        .setOptimizeFooterRead(optimizeFooterRead);
    if (fileSze <= getAbfsStore(fs).getAbfsConfiguration()
        .getReadBufferSize()) {
      getAbfsStore(fs).getAbfsConfiguration()
          .setReadSmallFilesCompletely(false);
    }
    return fs;
  }

  private Path createFileWithContent(FileSystem fs, String fileName,
      byte[] fileContent) throws IOException {
    Path testFilePath = path(fileName);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testFilePath;
  }

  private void seekReadAndTest(final FileSystem fs, final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    AbfsConfiguration conf = getAbfsStore(fs).getAbfsConfiguration();
    long actualContentLength = fileContent.length;
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      long bufferSize = abfsInputStream.getBufferSize();
      iStream.seek(seekPos);
      byte[] buffer = new byte[length];
      long bytesRead = iStream.read(buffer, 0, length);

      long footerStart = max(0,
          actualContentLength - AbfsInputStream.FOOTER_SIZE);
      boolean optimizationOn =
          conf.optimizeFooterRead() && seekPos >= footerStart;

      long expectedLimit;
      long expectedBCurson;
      long expectedFCursor;
      if (optimizationOn) {
        if (actualContentLength <= bufferSize) {
          expectedLimit = actualContentLength;
          expectedBCurson = seekPos + length;
        } else {
          expectedLimit = bufferSize;
          long lastBlockStart = max(0, actualContentLength - bufferSize);
          expectedBCurson = seekPos - lastBlockStart + length;
        }
        expectedFCursor = actualContentLength;
      } else {
        if (seekPos + bufferSize < actualContentLength) {
          expectedLimit = expectedFCursor = bufferSize;
        } else {
          expectedLimit = actualContentLength - seekPos;
          expectedFCursor = min(seekPos + bufferSize, actualContentLength);
        }
        expectedBCurson = length;
      }

      assertEquals(expectedFCursor, abfsInputStream.getFCursor());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
      assertEquals(expectedBCurson, abfsInputStream.getBCursor());
      assertEquals(length, bytesRead);
      //  Verify user-content read
      assertSuccessfulRead(fileContent, seekPos, length, buffer);
      //  Verify data read to AbfsInputStream buffer
      int from = seekPos;
      if (optimizationOn) {
        from = (int) max(0, actualContentLength - bufferSize);
      }
      assertSuccessfulRead(fileContent, from, (int) abfsInputStream.getLimit(),
          abfsInputStream.getBuffer());
    }
  }

  private AzureBlobFileSystemStore getAbfsStore(FileSystem fs)
      throws NoSuchFieldException, IllegalAccessException {
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
    Field abfsStoreField = AzureBlobFileSystem.class
        .getDeclaredField("abfsStore");
    abfsStoreField.setAccessible(true);
    return (AzureBlobFileSystemStore) abfsStoreField.get(abfs);
  }

  private void assertSuccessfulRead(byte[] actualFileContent, int from, int len,
      byte[] contentRead) {
    for (int i = 0; i < len; i++) {
      assertEquals(contentRead[i], actualFileContent[i + from]);
    }
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  @Test
  public void testPartialReadWithNoDataSeekToEndAndReadWithConfTrue()
      throws Exception {
    for (int i = 2; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      partialReadWithNoDataSeekReadAndTest(fs, testFilePath,
          fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
          fileContent);
    }
  }

  private void partialReadWithNoDataSeekReadAndTest(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      doReturn(10).doReturn(10).doCallRealMethod().when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt());

      iStream = new FSDataInputStream(abfsInputStream);
      iStream.seek(seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      assertSuccessfulRead(fileContent, seekPos, length, buffer);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testPartialReadWithSomeDataSeekToEndAndReadWithConfTrue()
      throws Exception {
    for (int i = 3; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      partialReadWithSomeDataSeekReadAndTest(fs, testFilePath,
          fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
          fileContent);
    }
  }

  private void partialReadWithSomeDataSeekReadAndTest(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      //  first readRemote, will return first 10 bytes
      //  second readRemote returns data till the last 2 bytes
      int someDataLength = 2;
      int secondReturnSize =
          min(fileContent.length, abfsInputStream.getBufferSize()) - 10
              - someDataLength;
      doReturn(10).doReturn(secondReturnSize).doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt());

      iStream = new FSDataInputStream(abfsInputStream);
      iStream.seek(seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      //  someDataLength(2), because in the do-while loop in read, the 2nd loop
      //  will go to readoneblock and that resets the bCursor to 0 as
      //  bCursor == limit finally when the 2 bytes are read bCursor and limit
      //  will be at someDataLength(2)
      assertEquals(someDataLength, abfsInputStream.getBCursor());
      assertEquals(someDataLength, abfsInputStream.getLimit());
    } finally {
      iStream.close();
    }
  }

  private enum SeekTo {
    BEGIN, AT_FOOTER_START, BEFORE_FOOTER_START, AFTER_FOOTER_START
  }
}
