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
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
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

public class ITestAbfsInputStreamReadFooter extends ITestAbfsInputStream {

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

  @Test
  public void testSeekToEndAndReadWithConfTrue() throws Exception {
    testSeekAndReadWithConf(true, SeekTo.END);
  }

  @Test
  public void testSeekToEndAndReadWithConfFalse() throws Exception {
    testSeekAndReadWithConf(false, SeekTo.END);
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
      seekReadAndTest(fs, testFilePath, seekPos(seekTo, fileSize), HUNDRED,
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
    if (seekTo == SeekTo.END) {
      return fileSize - 1;
    }
    //seekTo == SeekTo.AFTER_FOOTER_START
    return fileSize - AbfsInputStream.FOOTER_SIZE + 1;
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
      seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      long bytesRead = iStream.read(buffer, 0, length);

      long footerStart = max(0,
          actualContentLength - AbfsInputStream.FOOTER_SIZE);
      boolean optimizationOn =
          conf.optimizeFooterRead() && seekPos >= footerStart;

      long actualLength = length;
      if (seekPos + length > actualContentLength) {
        long delta = seekPos + length - actualContentLength;
        actualLength = length - delta;
      }
      long expectedLimit;
      long expectedBCurson;
      long expectedFCursor;
      if (optimizationOn) {
        if (actualContentLength <= bufferSize) {
          expectedLimit = actualContentLength;
          expectedBCurson = seekPos + actualLength;
        } else {
          expectedLimit = bufferSize;
          long lastBlockStart = max(0, actualContentLength - bufferSize);
          expectedBCurson = seekPos - lastBlockStart + actualLength;
        }
        expectedFCursor = actualContentLength;
      } else {
        if (seekPos + bufferSize < actualContentLength) {
          expectedLimit = bufferSize;
          expectedFCursor = bufferSize;
        } else {
          expectedLimit = actualContentLength - seekPos;
          expectedFCursor = min(seekPos + bufferSize, actualContentLength);
        }
        expectedBCurson = actualLength;
      }

      assertEquals(expectedFCursor, abfsInputStream.getFCursor());
      assertEquals(expectedFCursor, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
      assertEquals(expectedBCurson, abfsInputStream.getBCursor());
      assertEquals(actualLength, bytesRead);
      //  Verify user-content read
      assertContentReadCorrectly(fileContent, seekPos, (int) actualLength, buffer);
      //  Verify data read to AbfsInputStream buffer
      int from = seekPos;
      if (optimizationOn) {
        from = (int) max(0, actualContentLength - bufferSize);
      }
      assertContentReadCorrectly(fileContent, from, (int) abfsInputStream.getLimit(),
          abfsInputStream.getBuffer());
    }
  }

  @Test
  public void testPartialReadWithNoData()
      throws Exception {
    for (int i = 2; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      testPartialReadWithNoData(fs, testFilePath,
          fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
          fileContent);
    }
  }

  private void testPartialReadWithNoData(final FileSystem fs,
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
      seek(iStream, seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      assertContentReadCorrectly(fileContent, seekPos, length, buffer);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testPartialReadWithSomeDat()
      throws Exception {
    for (int i = 3; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      testPartialReadWithSomeDat(fs, testFilePath,
          fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
          fileContent);
    }
  }

  private void testPartialReadWithSomeDat(final FileSystem fs,
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
      seek(iStream, seekPos);

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

  private AzureBlobFileSystem getFileSystem(boolean optimizeFooterRead,
      int fileSize) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration()
        .setOptimizeFooterRead(optimizeFooterRead);
    if (fileSize <= getAbfsStore(fs).getAbfsConfiguration()
        .getReadBufferSize()) {
      getAbfsStore(fs).getAbfsConfiguration()
          .setReadSmallFilesCompletely(false);
    }
    return fs;
  }

  private enum SeekTo {
    BEGIN, AT_FOOTER_START, BEFORE_FOOTER_START, AFTER_FOOTER_START, END
  }
}
