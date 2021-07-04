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

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsInputStreamSmallFileReads extends ITestAbfsInputStream {

  public ITestAbfsInputStreamSmallFileReads() throws Exception {
  }

  @Test
  public void testMockFastpathOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testNumBackendCalls(true, true);
  }

  @Test
  public void testOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    testNumBackendCalls(true, false);
  }

  public void testOnlyOneServerCallIsMadeWhenTheConfIsTrue(boolean isMockFastpathTest) throws Exception {
    testNumBackendCalls(true, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testMultipleServerCallsAreMadeWhenTheConfIsFalse(true);
  }

  @Test
  public void testMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    testMultipleServerCallsAreMadeWhenTheConfIsFalse(false);
  }

  public void testMultipleServerCallsAreMadeWhenTheConfIsFalse(boolean isMockFastpathTest)
      throws Exception {
    testNumBackendCalls(false, isMockFastpathTest);
  }

  private void testNumBackendCalls(boolean readSmallFilesCompletely, boolean isMockFastpathTest)
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem(readSmallFilesCompletely);
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      FSDataInputStream iStream = fs.open(testFilePath);
      if (isMockFastpathTest) {
        iStream = openMockAbfsInputStream((AzureBlobFileSystem) fs, iStream);
      }
      byte[] buffer = new byte[length];

      Map<String, Long> metricMap = getInstrumentationMap(fs);
      long requestsMadeBeforeTest = metricMap
          .get(CONNECTIONS_MADE.getStatName());

      iStream.seek(seekPos(SeekTo.END, fileSize, length));
      iStream.read(buffer, 0, length);

      iStream.seek(seekPos(SeekTo.MIDDLE, fileSize, length));
      iStream.read(buffer, 0, length);

      iStream.seek(seekPos(SeekTo.BEGIN, fileSize, length));
      iStream.read(buffer, 0, length);

      metricMap = getInstrumentationMap(fs);
      long requestsMadeAfterTest = metricMap
          .get(CONNECTIONS_MADE.getStatName());

      if (readSmallFilesCompletely) {
        assertEquals(1, requestsMadeAfterTest - requestsMadeBeforeTest);
      } else {
        assertEquals(3, requestsMadeAfterTest - requestsMadeBeforeTest);
      }
    }
  }

  @Test
  public void testMockFastpathSeekToBeginingAndReadSmallFileWithConfTrue()
      throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeginingAndReadSmallFileWithConfTrue(true);
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfTrue()
      throws Exception {
    testSeekToBeginingAndReadSmallFileWithConfTrue(false);
  }

  public void testSeekToBeginingAndReadSmallFileWithConfTrue(boolean isMockFastpathTest)
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, true, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToBeginingAndReadSmallFileWithConfFalse()
      throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeginingAndReadSmallFileWithConfFalse(true);
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfFalse()
      throws Exception {
    testSeekToBeginingAndReadSmallFileWithConfFalse(false);
  }

  public void testSeekToBeginingAndReadSmallFileWithConfFalse(boolean isMockFastpathTest)
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, false, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToBeginingAndReadBigFileWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeginingAndReadBigFileWithConfTrue(true);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfTrue() throws Exception {
    testSeekToBeginingAndReadBigFileWithConfTrue(false);
  }

  public void testSeekToBeginingAndReadBigFileWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, true, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToBeginingAndReadBigFileWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeginingAndReadBigFileWithConfFalse(true);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfFalse() throws Exception {
    testSeekToBeginingAndReadBigFileWithConfFalse(false);
  }

  public void testSeekToBeginingAndReadBigFileWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, false, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToEndAndReadSmallFileWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToEndAndReadSmallFileWithConfTrue(true);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfTrue() throws Exception {
    testSeekToEndAndReadSmallFileWithConfTrue(false);
  }

  public void testSeekToEndAndReadSmallFileWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, true, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToEndAndReadSmallFileWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToEndAndReadSmallFileWithConfFalse(true);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfFalse() throws Exception {
    testSeekToEndAndReadSmallFileWithConfFalse(false);
  }

  public void testSeekToEndAndReadSmallFileWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, false, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToEndAndReadBigFileWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToEndAndReadBigFileWithConfTrue(true);
  }

  @Test
  public void testSeekToEndAndReadBigFileWithConfTrue() throws Exception {
    testSeekToEndAndReadBigFileWithConfTrue(false);
  }

  public void testSeekToEndAndReadBigFileWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, true, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToEndAndReaBigFiledWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToEndAndReaBigFiledWithConfFalse(true);
  }

  @Test
  public void testSeekToEndAndReaBigFiledWithConfFalse() throws Exception {
    testSeekToEndAndReaBigFiledWithConfFalse(false);
  }

  public void testSeekToEndAndReaBigFiledWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, false, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToMiddleAndReadSmallFileWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToMiddleAndReadSmallFileWithConfTrue(true);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfTrue() throws Exception {
    testSeekToMiddleAndReadSmallFileWithConfTrue(false);
  }

  public void testSeekToMiddleAndReadSmallFileWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, true, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToMiddleAndReadSmallFileWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToMiddleAndReadSmallFileWithConfFalse(true);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfFalse() throws Exception {
    testSeekToMiddleAndReadSmallFileWithConfFalse(false);
  }

  public void testSeekToMiddleAndReadSmallFileWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, false, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToMiddleAndReaBigFileWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToMiddleAndReaBigFileWithConfTrue(true);
  }

  @Test
  public void testSeekToMiddleAndReaBigFileWithConfTrue() throws Exception {
    testSeekToMiddleAndReaBigFileWithConfTrue(false);
  }

  public void testSeekToMiddleAndReaBigFileWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, true, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToMiddleAndReadBigFileWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToMiddleAndReadBigFileWithConfFalse(true);
  }

  @Test
  public void testSeekToMiddleAndReadBigFileWithConfFalse() throws Exception {
    testSeekToMiddleAndReadBigFileWithConfFalse(false);
  }

  public void testSeekToMiddleAndReadBigFileWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, false, isMockFastpathTest);
  }

  private void testSeekAndReadWithConf(SeekTo seekTo, int startFileSizeInMB,
      int endFileSizeInMB, boolean readSmallFilesCompletely, boolean isMockFastpathTest) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem(readSmallFilesCompletely);
    for (int i = startFileSizeInMB; i <= endFileSizeInMB; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      int seekPos = seekPos(seekTo, fileSize, length);
      seekReadAndTest(fs, testFilePath, seekPos, length, fileContent, isMockFastpathTest);
    }
  }

  private int seekPos(SeekTo seekTo, int fileSize, int length) {
    if (seekTo == SeekTo.BEGIN) {
      return 0;
    }
    if (seekTo == SeekTo.END) {
      return fileSize - length;
    }
    return fileSize / 2;
  }

  private void seekReadAndTest(FileSystem fs, Path testFilePath, int seekPos,
      int length, byte[] fileContent, boolean isMockFastpathTest)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    AbfsConfiguration conf = getAbfsStore(fs).getAbfsConfiguration();
    FSDataInputStream iStream = getFileSystem().open(testFilePath);
    if (isMockFastpathTest) {
      iStream = openMockAbfsInputStream((AzureBlobFileSystem) fs, iStream);
    }
    seek(iStream, seekPos);
    byte[] buffer = new byte[length];
    int bytesRead = iStream.read(buffer, 0, length);
    assertEquals(bytesRead, length);
    assertContentReadCorrectly(fileContent, seekPos, length, buffer);
    AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
        .getWrappedStream();

    final int readBufferSize = conf.getReadBufferSize();
    final int fileContentLength = fileContent.length;
    final boolean smallFile = fileContentLength <= readBufferSize;
    int expectedLimit, expectedFCursor;
    int expectedBCursor;
    if (conf.readSmallFilesCompletely() && smallFile) {
      assertBuffersAreEqual(fileContent, abfsInputStream.getBuffer(), conf);
      expectedFCursor = fileContentLength;
      expectedLimit = fileContentLength;
      expectedBCursor = seekPos + length;
    } else {
      if ((seekPos == 0)) {
        assertBuffersAreEqual(fileContent, abfsInputStream.getBuffer(), conf);
      } else {
        assertBuffersAreNotEqual(fileContent, abfsInputStream.getBuffer(),
            conf);
      }
      expectedBCursor = length;
      expectedFCursor = (fileContentLength < (seekPos + readBufferSize))
          ? fileContentLength
          : (seekPos + readBufferSize);
      expectedLimit = (fileContentLength < (seekPos + readBufferSize))
          ? (fileContentLength - seekPos)
          : readBufferSize;
    }
    assertEquals(expectedFCursor, abfsInputStream.getFCursor());
    assertEquals(expectedFCursor, abfsInputStream.getFCursorAfterLastRead());
    assertEquals(expectedBCursor, abfsInputStream.getBCursor());
    assertEquals(expectedLimit, abfsInputStream.getLimit());
  }

  @Test
  public void testMockFastpathPartialReadWithNoData() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testPartialReadWithNoData(true);
  }

  @Test
  public void testPartialReadWithNoData() throws Exception {
    testPartialReadWithNoData(false);
  }

  public void testPartialReadWithNoData(boolean isMockFastpathTest) throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      partialReadWithNoData(fs, testFilePath, fileSize / 2, fileSize / 4,
          fileContent, isMockFastpathTest);
    }
  }

  private void partialReadWithNoData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent,
      boolean isMockFastpathTest)
      throws IOException {

    FSDataInputStream iStream = fs.open(testFilePath);
    if (isMockFastpathTest) {
      iStream = openMockAbfsInputStream((AzureBlobFileSystem) fs, iStream);
    }

    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      doReturn(10)
          .doReturn(10)
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

      iStream = new FSDataInputStream(abfsInputStream);
      seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(bytesRead, length);
      assertContentReadCorrectly(fileContent, seekPos, length, buffer);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(fileContent.length,
          abfsInputStream.getFCursorAfterLastRead());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testMockFastpathPartialReadWithSomeData() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testPartialReadWithSomeData(true);
  }

  @Test
  public void testPartialReadWithSomeData() throws Exception {
    testPartialReadWithSomeData(false);
  }

  public void testPartialReadWithSomeData(boolean isMockFastpathTest) throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      partialReadWithSomeData(fs, testFilePath, fileSize / 2,
          fileSize / 4, fileContent, isMockFastpathTest);
    }
  }

  private void partialReadWithSomeData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent,
      boolean isMockFastpathTest)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    if (isMockFastpathTest) {
      iStream = openMockAbfsInputStream((AzureBlobFileSystem) fs, iStream);
    }

    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      //  first readRemote, will return first 10 bytes
      //  second readRemote, seekPos - someDataLength(10) will reach the
      //  seekPos as 10 bytes are already read in the first call. Plus
      //  someDataLength(10)
      int someDataLength = 10;
      int secondReturnSize = seekPos - 10 + someDataLength;
      doReturn(10)
          .doReturn(secondReturnSize)
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

      iStream = new FSDataInputStream(abfsInputStream);
      seek(iStream, seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      assertTrue(abfsInputStream.getFCursor() > seekPos + length);
      assertTrue(abfsInputStream.getFCursorAfterLastRead() > seekPos + length);
      //  Optimized read was no complete but it got some user requested data
      //  from server. So obviously the buffer will contain data more than
      //  seekPos + len
      assertEquals(length - someDataLength, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() > length - someDataLength);
    } finally {
      iStream.close();
    }
  }

  private enum SeekTo {BEGIN, MIDDLE, END}

}
