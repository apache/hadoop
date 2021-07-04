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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

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
  public void testMockFastpathOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testNumBackendCalls(true, true);
  }

  @Test
  public void testOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    testNumBackendCalls(true, false);
  }

  @Test
  public void testMockFastpathMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testNumBackendCalls(false, true);
  }

  @Test
  public void testMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    testNumBackendCalls(false, false);
  }

  private void testNumBackendCalls(boolean optimizeFooterRead,
      boolean isMockFastpathTest)
      throws Exception {
    String fileNamePrefix = methodName.getMethodName() + java.util.UUID.randomUUID().toString() + "_";
    for (int i = 1; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(optimizeFooterRead,
          fileSize);
      String fileName = fileNamePrefix + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = AbfsInputStream.FOOTER_SIZE;
      FSDataInputStream iStream = fs.open(testFilePath);
      if (isMockFastpathTest) {
        iStream = openMockAbfsInputStream(fs, iStream);
      }

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

  @Test
  public void testMockFastpathSeekToBeginAndReadWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeginAndReadWithConfTrue(true);
  }

  @Test
  public void testSeekToBeginAndReadWithConfTrue() throws Exception {
    testSeekToBeginAndReadWithConfTrue(false);
  }

  public void testSeekToBeginAndReadWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(true, SeekTo.BEGIN, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToBeginAndReadWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeginAndReadWithConfFalse(true);
  }

  @Test
  public void testSeekToBeginAndReadWithConfFalse() throws Exception {
    testSeekToBeginAndReadWithConfFalse(false);
  }

  public void testSeekToBeginAndReadWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(false, SeekTo.BEGIN, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToBeforeFooterAndReadWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeforeFooterAndReadWithConfTrue(true);
  }

  @Test
  public void testSeekToBeforeFooterAndReadWithConfTrue() throws Exception {
    testSeekToBeforeFooterAndReadWithConfTrue(false);
  }

  public void testSeekToBeforeFooterAndReadWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(true, SeekTo.BEFORE_FOOTER_START, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToBeforeFooterAndReadWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToBeforeFooterAndReadWithConfFalse(true);
  }

  @Test
  public void testSeekToBeforeFooterAndReadWithConfFalse() throws Exception {
    testSeekToBeforeFooterAndReadWithConfFalse(false);
  }

  public void testSeekToBeforeFooterAndReadWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(false, SeekTo.BEFORE_FOOTER_START, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToFooterAndReadWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToFooterAndReadWithConfTrue(true);
  }

  @Test
  public void testSeekToFooterAndReadWithConfTrue() throws Exception {
    testSeekToFooterAndReadWithConfTrue(false);
  }

  public void testSeekToFooterAndReadWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(true, SeekTo.AT_FOOTER_START, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToFooterAndReadWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToFooterAndReadWithConfFalse(true);
  }

  @Test
  public void testSeekToFooterAndReadWithConfFalse() throws Exception {
    testSeekToFooterAndReadWithConfFalse(false);
  }

  public void testSeekToFooterAndReadWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(false, SeekTo.AT_FOOTER_START, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToAfterFooterAndReadWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToAfterFooterAndReadWithConfTrue(true);
  }

  @Test
  public void testSeekToAfterFooterAndReadWithConfTrue() throws Exception {
    testSeekToAfterFooterAndReadWithConfTrue(false);
  }

  public void testSeekToAfterFooterAndReadWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(true, SeekTo.AFTER_FOOTER_START, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToToAfterFooterAndReadWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToToAfterFooterAndReadWithConfFalse(true);
  }

  @Test
  public void testSeekToToAfterFooterAndReadWithConfFalse() throws Exception {
    testSeekToToAfterFooterAndReadWithConfFalse(false);
  }

  public void testSeekToToAfterFooterAndReadWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(false, SeekTo.AFTER_FOOTER_START, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToEndAndReadWithConfTrue() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToEndAndReadWithConfTrue(true);
  }

  @Test
  public void testSeekToEndAndReadWithConfTrue() throws Exception {
    testSeekToEndAndReadWithConfTrue(false);
  }

  public void testSeekToEndAndReadWithConfTrue(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(true, SeekTo.END, isMockFastpathTest);
  }

  @Test
  public void testMockFastpathSeekToEndAndReadWithConfFalse() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testSeekToEndAndReadWithConfFalse(true);
  }

  @Test
  public void testSeekToEndAndReadWithConfFalse() throws Exception {
    testSeekToEndAndReadWithConfFalse(false);
  }

  public void testSeekToEndAndReadWithConfFalse(boolean isMockFastpathTest) throws Exception {
    testSeekAndReadWithConf(false, SeekTo.END, isMockFastpathTest);
  }

  private void testSeekAndReadWithConf(boolean optimizeFooterRead,
      SeekTo seekTo, boolean isMockFastpathTest) throws Exception {
    String fileNamePrefix = methodName.getMethodName() + java.util.UUID.randomUUID().toString();
    for (int i = 2; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(optimizeFooterRead,
          fileSize);
      String fileName = fileNamePrefix + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      seekReadAndTest(fs, testFilePath, seekPos(seekTo, fileSize), HUNDRED,
          fileContent, isMockFastpathTest);
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
      final int seekPos, final int length, final byte[] fileContent,
      boolean isMockFastpathTest)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    AbfsConfiguration conf = getAbfsStore(fs).getAbfsConfiguration();
    long actualContentLength = fileContent.length;
    FSDataInputStream iStream = fs.open(testFilePath);
    if (isMockFastpathTest) {
      iStream = openMockAbfsInputStream((AzureBlobFileSystem) fs, iStream);
    }

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

  @Test
  public void testMockFastpathPartialReadWithNoData()
      throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testPartialReadWithNoData(true);
  }

  @Test
  public void testPartialReadWithNoData()
      throws Exception {
    testPartialReadWithNoData(false);
  }

  public void testPartialReadWithNoData(boolean isMockFastpathTest)
      throws Exception {
    String fileNamePrefix = methodName.getMethodName() + java.util.UUID.randomUUID().toString();
    for (int i = 2; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, fileSize);
      String fileName = fileNamePrefix + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      testPartialReadWithNoData(fs, testFilePath,
          fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
          fileContent, isMockFastpathTest);
    }
  }

  private void testPartialReadWithNoData(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent, boolean isMockFastpathTest)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    if (isMockFastpathTest) {
      iStream = openMockAbfsInputStream((AzureBlobFileSystem) fs, iStream);
    }

    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      doReturn(10).doReturn(10).doCallRealMethod().when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

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
  public void testMockFastpathPartialReadWithSomeDat() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    testPartialReadWithSomeDat(true);
  }

  @Test
  public void testPartialReadWithSomeDat() throws Exception {
    testPartialReadWithSomeDat(false);
  }

  public void testPartialReadWithSomeDat(boolean isMockFastpathTest) throws Exception {
    String fileNamePrefix = methodName.getMethodName() + java.util.UUID.randomUUID().toString();
    for (int i = 3; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, fileSize);
      String fileName = fileNamePrefix + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      testPartialReadWithSomeDat(fs, testFilePath,
          fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
          fileContent, isMockFastpathTest);
    }
  }

  private void testPartialReadWithSomeDat(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent, boolean isMockFastpathTest)
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
      //  second readRemote returns data till the last 2 bytes
      int someDataLength = 2;
      int secondReturnSize =
          min(fileContent.length, abfsInputStream.getBufferSize()) - 10
              - someDataLength;
      doReturn(10).doReturn(secondReturnSize).doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

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
