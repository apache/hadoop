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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.lang.Math.max;
import static java.lang.Math.min;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_FOOTER_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FOOTER_READ_BUFFER_SIZE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;

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
    int fileIdx = 0;
    for (int i = 0; i <= 4; i++) {
      for (int j = 0; j <= 2; j++) {
        int fileSize = (int) Math.pow(2, i) * 256 * ONE_KB;
        int footerReadBufferSize = (int) Math.pow(2, j) * 256 * ONE_KB;
        final AzureBlobFileSystem fs = getFileSystem(
            optimizeFooterRead, fileSize);
        Path testFilePath = createPathAndFileWithContent(
            fs, fileIdx++, fileSize);
        int length = AbfsInputStream.FOOTER_SIZE;
        FutureDataInputStreamBuilder builder = getParameterizedBuilder(
            testFilePath, fs, footerReadBufferSize);
        try (FSDataInputStream iStream = builder.build().get()) {
          verifyConfigValueInStream(iStream, footerReadBufferSize);
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
            assertEquals(1,
                requestsMadeAfterTest - requestsMadeBeforeTest);
          } else {
            assertEquals(3,
                requestsMadeAfterTest - requestsMadeBeforeTest);
          }
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
    // Running the test for file sizes ranging from 256 KB to 4 MB with
    // Footer Read Buffer size ranging from 256 KB to 1 MB
    // This will cover files less than footer read buffer size,
    // Files between footer read buffer and read buffer size
    // Files bigger than read buffer size
    int fileIdx = 0;
    for (int i = 0; i <= 4; i++) {
      for (int j = 0; j <= 2; j++) {
        int fileSize = (int) Math.pow(2, i) * 256 * ONE_KB;
        int footerReadBufferSize = (int) Math.pow(2, j) * 256 * ONE_KB;
        final AzureBlobFileSystem fs = getFileSystem(
            optimizeFooterRead, fileSize);
        String fileName = methodName.getMethodName() + fileIdx++;
        byte[] fileContent = getRandomBytesArray(fileSize);
        Path testFilePath = createFileWithContent(fs, fileName, fileContent);
        seekReadAndTest(fs, testFilePath, seekPos(seekTo, fileSize), HUNDRED,
            fileContent, footerReadBufferSize);
      }
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

  private void seekReadAndTest(final AzureBlobFileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent, int footerReadBufferSize) throws Exception {
    AbfsConfiguration conf = getAbfsStore(fs).getAbfsConfiguration();
    long actualContentLength = fileContent.length;
    FutureDataInputStreamBuilder builder = getParameterizedBuilder(
        testFilePath, fs, footerReadBufferSize);
    try (FSDataInputStream iStream = builder.build().get()) {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream.getWrappedStream();
      verifyConfigValueInStream(iStream, footerReadBufferSize);
      long readBufferSize = abfsInputStream.getBufferSize();
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
      long expectedBCursor;
      long expectedFCursor;
      if (optimizationOn) {
        if (actualContentLength <= footerReadBufferSize) {
          expectedLimit = actualContentLength;
          expectedBCursor = seekPos + actualLength;
        } else {
          expectedLimit = footerReadBufferSize;
          long lastBlockStart = max(0, actualContentLength - footerReadBufferSize);
          expectedBCursor = seekPos - lastBlockStart + actualLength;
        }
        expectedFCursor = actualContentLength;
      } else {
        if (seekPos + readBufferSize < actualContentLength) {
          expectedLimit = readBufferSize;
          expectedFCursor = readBufferSize;
        } else {
          expectedLimit = actualContentLength - seekPos;
          expectedFCursor = min(seekPos + readBufferSize, actualContentLength);
        }
        expectedBCursor = actualLength;
      }

      assertEquals(expectedFCursor, abfsInputStream.getFCursor());
      assertEquals(expectedFCursor, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
      assertEquals(expectedBCursor, abfsInputStream.getBCursor());
      assertEquals(actualLength, bytesRead);
      //  Verify user-content read
      assertContentReadCorrectly(fileContent, seekPos, (int) actualLength, buffer, testFilePath);
      //  Verify data read to AbfsInputStream buffer
      int from = seekPos;
      if (optimizationOn) {
        from = (int) max(0, actualContentLength - footerReadBufferSize);
      }
      assertContentReadCorrectly(fileContent, from, (int) abfsInputStream.getLimit(),
          abfsInputStream.getBuffer(), testFilePath);
    }
  }

  @Test
  public void testPartialReadWithNoData() throws Exception {
    int fileIdx = 0;
    for (int i = 0; i <= 4; i++) {
      for (int j = 0; j <= 2; j++) {
        int fileSize = (int) Math.pow(2, i) * 256 * ONE_KB;
        int footerReadBufferSize = (int) Math.pow(2, j) * 256 * ONE_KB;
        final AzureBlobFileSystem fs = getFileSystem(
            true, fileSize, footerReadBufferSize);
        String fileName = methodName.getMethodName() + fileIdx++;
        byte[] fileContent = getRandomBytesArray(fileSize);
        Path testFilePath = createFileWithContent(fs, fileName, fileContent);
        testPartialReadWithNoData(fs, testFilePath,
            fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
            fileContent, footerReadBufferSize);
      }
    }
  }

  private void testPartialReadWithNoData(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent, int footerReadBufferSize) throws IOException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      Assertions.assertThat(abfsInputStream.getFooterReadBufferSize())
          .describedAs("Footer Read Buffer Size Should be same as what set in builder")
          .isEqualTo(footerReadBufferSize);
      abfsInputStream = spy(abfsInputStream);
      doReturn(10).doReturn(10).doCallRealMethod().when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

      iStream = new FSDataInputStream(abfsInputStream);
      seek(iStream, seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      assertContentReadCorrectly(fileContent, seekPos, length, buffer, testFilePath);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testPartialReadWithSomeData() throws Exception {
    for (int i = 0; i <= 4; i++) {
      for (int j = 0; j <= 2; j++) {
        int fileSize = (int) Math.pow(2, i) * 256 * ONE_KB;
        int footerReadBufferSize = (int) Math.pow(2, j) * 256 * ONE_KB;
        final AzureBlobFileSystem fs = getFileSystem(true,
            fileSize, footerReadBufferSize);
        String fileName = methodName.getMethodName() + i;
        byte[] fileContent = getRandomBytesArray(fileSize);
        Path testFilePath = createFileWithContent(fs, fileName, fileContent);
        testPartialReadWithSomeData(fs, testFilePath,
            fileSize - AbfsInputStream.FOOTER_SIZE, AbfsInputStream.FOOTER_SIZE,
            fileContent, footerReadBufferSize);
      }
    }
  }

  private void testPartialReadWithSomeData(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent, final int footerReadBufferSize) throws IOException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      verifyConfigValueInStream(iStream, footerReadBufferSize);
      AbfsInputStream abfsInputStream = spy((AbfsInputStream) iStream
          .getWrappedStream());
      //  first readRemote, will return first 10 bytes
      //  second readRemote returns data till the last 2 bytes
      int someDataLength = 2;
      int secondReturnSize =
          min(fileContent.length, abfsInputStream.getFooterReadBufferSize()) - 10
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

  @Test
  public void testFooterReadBufferSizeConfiguration() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.unset(AZURE_FOOTER_READ_BUFFER_SIZE);
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(config)){
      Path testFilePath = createPathAndFileWithContent(fs, 0, ONE_KB);
      final int footerReadBufferSizeConfig = 4 * ONE_KB;
      final int footerReadBufferSizeBuilder = 5 * ONE_KB;

      // Verify that default value is used if nothing is set explicitly
      FSDataInputStream iStream = fs.open(testFilePath);
      verifyConfigValueInStream(iStream, DEFAULT_FOOTER_READ_BUFFER_SIZE);

      // Verify that value set in config is used if builder is not used
      getAbfsStore(fs).getAbfsConfiguration()
          .setFooterReadBufferSize(footerReadBufferSizeConfig);
      iStream = fs.open(testFilePath);
      verifyConfigValueInStream(iStream, footerReadBufferSizeConfig);

      // Verify that when builder is used value set in parameters is used
      getAbfsStore(fs).getAbfsConfiguration().unset(AZURE_FOOTER_READ_BUFFER_SIZE);
      FutureDataInputStreamBuilder builder = fs.openFile(testFilePath);
      builder.opt(AZURE_FOOTER_READ_BUFFER_SIZE,
          footerReadBufferSizeBuilder);
      iStream = builder.build().get();
      verifyConfigValueInStream(iStream, footerReadBufferSizeBuilder);

      // Verify that when builder is used value set in parameters is used
      // even if config is set
      getAbfsStore(fs).getAbfsConfiguration()
          .setFooterReadBufferSize(footerReadBufferSizeConfig);
      iStream = builder.build().get();
      verifyConfigValueInStream(iStream, footerReadBufferSizeBuilder);

      // Verify that when the builder is used and parameter in builder is not set,
      // the value set in configuration is used
      getAbfsStore(fs).getAbfsConfiguration()
          .setFooterReadBufferSize(footerReadBufferSizeConfig);
      builder = fs.openFile(testFilePath);
      iStream = builder.build().get();
      verifyConfigValueInStream(iStream, footerReadBufferSizeConfig);
    }
  }

  private void verifyConfigValueInStream(final FSDataInputStream inputStream,
      final int expectedValue) {
    AbfsInputStream stream = (AbfsInputStream) inputStream.getWrappedStream();
    Assertions.assertThat(stream.getFooterReadBufferSize())
        .describedAs(
            "Footer Read Buffer Size Value Is Not As Expected")
        .isEqualTo(expectedValue);
  }

  private Path createPathAndFileWithContent(final AzureBlobFileSystem fs,
      final int fileIdx, final int fileSize) throws Exception {
    String fileName = methodName.getMethodName() + fileIdx;
    byte[] fileContent = getRandomBytesArray(fileSize);
    return createFileWithContent(fs, fileName, fileContent);
  }

  private FutureDataInputStreamBuilder getParameterizedBuilder(final Path path,
      final AzureBlobFileSystem fs, int footerReadBufferSize) throws Exception {
    FutureDataInputStreamBuilder builder = fs.openFile(path);
    builder.opt(AZURE_FOOTER_READ_BUFFER_SIZE,
        footerReadBufferSize);
    return builder;
  }

  private AzureBlobFileSystem getFileSystem(final boolean optimizeFooterRead,
      final int fileSize) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore store = getAbfsStore(fs);
    store.getAbfsConfiguration().setOptimizeFooterRead(optimizeFooterRead);
    if (fileSize <= store.getAbfsConfiguration().getReadBufferSize()) {
      store.getAbfsConfiguration().setReadSmallFilesCompletely(false);
    }
    return fs;
  }

  private AzureBlobFileSystem getFileSystem(final boolean optimizeFooterRead,
      final int fileSize, final int footerReadBufferSize) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore store = getAbfsStore(fs);
    store.getAbfsConfiguration().setOptimizeFooterRead(optimizeFooterRead);
    store.getAbfsConfiguration().setFooterReadBufferSize(footerReadBufferSize);
    if (fileSize <= store.getAbfsConfiguration().getReadBufferSize()) {
      store.getAbfsConfiguration().setReadSmallFilesCompletely(false);
    }
    return fs;
  }

  private enum SeekTo {
    BEGIN, AT_FOOTER_START, BEFORE_FOOTER_START, AFTER_FOOTER_START, END
  }
}
