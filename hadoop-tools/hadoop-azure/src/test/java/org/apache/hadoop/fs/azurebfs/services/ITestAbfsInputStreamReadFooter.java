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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsScaleTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.util.functional.FutureIO;

import static java.lang.Math.max;
import static java.lang.Math.min;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_FOOTER_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FOOTER_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamTestUtils.HUNDRED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;

public class ITestAbfsInputStreamReadFooter extends AbstractAbfsScaleTest {

  private static final int TEN = 10;
  private static final int TWENTY = 20;

  private static ExecutorService executorService;

  private static final int SIZE_256_KB = 256 * ONE_KB;

  private static final Integer[] FILE_SIZES = {
      SIZE_256_KB,
      2 * SIZE_256_KB,
      ONE_MB,
      4 * ONE_MB
  };

  private static final Integer[] READ_BUFFER_SIZE = {
      SIZE_256_KB,
      2 * SIZE_256_KB,
      ONE_MB,
      4 * ONE_MB
  };

  private static final Integer[] FOOTER_READ_BUFFER_SIZE = {
      SIZE_256_KB,
      2 * SIZE_256_KB,
      ONE_MB
  };

  private final AbfsInputStreamTestUtils abfsInputStreamTestUtils;

  public ITestAbfsInputStreamReadFooter() throws Exception {
    this.abfsInputStreamTestUtils = new AbfsInputStreamTestUtils(this);
  }

  @BeforeAll
  public static void init() {
    executorService = Executors.newFixedThreadPool(
        2 * Runtime.getRuntime().availableProcessors());
  }

  @AfterAll
  public static void close() {
    executorService.shutdown();
  }

  @Test
  public void testOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    validateNumBackendCalls(true);
  }

  @Test
  public void testMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    validateNumBackendCalls(false);
  }


  /**
   * For different combination of file sizes, read buffer sizes and footer read
   * buffer size, assert the number of server calls made when the optimization
   * is enabled and disabled.
   * <p>
   * If the footer optimization is on, if the first read on the file is within the
   * footer range (given by {@link AbfsInputStream#FOOTER_SIZE}, then the last block
   * of size footerReadBufferSize is read from the server, and then subsequent
   * inputStream reads from that block is returned from the buffer maintained by the
   * AbfsInputStream. So, those reads will not result in server calls.
   */
  private void validateNumBackendCalls(boolean optimizeFooterRead)
      throws Exception {
    int fileIdx = 0;
    final List<Future<Void>> futureList = new ArrayList<>();
    for (int fileSize : FILE_SIZES) {
      final int fileId = fileIdx++;
      Future<Void> future = executorService.submit(() -> {
        try (AzureBlobFileSystem spiedFs = createSpiedFs(
            getRawConfiguration())) {
          Path testPath = createPathAndFileWithContent(
              spiedFs, fileId, fileSize);
          validateNumBackendCalls(spiedFs, optimizeFooterRead, fileSize,
              testPath);
          return null;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
      futureList.add(future);
    }
    FutureIO.awaitAllFutures(futureList);
  }

  private void validateNumBackendCalls(final AzureBlobFileSystem spiedFs,
      final boolean optimizeFooterRead, final int fileSize, final Path testFilePath) throws Exception {
    for (int readBufferSize : READ_BUFFER_SIZE) {
      for (int footerReadBufferSize : FOOTER_READ_BUFFER_SIZE) {
        changeFooterConfigs(spiedFs, optimizeFooterRead, fileSize, readBufferSize);
        int length = AbfsInputStream.FOOTER_SIZE;
        FutureDataInputStreamBuilder builder = getParameterizedBuilder(
            testFilePath, spiedFs, footerReadBufferSize);
        try (FSDataInputStream iStream = builder.build().get()) {
          verifyConfigValueInStream(iStream, footerReadBufferSize);
          byte[] buffer = new byte[length];

          Map<String, Long> metricMap =
              getInstrumentationMap(spiedFs);
          long requestsMadeBeforeTest = metricMap
              .get(CONNECTIONS_MADE.getStatName());

          iStream.seek(fileSize - 8);
          iStream.read(buffer, 0, length);

          iStream.seek(fileSize - (TEN * ONE_KB));
          iStream.read(buffer, 0, length);

          iStream.seek(fileSize - (TWENTY * ONE_KB));
          iStream.read(buffer, 0, length);

          metricMap = getInstrumentationMap(spiedFs);
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
  }

  @Test
  public void testSeekToBeginAndReadWithConfTrue() throws Exception {
    validateSeekAndReadWithConf(true, SeekTo.BEGIN);
  }

  @Test
  public void testSeekToBeginAndReadWithConfFalse() throws Exception {
    validateSeekAndReadWithConf(false, SeekTo.BEGIN);
  }

  @Test
  public void testSeekToBeforeFooterAndReadWithConfTrue() throws Exception {
    validateSeekAndReadWithConf(true, SeekTo.BEFORE_FOOTER_START);
  }

  @Test
  public void testSeekToBeforeFooterAndReadWithConfFalse() throws Exception {
    validateSeekAndReadWithConf(false, SeekTo.BEFORE_FOOTER_START);
  }

  @Test
  public void testSeekToFooterAndReadWithConfTrue() throws Exception {
    validateSeekAndReadWithConf(true, SeekTo.AT_FOOTER_START);
  }

  @Test
  public void testSeekToFooterAndReadWithConfFalse() throws Exception {
    validateSeekAndReadWithConf(false, SeekTo.AT_FOOTER_START);
  }

  @Test
  public void testSeekToAfterFooterAndReadWithConfTrue() throws Exception {
    validateSeekAndReadWithConf(true, SeekTo.AFTER_FOOTER_START);
  }

  @Test
  public void testSeekToToAfterFooterAndReadWithConfFalse() throws Exception {
    validateSeekAndReadWithConf(false, SeekTo.AFTER_FOOTER_START);
  }

  @Test
  public void testSeekToEndAndReadWithConfTrue() throws Exception {
    validateSeekAndReadWithConf(true, SeekTo.END);
  }

  @Test
  public void testSeekToEndAndReadWithConfFalse() throws Exception {
    validateSeekAndReadWithConf(false, SeekTo.END);
  }

  /**
   * For different combination of file sizes, read buffer sizes and footer read
   * buffer size, and read from different seek positions, validate the internal
   * state of AbfsInputStream.
   */
  private void validateSeekAndReadWithConf(boolean optimizeFooterRead,
      SeekTo seekTo) throws Exception {
    int fileIdx = 0;
    List<Future<Void>> futureList = new ArrayList<>();
    for (int fileSize : FILE_SIZES) {
      final int fileId = fileIdx++;
      futureList.add(executorService.submit(() -> {
        try (AzureBlobFileSystem spiedFs = createSpiedFs(
            getRawConfiguration())) {
          String fileName = methodName.getMethodName() + fileId;
          byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
          Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(spiedFs, fileName,
              fileContent);
          for (int readBufferSize : READ_BUFFER_SIZE) {
            validateSeekAndReadWithConf(spiedFs, optimizeFooterRead, seekTo,
                readBufferSize, fileSize, testFilePath, fileContent);
          }
          return null;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }));
    }
    FutureIO.awaitAllFutures(futureList);
  }

  private void validateSeekAndReadWithConf(final AzureBlobFileSystem spiedFs,
      final boolean optimizeFooterRead,
      final SeekTo seekTo,
      final int readBufferSize,
      final int fileSize,
      final Path testFilePath,
      final byte[] fileContent)
      throws Exception {
    // Running the test for file sizes ranging from 256 KB to 4 MB with
    // Footer Read Buffer size ranging from 256 KB to 1 MB
    // This will cover files less than footer read buffer size,
    // Files between footer read buffer and read buffer size
    // Files bigger than read buffer size
    for (int footerReadBufferSize : FOOTER_READ_BUFFER_SIZE) {
      changeFooterConfigs(spiedFs, optimizeFooterRead, fileSize,
          readBufferSize);

      seekReadAndTest(spiedFs, testFilePath, seekPos(seekTo, fileSize), HUNDRED,
          fileContent, footerReadBufferSize);
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
      abfsInputStreamTestUtils.seek(iStream, seekPos);
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
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos, (int) actualLength, buffer, testFilePath);
      //  Verify data read to AbfsInputStream buffer
      int from = seekPos;
      if (optimizationOn) {
        from = (int) max(0, actualContentLength - footerReadBufferSize);
      }
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, from, (int) abfsInputStream.getLimit(),
          abfsInputStream.getBuffer(), testFilePath);
    }
  }

  @Test
  public void testPartialReadWithNoData() throws Exception {
    int fileIdx = 0;
    List<Future<Void>> futureList = new ArrayList<>();
    for (int fileSize : FILE_SIZES) {
      final int fileId = fileIdx++;
      final String fileName = methodName.getMethodName() + fileId;
      futureList.add(executorService.submit(() -> {
        try (AzureBlobFileSystem spiedFs = createSpiedFs(
            getRawConfiguration())) {
          byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
          Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(spiedFs, fileName,
              fileContent);
          validatePartialReadWithNoData(spiedFs, fileSize, fileContent,
              testFilePath);
          return null;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }));
      FutureIO.awaitAllFutures(futureList);
    }
  }

  private void validatePartialReadWithNoData(final AzureBlobFileSystem spiedFs,
      final int fileSize,
      final byte[] fileContent,
      Path testFilePath) throws IOException {
    for (int readBufferSize : READ_BUFFER_SIZE) {
      for (int footerReadBufferSize : FOOTER_READ_BUFFER_SIZE) {
        changeFooterConfigs(spiedFs, true, fileSize,
            footerReadBufferSize, readBufferSize);

        validatePartialReadWithNoData(spiedFs, testFilePath,
            fileSize - AbfsInputStream.FOOTER_SIZE,
            AbfsInputStream.FOOTER_SIZE,
            fileContent, footerReadBufferSize, readBufferSize);
      }
    }
  }

  private void validatePartialReadWithNoData(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent, int footerReadBufferSize, final int readBufferSize) throws IOException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      int footerBufferSizeAssert = Math.min(readBufferSize, footerReadBufferSize);
      Assertions.assertThat(abfsInputStream.getFooterReadBufferSize())
          .describedAs("Footer Read Buffer Size Should be same as what set in builder")
          .isEqualTo(footerBufferSizeAssert);
      abfsInputStream = spy(abfsInputStream);
      doReturn(10).doReturn(10).doCallRealMethod().when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos, length, buffer, testFilePath);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testPartialReadWithSomeData() throws Exception {
    int fileIdx = 0;
    List<Future<Void>> futureList = new ArrayList<>();
    for (int fileSize : FILE_SIZES) {
      final int fileId = fileIdx++;
      futureList.add(executorService.submit(() -> {
        try (AzureBlobFileSystem spiedFs = createSpiedFs(
            getRawConfiguration())) {
          String fileName = methodName.getMethodName() + fileId;
          byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
          Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(spiedFs, fileName,
              fileContent);
          validatePartialReadWithSomeData(spiedFs, fileSize, testFilePath,
              fileContent);
          return null;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }));
    }
    FutureIO.awaitAllFutures(futureList);
  }

  private void validatePartialReadWithSomeData(final AzureBlobFileSystem spiedFs,
      final int fileSize, final Path testFilePath, final byte[] fileContent)
      throws IOException {
    for (int readBufferSize : READ_BUFFER_SIZE) {
      for (int footerReadBufferSize : FOOTER_READ_BUFFER_SIZE) {
        changeFooterConfigs(spiedFs, true,
            fileSize, footerReadBufferSize, readBufferSize);

        validatePartialReadWithSomeData(spiedFs, testFilePath,
            fileSize - AbfsInputStream.FOOTER_SIZE,
            AbfsInputStream.FOOTER_SIZE,
            fileContent, footerReadBufferSize, readBufferSize);
      }
    }
  }

  private void validatePartialReadWithSomeData(final FileSystem fs,
      final Path testFilePath, final int seekPos, final int length,
      final byte[] fileContent, final int footerReadBufferSize,
      final int readBufferSize) throws IOException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      verifyConfigValueInStream(iStream, Math.min(footerReadBufferSize, readBufferSize));
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
      abfsInputStreamTestUtils.seek(iStream, seekPos);

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
    try (AzureBlobFileSystem fs = createSpiedFs(config)){
      Path testFilePath = createPathAndFileWithContent(fs, 0, ONE_KB);
      final int footerReadBufferSizeConfig = 4 * ONE_KB;
      final int footerReadBufferSizeBuilder = 5 * ONE_KB;

      // Verify that default value is used if nothing is set explicitly
      FSDataInputStream iStream = fs.open(testFilePath);
      verifyConfigValueInStream(iStream, DEFAULT_FOOTER_READ_BUFFER_SIZE);

      // Verify that value set in config is used if builder is not used
      AbfsConfiguration spiedConfig = fs.getAbfsStore().getAbfsConfiguration();
      Mockito.doReturn(footerReadBufferSizeConfig).when(spiedConfig).getFooterReadBufferSize();
      iStream = fs.open(testFilePath);
      verifyConfigValueInStream(iStream, footerReadBufferSizeConfig);

      // Verify that when builder is used value set in parameters is used
      spiedConfig.unset(AZURE_FOOTER_READ_BUFFER_SIZE);
      FutureDataInputStreamBuilder builder = fs.openFile(testFilePath);
      builder.opt(AZURE_FOOTER_READ_BUFFER_SIZE,
          footerReadBufferSizeBuilder);
      iStream = builder.build().get();
      verifyConfigValueInStream(iStream, footerReadBufferSizeBuilder);

      // Verify that when builder is used value set in parameters is used
      // even if config is set
      Mockito.doReturn(footerReadBufferSizeConfig).when(spiedConfig).getFooterReadBufferSize();
      iStream = builder.build().get();
      verifyConfigValueInStream(iStream, footerReadBufferSizeBuilder);

      // Verify that when the builder is used and parameter in builder is not set,
      // the value set in configuration is used
      Mockito.doReturn(footerReadBufferSizeConfig).when(spiedConfig).getFooterReadBufferSize();
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
    byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
    return abfsInputStreamTestUtils.createFileWithContent(fs, fileName, fileContent);
  }

  private FutureDataInputStreamBuilder getParameterizedBuilder(final Path path,
      final AzureBlobFileSystem fs, int footerReadBufferSize) throws Exception {
    FutureDataInputStreamBuilder builder = fs.openFile(path);
    builder.opt(AZURE_FOOTER_READ_BUFFER_SIZE,
        footerReadBufferSize);
    return builder;
  }

  private void changeFooterConfigs(final AzureBlobFileSystem spiedFs,
      final boolean optimizeFooterRead, final int fileSize,
      final int readBufferSize) {
    AbfsConfiguration configuration = spiedFs.getAbfsStore()
        .getAbfsConfiguration();
    Mockito.doReturn(optimizeFooterRead)
        .when(configuration)
        .optimizeFooterRead();
    if (fileSize <= readBufferSize) {
      Mockito.doReturn(false).when(configuration).readSmallFilesCompletely();
    }
  }

  private AzureBlobFileSystem createSpiedFs(Configuration configuration)
      throws IOException {
    AzureBlobFileSystem spiedFs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(configuration));
    AzureBlobFileSystemStore store = Mockito.spy(spiedFs.getAbfsStore());
    Mockito.doReturn(store).when(spiedFs).getAbfsStore();
    AbfsConfiguration spiedConfig = Mockito.spy(store.getAbfsConfiguration());
    Mockito.doReturn(spiedConfig).when(store).getAbfsConfiguration();
    return spiedFs;
  }

  private void changeFooterConfigs(final AzureBlobFileSystem spiedFs,
      final boolean optimizeFooterRead, final int fileSize,
      final int footerReadBufferSize, final int readBufferSize) {
    AbfsConfiguration configuration = spiedFs.getAbfsStore()
        .getAbfsConfiguration();
    Mockito.doReturn(optimizeFooterRead)
        .when(configuration)
        .optimizeFooterRead();
    Mockito.doReturn(footerReadBufferSize)
        .when(configuration)
        .getFooterReadBufferSize();
    Mockito.doReturn(readBufferSize).when(configuration).getReadBufferSize();
    if (fileSize <= readBufferSize) {
      Mockito.doReturn(false).when(configuration).readSmallFilesCompletely();
    }
  }

  private enum SeekTo {
    BEGIN, AT_FOOTER_START, BEFORE_FOOTER_START, AFTER_FOOTER_START, END
  }
}
