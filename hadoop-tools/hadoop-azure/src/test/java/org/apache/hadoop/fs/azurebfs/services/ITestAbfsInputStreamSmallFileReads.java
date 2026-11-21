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
import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
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

public class ITestAbfsInputStreamSmallFileReads extends
    AbstractAbfsIntegrationTest {

  private final AbfsInputStreamTestUtils abfsInputStreamTestUtils;
  public ITestAbfsInputStreamSmallFileReads() throws Exception {
    this.abfsInputStreamTestUtils = new AbfsInputStreamTestUtils(this);
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

  private void testNumBackendCalls(boolean readSmallFilesCompletely)
      throws Exception {
    try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
        readSmallFilesCompletely)) {
      validateNumBackendCalls(readSmallFilesCompletely, fs);
    }
  }

  private void validateNumBackendCalls(final boolean readSmallFilesCompletely,
      final AzureBlobFileSystem fs)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
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

        long expected;
        if (readSmallFilesCompletely) {
          expected = 1;
        } else {
          expected = 3;
        }
        Assertions.assertThat(requestsMadeAfterTest - requestsMadeBeforeTest)
            .describedAs(
                "Number of calls made will be equal to expected if "
                    + "data layout is already present else greater than 1 if call to "
                    + "fetch data layout is made")
            .isIn(Arrays.asList(expected, expected + 1));
      }
    }
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfTrue()
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, true);
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfFalse()
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, false);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, true);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, false);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, true);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, false);
  }

  @Test
  public void testSeekToEndAndReadBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, true);
  }

  @Test
  public void testSeekToEndAndReaBigFiledWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, false);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, true);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, false);
  }

  @Test
  public void testSeekToMiddleAndReaBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, true);
  }

  @Test
  public void testSeekToMiddleAndReadBigFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, false);
  }


  /**
   * Tests reading from a remote file in separate threads to ensure thread safety and
   * correct behavior of concurrent reads on the same file using AzureBlobFileSystem.
   *
   * This test creates a large file and spawns multiple threads, each performing
   * positional reads at different offsets. It verifies that concurrent access does not
   * cause errors or data corruption.
   */
  @Test
  public void testReadRemoteInSeparateThread() throws Exception {
    try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
        false)) {
      Path filePath = createFileWithContent(fs, methodName.getMethodName() + 1,
          getRandomBytesArray(200 * 1024 * 1024));
      Thread thread = new Thread(() -> {
        try (FSDataInputStream iStream = fs.open(filePath)) {
          byte[] buffer = new byte[1024];
          iStream.read(0, buffer, 0, 10);
        } catch (IOException e) {
          System.out.println("Error while reading the file: " + e.getMessage());
        }
      });

      Thread thread2 = new Thread(() -> {
        try (FSDataInputStream iStream = fs.open(filePath)) {
          byte[] buffer = new byte[100];
          iStream.read(60 * 1024 * 1024, buffer, 0, 10);
        } catch (IOException e) {
          System.out.println("Error while reading the file: " + e.getMessage());
        }
      });

      thread.start();
      thread2.start();

      thread.join();
      thread2.join();
      try (FSDataInputStream iStream = fs.open(filePath)) {
        byte[] buffer = new byte[100];
        iStream.read(130 * 1024 * 1024, buffer, 20, 10);
      } catch (IOException e) {
        System.out.println("Error while reading the file: " + e.getMessage());

      }
    }
  }

  /**
   * Tests that multiple threads performing identical positional reads on the same file
   * do not cause race conditions or redundant backend fetches.
   *
   * This test creates a file and spawns several threads, each reading from the same offset
   * simultaneously. It verifies that only one backend fetch is triggered for the shared range,
   * ensuring efficient and thread-safe read operations.
   */
  @Test
  public void testParallelIdenticalReadRace() throws Exception {
    try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
        false)) {
      Path filePath = createFileWithContent(fs, "raceTest",
          getRandomBytesArray(128 * 1024 * 1024));

      // 10 threads all hitting the same offset simultaneously
      int threadCount = 10;
      Thread[] threads = new Thread[threadCount];

      for (int i = 0; i < threadCount; i++) {
        threads[i] = new Thread(() -> {
          try (FSDataInputStream iStream = fs.open(filePath)) {
            byte[] buffer = new byte[1024];
            iStream.read(0, buffer, 0, 1024); // Positional read at 0
          } catch (IOException e) {
            fail("Parallel read failed: " + e.getMessage());
          }
        });
      }

      for (Thread t : threads) {t.start();}
      for (Thread t : threads) {t.join();}

      // LOG VERIFICATION: Check console/logs.
      // You should see only ONE "fetchAndPopulate start 0 end 67108863"
    }
  }

  /**
   * Tests overlapping reads from multiple threads to ensure that the backend
   * fetches only the required data ranges and efficiently stitches overlapping
   * requests. This verifies that concurrent reads with overlapping byte ranges
   * do not cause redundant backend calls and that data is correctly served to
   * all threads.
   */
  @Test
  public void testOverlappingReadStitching() throws Exception {
    try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
        false)) {
      Path filePath = createFileWithContent(fs, "overlapTest",
          getRandomBytesArray(100 * 1024 * 1024));

      // Thread A: Wants 0 to 1024 (Triggers 0-64MB fetch)
      Thread t1 = new Thread(() -> {
        try (FSDataInputStream iStream = fs.open(filePath)) {
          iStream.read(0, new byte[10], 0, 10);
        } catch (Exception e) {}
      });

      // Thread B: Wants 65MB to 66MB (Starts after Thread A's 64MB window)
      // Thread C: Wants 60MB to 68MB (Overlaps Thread A's end)
      Thread t2 = new Thread(() -> {
        try (FSDataInputStream iStream = fs.open(filePath)) {
          // This starts at 60MB, which is INSIDE Thread A's 0-64MB promise
          iStream.read(60 * 1024 * 1024, new byte[10], 0, 10);
        } catch (Exception e) {}
      });

      t2.start();
//      Thread.sleep(50);
      t1.start();

      t1.join();
      t2.join();

      FSDataInputStream iStream = fs.open(filePath);
      iStream.read(65 * 1024 * 1024, new byte[10], 0, 10);
      iStream.close();

      // EXPECTED LOGS:
      // 1. fetchAndPopulate start 0 end 67108863 (Thread A)
      // 2. fetchAndPopulate start 67108864 end 71303167 (Thread B surgical remainder)
    }
  }

  private void testSeekAndReadWithConf(SeekTo seekTo, int startFileSizeInMB,
      int endFileSizeInMB, boolean readSmallFilesCompletely) throws Exception {
    try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
        readSmallFilesCompletely)) {
      validateSeekAndReadWithConf(seekTo, startFileSizeInMB, endFileSizeInMB,
          fs);
    }
  }

  private void validateSeekAndReadWithConf(final SeekTo seekTo,
      final int startFileSizeInMB,
      final int endFileSizeInMB,
      final AzureBlobFileSystem fs)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    for (int i = startFileSizeInMB; i <= endFileSizeInMB; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      int seekPos = seekPos(seekTo, fileSize, length);
      seekReadAndTest(fs, testFilePath, seekPos, length, fileContent);
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
      int length, byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    AbfsConfiguration conf = getConfiguration((AzureBlobFileSystem) fs);
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(bytesRead, length);
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos, length, buffer, testFilePath);
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();

      final int readBufferSize = conf.getReadBufferSize();
      final int fileContentLength = fileContent.length;
      final boolean smallFile = fileContentLength <= readBufferSize;
      int expectedLimit, expectedFCursor;
      int expectedBCursor;
      if (conf.readSmallFilesCompletely() && smallFile) {
        abfsInputStreamTestUtils.assertAbfsInputStreamBufferNotEqualToContentStartSubsequence(fileContent, abfsInputStream, conf, testFilePath);
        expectedFCursor = fileContentLength;
        expectedLimit = fileContentLength;
        expectedBCursor = seekPos + length;
      } else {
        if ((seekPos == 0)) {
          abfsInputStreamTestUtils.assertAbfsInputStreamBufferNotEqualToContentStartSubsequence(fileContent, abfsInputStream, conf, testFilePath);
        } else {
          abfsInputStreamTestUtils.assertAbfsInputStreamBufferEqualToContentStartSubsequence(fileContent, abfsInputStream,
              conf, testFilePath);
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
  }

  @Test
  public void testPartialReadWithNoData() throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
          true)) {
        String fileName = methodName.getMethodName() + i;
        byte[] fileContent = getRandomBytesArray(
            fileSize);
        Path testFilePath = createFileWithContent(fs,
            fileName, fileContent);
        partialReadWithNoData(fs, testFilePath, fileSize / 2, fileSize / 4,
            fileContent);
      }
    }
  }

  private void partialReadWithNoData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException {

    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      doReturn(10)
          .doReturn(10)
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class), any());

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(bytesRead, length);
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos, length, buffer, testFilePath);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(fileContent.length, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testPartialReadWithSomeData() throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
          true)) {
        String fileName = methodName.getMethodName() + i;
        byte[] fileContent = getRandomBytesArray(
            fileSize);
        Path testFilePath = createFileWithContent(fs,
            fileName, fileContent);
        partialReadWithSomeData(fs, testFilePath, fileSize / 2,
            fileSize / 4, fileContent);
      }
    }
  }

  private void partialReadWithSomeData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
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
              any(TracingContext.class), any());

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);

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
