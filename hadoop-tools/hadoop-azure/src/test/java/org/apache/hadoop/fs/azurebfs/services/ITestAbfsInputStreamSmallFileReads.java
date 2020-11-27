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
import java.util.Random;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsInputStreamSmallFileReads extends AbstractAbfsIntegrationTest {

  public ITestAbfsInputStreamSmallFileReads() throws Exception {
  }

  @Test
  public void testOnlyOneServerCallIsMadeWhenTheCOnfIsTrue() throws Exception {
    testNumBackendCalls(true);
  }

  @Test
  public void testMultipleServerCallsAreMadeWhenTheCOnfIsFalse()
      throws Exception {
    testNumBackendCalls(false);
  }

  private void testNumBackendCalls(boolean readSmallFilesCompletely)
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem(readSmallFilesCompletely);
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        byte[] buffer = new byte[length];

        Map<String, Long> metricMap = fs.getInstrumentationMap();
        long requestsMadeBeforeTest = metricMap
            .get(CONNECTIONS_MADE.getStatName());

        iStream.seek(seekPos(SeekTo.END, fileSize, length));
        iStream.read(buffer, 0, length);

        iStream.seek(seekPos(SeekTo.MIDDLE, fileSize, length));
        iStream.read(buffer, 0, length);

        iStream.seek(seekPos(SeekTo.BEGIN, fileSize, length));
        iStream.read(buffer, 0, length);

        metricMap = fs.getInstrumentationMap();
        long requestsMadeAfterTest = metricMap
            .get(CONNECTIONS_MADE.getStatName());

        if (readSmallFilesCompletely) {
          assertEquals(1, requestsMadeAfterTest - requestsMadeBeforeTest);
        } else {
          assertEquals(3, requestsMadeAfterTest - requestsMadeBeforeTest);
        }
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

  private void testSeekAndReadWithConf(SeekTo seekTo, int startFileSizeInMB,
      int endFileSizeInMB, boolean readSmallFilesCompletely) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem(readSmallFilesCompletely);
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

  private AzureBlobFileSystem getFileSystem(boolean readSmallFilesCompletely)
      throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.getAbfsStore().getAbfsConfiguration()
        .setReadSmallFilesCompletely(readSmallFilesCompletely);
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

  private void seekReadAndTest(FileSystem fs, Path testFilePath, int seekPos,
      int length, byte[] fileContent) throws IOException {
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      iStream.seek(seekPos);
      byte[] buffer = new byte[length];
      iStream.read(buffer, 0, length);
      assertContentReadCorrectly(fileContent, seekPos, length, buffer);
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();

      AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
      AbfsConfiguration conf = abfs.getAbfsStore().getAbfsConfiguration();
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
      assertEquals(expectedBCursor, abfsInputStream.getBCursor());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
    }
  }

  private void assertContentReadCorrectly(byte[] actualFileContent, int from,
      int len, byte[] contentRead) {
    for (int i = 0; i < len; i++) {
      assertEquals(contentRead[i], actualFileContent[i + from]);
    }
  }

  private void assertBuffersAreNotEqual(byte[] actualContent,
      byte[] contentRead, AbfsConfiguration conf) {
    assertBufferEquality(actualContent, contentRead, conf, false);
  }

  private void assertBuffersAreEqual(byte[] actualContent, byte[] contentRead,
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

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  private enum SeekTo {BEGIN, MIDDLE, END}
}
