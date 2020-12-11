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

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.mockito.Mockito;

import static java.lang.Math.min;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
      final AzureBlobFileSystem fs = getFileSystem(optimizeFooterRead, fileSize);
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
  public void testSeekToEndAndReadWithConfTrue() throws Exception {
    testSeekToEndAndReadWithConf(true);
  }

  @Test
  public void testSeekToEndAndReadWithConfFalse() throws Exception {
    testSeekToEndAndReadWithConf(false);
  }

  private void testSeekToEndAndReadWithConf(boolean optimizeFooterRead) throws Exception {
    for (int i = 2; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(optimizeFooterRead, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      seekReadAndTest(fs, testFilePath, fileSize - AbfsInputStream.FOOTER_SIZE,
          AbfsInputStream.FOOTER_SIZE, fileContent);
    }
  }

  private AzureBlobFileSystem getFileSystem(boolean optimizeFooterRead,
      int fileSze)
      throws IOException {
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
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      iStream.seek(seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      int expectedBytesRead = min(fileContent.length - seekPos,
          conf.getReadBufferSize());
      assertEquals(expectedBytesRead, bytesRead);
      assertSuccessfulRead(fileContent, seekPos, length, buffer);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertSuccessfulRead(fileContent, abfsInputStream.getBuffer(),
          conf, length);
      int expectedLimit = getExpectedLimit(conf, length, fileContent);
      int expectedBCursor = getEexpectedBCursor(conf, length, fileContent);
      assertEquals(expectedBCursor, abfsInputStream.getBCursor());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
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

  private AbfsClient getAbfsClient(AbfsInputStream abfsInputStream)
      throws NoSuchFieldException, IllegalAccessException {
    Field abfsClientField = AbfsInputStream.class.getDeclaredField("client");
    abfsClientField.setAccessible(true);
    return (AbfsClient) abfsClientField.get(abfsInputStream);
  }

  private int getEexpectedBCursor(final AbfsConfiguration conf,
      final int length, final byte[] fileContent) {
    int actualContentLength = fileContent.length;
    int bufferSize = conf.getReadBufferSize();
    if (conf.optimizeFooterRead()) {
      if (bufferSize < actualContentLength) {
        return bufferSize;
      }
      return actualContentLength;
    }
    return length;
  }

  private int getExpectedLimit(final AbfsConfiguration conf, final int length,
      final byte[] fileContent) {
    int actualContentLength = fileContent.length;
    int bufferSize = conf.getReadBufferSize();
    if (conf.optimizeFooterRead()) {
      if (bufferSize < actualContentLength) {
        return bufferSize;
      }
      return actualContentLength;
    }
    return length;
  }

  private void assertSuccessfulRead(byte[] actualFileContent,
      byte[] contentRead, AbfsConfiguration conf, int len) {
    int buffersize = conf.getReadBufferSize();
    int actualContentSize = actualFileContent.length;
    if (conf.optimizeFooterRead()) {
    len = (actualContentSize < buffersize)
        ? actualContentSize
        : buffersize;
    }
    assertSuccessfulRead(actualFileContent, actualContentSize - len, len,
        contentRead);
  }

  private void assertSuccessfulRead(byte[] actualFileContent, int from,
      int len, byte[] contentRead) {
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
  public void testPartialReadWithNoDataSeekToEndAndReadWithConf() throws Exception {
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
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      doReturn(10)
          .doReturn(10)
          .doCallRealMethod()
          .when(abfsInputStream)
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
  public void testPartialReadWithSomeDataSeekToEndAndReadWithConf() throws Exception {
    for (int i = 3; i <= 6; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      partialReadWithSomeDataSeekReadAndTest(fs, testFilePath,
          fileSize - AbfsInputStream.FOOTER_SIZE,
          AbfsInputStream.FOOTER_SIZE, fileContent);
    }
  }

  private void partialReadWithSomeDataSeekReadAndTest(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      //  first readRemote, will return first 10 bytes
      //  second readRemote returns data till the last 2 bytes
      int someDataLength = 2;
      int secondReturnSize = min(fileContent.length,
          abfsInputStream.getBufferSize()) - 10 - someDataLength;
      doReturn(10)
          .doReturn(secondReturnSize)
          .doCallRealMethod()
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
}
