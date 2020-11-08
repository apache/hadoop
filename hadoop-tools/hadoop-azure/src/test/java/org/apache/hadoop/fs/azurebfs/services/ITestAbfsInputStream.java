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
import java.util.Random;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {

  public ITestAbfsInputStream() throws Exception {
  }

  @Test
  public void testSeekToBeginingAndReadWithConfTrue() throws Exception {
    testSeekToBeginingAndReadWithConf(true);
  }

  @Test
  public void testSeekToBeginingAndReadWithConfFalse() throws Exception {
    testSeekToBeginingAndReadWithConf(false);
  }

  private void testSeekToBeginingAndReadWithConf(
      boolean readSmallFilesCompletely) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem(readSmallFilesCompletely);
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      seekReadAndTest(fs, testFilePath, 0, ONE_KB, fileContent);
    }
  }

  @Test
  public void testSeekToEndAndReadWithConfTrue() throws Exception {
    testSeekToEndAndReadWithConf(true);
  }

  @Test
  public void testSeekToEndAndReadWithConfFalse() throws Exception {
    testSeekToEndAndReadWithConf(false);
  }

  private void testSeekToEndAndReadWithConf(boolean readSmallFilesCompletely)
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem(readSmallFilesCompletely);
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      seekReadAndTest(fs, testFilePath, fileSize - ONE_KB, ONE_KB, fileContent);
    }
  }

  @Test
  public void testSeekToMiddleAndReadWithConfTrue() throws Exception {
    testSeekToMiddleAndReadWithConf(true);
  }

  @Test
  public void testSeekToMiddleAndReadWithConfFalse() throws Exception {
    testSeekToMiddleAndReadWithConf(false);
  }

  private void testSeekToMiddleAndReadWithConf(boolean readSmallFilesCompletely)
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem(readSmallFilesCompletely);
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      seekReadAndTest(fs, testFilePath, fileSize / 2, ONE_KB, fileContent);
    }
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

      int expectedFCursor = fileContent.length;
      int expectedLimit, expectedBCursor;
      if (conf.readSmallFilesCompletely()) {
        assertBuffersAreEqual(fileContent, abfsInputStream.getBuffer());
        expectedBCursor = seekPos + length;
        expectedLimit = fileContent.length;
      } else {
        expectedBCursor = length;
        if ((seekPos == 0)) {
          assertBuffersAreEqual(fileContent, abfsInputStream.getBuffer());
          expectedLimit = fileContent.length;
          // Buffer containf bytes from 0 to EOF
        } else {
          assertBuffersAreNotEqual(fileContent, abfsInputStream.getBuffer());
          expectedLimit = fileContent.length - seekPos;
          // Buffer containf bytes from seekPos to EOF
        }
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
      byte[] contentRead) {
    int matches = 0;
    for (int i = 0; i < actualContent.length; i++) {
      if (actualContent[i] == contentRead[i]) {
        matches++;
      }
    }
    assertNotEquals(actualContent.length, matches);
  }

  private void assertBuffersAreEqual(byte[] actualContent, byte[] contentRead) {
    int matches = 0;
    for (int i = 0; i < actualContent.length; i++) {
      if (actualContent[i] == contentRead[i]) {
        matches++;
      }
    }
    assertEquals(actualContent.length, matches);
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }
}
