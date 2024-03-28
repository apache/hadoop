/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

import static org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest.SHORTENED_GUID_LEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AbfsInputStreamTestUtils {

  public static final int HUNDRED = 100;

  private final AbstractAbfsIntegrationTest abstractAbfsIntegrationTest;

  public AbfsInputStreamTestUtils(AbstractAbfsIntegrationTest abstractAbfsIntegrationTest) {
    this.abstractAbfsIntegrationTest = abstractAbfsIntegrationTest;
  }

  private Path path(String filepath) throws IOException {
    return abstractAbfsIntegrationTest.getFileSystem().makeQualified(
        new Path(getTestPath(), getUniquePath(filepath)));
  }

  private Path getTestPath() {
    Path path = new Path(UriUtils.generateUniqueTestPath());
    return path;
  }

  /**
   * Generate a unique path using the given filepath.
   * @param filepath path string
   * @return unique path created from filepath and a GUID
   */
  private Path getUniquePath(String filepath) {
    if (filepath.equals("/")) {
      return new Path(filepath);
    }
    return new Path(filepath + StringUtils
        .right(UUID.randomUUID().toString(), SHORTENED_GUID_LEN));
  }

  public AzureBlobFileSystem getFileSystem(boolean readSmallFilesCompletely)
      throws IOException {
    final AzureBlobFileSystem fs = abstractAbfsIntegrationTest.getFileSystem();
    abstractAbfsIntegrationTest.getAbfsStore(fs).getAbfsConfiguration()
        .setReadSmallFilesCompletely(readSmallFilesCompletely);
    abstractAbfsIntegrationTest.getAbfsStore(fs).getAbfsConfiguration()
        .setOptimizeFooterRead(false);
    abstractAbfsIntegrationTest.getAbfsStore(fs).getAbfsConfiguration()
        .setIsChecksumValidationEnabled(true);
    return fs;
  }

  public byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  public Path createFileWithContent(FileSystem fs, String fileName,
      byte[] fileContent) throws IOException {
    Path testFilePath = path(fileName);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testFilePath;
  }

  public AzureBlobFileSystemStore getAbfsStore(FileSystem fs)
      throws NoSuchFieldException, IllegalAccessException {
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
    Field abfsStoreField = AzureBlobFileSystem.class
        .getDeclaredField("abfsStore");
    abfsStoreField.setAccessible(true);
    return (AzureBlobFileSystemStore) abfsStoreField.get(abfs);
  }

  public Map<String, Long> getInstrumentationMap(FileSystem fs)
      throws NoSuchFieldException, IllegalAccessException {
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
    Field abfsCountersField = AzureBlobFileSystem.class
        .getDeclaredField("abfsCounters");
    abfsCountersField.setAccessible(true);
    AbfsCounters abfsCounters = (AbfsCounters) abfsCountersField.get(abfs);
    return abfsCounters.toMap();
  }

  public void assertContentReadCorrectly(byte[] actualFileContent, int from,
      int len, byte[] contentRead, Path testFilePath) {
    for (int i = 0; i < len; i++) {
      assertEquals("The test file path is " + testFilePath, contentRead[i],
          actualFileContent[i + from]);
    }
  }

  public void assertBuffersAreNotEqual(byte[] actualContent,
      byte[] contentRead, AbfsConfiguration conf, Path testFilePath) {
    assertBufferEquality(actualContent, contentRead, conf, false, testFilePath);
  }

  public void assertBuffersAreEqual(byte[] actualContent, byte[] contentRead,
      AbfsConfiguration conf, Path testFilePath) {
    assertBufferEquality(actualContent, contentRead, conf, true, testFilePath);
  }

  private void assertBufferEquality(byte[] actualContent, byte[] contentRead,
      AbfsConfiguration conf, boolean assertEqual, Path testFilePath) {
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
      assertEquals("The test file path is " + testFilePath, n, matches);
    } else {
      assertNotEquals("The test file path is " + testFilePath, n, matches);
    }
  }

  public void seek(FSDataInputStream iStream, long seekPos)
      throws IOException {
    AbfsInputStream abfsInputStream
        = (AbfsInputStream) iStream.getWrappedStream();
    verifyBeforeSeek(abfsInputStream);
    iStream.seek(seekPos);
    verifyAfterSeek(abfsInputStream, seekPos);
  }

  public void verifyBeforeSeek(AbfsInputStream abfsInputStream) {
    assertEquals(0, abfsInputStream.getFCursor());
    assertEquals(-1, abfsInputStream.getFCursorAfterLastRead());
    assertEquals(0, abfsInputStream.getLimit());
    assertEquals(0, abfsInputStream.getBCursor());
  }

  public void verifyAfterSeek(AbfsInputStream abfsInputStream, long seekPos)
      throws IOException {
    assertEquals(seekPos, abfsInputStream.getPos());
    assertEquals(-1, abfsInputStream.getFCursorAfterLastRead());
    assertEquals(0, abfsInputStream.getLimit());
    assertEquals(0, abfsInputStream.getBCursor());
  }
}
