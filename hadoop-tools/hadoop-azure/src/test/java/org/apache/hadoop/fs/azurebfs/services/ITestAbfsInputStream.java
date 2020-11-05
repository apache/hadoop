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
  public void testSeekToBeginingAndRead() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        AbfsInputStream abfsInputStream = seekAndRead(iStream, 0, ONE_KB);
        assertBuffersAreEqual(fileContent, abfsInputStream.getBuffer());
      }
    }
  }

  @Test
  public void testSeekToEndAndRead() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        AbfsInputStream abfsInputStream = seekAndRead(iStream,
            fileSize - ONE_KB, ONE_KB);
        assertBuffersAreNotEqual(fileContent, abfsInputStream.getBuffer());
      }
    }
  }

  @Test
  public void testSeekToMiddleAndRead() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        AbfsInputStream abfsInputStream = seekAndRead(iStream, fileSize / 2,
            ONE_KB);
        assertBuffersAreNotEqual(fileContent, abfsInputStream.getBuffer());
      }
    }
  }

  private void assertBuffersAreEqual(byte[] actualContent, byte[] contentRead) {
    assertBufferEquality(actualContent, contentRead, true);
  }

  private void assertBuffersAreNotEqual(byte[] actualContent,
      byte[] contentRead) {
    assertBufferEquality(actualContent, contentRead, false);
  }

  private void assertBufferEquality(byte[] actualContent, byte[] contentRead,
      boolean assertEquals) {
    int matches = 0;
    for (int i = 0; i < actualContent.length; i++) {
      if (actualContent[i] == contentRead[i]) {
        matches++;
      }
    }
    if (assertEquals) {
      assertEquals(actualContent.length, matches);
    } else {
      assertNotEquals(actualContent.length, matches);
    }
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

  private AbfsInputStream seekAndRead(FSDataInputStream iStream, int seekPos,
      int length) throws IOException {
    iStream.seek(seekPos);
    iStream.read(new byte[length], 0, length);
    return (AbfsInputStream) iStream.getWrappedStream();
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }
}
