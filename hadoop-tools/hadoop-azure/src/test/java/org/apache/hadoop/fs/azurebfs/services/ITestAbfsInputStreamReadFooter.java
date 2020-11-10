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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsInputStreamReadFooter
    extends AbstractAbfsIntegrationTest {

  public ITestAbfsInputStreamReadFooter() throws Exception {
  }

  @Test
  public void testSeekToEndAndReadWithConf() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = 1; i <= 10; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      seekReadAndTest(fs, testFilePath, fileSize - 9, 8, fileContent);
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

  private void seekReadAndTest(FileSystem fs, Path testFilePath, int seekPos,
      int length, byte[] fileContent) throws IOException {
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      iStream.seek(seekPos);
      byte[] buffer = new byte[length];
      iStream.read(buffer, 0, length);
      assertSuccessfulRead(fileContent, seekPos, length, buffer);
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();

      AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
      AbfsConfiguration conf = abfs.getAbfsStore().getAbfsConfiguration();

      int expectedFCursor = fileContent.length;
      int expectedLimit = (conf.getReadBufferSize() < fileContent.length) ?
          conf.getReadBufferSize() :
          fileContent.length;
      int expectedBCursor = ((conf.getReadBufferSize() < fileContent.length) ?
          conf.getReadBufferSize() :
          fileContent.length) - 1;

      assertSuccessfulRead(fileContent, abfsInputStream.getBuffer(),
          conf);
      assertEquals(expectedFCursor, abfsInputStream.getFCursor());
      assertEquals(expectedBCursor, abfsInputStream.getBCursor());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
    }
  }

  private void assertSuccessfulRead(byte[] actualFileContent,
      byte[] contentRead, AbfsConfiguration conf) {
    int buffersize = conf.getReadBufferSize();
    int actualContentSize = actualFileContent.length;
    int len = (actualContentSize < buffersize) ? actualContentSize : buffersize;
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
}
