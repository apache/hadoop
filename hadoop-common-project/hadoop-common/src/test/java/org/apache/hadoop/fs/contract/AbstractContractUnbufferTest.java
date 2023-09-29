/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

/**
 * Contract tests for {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer}.
 */
public abstract class AbstractContractUnbufferTest extends AbstractFSContractTestBase {

  private Path file;
  private byte[] fileBytes;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_UNBUFFER);
    file = path("unbufferFile");
    fileBytes = dataset(TEST_FILE_LEN, 0, 255);
    createFile(getFileSystem(), file, true, fileBytes);
  }

  @Test
  public void testUnbufferAfterRead() throws IOException {
    describe("unbuffer a file after a single read");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      validateFullFileContents(stream);
      unbuffer(stream);
    }
  }

  @Test
  public void testUnbufferBeforeRead() throws IOException {
    describe("unbuffer a file before a read");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      unbuffer(stream);
      validateFullFileContents(stream);
    }
  }

  @Test
  public void testUnbufferEmptyFile() throws IOException {
    Path emptyFile = path("emptyUnbufferFile");
    getFileSystem().create(emptyFile, true).close();
    describe("unbuffer an empty file");
    try (FSDataInputStream stream = getFileSystem().open(emptyFile)) {
      unbuffer(stream);
    }
  }

  @Test
  public void testUnbufferOnClosedFile() throws IOException {
    describe("unbuffer a file before a read");
    FSDataInputStream stream = null;
    try {
      stream = getFileSystem().open(file);
      validateFullFileContents(stream);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
    if (stream != null) {
      unbuffer(stream);
    }
  }

  @Test
  public void testMultipleUnbuffers() throws IOException {
    describe("unbuffer a file multiple times");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      unbuffer(stream);
      unbuffer(stream);
      validateFullFileContents(stream);
      unbuffer(stream);
      unbuffer(stream);
    }
  }

  @Test
  public void testUnbufferMultipleReads() throws IOException {
    describe("unbuffer a file multiple times");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      unbuffer(stream);
      validateFileContents(stream, TEST_FILE_LEN / 8, 0);
      unbuffer(stream);
      validateFileContents(stream, TEST_FILE_LEN / 8, TEST_FILE_LEN / 8);
      validateFileContents(stream, TEST_FILE_LEN / 4, TEST_FILE_LEN / 4);
      unbuffer(stream);
      validateFileContents(stream, TEST_FILE_LEN / 2, TEST_FILE_LEN / 2);
      unbuffer(stream);
      assertEquals("stream should be at end of file", TEST_FILE_LEN,
              stream.getPos());
    }
  }

  private void unbuffer(FSDataInputStream stream) throws IOException {
    long pos = stream.getPos();
    stream.unbuffer();
    assertEquals("unbuffer unexpectedly changed the stream position", pos,
            stream.getPos());
  }

  protected void validateFullFileContents(FSDataInputStream stream)
          throws IOException {
    validateFileContents(stream, TEST_FILE_LEN, 0);
  }

  protected void validateFileContents(FSDataInputStream stream, int length,
                                      int startIndex)
          throws IOException {
    byte[] streamData = new byte[length];
    assertEquals("failed to read expected number of bytes from "
            + "stream. This may be transient",
        length, stream.read(streamData));
    byte[] validateFileBytes;
    if (startIndex == 0 && length == fileBytes.length) {
      validateFileBytes = fileBytes;
    } else {
      validateFileBytes = Arrays.copyOfRange(fileBytes, startIndex,
              startIndex + length);
    }
    assertArrayEquals("invalid file contents", validateFileBytes, streamData);
  }

  protected Path getFile() {
    return file;
  }
}
