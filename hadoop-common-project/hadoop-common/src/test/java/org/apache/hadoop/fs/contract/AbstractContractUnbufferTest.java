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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

/**
 * Contract tests for {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer}.
 */
public abstract class AbstractContractUnbufferTest extends AbstractFSContractTestBase {

  private Path file;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_UNBUFFER);
    file = path("unbufferFile");
    createFile(getFileSystem(), file, true,
            dataset(TEST_FILE_LEN, 0, 255));
  }

  @Test
  public void testUnbufferAfterRead() throws IOException {
    describe("unbuffer a file after a single read");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      assertEquals(128, stream.read(new byte[128]));
      unbuffer(stream);
    }
  }

  @Test
  public void testUnbufferBeforeRead() throws IOException {
    describe("unbuffer a file before a read");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      unbuffer(stream);
      assertEquals(128, stream.read(new byte[128]));
    }
  }

  @Test
  public void testUnbufferEmptyFile() throws IOException {
    Path emptyFile = path("emptyUnbufferFile");
    createFile(getFileSystem(), emptyFile, true,
            dataset(TEST_FILE_LEN, 0, 255));
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
      assertEquals(128, stream.read(new byte[128]));
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
    unbuffer(stream);
  }

  @Test
  public void testMultipleUnbuffers() throws IOException {
    describe("unbuffer a file multiple times");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      unbuffer(stream);
      unbuffer(stream);
      assertEquals(128, stream.read(new byte[128]));
      unbuffer(stream);
      unbuffer(stream);
    }
  }

   @Test
  public void testUnbufferMultipleReads() throws IOException {
    describe("unbuffer a file multiple times");
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      unbuffer(stream);
      assertEquals(128, stream.read(new byte[128]));
      unbuffer(stream);
      assertEquals(128, stream.read(new byte[128]));
      assertEquals(128, stream.read(new byte[128]));
      unbuffer(stream);
      assertEquals(128, stream.read(new byte[128]));
      assertEquals(128, stream.read(new byte[128]));
      assertEquals(128, stream.read(new byte[128]));
      unbuffer(stream);
    }
  }

  private void unbuffer(FSDataInputStream stream) throws IOException {
    long pos = stream.getPos();
    stream.unbuffer();
    assertEquals(pos, stream.getPos());
  }
}
