/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.read;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.BufferData;
import org.apache.hadoop.fs.common.ExceptionAsserts;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.junit.Assert.assertEquals;

public class TestS3BlockManager extends AbstractHadoopTestBase {

  static final int FILE_SIZE = 12;
  static final int BLOCK_SIZE = 3;

  @Test
  public void testArgChecks() throws Exception {
    BlockData blockData = new BlockData(FILE_SIZE, BLOCK_SIZE);
    MockS3File s3File = new MockS3File(FILE_SIZE, false);
    S3Reader reader = new S3Reader(s3File);

    // Should not throw.
    new S3BlockManager(reader, blockData);

    // Verify it throws correctly.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'reader' must not be null",
        () -> new S3BlockManager(null, blockData));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockData' must not be null",
        () -> new S3BlockManager(reader, null));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> new S3BlockManager(reader, blockData).get(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'data' must not be null",
        () -> new S3BlockManager(reader, blockData).release(null));
  }

  @Test
  public void testGet() throws IOException {
    BlockData blockData = new BlockData(FILE_SIZE, BLOCK_SIZE);
    MockS3File s3File = new MockS3File(FILE_SIZE, false);
    S3Reader reader = new S3Reader(s3File);
    S3BlockManager blockManager = new S3BlockManager(reader, blockData);

    for (int b = 0; b < blockData.getNumBlocks(); b++) {
      BufferData data = blockManager.get(b);
      ByteBuffer buffer = data.getBuffer();
      long startOffset = blockData.getStartOffset(b);
      for (int i = 0; i < BLOCK_SIZE; i++) {
        assertEquals(startOffset + i, buffer.get());
      }
    }
  }
}
