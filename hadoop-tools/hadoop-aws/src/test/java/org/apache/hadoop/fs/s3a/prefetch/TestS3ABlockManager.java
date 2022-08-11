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

package org.apache.hadoop.fs.s3a.prefetch;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.hadoop.fs.impl.prefetch.BlockData;
import org.apache.hadoop.fs.impl.prefetch.BufferData;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;

public class TestS3ABlockManager extends AbstractHadoopTestBase {

  static final int FILE_SIZE = 12;

  static final int BLOCK_SIZE = 3;

  @Test
  public void testArgChecks() throws Exception {
    BlockData blockData = new BlockData(FILE_SIZE, BLOCK_SIZE);
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File);

    // Should not throw.
    new S3ABlockManager(reader, blockData);

    // Verify it throws correctly.
    intercept(
        IllegalArgumentException.class,
        "'reader' must not be null",
        () -> new S3ABlockManager(null, blockData));

    intercept(
        IllegalArgumentException.class,
        "'blockData' must not be null",
        () -> new S3ABlockManager(reader, null));

    intercept(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> new S3ABlockManager(reader, blockData).get(-1));

    intercept(
        IllegalArgumentException.class,
        "'data' must not be null",
        () -> new S3ABlockManager(reader, blockData).release(null));
  }

  @Test
  public void testGet() throws IOException {
    BlockData blockData = new BlockData(FILE_SIZE, BLOCK_SIZE);
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File);
    S3ABlockManager blockManager = new S3ABlockManager(reader, blockData);

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
