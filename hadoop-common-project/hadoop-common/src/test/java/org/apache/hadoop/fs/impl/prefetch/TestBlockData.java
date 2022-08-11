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

package org.apache.hadoop.fs.impl.prefetch;

import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBlockData extends AbstractHadoopTestBase {

  @Test
  public void testArgChecks() throws Exception {
    // Should not throw.
    new BlockData(10, 5);
    new BlockData(5, 10);
    new BlockData(0, 10);

    // Verify it throws correctly.


    intercept(IllegalArgumentException.class, "'fileSize' must not be negative",
        () -> new BlockData(-1, 2));

    intercept(IllegalArgumentException.class,
        "'blockSize' must be a positive integer",
        () -> new BlockData(10, 0));

    intercept(IllegalArgumentException.class,
        "'blockSize' must be a positive integer",
        () -> new BlockData(10, -2));

    intercept(IllegalArgumentException.class,
        "'blockNumber' (-1) must be within the range [0, 3]",
        () -> new BlockData(10, 3).isLastBlock(
            -1));

    intercept(IllegalArgumentException.class,
        "'blockNumber' (11) must be within the range [0, 3]",
        () -> new BlockData(10, 3).isLastBlock(
            11));

  }

  @Test
  public void testComputedFields() throws Exception {
    testComputedFieldsHelper(0, 10);
    testComputedFieldsHelper(1, 10);
    testComputedFieldsHelper(10, 1);
    testComputedFieldsHelper(10, 2);
    testComputedFieldsHelper(10, 3);
  }

  private void testComputedFieldsHelper(long fileSize, int blockSize)
      throws Exception {
    BlockData bd = new BlockData(fileSize, blockSize);

    if (fileSize == 0) {
      assertFalse(bd.isLastBlock(0));
      assertFalse(bd.isLastBlock(1));
      assertFalse(bd.isValidOffset(0));
      assertEquals(0, bd.getSize(0));
      assertEquals("", bd.getStateString());

      ExceptionAsserts.assertThrows(
          IllegalArgumentException.class,
          "'offset' (0) must be within the range [0, -1]",
          () -> bd.getBlockNumber(0));

      ExceptionAsserts.assertThrows(
          IllegalArgumentException.class,
          "'blockNumber' (0) must be within the range [0, -1]",
          () -> bd.getStartOffset(0));

      ExceptionAsserts.assertThrows(
          IllegalArgumentException.class,
          "'offset' (0) must be within the range [0, -1]",
          () -> bd.getRelativeOffset(0, 0));

      ExceptionAsserts.assertThrows(
          IllegalArgumentException.class,
          "'blockNumber' (0) must be within the range [0, -1]",
          () -> bd.getState(0));

      ExceptionAsserts.assertThrows(
          IllegalArgumentException.class,
          "'blockNumber' (0) must be within the range [0, -1]",
          () -> bd.setState(0, BlockData.State.READY));

      return;
    }

    assertEquals(fileSize, bd.getFileSize());
    assertEquals(blockSize, bd.getBlockSize());

    int expectedNumBlocks = (int) (fileSize / blockSize);
    if (fileSize % blockSize > 0) {
      expectedNumBlocks++;
    }
    assertEquals(expectedNumBlocks, bd.getNumBlocks());

    int lastBlockNumber = expectedNumBlocks - 1;
    for (int b = 0; b < lastBlockNumber; b++) {
      assertFalse(bd.isLastBlock(b));
      assertEquals(blockSize, bd.getSize(b));
    }
    assertTrue(bd.isLastBlock(lastBlockNumber));
    int lastBlockSize = (int) (fileSize - blockSize * (expectedNumBlocks - 1));
    assertEquals(lastBlockSize, bd.getSize(lastBlockNumber));

    // Offset related methods.
    for (long offset = 0; offset < fileSize; offset++) {
      int expectedBlockNumber = (int) (offset / blockSize);
      assertEquals(expectedBlockNumber, bd.getBlockNumber(offset));

      for (int b = 0; b < expectedNumBlocks - 1; b++) {
        long expectedStartOffset = b * blockSize;
        assertEquals(expectedStartOffset, bd.getStartOffset(b));

        int expectedRelativeOffset = (int) (offset - expectedStartOffset);
        assertEquals(expectedRelativeOffset, bd.getRelativeOffset(b, offset));
      }
    }


    // State methods.
    for (int b = 0; b < expectedNumBlocks; b++) {
      assertEquals(b * blockSize, bd.getStartOffset(b));
      assertEquals(BlockData.State.NOT_READY, bd.getState(b));
      bd.setState(b, BlockData.State.QUEUED);
      assertEquals(BlockData.State.QUEUED, bd.getState(b));
      bd.setState(b, BlockData.State.READY);
      assertEquals(BlockData.State.READY, bd.getState(b));
      bd.setState(b, BlockData.State.CACHED);
      assertEquals(BlockData.State.CACHED, bd.getState(b));
    }
  }
}
