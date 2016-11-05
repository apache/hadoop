/**
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

package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Test of the utility of raw erasure coder.
 */
public class TestCoderUtil {
  private final int numInputs = 9;
  private final int chunkSize = 1024;

  @Test
  public void testGetEmptyChunk() {
    byte[] ret = CoderUtil.getEmptyChunk(chunkSize);
    for (int i = 0; i < chunkSize; i++) {
      assertEquals(0, ret[i]);
    }
  }

  @Test
  public void testResetBuffer() {
    ByteBuffer buf = ByteBuffer.allocate(chunkSize * 2).putInt(1234);
    buf.position(0);
    ByteBuffer ret = CoderUtil.resetBuffer(buf, chunkSize);
    for (int i = 0; i < chunkSize; i++) {
      assertEquals(0, ret.getInt(i));
    }

    byte[] inputs = ByteBuffer.allocate(numInputs)
        .putInt(1234).array();
    CoderUtil.resetBuffer(inputs, 0, numInputs);
    for (int i = 0; i < numInputs; i++) {
      assertEquals(0, inputs[i]);
    }
  }

  @Test
  public void testGetValidIndexes() {
    byte[][] inputs = new byte[numInputs][];
    inputs[0] = new byte[chunkSize];
    inputs[1] = new byte[chunkSize];
    inputs[7] = new byte[chunkSize];
    inputs[8] = new byte[chunkSize];

    int[] validIndexes = CoderUtil.getValidIndexes(inputs);

    assertEquals(4, validIndexes.length);

    // Check valid indexes
    assertEquals(0, validIndexes[0]);
    assertEquals(1, validIndexes[1]);
    assertEquals(7, validIndexes[2]);
    assertEquals(8, validIndexes[3]);
  }

  @Test
  public void testNoValidIndexes() {
    byte[][] inputs = new byte[numInputs][];
    for (int i = 0; i < numInputs; i++) {
      inputs[i] = null;
    }

    int[] validIndexes = CoderUtil.getValidIndexes(inputs);

    assertEquals(0, validIndexes.length);
  }

  @Test
  public void testGetNullIndexes() {
    byte[][] inputs = new byte[numInputs][];
    inputs[0] = new byte[chunkSize];
    inputs[1] = new byte[chunkSize];
    for (int i = 2; i < 7; i++) {
      inputs[i] = null;
    }
    inputs[7] = new byte[chunkSize];
    inputs[8] = new byte[chunkSize];

    int[] nullIndexes = CoderUtil.getNullIndexes(inputs);
    assertEquals(2, nullIndexes[0]);
    assertEquals(3, nullIndexes[1]);
    assertEquals(4, nullIndexes[2]);
    assertEquals(5, nullIndexes[3]);
    assertEquals(6, nullIndexes[4]);
  }

  @Test
  public void testFindFirstValidInput() {
    byte[][] inputs = new byte[numInputs][];
    inputs[8] = ByteBuffer.allocate(4).putInt(1234).array();

    byte[] firstValidInput = CoderUtil.findFirstValidInput(inputs);
    assertEquals(firstValidInput, inputs[8]);
  }

  @Test(expected = HadoopIllegalArgumentException.class)
  public void testNoValidInput() {
    byte[][] inputs = new byte[numInputs][];
    CoderUtil.findFirstValidInput(inputs);
  }
}