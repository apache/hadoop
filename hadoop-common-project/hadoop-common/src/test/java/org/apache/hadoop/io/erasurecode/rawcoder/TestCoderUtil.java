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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test of the utility of raw erasure coder.
 */
public class TestCoderUtil {
  private static final int INITIAL_EMPTY_CHUNK_LENGTH = 4096;
  private static final int SMALL_CHUNK_SIZE = INITIAL_EMPTY_CHUNK_LENGTH + 1;
  private static final int LARGE_CHUNK_SIZE = SMALL_CHUNK_SIZE * 2;

  private final int numInputs = 9;
  private final int chunkSize = 1024;

  @BeforeEach
  public void resetEmptyChunk() throws Exception {
    Field emptyChunk = CoderUtil.class.getDeclaredField("emptyChunk");
    emptyChunk.setAccessible(true);
    synchronized (CoderUtil.class) {
      emptyChunk.set(null, new byte[INITIAL_EMPTY_CHUNK_LENGTH]);
    }
  }

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
  public void testGetEmptyChunkDoesNotShrinkWhenCacheGrowsConcurrently()
      throws Exception {
    AtomicReference<Thread> workerThread = new AtomicReference<>();
    ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r, "get-empty-chunk-small");
      workerThread.set(thread);
      return thread;
    });

    try {
      Future<byte[]> smallChunk;
      synchronized (CoderUtil.class) {
        smallChunk = executor.submit(() -> CoderUtil.getEmptyChunk(
            SMALL_CHUNK_SIZE));
        waitUntilBlocked(workerThread);
        assertTrue(CoderUtil.getEmptyChunk(LARGE_CHUNK_SIZE).length
            >= LARGE_CHUNK_SIZE);
      }

      assertTrue(smallChunk.get(10, TimeUnit.SECONDS).length
          >= LARGE_CHUNK_SIZE,
          "concurrent caller should return the larger chunk already cached");
      assertTrue(CoderUtil.getEmptyChunk(LARGE_CHUNK_SIZE).length
          >= LARGE_CHUNK_SIZE, "empty chunk cache should not shrink");
    } finally {
      executor.shutdownNow();
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

  @Test
  public void testNoValidInput() {
    assertThrows(HadoopIllegalArgumentException.class, () -> {
      byte[][] inputs = new byte[numInputs][];
      CoderUtil.findFirstValidInput(inputs);
    });
  }

  private static void waitUntilBlocked(AtomicReference<Thread> threadRef)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      Thread thread = threadRef.get();
      if (thread != null && thread.getState() == Thread.State.BLOCKED) {
        return;
      }
      Thread.sleep(10);
    }

    Thread thread = threadRef.get();
    fail("small getEmptyChunk caller did not block on CoderUtil.class; state="
        + (thread == null ? "not started" : thread.getState()));
  }
}
