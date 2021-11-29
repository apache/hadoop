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

package org.apache.hadoop.fs.common;

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.ByteBuffer;

public class BufferPoolTest {

  static final int POOL_SIZE = 2;
  static final int BUFFER_SIZE = 10;


  @Test
  public void testArgChecks() {
    // Should not throw.
    BufferPool pool = new BufferPool(POOL_SIZE, BUFFER_SIZE);

    // Verify it throws correctly.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'size' must be a positive integer",
        () -> new BufferPool(0, 10));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'size' must be a positive integer",
        () -> new BufferPool(-1, 10));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'bufferSize' must be a positive integer",
        () -> new BufferPool(10, 0));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'bufferSize' must be a positive integer",
        () -> new BufferPool(1, -10));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> pool.acquire(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> pool.tryAcquire(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'data' must not be null",
        () -> pool.release((BufferData) null));
  }

  @Test
  public void testGetAndRelease() {
    BufferPool pool = new BufferPool(POOL_SIZE, BUFFER_SIZE);
    assertInitialState(pool, POOL_SIZE);

    int count = 0;
    for (BufferData data : pool.getAll()) {
      count++;
    }
    assertEquals(0, count);

    BufferData data1 = this.acquire(pool, 1);
    BufferData data2 = this.acquire(pool, 2);
    BufferData data3 = pool.tryAcquire(3);
    assertNull(data3);

    count = 0;
    for (BufferData data : pool.getAll()) {
      count++;
    }
    assertEquals(2, count);

    assertEquals(2, pool.numCreated());
    assertEquals(0, pool.numAvailable());

    data1.updateState(BufferData.State.READY, BufferData.State.BLANK);
    pool.release(data1);

    assertEquals(2, pool.numCreated());
    assertEquals(1, pool.numAvailable());

    data2.updateState(BufferData.State.READY, BufferData.State.BLANK);
    pool.release(data2);

    assertEquals(2, pool.numCreated());
    assertEquals(2, pool.numAvailable());
  }

  @Test
  public void testRelease() {
    testReleaseHelper(BufferData.State.BLANK, true);
    testReleaseHelper(BufferData.State.PREFETCHING, true);
    testReleaseHelper(BufferData.State.CACHING, true);
    testReleaseHelper(BufferData.State.READY, false);
  }

  private void testReleaseHelper(BufferData.State stateBeforeRelease, boolean expectThrow) {
    BufferPool pool = new BufferPool(POOL_SIZE, BUFFER_SIZE);
    assertInitialState(pool, POOL_SIZE);

    BufferData data = this.acquire(pool, 1);
    data.updateState(stateBeforeRelease, BufferData.State.BLANK);

    if (expectThrow) {
      ExceptionAsserts.assertThrows(
          IllegalArgumentException.class,
          "Unable to release buffer",
          () -> pool.release(data));
    } else {
      pool.release(data);
    }
  }

  private BufferData acquire(BufferPool pool, int blockNumber) {
    BufferData data = pool.acquire(blockNumber);
    assertNotNull(data);
    assertSame(data, pool.acquire(blockNumber));
    assertEquals(blockNumber, data.blockNumber);
    return data;
  }

  private void assertInitialState(BufferPool pool, int poolSize) {
    assertEquals(poolSize, pool.numAvailable());
    assertEquals(0, pool.numCreated());
  }
}
