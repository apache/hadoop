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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ExcessRedundancyMap} — verifies correctness of add/remove/
 * contains operations and that the ReadWriteLock-based implementation allows
 * concurrent readers without blocking each other.
 */
public class TestExcessRedundancyMap {

  private ExcessRedundancyMap excessMap;
  private DatanodeDescriptor dn;

  @BeforeEach
  public void setUp() throws Exception {
    excessMap = new ExcessRedundancyMap();
    dn = DFSTestUtil.getLocalDatanodeDescriptor();
  }

  private static BlockInfo makeBlock(long id) {
    return new BlockInfoContiguous(new Block(id, 0L, 0L), (short) 3);
  }

  /** Basic add / contains / remove semantics. */
  @Test
  public void testAddContainsRemove() {
    BlockInfo blk = makeBlock(1L);

    assertFalse(excessMap.contains(dn, blk));
    assertEquals(0, excessMap.size());

    assertTrue(excessMap.add(dn, blk));
    assertTrue(excessMap.contains(dn, blk));
    assertEquals(1, excessMap.size());
    assertEquals(1, excessMap.getSize4Testing(dn.getDatanodeUuid()));

    // Adding the same block again returns false and does not increment size.
    assertFalse(excessMap.add(dn, blk));
    assertEquals(1, excessMap.size());

    assertTrue(excessMap.remove(dn, blk));
    assertFalse(excessMap.contains(dn, blk));
    assertEquals(0, excessMap.size());

    // Removing an absent block returns false.
    assertFalse(excessMap.remove(dn, blk));
  }

  /** clear() resets both the map and the size counter. */
  @Test
  public void testClear() {
    excessMap.add(dn, makeBlock(10L));
    excessMap.add(dn, makeBlock(11L));
    assertEquals(2, excessMap.size());

    excessMap.clear();
    assertEquals(0, excessMap.size());
    assertFalse(excessMap.contains(dn, makeBlock(10L)));
  }

  /**
   * Multiple concurrent readers must all proceed simultaneously — no reader
   * should be blocked by another reader.
   */
  @Test
  public void testConcurrentReads() throws Exception {
    // Populate the map so contains() does real work.
    for (int i = 0; i < 20; i++) {
      excessMap.add(dn, makeBlock(i));
    }

    int numReaders = 8;
    CountDownLatch allReady = new CountDownLatch(numReaders);
    CountDownLatch allDone = new CountDownLatch(numReaders);
    AtomicInteger successCount = new AtomicInteger(0);

    ExecutorService pool = Executors.newFixedThreadPool(numReaders);
    List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < numReaders; t++) {
      final int tid = t;
      futures.add(pool.submit(() -> {
        allReady.countDown();
        try {
          allReady.await();  // all threads start at the same time
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        // Each reader checks several blocks.
        for (int i = 0; i < 20; i++) {
          excessMap.contains(dn, makeBlock(i));
          excessMap.getSize4Testing(dn.getDatanodeUuid());
        }
        successCount.incrementAndGet();
        allDone.countDown();
      }));
    }

    assertTrue(allDone.await(10, TimeUnit.SECONDS),
        "Concurrent readers did not finish in time — possible deadlock");
    assertEquals(numReaders, successCount.get());
    pool.shutdown();
  }

  /**
   * Writer must be exclusive: verify that a write (add) under concurrent reads
   * produces a consistent final size.
   */
  @Test
  public void testConcurrentWritesProduceCorrectSize() throws Exception {
    int numWriters = 10;
    ExecutorService pool = Executors.newFixedThreadPool(numWriters);
    CountDownLatch allReady = new CountDownLatch(numWriters);
    List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < numWriters; t++) {
      final long blockId = t * 100L;
      futures.add(pool.submit(() -> {
        allReady.countDown();
        try {
          allReady.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        excessMap.add(dn, makeBlock(blockId));
      }));
    }

    for (Future<?> f : futures) {
      f.get(10, TimeUnit.SECONDS);
    }
    pool.shutdown();

    assertEquals(numWriters, excessMap.size(),
        "Size must equal the number of distinct blocks added");
  }
}
