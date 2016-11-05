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

package org.apache.hadoop.hdfs;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;

import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.util.concurrent.HadoopExecutors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This tests basic operations of {@link DFSOpsCountStatistics} class.
 */
public class TestDFSOpsCountStatistics {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestDFSOpsCountStatistics.class);
  private static final String NO_SUCH_OP = "no-such-dfs-operation-dude";

  private final DFSOpsCountStatistics statistics =
      new DFSOpsCountStatistics();
  private final Map<OpType, AtomicLong> expectedOpsCountMap = new HashMap<>();

  @Rule
  public final Timeout globalTimeout = new Timeout(10 * 1000);
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() {
    for (OpType opType : OpType.values()) {
      expectedOpsCountMap.put(opType, new AtomicLong());
    }
    incrementOpsCountByRandomNumbers();
  }

  /**
   * This is to test the the {@link OpType} symbols are unique.
   */
  @Test
  public void testOpTypeSymbolsAreUnique() {
    final Set<String> opTypeSymbols = new HashSet<>();
    for (OpType opType : OpType.values()) {
      assertFalse(opTypeSymbols.contains(opType.getSymbol()));
      opTypeSymbols.add(opType.getSymbol());
    }
    assertEquals(OpType.values().length, opTypeSymbols.size());
  }

  @Test
  public void testGetLongStatistics() {
    short iterations = 0; // number of the iter.hasNext()
    final Iterator<LongStatistic> iter = statistics.getLongStatistics();

    while (iter.hasNext()) {
      final LongStatistic longStat = iter.next();
      assertNotNull(longStat);
      final OpType opType = OpType.fromSymbol(longStat.getName());
      assertNotNull(opType);
      assertTrue(expectedOpsCountMap.containsKey(opType));
      assertEquals(expectedOpsCountMap.get(opType).longValue(),
          longStat.getValue());
      iterations++;
    }

    // check that all the OpType enum entries are iterated via iter
    assertEquals(OpType.values().length, iterations);
  }

  @Test
  public void testGetLong() {
    assertNull(statistics.getLong(null));
    assertNull(statistics.getLong(NO_SUCH_OP));
    verifyStatistics();
  }

  @Test
  public void testIsTracked() {
    assertFalse(statistics.isTracked(null));
    assertFalse(statistics.isTracked(NO_SUCH_OP));

    final Iterator<LongStatistic> iter = statistics.getLongStatistics();
    while (iter.hasNext()) {
      final LongStatistic longStatistic = iter.next();
      assertTrue(statistics.isTracked(longStatistic.getName()));
    }
  }

  @Test
  public void testReset() {
    statistics.reset();
    for (OpType opType : OpType.values()) {
      expectedOpsCountMap.get(opType).set(0);
    }

    final Iterator<LongStatistic> iter = statistics.getLongStatistics();
    while (iter.hasNext()) {
      final LongStatistic longStat = iter.next();
      assertEquals(0, longStat.getValue());
    }

    incrementOpsCountByRandomNumbers();
    verifyStatistics();
  }

  @Test
  public void testCurrentAccess() throws InterruptedException {
    final int numThreads = 10;
    final ExecutorService threadPool = newFixedThreadPool(numThreads);

    try {
      final CountDownLatch allReady = new CountDownLatch(numThreads);
      final CountDownLatch startBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(numThreads);
      final AtomicReference<Throwable> childError = new AtomicReference<>();

      for (int i = 0; i < numThreads; i++) {
        threadPool.submit(new Runnable() {
          @Override
          public void run() {
            allReady.countDown();
            try {
              startBlocker.await();
              incrementOpsCountByRandomNumbers();
            } catch (Throwable t) {
              LOG.error("Child failed when calling mkdir", t);
              childError.compareAndSet(null, t);
            } finally {
              allDone.countDown();
            }
          }
        });
      }

      allReady.await(); // wait until all threads are ready
      startBlocker.countDown(); // all threads start making directories
      allDone.await(); // wait until all threads are done

      assertNull("Child failed with exception.", childError.get());
      verifyStatistics();
    } finally {
      threadPool.shutdownNow();
    }
  }

  /**
   * This is helper method to increment the statistics by random data.
   */
  private void incrementOpsCountByRandomNumbers() {
    for (OpType opType : OpType.values()) {
      final Long randomCount = RandomUtils.nextLong(0, 100);
      expectedOpsCountMap.get(opType).addAndGet(randomCount);
      for (long i = 0; i < randomCount; i++) {
        statistics.incrementOpCounter(opType);
      }
    }
  }

  /**
   * We have the expected ops count in {@link #expectedOpsCountMap}, and this
   * method is to verify that its ops count is the same as the one in
   * {@link #statistics}.
   */
  private void verifyStatistics() {
    for (OpType opType : OpType.values()) {
      assertNotNull(expectedOpsCountMap.get(opType));
      assertNotNull(statistics.getLong(opType.getSymbol()));
      assertEquals("Not expected count for operation " + opType.getSymbol(),
          expectedOpsCountMap.get(opType).longValue(),
          statistics.getLong(opType.getSymbol()).longValue());
    }
  }

}
