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

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This tests basic operations of {@link DFSOpsCountStatistics} class.
 */
public class TestDFSOpsCountStatistics {

  private static final DFSOpsCountStatistics STORAGE_STATISTICS =
      new DFSOpsCountStatistics();
  private static final Map<String, Long> OP_COUNTER_MAP = new HashMap<>();
  private static final String NO_SUCH_OP = "no-such-dfs-operation-dude";

  @Rule
  public final Timeout globalTimeout = new Timeout(10 * 1000);
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setup() {
    for (OpType opType : OpType.values()) {
      final Long opCount = RandomUtils.nextLong(0, 100);
      OP_COUNTER_MAP.put(opType.getSymbol(), opCount);
      for (long i = 0; i < opCount; i++) {
        STORAGE_STATISTICS.incrementOpCounter(opType);
      }
    }
  }

  @Test
  public void testGetLongStatistics() {
    short iterations = 0; // number of the iter.hasNext()
    final Iterator<LongStatistic> iter = STORAGE_STATISTICS.getLongStatistics();

    while (iter.hasNext()) {
      final LongStatistic longStat = iter.next();
      assertNotNull(longStat);
      assertTrue(OP_COUNTER_MAP.containsKey(longStat.getName()));
      assertEquals(OP_COUNTER_MAP.get(longStat.getName()).longValue(),
          longStat.getValue());
      iterations++;
    }

    // check that all the OpType enum entries are iterated via iter
    assertEquals(OpType.values().length, iterations);
  }

  @Test
  public void testGetLong() {
    assertNull(STORAGE_STATISTICS.getLong(NO_SUCH_OP));

    for (OpType opType : OpType.values()) {
      final String key = opType.getSymbol();
      assertEquals(OP_COUNTER_MAP.get(key), STORAGE_STATISTICS.getLong(key));
    }
  }

  @Test
  public void testIsTracked() {
    assertFalse(STORAGE_STATISTICS.isTracked(NO_SUCH_OP));

    final Iterator<LongStatistic> iter = STORAGE_STATISTICS.getLongStatistics();
    while (iter.hasNext()) {
      final LongStatistic longStatistic = iter.next();
      assertTrue(STORAGE_STATISTICS.isTracked(longStatistic.getName()));
    }
  }

}
