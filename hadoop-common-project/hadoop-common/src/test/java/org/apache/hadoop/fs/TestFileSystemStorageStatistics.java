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

package org.apache.hadoop.fs;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This tests basic operations of {@link FileSystemStorageStatistics} class.
 */
public class TestFileSystemStorageStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestFileSystemStorageStatistics.class);

  private final FileSystemStorageStatistics storageStatistics =
      new FileSystemStorageStatistics("test-scheme");

  @Rule
  public final Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

  private final Map<String, Long> expectedStats = new HashMap<>();

  @Before
  public void setup() {
    expectedStats.clear();
    for (Statistic op : Statistic.VALUES) {
      expectedStats.put(op.getSymbol(), RandomUtils.nextLong(0, 100));
    }
    storageStatistics.reset();
    for (Statistic op : Statistic.VALUES) {
      storageStatistics.incrementCounter(op, expectedStats.get(op.getSymbol()));
    }
  }

  @Test
  public void testGetLongStatistics() {
    Iterator<LongStatistic> iter = storageStatistics.getLongStatistics();
    while (iter.hasNext()) {
      final LongStatistic longStat = iter.next();
      assertNotNull(longStat);
      final long expectedStat = expectedStats.get(longStat.getName());
      LOG.info("{}: FileSystem.Statistics={}, FileSystemStorageStatistics={}",
          longStat.getName(), expectedStat, longStat.getValue());
      assertEquals(expectedStat, longStat.getValue());
    }
  }

  @Test
  public void testGetLong() {
    for (Statistic op : Statistic.VALUES) {
      final long expectedStat = expectedStats.get(op.getSymbol());
      final long storageStatFromOp = storageStatistics.getLong(op);
      final long storageStat = storageStatistics.getLong(op.getSymbol());
      LOG.info("{}: FileSystem.Statistics={}, FileSystemStorageStatistics={}",
          op, expectedStat, storageStat);
      assertEquals(expectedStat, storageStatFromOp);
      assertEquals(expectedStat, storageStat);
    }
  }
}
