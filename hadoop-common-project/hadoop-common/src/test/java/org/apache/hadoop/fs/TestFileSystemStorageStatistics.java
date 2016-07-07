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

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This tests basic operations of {@link FileSystemStorageStatistics} class.
 */
public class TestFileSystemStorageStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestFileSystemStorageStatistics.class);
  private static final String FS_STORAGE_STATISTICS_NAME = "test-fs-statistics";
  private static final String[] STATISTICS_KEYS = {
      "bytesRead",
      "bytesWritten",
      "readOps",
      "largeReadOps",
      "writeOps",
      "bytesReadLocalHost",
      "bytesReadDistanceOfOneOrTwo",
      "bytesReadDistanceOfThreeOrFour",
      "bytesReadDistanceOfFiveOrLarger"
  };

  private FileSystem.Statistics statistics =
      new FileSystem.Statistics("test-scheme");
  private FileSystemStorageStatistics storageStatistics =
      new FileSystemStorageStatistics(FS_STORAGE_STATISTICS_NAME, statistics);

  @Rule
  public final Timeout globalTimeout = new Timeout(10 * 1000);
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() {
    statistics.incrementBytesRead(RandomUtils.nextInt(100));
    statistics.incrementBytesWritten(RandomUtils.nextInt(100));
    statistics.incrementLargeReadOps(RandomUtils.nextInt(100));
    statistics.incrementWriteOps(RandomUtils.nextInt(100));

    statistics.incrementBytesReadByDistance(0, RandomUtils.nextInt(100));
    statistics.incrementBytesReadByDistance(1, RandomUtils.nextInt(100));
    statistics.incrementBytesReadByDistance(3, RandomUtils.nextInt(100));
  }

  @Test
  public void testGetLongStatistics() {
    Iterator<LongStatistic> iter = storageStatistics.getLongStatistics();
    while (iter.hasNext()) {
      final LongStatistic longStat = iter.next();
      assertNotNull(longStat);
      final long expectedStat = getStatisticsValue(longStat.getName());
      LOG.info("{}: FileSystem.Statistics={}, FileSystemStorageStatistics={}",
          longStat.getName(), expectedStat, longStat.getValue());
      assertEquals(expectedStat, longStat.getValue());
    }
  }

  @Test
  public void testGetLong() {
    for (String key : STATISTICS_KEYS) {
      final long expectedStat = getStatisticsValue(key);
      final long storageStat = storageStatistics.getLong(key);
      LOG.info("{}: FileSystem.Statistics={}, FileSystemStorageStatistics={}",
          key, expectedStat, storageStat);
      assertEquals(expectedStat, storageStat);
    }
  }

  /**
   * Helper method to retrieve the specific FileSystem.Statistics value by name.
   *
   * Basically, the {@link FileSystemStorageStatistics} should do this
   * internally in a similar approach.
   */
  private long getStatisticsValue(String name) {
    switch (name) {
    case "bytesRead":
      return statistics.getBytesRead();
    case "bytesWritten":
      return statistics.getBytesWritten();
    case "readOps":
      return statistics.getReadOps();
    case "largeReadOps":
      return statistics.getLargeReadOps();
    case "writeOps":
      return statistics.getWriteOps();
    case "bytesReadLocalHost":
      return statistics.getBytesReadByDistance(0);
    case "bytesReadDistanceOfOneOrTwo":
      return statistics.getBytesReadByDistance(1);
    case "bytesReadDistanceOfThreeOrFour":
      return statistics.getBytesReadByDistance(3);
    case "bytesReadDistanceOfFiveOrLarger":
      return statistics.getBytesReadByDistance(5);
    default:
      return 0;
    }
  }

}
