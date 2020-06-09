/*
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

package org.apache.hadoop.fs.statistics.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.statistics.IOStatisticEntry;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics;

/**
 * Implement counter statistics as a map of AtomicLong counters
 * created in the constructor.
 */
final class CounterIOStatisticsImpl extends WrappedIOStatistics
    implements CounterIOStatistics {

  private final Map<String, AtomicLong> counters = new HashMap<>();

  /**
   * Constructor.
   * @param keys keys to use for the counter statistics.
   */
  CounterIOStatisticsImpl(String[] keys) {
    super(null);
    DynamicIOStatisticsBuilder builder = dynamicIOStatistics();
    for (String key : keys) {
      AtomicLong counter = new AtomicLong();
      counters.put(key, counter);
      builder.withAtomicLong(key, counter);
    }
    setSource(builder.build());
  }

  @Override
  public long increment(final String key, final long value) {
    AtomicLong counter = counters.get(key);
    if (counter != null) {
      return counter.getAndAdd(value);
    } else {
      return 0;
    }
  }

  @Override
  public void set(final String key, final long value) {
    AtomicLong counter = counters.get(key);
    if (counter != null) {
      counter.set(value);
    }
  }

  /**
   * Reset all counters.
   * Unsynchronized.
   */
  @Override
  public void resetCounters() {
    counters.values().forEach(a -> a.set(0));
  }

  @Override
  public void copy(final IOStatistics source) {
    counters.entrySet().forEach(e -> {
      String key = e.getKey();
      IOStatisticEntry statisticValue = source.getStatistic(key);
      Preconditions.checkState(statisticValue != null,
          "No statistic %s in IOStatistic source %s",
          key, source);
      e.getValue().set(statisticValue.scalar(
          IOStatisticEntry.IOSTATISTIC_COUNTER
      ));
    });
  }

  @Override
  public void add(final IOStatistics source) {
    counters.entrySet().forEach(e -> {
      String key = e.getKey();
      IOStatisticEntry statisticValue = source.getStatistic(key);
      Preconditions.checkState(statisticValue != null,
          "No statistic %s in IOStatistic source %s",
          key, source);
      long v = statisticValue.scalar(
          IOStatisticEntry.IOSTATISTIC_COUNTER);
      e.getValue().addAndGet(v);
    });
  }

  @Override
  public void subtract(final IOStatistics source) {
    counters.entrySet().forEach(e -> {
      String key = e.getKey();
      IOStatisticEntry statisticValue = source.getStatistic(key);
      Preconditions.checkState(statisticValue != null,
          "No statistic %s in IOStatistic source %s",
          key, source);
      long v = statisticValue.scalar(
          IOStatisticEntry.IOSTATISTIC_COUNTER);
      e.getValue().addAndGet(-v);
    });
  }
}
