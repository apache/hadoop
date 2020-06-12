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

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StatisticsMap;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics;

/**
 * Implement counter statistics as a map of AtomicLong counters
 * created in the constructor.
 */
final class CounterIOStatisticsImpl extends WrappedIOStatistics
    implements CounterIOStatistics {

  private final Map<String, AtomicLong> atomicCounters = new HashMap<>();

  /**
   * Constructor.
   * @param keys keys to use for the counter statistics.
   */
  CounterIOStatisticsImpl(String[] keys) {
    super(null);
    DynamicIOStatisticsBuilder builder = dynamicIOStatistics();
    for (String key : keys) {
      AtomicLong counter = new AtomicLong();
      atomicCounters.put(key, counter);
      builder.withAtomicLongCounter(key, counter);
    }
    setSource(builder.build());
  }

  @Override
  public long incrementCounter(final String key, final long value) {
    AtomicLong counter = atomicCounters.get(key);
    if (counter != null) {
      return counter.getAndAdd(value);
    } else {
      return 0;
    }
  }

  @Override
  public void setCounter(final String key, final long value) {
    AtomicLong counter = atomicCounters.get(key);
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
    atomicCounters.values().forEach(a -> a.set(0));
  }

  @Override
  public void copy(final IOStatistics source) {
    StatisticsMap<Long> sourceCounters = source.counters();
    atomicCounters.entrySet().forEach(e -> {
      e.getValue().set(getCounterValue(source, e.getKey()));
    });
  }

  @Override
  public void aggregate(final IOStatistics source) {
    atomicCounters.entrySet().forEach(e -> {
      e.getValue().addAndGet(getCounterValue(source, e.getKey()));
    });
  }

  @Override
  public void subtract(final IOStatistics source) {
    atomicCounters.entrySet().forEach(e -> {
      e.getValue().addAndGet(-getCounterValue(source, e.getKey()));
    });
  }

  private Long getCounterValue(final IOStatistics source, final String key) {
    Long statisticValue = source.counters().get(key);
    Preconditions.checkState(statisticValue != null,
        "No statistic %s in IOStatistic source %s",
        key, source);
    return statisticValue;
  }
}
