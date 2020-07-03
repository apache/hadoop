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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics;

/**
 * Implement statistics as a map of AtomicLong counters/gauges
 * etc. created in the constructor.
 */
final class CounterIOStatisticsImpl extends WrappedIOStatistics
    implements CounterIOStatistics {

  private final Map<String, AtomicLong> counterMap = new HashMap<>();

  private final Map<String, AtomicLong> gaugeMap = new HashMap<>();

  private final Map<String, AtomicLong> minimumMap = new HashMap<>();

  private final Map<String, AtomicLong> maximumMap = new HashMap<>();

  private final Map<String, AtomicReference<MeanStatistic>> meanStatisticMap
      = new HashMap<>();

  /**
   * Constructor invoked via the builder.
   * @param counters keys to use for the counter statistics.
   * @param gauges names of gauges
   * @param minimums names of minimums
   * @param maximums names of maximums
   * @param meanStatistics names of mean statistics.
   */
  CounterIOStatisticsImpl(
      final List<String> counters,
      final List<String> gauges,
      final List<String> minimums,
      final List<String> maximums,
      final List<String> meanStatistics) {
    // initially create the superclass with no wrapped mapping;
    super(null);

    // now construct a dynamic statistics source mapping to
    // the various counters, gauges etc dynamically created
    // into maps
    DynamicIOStatisticsBuilder builder = dynamicIOStatistics();
    if (counters != null) {
      for (String key : counters) {
        AtomicLong counter = new AtomicLong();
        counterMap.put(key, counter);
        builder.withAtomicLongCounter(key, counter);
      }
    }
    if (gauges != null) {
      for (String key : gauges) {
        AtomicLong gauge = new AtomicLong();
        gaugeMap.put(key, gauge);
        builder.withAtomicLongGauge(key, gauge);
      }
    }
    if (maximums != null) {
      for (String key : maximums) {
        AtomicLong maximum = new AtomicLong();
        maximumMap.put(key, maximum);
        builder.withAtomicLongMaximum(key, maximum);
      }
    }
    if (minimums != null) {
      for (String key : minimums) {
        AtomicLong minimum = new AtomicLong();
        minimumMap.put(key, minimum);
        builder.withAtomicLongMinimum(key, minimum);
      }
    }
    if (meanStatistics != null) {
      for (String key : meanStatistics) {
        AtomicReference<MeanStatistic> msr = new AtomicReference<>();
        meanStatisticMap.put(key, msr);
        builder.withMeanStatisticFunction(key, k -> {
          AtomicReference<MeanStatistic> r
              = meanStatisticMap.get(key);
          return r != null ? r.get() : null;
        });
      }
    }
    setWrapped(builder.build());
  }

  /**
   * Set an atomic long to a value.
   * @param aLong atomic long; may be null
   * @param value value to set to
   */
  private void setAtomicLong(final AtomicLong aLong, final long value) {
    if (aLong != null) {
      aLong.set(value);
    }
  }

  /**
   * increment an atomic long and return its value;
   * null long is no-op returning 0.
   * @param aLong atomic long; may be null
   * @param increment amount to increment; -ve for a decrement
   * @return final value or 0
   */
  private long incAtomicLong(final AtomicLong aLong,
      final long increment) {
    if (aLong != null) {
      return aLong.getAndAdd(increment);
    } else {
      return 0;
    }
  }

  @Override
  public void setCounter(final String key, final long value) {
    setAtomicLong(counterMap.get(key), value);
  }

  @Override
  public long incrementCounter(final String key, final long value) {
    return incAtomicLong(counterMap.get(key), value);
  }

  @Override
  public void setMaximum(final String key, final long value) {
    setAtomicLong(maximumMap.get(key), value);
  }

  @Override
  public long incrementMaximum(final String key, final long value) {
    return incAtomicLong(maximumMap.get(key), value);
  }

  @Override
  public void setMinimum(final String key, final long value) {
    setAtomicLong(minimumMap.get(key), value);
  }

  @Override
  public long incrementMinimum(final String key, final long value) {
    return incAtomicLong(minimumMap.get(key), value);
  }

  @Override
  public void setGauge(final String key, final long value) {
    setAtomicLong(gaugeMap.get(key), value);
  }

  @Override
  public long incrementGauge(final String key, final long value) {
    return incAtomicLong(gaugeMap.get(key), value);
  }

  @Override
  public void setMeanStatistic(final String key, final MeanStatistic value) {
    final AtomicReference<MeanStatistic> ref = meanStatisticMap.get(key);
    if (ref != null) {
      ref.set(value);
    }
  }

  /**
   * Reset all statistics.
   * Unsynchronized.
   */
  @Override
  public void reset() {
    counterMap.values().forEach(a -> a.set(0));
    gaugeMap.values().forEach(a -> a.set(0));
    minimumMap.values().forEach(a -> a.set(0));
    maximumMap.values().forEach(a -> a.set(0));
    meanStatisticMap.values().forEach(a -> a.set(new MeanStatistic()));
  }

  @Override
  public void copy(final IOStatistics source) {
    counterMap.entrySet().forEach(e -> {
      e.getValue().set(lookup(source.counters(), e.getKey()));
    });
    gaugeMap.entrySet().forEach(e -> {
      e.getValue().set(lookup(source.gauges(), e.getKey()));
    });
    maximumMap.entrySet().forEach(e -> {
      e.getValue().set(lookup(source.maximums(), e.getKey()));
    });
    minimumMap.entrySet().forEach(e -> {
      e.getValue().set(lookup(source.minimums(), e.getKey()));
    });
    meanStatisticMap.entrySet().forEach(e -> {
      String key = e.getKey();
      MeanStatistic statisticValue = lookup(source.meanStatistics(), key);
      e.getValue().set(statisticValue.copy());
    });
  }

  @Override
  public void aggregate(final IOStatistics source) {
    // counters: addition
    counterMap.entrySet().forEach(e -> {
      e.getValue().addAndGet(lookup(source.counters(), e.getKey()));
    });
    // gauge: add positive values only
    gaugeMap.entrySet().forEach(e -> {
      long sourceGauge = lookup(source.gauges(), e.getKey());
      if (sourceGauge > 0) {
        e.getValue().addAndGet(sourceGauge);
      }
    });
    // min: min of current and source
    minimumMap.entrySet().forEach(e -> {
      AtomicLong dest = e.getValue();
      long sourceValue = lookup(source.minimums(), e.getKey());
      dest.set(Math.min(dest.get(), sourceValue));
    });
    // max: max of current and source
    maximumMap.entrySet().forEach(e -> {
      AtomicLong dest = e.getValue();
      long sourceValue = lookup(source.maximums(), e.getKey());
      dest.set(Math.max(dest.get(), sourceValue));
    });
    // the most complex, as the reference itself is resolved and then updated.
    meanStatisticMap.entrySet().forEach(e -> {
      AtomicReference<MeanStatistic> dest = e.getValue();
      MeanStatistic current = dest.get();
      MeanStatistic sourceValue = lookup(source.meanStatistics(), e.getKey());
      current.add(sourceValue);
    });

  }

  @Override
  public void subtractCounters(final IOStatistics source) {
    counterMap.entrySet().forEach(e -> {
      e.getValue().addAndGet(-lookup(source.counters(), e.getKey()));
    });
  }

  /**
   * Get a reference to the map type providing the
   * value for a specific key, raising an exception if
   * there is no entry for that key.
   * @param <T> type of map/return type.
   * @param map map to look up
   * @param key statistic name
   * @return the value
   * @throws NullPointerException if there is no entry of that name
   */
  private static <T> T lookup(final Map<String, T> map, String key) {
    T val = map.get(key);
    requireNonNull(val, () -> ("unknown statistic " + key));
    return val;
  }

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific counter. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  @Override
  public AtomicLong getCounterReference(String key) {
    return lookup(counterMap, key);
  }

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific maximum. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  @Override
  public AtomicLong getMaximumReference(String key) {
    return lookup(maximumMap, key);
  }

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific minimum. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  @Override
  public AtomicLong getMinimumReference(String key) {
    return lookup(minimumMap, key);
  }

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific gauge. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  @Override
  public AtomicLong getGaugeReference(String key) {
    return lookup(gaugeMap, key);
  }

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific meanStatistic. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  @Override
  public AtomicReference<MeanStatistic> getMeanStatisticReference(String key) {
    return lookup(meanStatisticMap, key);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
