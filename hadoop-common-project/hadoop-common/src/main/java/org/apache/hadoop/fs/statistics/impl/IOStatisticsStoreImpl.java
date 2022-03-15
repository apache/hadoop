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

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.stubDurationTracker;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MEAN;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MIN;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.aggregateMaximums;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.aggregateMinimums;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.maybeUpdateMaximum;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.maybeUpdateMinimum;

/**
 * Implementation of {@link IOStatisticsStore}.
 * <p>
 *   A ConcurrentHashMap of each set of statistics is created;
 *   the AtomicLong/MeanStatistic entries are fetched as required.
 *   When the statistics are updated, the referenced objects
 *   are updated rather than new values set in the map.
 * </p>
 */
final class IOStatisticsStoreImpl extends WrappedIOStatistics
    implements IOStatisticsStore {

  /**
   * Log changes at debug.
   * Noisy, but occasionally useful.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsStoreImpl.class);

  /** All the counters are atomic longs. */
  private final Map<String, AtomicLong> counterMap = new ConcurrentHashMap<>();

  /** All the gauges are atomic longs. */
  private final Map<String, AtomicLong> gaugeMap = new ConcurrentHashMap<>();

  /** All the minimum values are atomic longs. */
  private final Map<String, AtomicLong> minimumMap = new ConcurrentHashMap<>();

  /** All the maximum values are atomic longs. */
  private final Map<String, AtomicLong> maximumMap = new ConcurrentHashMap<>();

  /**
   * The mean statistics.
   * Relies on the MeanStatistic operations being synchronized.
   */
  private final Map<String, MeanStatistic> meanStatisticMap
      = new ConcurrentHashMap<>();

  /**
   * Constructor invoked via the builder.
   * @param counters keys to use for the counter statistics.
   * @param gauges names of gauges
   * @param minimums names of minimums
   * @param maximums names of maximums
   * @param meanStatistics names of mean statistics.
   */
  IOStatisticsStoreImpl(
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
        AtomicLong maximum = new AtomicLong(MAX_UNSET_VALUE);
        maximumMap.put(key, maximum);
        builder.withAtomicLongMaximum(key, maximum);
      }
    }
    if (minimums != null) {
      for (String key : minimums) {
        AtomicLong minimum = new AtomicLong(MIN_UNSET_VALUE);
        minimumMap.put(key, minimum);
        builder.withAtomicLongMinimum(key, minimum);
      }
    }
    if (meanStatistics != null) {
      for (String key : meanStatistics) {
        meanStatisticMap.put(key, new MeanStatistic());
        builder.withMeanStatisticFunction(key, k -> meanStatisticMap.get(k));
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
   * param increment amount to increment; negative for a decrement
   * @return final value or 0 if the long is null
   */
  private long incAtomicLong(final AtomicLong aLong,
      final long increment) {
    if (aLong != null) {
      // optimization: zero is a get rather than addAndGet()
      return increment != 0
          ? aLong.addAndGet(increment)
          : aLong.get();
    } else {
      return 0;
    }
  }

  @Override
  public void setCounter(final String key, final long value) {
    setAtomicLong(counterMap.get(key), value);
    LOG.debug("Setting counter {} to {}", key, value);
  }

  @Override
  public long incrementCounter(final String key, final long value) {
    AtomicLong counter = counterMap.get(key);
    if (counter == null) {
      LOG.debug("Ignoring counter increment for unknown counter {}",
          key);
      return 0;
    }
    if (value < 0) {
      LOG.debug("Ignoring negative increment value {} for counter {}",
          value, key);
      // returns old value
      return counter.get();
    } else {
      long l = incAtomicLong(counter, value);
      LOG.debug("Incrementing counter {} by {} with final value {}",
          key, value, l);
      return l;
    }
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
  public void addMinimumSample(final String key, final long value) {
    AtomicLong min = minimumMap.get(key);
    if (min != null) {
      maybeUpdateMinimum(min, value);
    }
  }

  @Override
  public void addMaximumSample(final String key, final long value) {
    AtomicLong max = maximumMap.get(key);
    if (max != null) {
      maybeUpdateMaximum(max, value);
    }
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
    final MeanStatistic ref = meanStatisticMap.get(key);
    if (ref != null) {
      ref.set(value);
    }
  }

  @Override
  public void addMeanStatisticSample(final String key, final long value) {
    final MeanStatistic ref = meanStatisticMap.get(key);
    if (ref != null) {
      ref.addSample(value);
    }
  }

  /**
   * Reset all statistics.
   */
  @Override
  public synchronized void reset() {
    counterMap.values().forEach(a -> a.set(0));
    gaugeMap.values().forEach(a -> a.set(0));
    minimumMap.values().forEach(a -> a.set(0));
    maximumMap.values().forEach(a -> a.set(0));
    meanStatisticMap.values().forEach(a -> a.clear());
  }

  /**
   * Aggregate those statistics which the store is tracking;
   * ignore the rest.
   *
   * @param source statistics; may be null
   * @return true if a statistics reference was supplied/aggregated.
   */
  @Override
  public synchronized boolean aggregate(
      @Nullable final IOStatistics source) {

    if (source == null) {
      return false;
    }
    // counters: addition
    Map<String, Long> sourceCounters = source.counters();
    counterMap.entrySet().
        forEach(e -> {
          Long sourceValue = lookupQuietly(sourceCounters, e.getKey());
          if (sourceValue != null) {
            e.getValue().addAndGet(sourceValue);
          }
        });
    // gauge: add positive values only
    Map<String, Long> sourceGauges = source.gauges();
    gaugeMap.entrySet().forEach(e -> {
      Long sourceGauge = lookupQuietly(sourceGauges, e.getKey());
      if (sourceGauge != null && sourceGauge > 0) {
        e.getValue().addAndGet(sourceGauge);
      }
    });
    // min: min of current and source
    Map<String, Long> sourceMinimums = source.minimums();
    minimumMap.entrySet().forEach(e -> {
      Long sourceValue = lookupQuietly(sourceMinimums, e.getKey());
      if (sourceValue != null) {
        AtomicLong dest = e.getValue();
        dest.set(aggregateMaximums(dest.get(), sourceValue));
        dest.set(aggregateMinimums(dest.get(), sourceValue));
      }
    });
    // max: max of current and source
    Map<String, Long> sourceMaximums = source.maximums();
    maximumMap.entrySet().forEach(e -> {
      Long sourceValue = lookupQuietly(sourceMaximums, e.getKey());
      if (sourceValue != null) {
        AtomicLong dest = e.getValue();
        dest.set(aggregateMaximums(dest.get(), sourceValue));
      }
    });
    // the most complex
    Map<String, MeanStatistic> sourceMeans = source.meanStatistics();
    meanStatisticMap.entrySet().forEach(e -> {
      MeanStatistic current = e.getValue();
      MeanStatistic sourceValue = lookupQuietly(
          sourceMeans, e.getKey());
      if (sourceValue != null) {
        current.add(sourceValue);
      }
    });
    return true;
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
   * Get a reference to the map type providing the
   * value for a specific key, returning null if it not found.
   * @param <T> type of map/return type.
   * @param map map to look up
   * @param key statistic name
   * @return the value
   */
  private static <T> T lookupQuietly(final Map<String, T> map, String key) {
    return map.get(key);
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
   * Get a mean statistic.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  @Override
  public MeanStatistic getMeanStatistic(String key) {
    return lookup(meanStatisticMap, key);
  }

  /**
   * Add a duration to the min/mean/max statistics, using the
   * given prefix and adding a suffix for each specific value.
   * <p>
   * The update is non -atomic, even though each individual statistic
   * is updated thread-safely. If two threads update the values
   * simultaneously, at the end of each operation the state will
   * be correct. It is only during the sequence that the statistics
   * may be observably inconsistent.
   * </p>
   * @param prefix statistic prefix
   * @param durationMillis duration in milliseconds.
   */
  @Override
  public void addTimedOperation(String prefix, long durationMillis) {
    addMeanStatisticSample(prefix + SUFFIX_MEAN, durationMillis);
    addMinimumSample(prefix + SUFFIX_MIN, durationMillis);
    addMaximumSample(prefix + SUFFIX_MAX, durationMillis);
  }

  @Override
  public void addTimedOperation(String prefix, Duration duration) {
    addTimedOperation(prefix, duration.toMillis());
  }

  /**
   * If the store is tracking the given key, return the
   * duration tracker for it. If not tracked, return the
   * stub tracker.
   * @param key statistic key prefix
   * @param count  #of times to increment the matching counter in this
   * operation.
   * @return a tracker.
   */
  @Override
  public DurationTracker trackDuration(final String key, final long count) {
    if (counterMap.containsKey(key)) {
      return new StatisticDurationTracker(this, key, count);
    } else {
      return stubDurationTracker();
    }
  }
}
