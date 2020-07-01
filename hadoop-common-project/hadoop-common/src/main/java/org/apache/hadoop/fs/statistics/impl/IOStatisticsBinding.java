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

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.MeanStatistic;

/**
 * Support for implementing IOStatistics interfaces.
 */
public final class IOStatisticsBinding {

  /** Pattern used for each entry. */
  public static final String ENTRY_PATTERN = "(%s=%s)";

  /** String to return when a source is null. */
  @VisibleForTesting
  public static final String NULL_SOURCE = "()";

  private IOStatisticsBinding() {
  }


  /**
   * Create  IOStatistics from a storage statistics instance.
   * This will be updated as the storage statistics change.
   * @param storageStatistics source data.
   * @return an IO statistics source.
   */
  public static IOStatistics fromStorageStatistics(
      StorageStatistics storageStatistics) {
    DynamicIOStatisticsBuilder builder = dynamicIOStatistics();
    Iterator<StorageStatistics.LongStatistic> it = storageStatistics
        .getLongStatistics();
    while (it.hasNext()) {
      StorageStatistics.LongStatistic next = it.next();
      builder.withLongFunctionCounter(next.getName(),
          k -> storageStatistics.getLong(k));
    }
    return builder.build();
  }

  /**
   * Create a builder for dynamic IO Statistics.
   * @return a builder to be completed.
   */
  public static DynamicIOStatisticsBuilder dynamicIOStatistics() {
    return new DynamicIOStatisticsBuilder();
  }

  /**
   * Get the shared instance of the immutable empty statistics
   * object.
   * @return an empty statistics object.
   */
  public static IOStatistics emptyStatistics() {
    return EmptyIOStatistics.getInstance();
  }

  /**
   * Take an IOStatistics instance and wrap it in a source.
   * @param statistics statistics.
   * @return a source which will return the values
   */
  public static IOStatisticsSource wrap(IOStatistics statistics) {
    return new SourceWrappedStatistics(statistics);
  }

  /**
   * Create a builder from an IOStatistics instance
   * which creates the appropriate counters, gauges etc in maps
   * of atomic references.
   * This is the simplest way to build an IOStatistics instance as all
   * the details are handled internally.
   *
   * @return a builder instance.
   */
  public static CounterIOStatisticsBuilder counterIOStatistics() {
    return new CounterIOStatisticsBuilderImpl();
  }

  /**
   * Convert an entry to the string format used in logging.
   *
   * @param entry entry to evaluate
   * @return formatted string
   */
  public static String entrytoString(
      final Map.Entry<String, Long> entry) {
    return entrytoString(entry.getKey(), entry.getValue());
  }

  /**
   * Convert entry values to the string format used in logging.
   *
   * @param name statistic name
   * @param value stat value
   * @return formatted string
   */
  public static String entrytoString(
      final String name, final Long value) {
    return String.format(
        ENTRY_PATTERN,
        name,
        value);
  }

  /**
   * Copy into the dest map all the source entries
   * @param <E>
   * @param dest
   * @param source
   * @param copyFn
   * @return
   */
  public static <E> Map<String, E> copyMap(
      Map<String, E> dest,
      Map<String, E> source,
      Function<E, E> copyFn) {
    // we have to clone the values so that they aren't
    // bound to the original values
    dest.clear();
    source.entrySet()
        .forEach(entry ->
            dest.put(entry.getKey(), copyFn.apply(entry.getValue())));
    return dest;
  }

  /**
   * A passthrough copy operation suitable for immutable
   * types, including numbers
   * @param src source object
   * @return the source object
   */
  public static <E extends Serializable> E passthroughFn(E src) {
    return src;
  }

  public static <E extends Serializable> TreeMap<String, E> snapshotMap(
      Map<String, E> source) {
    return snapshotMap(source,
        IOStatisticsBinding::passthroughFn);
  }

  public static <E extends Serializable> TreeMap<String, E> snapshotMap(
      Map<String, E> source,
      Function<E, E> copyFn) {
    TreeMap<String, E> dest = new TreeMap<>();
    copyMap(dest, source, copyFn);
    return dest;
  }

  public static <E> void aggregateMaps(
      Map<String, E> left,
      Map<String, E> right,
      BiFunction<E, E, E> aggregateFn) {
    // scan through the right hand map; copy
    // any values not in the left map,
    // aggregate those for which there is already
    // an entry
    right.entrySet().forEach(entry -> {
      String key = entry.getKey();
      E rVal = entry.getValue();
      E lVal = left.get(key);
      if (lVal == null) {
        left.put(key, rVal);
      } else {
        left.put(key, aggregateFn.apply(lVal, rVal));
      }
    });
  }

  public static Long aggregateCounters(Long l, Long r) {
    return Math.max(l, 0) + Math.max(r, 0);
  }

  /**
   * Add two gauges.
   * @param l left value
   * @param r right value
   * @return aggregate value
   */
  public static Long aggregateGauges(Long l, Long r) {
    return l + r;
  }


  public static Long aggregateMinimums(Long l, Long r) {
    return Math.min(l, r);
  }
  public static Long aggregateMaximums(Long l, Long r) {
    return Math.max(l, r);
  }

  /**
   * Aggregate the mean statistics.
   * This returns a new instance.
   * @param l left value
   * @param r right value
   * @return aggregate value
   */
  public static MeanStatistic aggregateMeanStatistics(
      MeanStatistic l, MeanStatistic r) {
    MeanStatistic res = l.copy();
    res.add(r);
    return res;
  }
}
