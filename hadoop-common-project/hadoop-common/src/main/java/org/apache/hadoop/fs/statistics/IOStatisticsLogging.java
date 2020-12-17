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

package org.apache.hadoop.fs.statistics;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * Utility operations convert IO Statistics sources/instances
 * to strings, especially for robustly logging.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class IOStatisticsLogging {

  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsLogging.class);

  private IOStatisticsLogging() {
  }

  /**
   * Extract the statistics from a source object -or ""
   * if it is not an instance of {@link IOStatistics},
   * {@link IOStatisticsSource} or the retrieved
   * statistics are null.
   * <p>
   * Exceptions are caught and downgraded to debug logging.
   * @param source source of statistics.
   * @return a string for logging.
   */
  public static String ioStatisticsSourceToString(@Nullable Object source) {
    try {
      return ioStatisticsToString(retrieveIOStatistics(source));
    } catch (RuntimeException e) {
      LOG.debug("Ignoring", e);
      return "";
    }
  }

  /**
   * Convert IOStatistics to a string form.
   * @param statistics A statistics instance.
   * @return string value or the empty string if null
   */
  public static String ioStatisticsToString(
      @Nullable final IOStatistics statistics) {
    if (statistics != null) {
      StringBuilder sb = new StringBuilder();
      mapToString(sb, "counters", statistics.counters(), " ");
      mapToString(sb, "gauges", statistics.gauges(), " ");
      mapToString(sb, "minimums", statistics.minimums(), " ");
      mapToString(sb, "maximums", statistics.maximums(), " ");
      mapToString(sb, "means", statistics.meanStatistics(), " ");

      return sb.toString();
    } else {
      return "";
    }
  }

  /**
   * Convert IOStatistics to a string form, with all the metrics sorted
   * and empty value stripped.
   * This is more expensive than the simple conversion, so should only
   * be used for logging/output where it's known/highly likely that the
   * caller wants to see the values. Not for debug logging.
   * @param statistics A statistics instance.
   * @return string value or the empty string if null
   */
  public static String ioStatisticsToPrettyString(
      @Nullable final IOStatistics statistics) {
    if (statistics != null) {
      StringBuilder sb = new StringBuilder();
      mapToSortedString(sb, "counters", statistics.counters(),
          p -> p == 0);
      mapToSortedString(sb, "\ngauges", statistics.gauges(),
          p -> p == 0);
      mapToSortedString(sb, "\nminimums", statistics.minimums(),
          p -> p  < 0);
      mapToSortedString(sb, "\nmaximums", statistics.maximums(),
          p -> p < 0);
      mapToSortedString(sb, "\nmeans", statistics.meanStatistics(),
          MeanStatistic::isEmpty);

      return sb.toString();
    } else {
      return "";
    }
  }

  /**
   * Given a map, add its entryset to the string.
   * The entries are only sorted if the source entryset
   * iterator is sorted, such as from a TreeMap.
   * @param sb string buffer to append to
   * @param type type (for output)
   * @param map map to evaluate
   * @param separator separator
   * @param <E> type of values of the map
   */
  private static <E> void mapToString(StringBuilder sb,
      final String type,
      final Map<String, E> map,
      final String separator) {
    int count = 0;
    sb.append(type);
    sb.append("=(");
    for (Map.Entry<String, E> entry : map.entrySet()) {
      if (count > 0) {
        sb.append(separator);
      }
      count++;
      sb.append(IOStatisticsBinding.entryToString(
          entry.getKey(), entry.getValue()));
    }
    sb.append(");\n");
  }

  /**
   * Given a map, produce a string with all the values, sorted.
   * Needs to create a treemap and insert all the entries.
   * @param sb string buffer to append to
   * @param type type (for output)
   * @param map map to evaluate
   * @param <E> type of values of the map
   */
  private static <E> void mapToSortedString(StringBuilder sb,
      final String type,
      final Map<String, E> map,
      final Predicate<E> isEmpty) {
    mapToString(sb, type, sortedMap(map, isEmpty), "\n");
  }

  /**
   * Create a sorted (tree) map from an unsorted map.
   * This incurs the cost of creating a map and that
   * of inserting every object into the tree.
   * @param source source map
   * @param <E> value type
   * @return a treemap with all the entries.
   */
  private static <E> Map<String, E> sortedMap(
      final Map<String, E> source,
      final Predicate<E> isEmpty) {
    Map<String, E> tm = new TreeMap<>();
    for (Map.Entry<String, E> entry : source.entrySet()) {
      if (!isEmpty.test(entry.getValue())) {
        tm.put(entry.getKey(), entry.getValue());
      }
    }
    return tm;
  }

  /**
   * On demand stringifier of an IOStatisticsSource instance.
   * <p>
   * Whenever this object's toString() method is called, it evaluates the
   * statistics.
   * <p>
   * This is designed to affordable to use in log statements.
   * @param source source of statistics -may be null.
   * @return an object whose toString() operation returns the current values.
   */
  public static Object demandStringifyIOStatisticsSource(
      @Nullable IOStatisticsSource source) {
    return new SourceToString(source);
  }

  /**
   * On demand stringifier of an IOStatistics instance.
   * <p>
   * Whenever this object's toString() method is called, it evaluates the
   * statistics.
   * <p>
   * This is for use in log statements where for the cost of creation
   * of this entry is low; it is affordable to use in log statements.
   * @param statistics statistics to stringify -may be null.
   * @return an object whose toString() operation returns the current values.
   */
  public static Object demandStringifyIOStatistics(
      @Nullable IOStatistics statistics) {
    return new StatisticsToString(statistics);
  }

  /**
   * Extract any statistics from the source and log at debug, if
   * the log is set to log at debug.
   * No-op if logging is not at debug or the source is null/of
   * the wrong type/doesn't provide statistics.
   * @param log log to log to
   * @param message message for log -this must contain "{}" for the
   * statistics report to actually get logged.
   * @param source source object
   */
  public static void logIOStatisticsAtDebug(
      Logger log,
      String message,
      Object source) {
    if (log.isDebugEnabled()) {
      // robust extract and convert to string
      String stats = ioStatisticsSourceToString(source);
      if (!stats.isEmpty()) {
        log.debug(message, stats);
      }
    }
  }

  /**
   * Extract any statistics from the source and log to
   * this class's log at debug, if
   * the log is set to log at debug.
   * No-op if logging is not at debug or the source is null/of
   * the wrong type/doesn't provide statistics.
   * @param message message for log -this must contain "{}" for the
   * statistics report to actually get logged.
   * @param source source object
   */
  public static void logIOStatisticsAtDebug(
      String message,
      Object source) {
    logIOStatisticsAtDebug(LOG, message, source);
  }

  /**
   * On demand stringifier.
   * <p>
   * Whenever this object's toString() method is called, it
   * retrieves the latest statistics instance and re-evaluates it.
   */
  private static final class SourceToString {

    private final IOStatisticsSource source;

    private SourceToString(@Nullable IOStatisticsSource source) {
      this.source = source;
    }

    @Override
    public String toString() {
      return source != null
          ? ioStatisticsSourceToString(source)
          : IOStatisticsBinding.NULL_SOURCE;
    }
  }

  /**
   * Stringifier of statistics: low cost to instantiate and every
   * toString/logging will re-evaluate the statistics.
   */
  private static final class StatisticsToString {

    private final IOStatistics statistics;

    /**
     * Constructor.
     * @param statistics statistics
     */
    private StatisticsToString(@Nullable IOStatistics statistics) {
      this.statistics = statistics;
    }

    /**
     * Evaluate and stringify the statistics.
     * @return a string value.
     */
    @Override
    public String toString() {
      return statistics != null
          ? ioStatisticsToString(statistics)
          : IOStatisticsBinding.NULL_SOURCE;
    }
  }
}
