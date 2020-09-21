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
      mapToString(sb, "counters", statistics.counters());
      mapToString(sb, "gauges", statistics.gauges());
      mapToString(sb, "minimums", statistics.minimums());
      mapToString(sb, "maximums", statistics.maximums());
      mapToString(sb, "means", statistics.meanStatistics());

      return sb.toString();
    } else {
      return "";
    }
  }

  /**
   * Given a map, add its entryset to the string.
   * @param sb string buffer to append to
   * @param type type (for output)
   * @param map map to evaluate
   * @param <E> type of values of the map
   */
  private static <E> void mapToString(StringBuilder sb,
      final String type,
      final Map<String, E> map) {
    int count = 0;
    sb.append(type);
    sb.append("=(");
    for (Map.Entry<String, E> entry : map.entrySet()) {
      if (count > 0) {
        sb.append(' ');
      }
      count++;
      sb.append(IOStatisticsBinding.entryToString(
          entry.getKey(), entry.getValue()));
    }
    sb.append("); ");
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
   * statistics report
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
