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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * Utility operations convert IO Statistics sources/instances
 * to strings, especially for robustly logging.
 */
public class IOStatisticsLogging {

  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsLogging.class);

  /** Pattern used for each entry. */
  @VisibleForTesting
  static final String ENTRY_PATTERN = "(%s=%s)";

  /** used when a source is null. */
  static final String NULL_SOURCE = "()";

  /**
   * Convert IOStatistics to a string form.
   * @param statistics A statistics instance.
   * @return string value or the empty string if null
   */
  public static String iostatisticsToString(
      @Nullable final IOStatistics statistics) {
    if (statistics != null) {
      int count = 0;
      StringBuilder sb = new StringBuilder("(");
      for (Map.Entry entry : statistics) {
        if (count > 0) {
          sb.append(' ');
        }
        count++;
        sb.append(String.format(ENTRY_PATTERN,
            entry.getKey(),
            entry.getValue()));
      }
      sb.append(")");
      return sb.toString();
    } else {
      return "";
    }
  }

  /**
   * Extract the statistics from a source.
   * Exceptions are caught and downgraded to debug logging.
   * @param source source of statistics.
   * @return a string for logging.
   */
  public static String sourceToString(@Nullable IOStatisticsSource source) {
    try {
      return iostatisticsToString(retrieveIOStatistics(source));
    } catch (RuntimeException e) {
      LOG.debug("Ignoring", e);
      return "";
    }
  }

  /**
   * On demand stringifier.
   * Whenever this object's toString() method is called, it evaluates the
   * statistics.
   * This is designed to affordable to use in log statements.
   * @param source source of statistics.
   * @return an object whose toString() operation returns the current values.
   */
   public static Object demandStringify(
       @Nullable IOStatisticsSource source) {
     return new SourceToString(source);
  }

  /**
   * On demand stringifier.
   * Whenever this object's toString() method is called, it evaluates the
   * statistics.
   * This is for use in log statements where for the cost of creation
   * of this entry is low; it is affordable to use in log statements.
   * @param statistics statistics to scan.
   * @return an object whose toString() operation returns the current values.
   */
   public static Object demandStringify(@Nullable IOStatistics statistics) {
     return new StatisticsToString(statistics);
  }

  /**
   * On demand stringifier.
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
          ? sourceToString(source)
          : NULL_SOURCE;
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
          ? iostatisticsToString(statistics)
          : NULL_SOURCE;
    }
  }
}
