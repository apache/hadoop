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

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsImplementationSupport.retrieveIOStatistics;

/**
 * Utility operations to work with IO Statistics, especially log them.
 */
public class IOStatisticsLogging {

  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsLogging.class);

  /**
   * Convert a set of statistics to a string form.
   * @param statistics A statistics instance.
   * @return string value
   */
  private static String statisticsToString(
      final Optional<IOStatistics> statistics) {
    return statistics.map(IOStatisticsLogging::statisticsToString)
        .orElse("");
  }

  /**
   * Convert a set of statistics to a string form.
   * @param statistics A statistics instance.
   * @return string value
   */
  private static String statisticsToString(
      final IOStatistics statistics) {
    StringBuilder sb = new StringBuilder(" {");
    for (Map.Entry entry : statistics) {
      sb.append("{")
          .append(entry.getKey())
          .append("=")
          .append(entry.getValue())
          .append("} ");
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Extract the statistics from a source.
   * Exceptions are caught and downgraded to debug logging.
   * @param source source of statistics.
   * @return a string for logging.n
   */
  public static String sourceToString(final IOStatisticsSource source) {
    try {
      return retrieveIOStatistics(source)
          .map(p -> statisticsToString(p))
          .orElse("");
    } catch (RuntimeException e) {
      LOG.debug("Ignoring", e);
      return "{}";
    }
  }

  /**
   * On demand stringifier.
   * Whenever this object's toString() method is called, it
   * retrieves the latest statistics instance and re-evaluates it.
   */
  public static class SourceToString {

    private final String origin;

    private final IOStatisticsSource source;

    public SourceToString(String origin, IOStatisticsSource source) {
      this.origin = origin;
      this.source = source;
    }

    @Override
    public String toString() {
      return "Statistics of " + origin + " " + sourceToString(source);
    }
  }

  /**
   * Stringifier of statistics: low cost to instantiate and every
   * toString/logging will re-evaluate the statistics.
   */
  public static class StatisticsToString {

    private final String origin;

    private final Optional<IOStatistics> statistics;

    /**
     * Constructor.
     * @param origin source (for message)
     * @param statistics statistics
     */
    public StatisticsToString(String origin,
        Optional<IOStatistics> statistics) {
      this.origin = origin;
      this.statistics = statistics;
    }

    /**
     * Constructor.
     * @param origin source (for message)
     * @param statistics statistics
     */
    public StatisticsToString(String origin, IOStatistics statistics) {
      this.origin = origin;
      this.statistics = Optional.of(statistics);
    }

    @Override
    public String toString() {
      return "Statistics of " + origin + " " + statisticsToString(statistics);
    }
  }
}
