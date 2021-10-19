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

/**
 * Builder of the {@link IOStatisticsStore} implementation.
 */
public interface IOStatisticsStoreBuilder {

  /**
   * Declare a varargs list of counters to add.
   * @param keys names of statistics.
   * @return this builder.
   */
  IOStatisticsStoreBuilder withCounters(String... keys);

  /**
   * Declare a varargs list of gauges to add.
   * @param keys names of statistics.
   * @return this builder.
   */
  IOStatisticsStoreBuilder withGauges(String... keys);

  /**
   * Declare a varargs list of maximums to add.
   * @param keys names of statistics.
   * @return this builder.
   */
  IOStatisticsStoreBuilder withMaximums(String... keys);

  /**
   * Declare a varargs list of minimums to add.
   * @param keys names of statistics.
   * @return this builder.
   */
  IOStatisticsStoreBuilder withMinimums(String... keys);

  /**
   * Declare a varargs list of means to add.
   * @param keys names of statistics.
   * @return this builder.
   */
  IOStatisticsStoreBuilder withMeanStatistics(String... keys);

  /**
   * Add a statistic in the counter, min, max and mean maps for each
   * declared statistic prefix.
   * @param prefixes prefixes for the stats.
   * @return this
   */
  IOStatisticsStoreBuilder withDurationTracking(
      String... prefixes);

  /**
   * Build the collector.
   * @return a new collector.
   */
  IOStatisticsStore build();
}
