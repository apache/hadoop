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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MEAN;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MIN;

/**
 * Builder for an IOStatistics store..
 */
final class IOStatisticsStoreBuilderImpl implements
    IOStatisticsStoreBuilder {

  private final List<String> counters = new ArrayList<>();

  private final List<String> gauges = new ArrayList<>();

  private final List<String> minimums = new ArrayList<>();

  private final List<String> maximums = new ArrayList<>();

  private final List<String> meanStatistics = new ArrayList<>();

  @Override
  public IOStatisticsStoreBuilderImpl withCounters(final String... keys) {
    counters.addAll(Arrays.asList(keys));
    return this;
  }

  @Override
  public IOStatisticsStoreBuilderImpl withGauges(final String... keys) {
    gauges.addAll(Arrays.asList(keys));
    return this;
  }

  @Override
  public IOStatisticsStoreBuilderImpl withMaximums(final String... keys) {
    maximums.addAll(Arrays.asList(keys));
    return this;
  }

  @Override
  public IOStatisticsStoreBuilderImpl withMinimums(final String... keys) {
    minimums.addAll(Arrays.asList(keys));
    return this;
  }

  @Override
  public IOStatisticsStoreBuilderImpl withMeanStatistics(
      final String... keys) {
    meanStatistics.addAll(Arrays.asList(keys));
    return this;
  }

  @Override
  public IOStatisticsStoreBuilderImpl withDurationTracking(
      final String... prefixes) {
    for (String p : prefixes) {
      withCounters(p, p + SUFFIX_FAILURES);
      withMinimums(
          p + SUFFIX_MIN,
          p + SUFFIX_FAILURES + SUFFIX_MIN);
      withMaximums(
          p + SUFFIX_MAX,
          p + SUFFIX_FAILURES + SUFFIX_MAX);
      withMeanStatistics(
          p + SUFFIX_MEAN,
          p + SUFFIX_FAILURES + SUFFIX_MEAN);
    }
    return this;
  }

  @Override
  public IOStatisticsStoreBuilderImpl withSampleTracking(
      final String... prefixes) {
    for (String p : prefixes) {
      withCounters(p);
      withMinimums(p);
      withMaximums(p);
      withMeanStatistics(p);
    }
    return this;
  }

  @Override
  public IOStatisticsStore build() {
    return new IOStatisticsStoreImpl(counters, gauges, minimums,
        maximums, meanStatistics);
  }
}
