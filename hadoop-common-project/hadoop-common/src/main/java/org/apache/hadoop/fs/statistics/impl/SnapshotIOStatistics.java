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
import java.util.Map;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Snapshot of statistics from a different source.
 * <p>
 * It is serializable so that frameworks which can use java serialization
 * to propagate data (Spark, Flink...) can send the statistics
 * back.
 */
final class SnapshotIOStatistics implements IOStatistics, Serializable {

  private static final long serialVersionUID = -1762522703841538084L;

  private StatisticsMapSnapshot<Long> counters;

  private StatisticsMapSnapshot<Long> gauges;

  private StatisticsMapSnapshot<Long> minumums;

  private StatisticsMapSnapshot<Long> maximums;

  private StatisticsMapSnapshot<MeanStatistic> meanStatistics;

  /**
   * Construct.
   */
  SnapshotIOStatistics() {
  }

  /**
   * Take a snapshot.
   * @param source statistics source.
   */
  public void snapshot(IOStatistics source) {
    checkNotNull(source);
    counters = new StatisticsMapSnapshot<>(source.counters());
    gauges = new StatisticsMapSnapshot<>(source.gauges());
    minumums = new StatisticsMapSnapshot<>(source.minimums());
    maximums = new StatisticsMapSnapshot<>(source.maximums());
    meanStatistics = new StatisticsMapSnapshot<>(source.meanStatistics(),
        MeanStatistic::copy);
  }

  @Override
  public Map<String, Long> counters() {
    return counters;
  }

  @Override
  public Map<String, Long> gauges() {
    return gauges;
  }

  @Override
  public Map<String, Long> minimums() {
    return minumums;
  }

  @Override
  public Map<String, Long> maximums() {
    return maximums;
  }

  @Override
  public Map<String, MeanStatistic> meanStatistics() {
    return meanStatistics;
  }

}
