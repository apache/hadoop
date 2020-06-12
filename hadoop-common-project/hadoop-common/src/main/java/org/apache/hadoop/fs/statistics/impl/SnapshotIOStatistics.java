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

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.fs.statistics.StatisticsMap;

/**
 * Snapshot of statistics from a different source.
 * <p>
 * It is serializable so that frameworks which can use java serialization
 * to propagate data (Spark, Flink...) can send the statistics
 * back.
 */
class SnapshotIOStatistics implements IOStatistics, Serializable {


  private static final long serialVersionUID = -1762522703841538084L;

  private StatisticsMapSnapshot<Long> counters;

  private StatisticsMapSnapshot<Long> gauges;

  private StatisticsMapSnapshot<Long> minumums;

  private StatisticsMapSnapshot<Long> maximums;

  private StatisticsMapSnapshot<MeanStatistic> meanStatistics;

  /**
   * Construct from a source statistics instance.
   * @param source source stats.
   */
  SnapshotIOStatistics(final IOStatistics source) {
    snapshot(source);
  }

  /**
   * Take a snapshot.
   * @param source statistics source.
   */
  private void snapshot(IOStatistics source) {
    counters = new StatisticsMapSnapshot<>(source.counters());
    gauges = new StatisticsMapSnapshot<>(source.gauges());
    minumums = new StatisticsMapSnapshot<>(source.minumums());
    maximums = new StatisticsMapSnapshot<>(source.maximums());
    meanStatistics = new StatisticsMapSnapshot<>(source.meanStatistics(),
        MeanStatistic::copy);
  }

  /**
   * Empty constructor for deserialization.
   */
  SnapshotIOStatistics() {
  }

  @Override
  public StatisticsMap<Long> counters() {
    return counters;
  }

  @Override
  public StatisticsMap<Long> gauges() {
    return gauges;
  }

  @Override
  public StatisticsMap<Long> minumums() {
    return minumums;
  }

  @Override
  public StatisticsMap<Long> maximums() {
    return maximums;
  }

  @Override
  public StatisticsMap<MeanStatistic> meanStatistics() {
    return meanStatistics;
  }

}
