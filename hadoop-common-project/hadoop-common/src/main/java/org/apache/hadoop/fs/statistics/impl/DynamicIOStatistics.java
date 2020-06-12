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

import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.fs.statistics.StatisticsMap;

/**
 * These statistics are dynamically evaluated by the supplied
 * String -&gt; type functions.
 * <p></p>
 * This allows statistic sources to supply a list of callbacks used to
 * generate the statistics on demand; similar to some of the Coda Hale metrics.
 * <p></p>
 * The evaluation actually takes place during the iteration's {@code next()}
 * call.
 */
final class DynamicIOStatistics
    extends AbstractIOStatisticsImpl {

  /**
   * Counter evaluators.
   */
  private final EvaluatingStatisticsMap<Long> counters
      = new EvaluatingStatisticsMap<>();

  private final EvaluatingStatisticsMap<Long> gauges
      = new EvaluatingStatisticsMap<>();

  private final EvaluatingStatisticsMap<Long> minumums
      = new EvaluatingStatisticsMap<>();

  private final EvaluatingStatisticsMap<Long> maximums
      = new EvaluatingStatisticsMap<>();

  private final EvaluatingStatisticsMap<MeanStatistic> meanStatistics
      = new EvaluatingStatisticsMap<>(MeanStatistic::copy);

  DynamicIOStatistics() {
  }

  @Override
  public EvaluatingStatisticsMap<Long> counters() {
    return counters;
  }

  @Override
  public EvaluatingStatisticsMap<Long> gauges() {
    return gauges;
  }

  @Override
  public EvaluatingStatisticsMap<Long> minumums() {
    return minumums;
  }

  @Override
  public EvaluatingStatisticsMap<Long> maximums() {
    return maximums;
  }

  @Override
  public StatisticsMap<MeanStatistic> meanStatistics() {
    return meanStatistics;
  }

}
