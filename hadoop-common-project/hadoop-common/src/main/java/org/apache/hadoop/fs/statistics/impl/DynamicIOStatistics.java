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

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.fs.statistics.MeanStatistic;

/**
 * These statistics are dynamically evaluated by the supplied
 * String -&gt; type functions.
 *
 * This allows statistic sources to supply a list of callbacks used to
 * generate the statistics on demand; similar to some of the Coda Hale metrics.
 *
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

  private final EvaluatingStatisticsMap<Long> minimums
      = new EvaluatingStatisticsMap<>();

  private final EvaluatingStatisticsMap<Long> maximums
      = new EvaluatingStatisticsMap<>();

  private final EvaluatingStatisticsMap<MeanStatistic> meanStatistics
      = new EvaluatingStatisticsMap<>(MeanStatistic::copy);

  DynamicIOStatistics() {
  }

  @Override
  public Map<String, Long> counters() {
    return Collections.unmodifiableMap(counters);
  }

  @Override
  public Map<String, Long> gauges() {
    return Collections.unmodifiableMap(gauges);
  }

  @Override
  public Map<String, Long> minimums() {
    return Collections.unmodifiableMap(minimums);
  }

  @Override
  public Map<String, Long> maximums() {
    return Collections.unmodifiableMap(maximums);
  }

  @Override
  public Map<String, MeanStatistic> meanStatistics() {
    return Collections.unmodifiableMap(meanStatistics);
  }

  /**
   * add a mapping of a key to a counter function.
   * @param key the key
   * @param eval the evaluator
   */
  void addCounterFunction(String key, Function<String, Long> eval) {
    counters.addFunction(key, eval);
  }

  /**
   * add a mapping of a key to a gauge function.
   * @param key the key
   * @param eval the evaluator
   */
  void addGaugeFunction(String key, Function<String, Long> eval) {
    gauges.addFunction(key, eval);
  }

  /**
   * add a mapping of a key to a minimum function.
   * @param key the key
   * @param eval the evaluator
   */
  void addMinimumFunction(String key, Function<String, Long> eval) {
    minimums.addFunction(key, eval);
  }

  /**
   * add a mapping of a key to a maximum function.
   * @param key the key
   * @param eval the evaluator
   */
  void addMaximumFunction(String key, Function<String, Long> eval) {
    maximums.addFunction(key, eval);
  }

  /**
   * add a mapping of a key to a meanStatistic function.
   * @param key the key
   * @param eval the evaluator
   */
  void addMeanStatisticFunction(String key,
      Function<String, MeanStatistic> eval) {
    meanStatistics.addFunction(key, eval);
  }

}
