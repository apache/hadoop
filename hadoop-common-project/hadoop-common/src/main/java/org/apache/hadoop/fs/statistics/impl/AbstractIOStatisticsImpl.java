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

import java.util.Set;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.fs.statistics.StatisticsMap;

/**
 * The base implementation returns an empty map for
 * all the accessors.
 *
 */
public abstract class AbstractIOStatisticsImpl implements IOStatistics {

  @Override
  public Long getStatistic(final String key) {
    return counters().get(key);
  }

  @Override
  public boolean isTracked(final String key) {
    return counters().containsKey(key);
  }

  @Override
  public Set<String> keys() {
    return counters().keySet();
  }


  @Override
  public StatisticsMap<Long> counters() {
    return EmptyStatisticsMap.of();
  }

  @Override
  public StatisticsMap<Long> gauges() {
    return EmptyStatisticsMap.of();
  }

  @Override
  public StatisticsMap<Long> minumums() {
    return EmptyStatisticsMap.of();
  }

  @Override
  public StatisticsMap<Long> maximums() {
    return EmptyStatisticsMap.of();
  }

  @Override
  public StatisticsMap<MeanStatistic> meanStatistics() {
    return EmptyStatisticsMap.of();
  }
}
