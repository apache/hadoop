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

import java.util.Map;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;

import static java.util.Collections.emptyMap;

/**
 * An empty IO Statistics implementation for classes which always
 * want to return a non-null set of statistics.
 */
final class EmptyIOStatistics extends AbstractIOStatisticsImpl {

  /**
   * The sole instance of this class.
   */
  private static final EmptyIOStatistics INSTANCE = new EmptyIOStatistics();

  private EmptyIOStatistics() {
  }

  @Override
  public Map<String, Long> counters() {
    return emptyMap();
  }

  @Override
  public Map<String, Long> gauges() {
    return emptyMap();
  }

  @Override
  public Map<String, Long> minimums() {
    return emptyMap();
  }

  @Override
  public Map<String, Long> maximums() {
    return emptyMap();
  }

  @Override
  public Map<String, MeanStatistic> meanStatistics() {
    return emptyMap();
  }

  /**
   * Get the single instance of this class.
   * @return a shared, empty instance.
   */
  public static IOStatistics getInstance() {
    return INSTANCE;
  }
}
