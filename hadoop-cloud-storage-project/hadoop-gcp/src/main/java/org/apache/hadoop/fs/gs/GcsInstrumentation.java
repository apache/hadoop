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

package org.apache.hadoop.fs.gs;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

class GcsInstrumentation implements Closeable, IOStatisticsSource {
  private final IOStatisticsStore instanceIOStatistics;

  GcsInstrumentation() {
    IOStatisticsStoreBuilder storeBuilder = iostatisticsStore();

    // declare all counter statistics
    EnumSet.allOf(GcsStatistics.class).stream()
        .filter(statistic ->
            statistic.getType() == StatisticTypeEnum.TYPE_COUNTER)
        .forEach(stat -> {
          storeBuilder.withCounters(stat.getSymbol());
        });

    EnumSet.allOf(GcsStatistics.class).stream()
        .filter(statistic ->
            statistic.getType() == StatisticTypeEnum.TYPE_DURATION)
        .forEach(stat -> {
          storeBuilder.withDurationTracking(stat.getSymbol());
        });

    this.instanceIOStatistics = storeBuilder.build();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public IOStatisticsStore getIOStatistics() {
    return instanceIOStatistics;
  }
}
