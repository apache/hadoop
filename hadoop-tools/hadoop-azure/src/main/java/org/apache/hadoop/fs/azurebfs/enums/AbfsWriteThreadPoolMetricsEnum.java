/**
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

package org.apache.hadoop.fs.azurebfs.enums;

import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;

/**
 * Enum representing metrics for ABFS write thread pool monitoring.
 */
public enum AbfsWriteThreadPoolMetricsEnum {
  CURRENT_POOL_SIZE("CP", StatisticTypeEnum.TYPE_GAUGE),
  MAX_POOL_SIZE("MP", StatisticTypeEnum.TYPE_GAUGE),
  ACTIVE_THREADS("AT", StatisticTypeEnum.TYPE_GAUGE),
  JVM_CPU_UTILIZATION("JvmCpu", StatisticTypeEnum.TYPE_GAUGE),
  CPU_UTILIZATION("Cpu", StatisticTypeEnum.TYPE_GAUGE),
  MEMORY_UTILIZATION("AvlMem", StatisticTypeEnum.TYPE_GAUGE);

  private final String name;
  private final StatisticTypeEnum statisticType;

  AbfsWriteThreadPoolMetricsEnum(String name, StatisticTypeEnum type) {
    this.name = name;
    this.statisticType = type;
  }

  public String getName() {
    return name;
  }

  public StatisticTypeEnum getStatisticType() {
    return statisticType;
  }
}
