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

/**
 * Enum representing the set of metrics tracked for the ABFS read thread pool.
 * Each metric includes a short name used for reporting and its corresponding
 * {@link StatisticTypeEnum}, which defines how the metric is measured (e.g., gauge).
 */
public enum AbfsReadResourceUtilizationMetricsEnum implements
    AbfsResourceUtilizationMetricsEnum {

  /** Current number of threads in the read thread pool. */
  CURRENT_POOL_SIZE("CP", StatisticTypeEnum.TYPE_GAUGE),

  /** Maximum configured size of the read thread pool. */
  MAX_POOL_SIZE("MP", StatisticTypeEnum.TYPE_GAUGE),

  /** Number of threads currently executing read operations. */
  ACTIVE_THREADS("AT", StatisticTypeEnum.TYPE_GAUGE),

  /** Number of threads currently idle. */
  IDLE_THREADS("IT", StatisticTypeEnum.TYPE_GAUGE),

  /** Recent JVM CPU load value as reported by the JVM (0.0 to 1.0). */
  JVM_CPU_UTILIZATION("JC", StatisticTypeEnum.TYPE_GAUGE),

  /** Overall system-wide CPU utilization percentage during read operations. */
  SYSTEM_CPU_UTILIZATION("SC", StatisticTypeEnum.TYPE_GAUGE),

  /** Available heap memory (in GB) measured during read operations. */
  AVAILABLE_MEMORY("AM", StatisticTypeEnum.TYPE_GAUGE),

  /** Committed heap memory (in GB) measured during read operations. */
  COMMITTED_MEMORY("CM", StatisticTypeEnum.TYPE_GAUGE),

  /** Used heap memory (in GB) measured during read operations. */
  USED_MEMORY("UM", StatisticTypeEnum.TYPE_GAUGE),

  /** Maximum heap memory (in GB) measured during read operations. */
  MAX_HEAP_MEMORY("MM", StatisticTypeEnum.TYPE_GAUGE),

  /** Available heap memory (in GB) measured during read operations. */
  MEMORY_LOAD("ML", StatisticTypeEnum.TYPE_GAUGE),

  /** Direction of the last scaling decision (e.g., scale-up or scale-down). */
  LAST_SCALE_DIRECTION("SD", StatisticTypeEnum.TYPE_GAUGE),

  /** Maximum CPU utilization recorded during the monitoring interval. */
  MAX_CPU_UTILIZATION("MC", StatisticTypeEnum.TYPE_GAUGE),

  /** The process ID (PID) of the running JVM, useful for correlating metrics with system-level process information. */
  JVM_PROCESS_ID("JI", StatisticTypeEnum.TYPE_GAUGE);

  private final String name;
  private final StatisticTypeEnum statisticType;

  /**
   * Constructs a metric enum constant with its short name and type.
   *
   * @param name  the short name or label for the metric.
   * @param type  the {@link StatisticTypeEnum} indicating the metric type.
   */
  AbfsReadResourceUtilizationMetricsEnum(String name, StatisticTypeEnum type) {
    this.name = name;
    this.statisticType = type;
  }

  /**
   * Returns the short name of the metric.
   *
   * @return the metric name.
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Returns the {@link StatisticTypeEnum} associated with this metric.
   *
   * @return the metric's statistic type.
   */
  @Override
  public StatisticTypeEnum getStatisticType() {
    return statisticType;
  }
}
