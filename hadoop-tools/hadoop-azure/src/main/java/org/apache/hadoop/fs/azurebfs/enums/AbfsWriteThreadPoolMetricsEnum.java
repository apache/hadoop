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
 * Enum representing the set of metrics tracked for the ABFS write thread pool.
 * Each metric entry defines a short name identifier and its corresponding
 * {@link StatisticTypeEnum}, which specifies the type of measurement (e.g., gauge).
 * These metrics are used for monitoring and analyzing the performance and
 * resource utilization of the write thread pool.
 */
public enum AbfsWriteThreadPoolMetricsEnum {

  /** Current number of threads in the write thread pool. */
  CURRENT_POOL_SIZE("CP", StatisticTypeEnum.TYPE_GAUGE),

  /** Maximum configured size of the write thread pool. */
  MAX_POOL_SIZE("MP", StatisticTypeEnum.TYPE_GAUGE),

  /** Number of threads currently executing write operations. */
  ACTIVE_THREADS("AT", StatisticTypeEnum.TYPE_GAUGE),

  /** JVM CPU utilization percentage during write operations. */
  JVM_CPU_UTILIZATION("JvmCpu", StatisticTypeEnum.TYPE_GAUGE),

  JVM_CPU_LOAD("JvmCpuLoad", StatisticTypeEnum.TYPE_GAUGE),

  JVM_CPU_LOAD_OSHI("JvmCpuLoadOshi", StatisticTypeEnum.TYPE_GAUGE),

  /** Overall system CPU utilization percentage observed during writes. */
  CPU_UTILIZATION("Cpu", StatisticTypeEnum.TYPE_GAUGE),

  /** Available heap memory (in GB) during write operations. */
  MEMORY_UTILIZATION("AvlMem", StatisticTypeEnum.TYPE_GAUGE),

  LAST_SCALE_DIRECTION("ScaleDirection", StatisticTypeEnum.TYPE_GAUGE);

  private final String name;
  private final StatisticTypeEnum statisticType;

  /**
   * Constructs a metric definition for the ABFS write thread pool.
   *
   * @param name  the short name identifier for the metric.
   * @param type  the {@link StatisticTypeEnum} describing the metric type.
   */
  AbfsWriteThreadPoolMetricsEnum(String name, StatisticTypeEnum type) {
    this.name = name;
    this.statisticType = type;
  }

  /**
   * Returns the short name identifier of the metric.
   *
   * @return the metric name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the {@link StatisticTypeEnum} associated with this metric.
   *
   * @return the metric's statistic type.
   */
  public StatisticTypeEnum getStatisticType() {
    return statisticType;
  }
}

