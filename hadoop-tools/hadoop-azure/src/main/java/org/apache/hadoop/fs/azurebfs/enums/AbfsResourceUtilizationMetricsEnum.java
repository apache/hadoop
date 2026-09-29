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
 * Defines the contract for all ABFS resource-level metric keys.
 * <p>
 * Metric enums implementing this interface supply the metric name and its
 * {@link StatisticTypeEnum} (e.g., gauge or counter), allowing consistent
 * registration and updates across ABFS metric sources.
 * </p>
 */
public interface AbfsResourceUtilizationMetricsEnum {

  /**
   * Returns the unique metric name used for registration and reporting.
   *
   * @return the metric name
   */
  String getName();

  /**
   * Returns the statistic type associated with this metric
   * (gauge, counter, etc.).
   *
   * @return the metric's {@link StatisticTypeEnum}
   */
  StatisticTypeEnum getStatisticType();
}

