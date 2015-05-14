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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * <p>Status of Log aggregation.</p>
 */
public enum LogAggregationStatus {

  /** Log Aggregation is Disabled. */
  DISABLED,

  /** Log Aggregation does not Start. */
  NOT_START,

  /** Log Aggregation is Running. */
  RUNNING,

  /** Log Aggregation is Running, but has failures in previous cycles. */
  RUNNING_WITH_FAILURE,
  /**
   * Log Aggregation is Succeeded. All of the logs have been aggregated
   * successfully.
   */
  SUCCEEDED,

  /**
   * Log Aggregation is completed. But at least one of the logs have not been
   * aggregated.
   */
  FAILED,

  /**
   * The application is finished, but the log aggregation status is not updated
   * for a long time. 
   * @see YarnConfiguration#LOG_AGGREGATION_STATUS_TIME_OUT_MS
   */
  TIME_OUT
}
