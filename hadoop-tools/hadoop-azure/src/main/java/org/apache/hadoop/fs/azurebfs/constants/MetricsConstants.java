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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Responsible to keep all constant keys related to ABFS metrics.
 */
@InterfaceAudience.Private
public final class MetricsConstants {
    /**
     * Type of ABFS Backoff Metrics: Base or Retry
     */
    public static final String RETRY = "RETRY";
    /**
     * Type of ABFS Backoff Metrics: Base or Retry
     */
    public static final String BASE = "BASE";
    /**
     * Type of ABFS Readfooter Metrics: File
     */
    public static final String FILE = "FILE";
    /**
     * Precision Format for double data type
     */
    public static final String DOUBLE_PRECISION_FORMAT = "%.3f";
    /**
     * Request count that succeeded in x retries
     */
    public static final String REQUEST_COUNT = "$RCTSI$_";
    /**
     * Min Max Average (This refers to the backoff or sleep time between 2 requests)
     */
    public static final String MIN_MAX_AVERAGE = "$MMA$_";
    /**
     * Time unit: Seconds
     */
    public static final String SECONDS = "s";
    /**
     * Number of requests with x retries
     */
    public static final String REQUESTS = "R=";
    /**
     * Number of Bandwidth throttled requests
     */
    public static final String BANDWIDTH_THROTTLED_REQUESTS = "$BWT=";
    /**
     * Number of IOPS throttled requests
     */
    public static final String IOPS_THROTTLED_REQUESTS = "$IT=";
    /**
     * Number of Other throttled requests
     */
    public static final String OTHER_THROTTLED_REQUESTS = "$OT=";
    /**
     * Percentage of requests that are throttled
     */
    public static final String PERCENTAGE_THROTTLED_REQUESTS = "$RT=";
    /**
     * Number of requests which failed due to network errors
     */
    public static final String NETWORK_ERROR_REQUESTS = "$NFR=";
    /**
     * Total number of requests which succeeded without retrying
     */
    public static final String SUCCESS_REQUESTS_WITHOUT_RETRY = "$TRNR=";
    /**
     * Total number of requests which failed
     */
    public static final String FAILED_REQUESTS = "$TRF=";
    /**
     * Total number of requests which were made
     */
    public static final String TOTAL_REQUESTS_COUNT = "$TR=";
    /**
     * Max retry count across all requests
     */
    public static final String MAX_RETRY = "$MRC=";
    /**
     * Special character: Dollar
     */
    public static final String CHAR_DOLLAR = "$";
    /**
     * String to represent the average first read
     */
    public static final String FIRST_READ = ":$FR=";
    /**
     * String to represent the average second read
     */
    public static final String SECOND_READ = "$SR=";
    /**
     * String to represent the average file length
     */
    public static final String FILE_LENGTH = "$FL=";
    /**
     * String to represent the average read length
     */
    public static final String READ_LENGTH = "$RL=";

  // Indicates a successful scale-up operation
  public static final int SCALE_UP = 1;

  // Indicates a successful scale-down operation
  public static final int SCALE_DOWN = -1;

  // Indicates a down-scale was requested but already at minimum
  public static final int NO_SCALE_DOWN_AT_MIN = -2;

  // Indicates an up-scale was requested but already at maximum
  public static final int NO_SCALE_UP_AT_MAX = 2;

  // Indicates no scaling action was taken
  public static final int SCALE_NONE = 0;

  // Indicates no action is needed based on current metrics
  public static final int NO_ACTION_NEEDED = 3;

  // Indicates a successful scale-up operation
  public static final String SCALE_DIRECTION_UP = "I";

  // Indicates a successful scale-down operation
  public static final String SCALE_DIRECTION_DOWN = "D";

  // Indicates a down-scale was requested but pool is already at minimum
  public static final String SCALE_DIRECTION_NO_DOWN_AT_MIN = "-D";

  // Indicates an up-scale was requested but pool is already at maximum
  public static final String SCALE_DIRECTION_NO_UP_AT_MAX = "+F";

  // Indicates no scaling action is needed based on current metrics
  public static final String SCALE_DIRECTION_NO_ACTION_NEEDED = "NA";

    // Private constructor to prevent instantiation
    private MetricsConstants() {
        throw new AssertionError("Cannot instantiate MetricsConstants");
    }
}
