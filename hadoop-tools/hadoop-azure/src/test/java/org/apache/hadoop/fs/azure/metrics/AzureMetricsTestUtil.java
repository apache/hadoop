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

package org.apache.hadoop.fs.azure.metrics;

import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_BYTES_READ;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_BYTES_WRITTEN;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_RAW_BYTES_DOWNLOADED;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_RAW_BYTES_UPLOADED;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_WEB_RESPONSES;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

public final class AzureMetricsTestUtil {
  public static long getLongGaugeValue(AzureFileSystemInstrumentation instrumentation,
      String gaugeName) {
	  return getLongGauge(gaugeName, getMetrics(instrumentation));
  }
  
  /**
   * Gets the current value of the given counter.
   */
  public static long getLongCounterValue(AzureFileSystemInstrumentation instrumentation,
      String counterName) {
    return getLongCounter(counterName, getMetrics(instrumentation));
  }



  /**
   * Gets the current value of the wasb_bytes_written_last_second counter.
   */
  public static long getCurrentBytesWritten(AzureFileSystemInstrumentation instrumentation) {
    return getLongGaugeValue(instrumentation, WASB_BYTES_WRITTEN);
  }

  /**
   * Gets the current value of the wasb_bytes_read_last_second counter.
   */
  public static long getCurrentBytesRead(AzureFileSystemInstrumentation instrumentation) {
    return getLongGaugeValue(instrumentation, WASB_BYTES_READ);
  }

  /**
   * Gets the current value of the wasb_raw_bytes_uploaded counter.
   */
  public static long getCurrentTotalBytesWritten(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounterValue(instrumentation, WASB_RAW_BYTES_UPLOADED);
  }

  /**
   * Gets the current value of the wasb_raw_bytes_downloaded counter.
   */
  public static long getCurrentTotalBytesRead(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounterValue(instrumentation, WASB_RAW_BYTES_DOWNLOADED);
  }

  /**
   * Gets the current value of the asv_web_responses counter.
   */
  public static long getCurrentWebResponses(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounter(WASB_WEB_RESPONSES, getMetrics(instrumentation));
  }
}
