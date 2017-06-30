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
package org.apache.hadoop.hdfs.client.impl.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableRollingAverages;

/**
 * This class maintains a metric of rolling average latency for short circuit
 * reads.
 */
@InterfaceAudience.Private
@Metrics(name="HdfsShortCircuitReads",
         about="Block Reader Local's Short Circuit Read latency",
         context="dfs")
public class BlockReaderLocalMetrics {

  @Metric(value = "short circuit read operation rate", valueName = "LatencyMs")
  private MutableRollingAverages shortCircuitReadRollingAverages;

  private static final String SHORT_CIRCUIT_READ_METRIC_REGISTERED_NAME =
      "HdfsShortCircuitReads";
  private static final String SHORT_CIRCUIT_LOCAL_READS_METRIC_VALUE_NAME =
      "ShortCircuitLocalReads";

  public static BlockReaderLocalMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    BlockReaderLocalMetrics metrics = new BlockReaderLocalMetrics();

    ms.register(
        SHORT_CIRCUIT_READ_METRIC_REGISTERED_NAME, null, metrics);
    return metrics;
  }

  /**
   * Adds short circuit read elapsed time.
   */
  public void addShortCircuitReadLatency(final long latency) {
    shortCircuitReadRollingAverages.add(
        SHORT_CIRCUIT_LOCAL_READS_METRIC_VALUE_NAME, latency);
  }

  /**
   * Collects states maintained in {@link ThreadLocal}, if any.
   */
  public void collectThreadLocalStates() {
    shortCircuitReadRollingAverages.collectThreadLocalStates();
  }

  /**
   * Get the MutableRollingAverage metric for testing only.
   * @return
   */
  @VisibleForTesting
  public MutableRollingAverages getShortCircuitReadRollingAverages() {
    return shortCircuitReadRollingAverages;
  }
}
