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

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class maintains Node related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "SCM NodeManager Metrics", context = "ozone")
public final class SCMNodeMetrics {

  private static final String SOURCE_NAME =
      SCMNodeMetrics.class.getSimpleName();

  private @Metric MutableCounterLong numHBProcessed;
  private @Metric MutableCounterLong numHBProcessingFailed;
  private @Metric MutableCounterLong numNodeReportProcessed;
  private @Metric MutableCounterLong numNodeReportProcessingFailed;

  /** Private constructor. */
  private SCMNodeMetrics() { }

  /**
   * Create and returns SCMNodeMetrics instance.
   *
   * @return SCMNodeMetrics
   */
  public static SCMNodeMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "SCM NodeManager Metrics",
        new SCMNodeMetrics());
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  /**
   * Increments number of heartbeat processed count.
   */
  void incNumHBProcessed() {
    numHBProcessed.incr();
  }

  /**
   * Increments number of heartbeat processing failed count.
   */
  void incNumHBProcessingFailed() {
    numHBProcessingFailed.incr();
  }

  /**
   * Increments number of node report processed count.
   */
  void incNumNodeReportProcessed() {
    numNodeReportProcessed.incr();
  }

  /**
   * Increments number of node report processing failed count.
   */
  void incNumNodeReportProcessingFailed() {
    numNodeReportProcessingFailed.incr();
  }

}
