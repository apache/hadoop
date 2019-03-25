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
package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * The client metrics for the Storage Container protocol.
 */
@InterfaceAudience.Private
@Metrics(about = "Storage Container Client Metrics", context = "dfs")
public class XceiverClientMetrics {
  public static final String SOURCE_NAME = XceiverClientMetrics.class
      .getSimpleName();

  private @Metric MutableCounterLong pendingOps;
  private @Metric MutableCounterLong totalOps;
  private MutableCounterLong[] pendingOpsArray;
  private MutableCounterLong[] opsArray;
  private MutableRate[] containerOpsLatency;
  private MetricsRegistry registry;

  public XceiverClientMetrics() {
    int numEnumEntries = ContainerProtos.Type.values().length;
    this.registry = new MetricsRegistry(SOURCE_NAME);

    this.pendingOpsArray = new MutableCounterLong[numEnumEntries];
    this.opsArray = new MutableCounterLong[numEnumEntries];
    this.containerOpsLatency = new MutableRate[numEnumEntries];
    for (int i = 0; i < numEnumEntries; i++) {
      pendingOpsArray[i] = registry.newCounter(
          "numPending" + ContainerProtos.Type.forNumber(i + 1),
          "number of pending" + ContainerProtos.Type.forNumber(i + 1) + " ops",
          (long) 0);
      opsArray[i] = registry
          .newCounter("opCount" + ContainerProtos.Type.forNumber(i + 1),
              "number of" + ContainerProtos.Type.forNumber(i + 1) + " ops",
              (long) 0);

      containerOpsLatency[i] = registry.newRate(
          ContainerProtos.Type.forNumber(i + 1) + "Latency",
          "latency of " + ContainerProtos.Type.forNumber(i + 1)
          + " ops");
    }
  }

  public static XceiverClientMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Storage Container Client Metrics",
        new XceiverClientMetrics());
  }

  public void incrPendingContainerOpsMetrics(ContainerProtos.Type type) {
    pendingOps.incr();
    totalOps.incr();
    opsArray[type.ordinal()].incr();
    pendingOpsArray[type.ordinal()].incr();
  }

  public void decrPendingContainerOpsMetrics(ContainerProtos.Type type) {
    pendingOps.incr(-1);
    pendingOpsArray[type.ordinal()].incr(-1);
  }

  public void addContainerOpsLatency(ContainerProtos.Type type,
      long latencyNanos) {
    containerOpsLatency[type.ordinal()].add(latencyNanos);
  }

  public long getContainerOpsMetrics(ContainerProtos.Type type) {
    return pendingOpsArray[type.ordinal()].value();
  }

  @VisibleForTesting
  public long getTotalOpCount() {
    return totalOps.value();
  }

  @VisibleForTesting
  public long getContainerOpCountMetrics(ContainerProtos.Type type) {
    return opsArray[type.ordinal()].value();
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
