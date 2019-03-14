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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class maintains Node related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "SCM NodeManager Metrics", context = "ozone")
public final class SCMNodeMetrics implements MetricsSource {

  private static final String SOURCE_NAME =
      SCMNodeMetrics.class.getSimpleName();

  private @Metric MutableCounterLong numHBProcessed;
  private @Metric MutableCounterLong numHBProcessingFailed;
  private @Metric MutableCounterLong numNodeReportProcessed;
  private @Metric MutableCounterLong numNodeReportProcessingFailed;

  private final MetricsRegistry registry;
  private final NodeManagerMXBean managerMXBean;
  private final MetricsInfo recordInfo = Interns.info("SCMNodeManager",
      "SCM NodeManager metrics");

  /** Private constructor. */
  private SCMNodeMetrics(NodeManagerMXBean managerMXBean) {
    this.managerMXBean = managerMXBean;
    this.registry = new MetricsRegistry(recordInfo);
  }

  /**
   * Create and returns SCMNodeMetrics instance.
   *
   * @return SCMNodeMetrics
   */
  public static SCMNodeMetrics create(NodeManagerMXBean managerMXBean) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "SCM NodeManager Metrics",
        new SCMNodeMetrics(managerMXBean));
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

  /**
   * Get aggregated counter and gauage metrics.
   */
  @Override
  @SuppressWarnings("SuspiciousMethodCalls")
  public void getMetrics(MetricsCollector collector, boolean all) {
    Map<String, Integer> nodeCount = managerMXBean.getNodeCount();
    Map<String, Long> nodeInfo = managerMXBean.getNodeInfo();

    registry.snapshot(
        collector.addRecord(registry.info()) // Add annotated ones first
            .addGauge(Interns.info(
                "HealthyNodes",
                "Number of healthy datanodes"),
                nodeCount.get(HEALTHY.toString()))
            .addGauge(Interns.info("StaleNodes",
                "Number of stale datanodes"),
                nodeCount.get(STALE.toString()))
            .addGauge(Interns.info("DeadNodes",
                "Number of dead datanodes"),
                nodeCount.get(DEAD.toString()))
            .addGauge(Interns.info("DecommissioningNodes",
                "Number of decommissioning datanodes"),
                nodeCount.get(DECOMMISSIONING.toString()))
            .addGauge(Interns.info("DecommissionedNodes",
                "Number of decommissioned datanodes"),
                nodeCount.get(DECOMMISSIONED.toString()))
            .addGauge(Interns.info("DiskCapacity",
                "Total disk capacity"),
                nodeInfo.get("DISKCapacity"))
            .addGauge(Interns.info("DiskUsed",
                "Total disk capacity used"),
                nodeInfo.get("DISKUsed"))
            .addGauge(Interns.info("DiskRemaining",
                "Total disk capacity remaining"),
                nodeInfo.get("DISKRemaining"))
            .addGauge(Interns.info("SSDCapacity",
                "Total ssd capacity"),
                nodeInfo.get("SSDCapacity"))
            .addGauge(Interns.info("SSDUsed",
                "Total ssd capacity used"),
                nodeInfo.get("SSDUsed"))
            .addGauge(Interns.info("SSDRemaining",
                "Total disk capacity remaining"),
                nodeInfo.get("SSDRemaining")),
        all);
  }
}
