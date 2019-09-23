/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

/**
 * Metrics source to report number of containers in different states.
 */
@InterfaceAudience.Private
@Metrics(about = "SCM Container Manager Metrics", context = "ozone")
public class SCMContainerMetrics implements MetricsSource {

  private final SCMMXBean scmmxBean;
  private static final String SOURCE =
      SCMContainerMetrics.class.getSimpleName();

  public SCMContainerMetrics(SCMMXBean scmmxBean) {
    this.scmmxBean = scmmxBean;
  }

  public static SCMContainerMetrics create(SCMMXBean scmmxBean) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE, "Storage " +
        "Container Manager Metrics", new SCMContainerMetrics(scmmxBean));
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE);
  }

  @Override
  @SuppressWarnings("SuspiciousMethodCalls")
  public void getMetrics(MetricsCollector collector, boolean all) {
    Map<String, Integer> stateCount = scmmxBean.getContainerStateCount();

    collector.addRecord(SOURCE)
        .addGauge(Interns.info("OpenContainers",
            "Number of open containers"),
            stateCount.get(OPEN.toString()))
        .addGauge(Interns.info("ClosingContainers",
            "Number of containers in closing state"),
            stateCount.get(CLOSING.toString()))
        .addGauge(Interns.info("QuasiClosedContainers",
            "Number of containers in quasi closed state"),
            stateCount.get(QUASI_CLOSED.toString()))
        .addGauge(Interns.info("ClosedContainers",
            "Number of containers in closed state"),
            stateCount.get(CLOSED.toString()))
        .addGauge(Interns.info("DeletingContainers",
            "Number of containers in deleting state"),
            stateCount.get(DELETING.toString()))
        .addGauge(Interns.info("DeletedContainers",
            "Number of containers in deleted state"),
            stateCount.get(DELETED.toString()));
  }
}
