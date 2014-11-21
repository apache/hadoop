/*
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
package org.apache.hadoop.yarn.server.nodemanager.metrics;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.test.MetricsAsserts.*;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import org.junit.Test;

public class TestNodeManagerMetrics {
  static final int GiB = 1024; // MiB

  @Test public void testNames() {
    NodeManagerMetrics metrics = NodeManagerMetrics.create();
    Resource total = Records.newRecord(Resource.class);
    total.setMemory(8*GiB);
    total.setVirtualCores(16);
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(1*GiB);
    resource.setVirtualCores(2);


    metrics.addResource(total);

    for (int i = 5; i-- > 0;) {
      metrics.launchedContainer();
      metrics.allocateContainer(resource);
    }

    metrics.initingContainer();
    metrics.endInitingContainer();
    metrics.runningContainer();
    metrics.endRunningContainer();
    metrics.completedContainer();
    metrics.releaseContainer(resource);

    metrics.failedContainer();
    metrics.releaseContainer(resource);

    metrics.killedContainer();
    metrics.releaseContainer(resource);

    metrics.initingContainer();
    metrics.runningContainer();
    metrics.addContainerLaunchDuration(1);

    checkMetrics(5, 1, 1, 1, 1, 1, 2, 2, 6, 4, 12);
  }

  private void checkMetrics(int launched, int completed, int failed, int killed,
                            int initing, int running, int allocatedGB,
                            int allocatedContainers, int availableGB, int allocatedVCores, int availableVCores) {
    MetricsRecordBuilder rb = getMetrics("NodeManagerMetrics");
    assertCounter("ContainersLaunched", launched, rb);
    assertCounter("ContainersCompleted", completed, rb);
    assertCounter("ContainersFailed", failed, rb);
    assertCounter("ContainersKilled", killed, rb);
    assertGauge("ContainersIniting", initing, rb);
    assertGauge("ContainersRunning", running, rb);
    assertGauge("AllocatedGB", allocatedGB, rb);
    assertGauge("AllocatedVCores", allocatedVCores, rb);
    assertGauge("AllocatedContainers", allocatedContainers, rb);
    assertGauge("AvailableGB", availableGB, rb);
    assertGauge("AvailableVCores",availableVCores, rb);

  }
}
