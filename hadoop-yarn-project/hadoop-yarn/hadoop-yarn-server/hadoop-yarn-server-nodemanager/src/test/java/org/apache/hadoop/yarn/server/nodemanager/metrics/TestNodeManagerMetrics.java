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

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import static org.apache.hadoop.test.MetricsAsserts.*;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNodeManagerMetrics {
  static final int GiB = 1024; // MiB

  private NodeManagerMetrics metrics;

  @Before
  public void setup() {
    DefaultMetricsSystem.initialize("NodeManager");
    metrics = NodeManagerMetrics.create();
  }

  @After
  public void tearDown() {
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testReferenceOfSingletonJvmMetrics()  {
    JvmMetrics jvmMetrics = JvmMetrics.initSingleton("NodeManagerModule", null);
    Assert.assertEquals("NodeManagerMetrics should reference the singleton" +
        " JvmMetrics instance", jvmMetrics, metrics.getJvmMetrics());
  }

  @Test public void testNames() {
    Resource total = Records.newRecord(Resource.class);
    total.setMemorySize(8*GiB);
    total.setVirtualCores(16);
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(512); //512MiB
    resource.setVirtualCores(2);
    Resource largerResource = Records.newRecord(Resource.class);
    largerResource.setMemorySize(1024);
    largerResource.setVirtualCores(2);
    Resource smallerResource = Records.newRecord(Resource.class);
    smallerResource.setMemorySize(256);
    smallerResource.setVirtualCores(1);

    metrics.addResource(total);

    for (int i = 10; i-- > 0;) {
      // allocate 10 containers(allocatedGB: 5GiB, availableGB: 3GiB)
      metrics.launchedContainer();
      metrics.allocateContainer(resource);
    }

    metrics.initingContainer();
    metrics.endInitingContainer();
    metrics.runningContainer();
    metrics.endRunningContainer();
    // Releasing 3 containers(allocatedGB: 3.5GiB, availableGB: 4.5GiB)
    metrics.completedContainer();
    metrics.releaseContainer(resource);

    metrics.failedContainer();
    metrics.releaseContainer(resource);

    metrics.killedContainer();
    metrics.releaseContainer(resource);

    metrics.initingContainer();
    metrics.runningContainer();

    // Increase resource for a container
    metrics.changeContainer(resource, largerResource);
    // Decrease resource for a container
    metrics.changeContainer(resource, smallerResource);

    Assert.assertTrue(!metrics.containerLaunchDuration.changed());
    metrics.addContainerLaunchDuration(1);
    Assert.assertTrue(metrics.containerLaunchDuration.changed());

    // Set node gpu utilization
    metrics.setNodeGpuUtilization(35.5F);

    // ApplicationsRunning expected to be 1
    metrics.runningApplication();
    metrics.runningApplication();
    metrics.endRunningApplication();

    // availableGB is expected to be floored,
    // while allocatedGB is expected to be ceiled.
    // allocatedGB: 3.75GB allocated memory is shown as 4GB
    // availableGB: 4.25GB available memory is shown as 4GB
    checkMetrics(10, 1, 1, 1, 1, 1, 4, 7, 4, 13, 3, 35.5F, 1);

    // Update resource and check available resource again
    metrics.addResource(total);
    MetricsRecordBuilder rb = getMetrics("NodeManagerMetrics");
    assertGauge("AvailableGB", 12, rb);
    assertGauge("AvailableVCores", 19, rb);
  }

  public static void checkMetrics(int launched, int completed, int failed,
      int killed, int initing, int running, int allocatedGB,
      int allocatedContainers, int availableGB, int allocatedVCores,
      int availableVCores, Float nodeGpuUtilization, int applicationsRunning) {
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
    assertGauge("AvailableVCores", availableVCores, rb);
    assertGauge("NodeGpuUtilization", nodeGpuUtilization, rb);
    assertGauge("ApplicationsRunning", applicationsRunning, rb);
  }
}
