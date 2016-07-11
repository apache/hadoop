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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestContainerMetrics {

  @Test
  public void testContainerMetricsFlow() throws InterruptedException {
    final String ERR = "Error in number of records";

    // Create a dummy MetricsSystem
    MetricsSystem system = mock(MetricsSystem.class);
    doReturn(this).when(system).register(anyString(), anyString(), any());

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    ContainerId containerId = mock(ContainerId.class);
    ContainerMetrics metrics = ContainerMetrics.forContainer(containerId,
        100, 1);

    metrics.recordMemoryUsage(1024);
    metrics.getMetrics(collector, true);
    assertEquals(ERR, 0, collector.getRecords().size());

    Thread.sleep(110);
    metrics.getMetrics(collector, true);
    assertEquals(ERR, 1, collector.getRecords().size());
    collector.clear();

    Thread.sleep(110);
    metrics.getMetrics(collector, true);
    assertEquals(ERR, 1, collector.getRecords().size());
    collector.clear();

    metrics.finished();
    metrics.getMetrics(collector, true);
    assertEquals(ERR, 1, collector.getRecords().size());
    collector.clear();

    metrics.getMetrics(collector, true);
    assertEquals(ERR, 0, collector.getRecords().size());

    Thread.sleep(110);
    metrics.getMetrics(collector, true);
    assertEquals(ERR, 0, collector.getRecords().size());
  }

  @Test
  public void testContainerMetricsLimit() throws InterruptedException {
    final String ERR = "Error in number of records";

    MetricsSystem system = mock(MetricsSystem.class);
    doReturn(this).when(system).register(anyString(), anyString(), any());

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    ContainerId containerId = mock(ContainerId.class);
    ContainerMetrics metrics = ContainerMetrics.forContainer(containerId,
        100, 1);

    int anyPmemLimit = 1024;
    int anyVmemLimit = 2048;
    int anyVcores = 10;
    String anyProcessId = "1234";

    metrics.recordResourceLimit(anyVmemLimit, anyPmemLimit, anyVcores);
    metrics.recordProcessId(anyProcessId);

    Thread.sleep(110);
    metrics.getMetrics(collector, true);
    assertEquals(ERR, 1, collector.getRecords().size());
    MetricsRecord record = collector.getRecords().get(0);

    MetricsRecords.assertTag(record, ContainerMetrics.PROCESSID_INFO.name(),
        anyProcessId);

    MetricsRecords.assertMetric(record, ContainerMetrics
        .PMEM_LIMIT_METRIC_NAME, anyPmemLimit);
    MetricsRecords.assertMetric(record, ContainerMetrics.VMEM_LIMIT_METRIC_NAME, anyVmemLimit);
    MetricsRecords.assertMetric(record, ContainerMetrics.VCORE_LIMIT_METRIC_NAME, anyVcores);

    collector.clear();
  }

  @Test
  public void testContainerMetricsFinished() throws InterruptedException {
    MetricsSystemImpl system = new MetricsSystemImpl();
    system.init("test");
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    ApplicationId appId = ApplicationId.newInstance(1234, 3);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 4);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    ContainerMetrics metrics1 = ContainerMetrics.forContainer(system,
        containerId1, 1, 0);
    ContainerId containerId2 = ContainerId.newContainerId(appAttemptId, 2);
    ContainerMetrics metrics2 = ContainerMetrics.forContainer(system,
        containerId2, 1, 0);
    ContainerId containerId3 = ContainerId.newContainerId(appAttemptId, 3);
    ContainerMetrics metrics3 = ContainerMetrics.forContainer(system,
        containerId3, 1, 0);
    metrics1.finished();
    metrics2.finished();
    system.sampleMetrics();
    system.sampleMetrics();
    Thread.sleep(100);
    // verify metrics1 is unregistered
    assertTrue(metrics1 != ContainerMetrics.forContainer(
        system, containerId1, 1, 0));
    // verify metrics2 is unregistered
    assertTrue(metrics2 != ContainerMetrics.forContainer(
        system, containerId2, 1, 0));
    // verify metrics3 is still registered
    assertTrue(metrics3 == ContainerMetrics.forContainer(
        system, containerId3, 1, 0));
    // move stop() to the end to verify registering containerId1 and
    // containerId2 won't get MetricsException thrown.
    system.stop();
    system.shutdown();
  }
}
