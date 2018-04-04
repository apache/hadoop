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

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestContainerMetrics {

  @Test
  public void testContainerMetricsFlow() throws InterruptedException {
    final String ERR = "Error in number of records";

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
    assertEquals(ERR, 1, collector.getRecords().size());
    collector.clear();

    Thread.sleep(110);
    metrics.getMetrics(collector, true);
    assertEquals(ERR, 1, collector.getRecords().size());
  }

  @Test
  public void testContainerMetricsLimit() throws InterruptedException {
    final String ERR = "Error in number of records";

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    ContainerId containerId = mock(ContainerId.class);
    ContainerMetrics metrics = ContainerMetrics.forContainer(containerId,
        100, 1);

    int anyPmemLimit = 1024;
    int anyVmemLimit = 2048;
    int anyVcores = 10;
    long anyLaunchDuration = 20L;
    long anyLocalizationDuration = 1000L;
    String anyProcessId = "1234";

    metrics.recordResourceLimit(anyVmemLimit, anyPmemLimit, anyVcores);
    metrics.recordProcessId(anyProcessId);
    metrics.recordStateChangeDurations(anyLaunchDuration,
        anyLocalizationDuration);

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

    MetricsRecords.assertMetric(record,
        ContainerMetrics.LAUNCH_DURATION_METRIC_NAME, anyLaunchDuration);
    MetricsRecords.assertMetric(record,
        ContainerMetrics.LOCALIZATION_DURATION_METRIC_NAME,
        anyLocalizationDuration);

    collector.clear();
  }

  @Test
  public void testContainerMetricsFinished() throws InterruptedException {
    MetricsSystemImpl system = new MetricsSystemImpl();
    system.init("test");

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
    // YARN-5190: move stop() to the end to verify registering containerId1 and
    // containerId2 won't get MetricsException thrown.
    system.stop();
    system.shutdown();
  }

  /**
   * Run a test to submit values for actual memory usage and see if the
   * histogram comes out correctly.
   * @throws Exception
   */
  @Test
  public void testContainerMetricsHistogram() throws Exception {

    // submit 2 values - 1024 and 2048. 75th, 90th, 95th and 99th percentiles
    // will be 2048. 50th percentile will be 1536((1024+2048)/2)
    // if we keep recording 1024 and 2048 in a loop, the 50th percentile
    // will tend closer to 2048
    Map<String, Long> expectedValues = new HashMap<>();
    expectedValues.put("PMemUsageMBHistogram50thPercentileMBs", 1536L);
    expectedValues.put("PMemUsageMBHistogram75thPercentileMBs", 2048L);
    expectedValues.put("PMemUsageMBHistogram90thPercentileMBs", 2048L);
    expectedValues.put("PMemUsageMBHistogram95thPercentileMBs", 2048L);
    expectedValues.put("PMemUsageMBHistogram99thPercentileMBs", 2048L);
    expectedValues.put("PCpuUsagePercentHistogram50thPercentilePercents", 0L);
    expectedValues.put("PCpuUsagePercentHistogram75thPercentilePercents", 0L);
    expectedValues.put("PCpuUsagePercentHistogram90thPercentilePercents", 0L);
    expectedValues.put("PCpuUsagePercentHistogram95thPercentilePercents", 0L);
    expectedValues.put("PCpuUsagePercentHistogram99thPercentilePercents", 0L);
    Set<String> testResults = new HashSet<>();
    int delay = 10;
    int rolloverDelay = 1000;
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    ContainerId containerId = mock(ContainerId.class);
    ContainerMetrics metrics =
        ContainerMetrics.forContainer(containerId, delay, 0);

    metrics.recordMemoryUsage(1024);
    metrics.recordMemoryUsage(2048);
    Thread.sleep(rolloverDelay + 10);
    metrics.getMetrics(collector, true);
    for (MetricsRecord record : collector.getRecords()) {
      for (AbstractMetric metric : record.metrics()) {
        String metricName = metric.name();
        if (expectedValues.containsKey(metricName)) {
          Long expectedValue = expectedValues.get(metricName);
          Assert.assertEquals(
              "Metric " + metricName + " doesn't have expected value",
              expectedValue, metric.value());
          testResults.add(metricName);
        }
      }
    }
    Assert.assertEquals(expectedValues.keySet(), testResults);
  }

  @Test
  public void testContainerMetricsUpdateContainerPid() {
    ContainerId containerId = mock(ContainerId.class);
    ContainerMetrics metrics = ContainerMetrics.forContainer(containerId,
        100, 1);

    String origPid = "1234";
    metrics.recordProcessId(origPid);
    assertEquals(origPid, metrics.registry.getTag(
        ContainerMetrics.PROCESSID_INFO.name()).value());

    String newPid = "4321";
    metrics.recordProcessId(newPid);
    assertEquals(newPid, metrics.registry.getTag(
        ContainerMetrics.PROCESSID_INFO.name()).value());
  }
}
