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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestQueueMetrics;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * The test class for {@link FSQueueMetrics}.
 */
public class TestFSQueueMetrics {
  private static final Configuration CONF = new Configuration();

  private MetricsSystem ms;
  private static final String RESOURCE_NAME = "test1";
  private static final String QUEUE_NAME = "single";

  @Before
  public void setUp() {
    ms = new MetricsSystemImpl();
    QueueMetrics.clearQueueMetrics();
  }

  private FSQueueMetrics setupMetrics(String resourceName) {
    CONF.set(YarnConfiguration.RESOURCE_TYPES, resourceName);
    ResourceUtils.resetResourceTypes(CONF);

    return FSQueueMetrics.forQueue(ms, QUEUE_NAME, null, false, CONF);
  }

  private String getErrorMessage(String metricsType) {
    return metricsType + " is not the expected!";
  }

  /**
   * Test if the metric scheduling policy is set correctly.
   */
  @Test
  public void testSchedulingPolicy() {
    String queueName = "single";

    FSQueueMetrics metrics = FSQueueMetrics.forQueue(ms, queueName, null, false,
        CONF);
    metrics.setSchedulingPolicy("drf");
    checkSchedulingPolicy(queueName, "drf");

    // test resetting the scheduling policy
    metrics.setSchedulingPolicy("fair");
    checkSchedulingPolicy(queueName, "fair");
  }

  private void checkSchedulingPolicy(String queueName, String policy) {
    MetricsSource queueSource = TestQueueMetrics.queueSource(ms, queueName);
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    queueSource.getMetrics(collector, true);
    MetricsRecords.assertTag(collector.getRecords().get(0), "SchedulingPolicy",
        policy);
  }

  @Test
  public void testSetFairShare() {
    FSQueueMetrics metrics = setupMetrics(RESOURCE_NAME);

    Resource res = Resource.newInstance(2048L, 4, ImmutableMap.of(RESOURCE_NAME,
        20L));
    metrics.setFairShare(res);

    assertEquals(getErrorMessage("fairShareMB"),
        2048L, metrics.getFairShareMB());
    assertEquals(getErrorMessage("fairShareVcores"),
        4L, metrics.getFairShareVirtualCores());
    assertEquals(getErrorMessage("fairShareMB"),
        2048L, metrics.getFairShare().getMemorySize());
    assertEquals(getErrorMessage("fairShareVcores"),
        4L, metrics.getFairShare().getVirtualCores());
    assertEquals(getErrorMessage("fairShare for resource: " + RESOURCE_NAME),
        20L, metrics.getFairShare().getResourceValue(RESOURCE_NAME));

    res = Resource.newInstance(2049L, 5);
    metrics.setFairShare(res);

    assertEquals(getErrorMessage("fairShareMB"),
        2049L, metrics.getFairShareMB());
    assertEquals(getErrorMessage("fairShareVcores"),
        5L, metrics.getFairShareVirtualCores());
    assertEquals(getErrorMessage("fairShareMB"),
        2049L, metrics.getFairShare().getMemorySize());
    assertEquals(getErrorMessage("fairShareVcores"),
        5L, metrics.getFairShare().getVirtualCores());
    assertEquals(getErrorMessage("fairShare for resource: " + RESOURCE_NAME),
        0, metrics.getFairShare().getResourceValue(RESOURCE_NAME));
  }

  @Test
  public void testSetSteadyFairShare() {
    FSQueueMetrics metrics = setupMetrics(RESOURCE_NAME);

    Resource res = Resource.newInstance(2048L, 4, ImmutableMap.of(RESOURCE_NAME,
        20L));
    metrics.setSteadyFairShare(res);

    assertEquals(getErrorMessage("steadyFairShareMB"),
        2048L, metrics.getSteadyFairShareMB());
    assertEquals(getErrorMessage("steadyFairShareVcores"),
        4L, metrics.getSteadyFairShareVCores());

    Resource steadyFairShare = metrics.getSteadyFairShare();
    assertEquals(getErrorMessage("steadyFairShareMB"),
        2048L, steadyFairShare.getMemorySize());
    assertEquals(getErrorMessage("steadyFairShareVcores"),
        4L, steadyFairShare.getVirtualCores());
    assertEquals(getErrorMessage("steadyFairShare for resource: " +
            RESOURCE_NAME),
        20L, steadyFairShare.getResourceValue(RESOURCE_NAME));

    res = Resource.newInstance(2049L, 5);
    metrics.setSteadyFairShare(res);

    assertEquals(getErrorMessage("steadyFairShareMB"),
        2049L, metrics.getSteadyFairShareMB());
    assertEquals(getErrorMessage("steadyFairShareVcores"),
        5L, metrics.getSteadyFairShareVCores());

    steadyFairShare = metrics.getSteadyFairShare();
    assertEquals(getErrorMessage("steadyFairShareMB"),
        2049L, steadyFairShare.getMemorySize());
    assertEquals(getErrorMessage("steadyFairShareVcores"),
        5L, steadyFairShare.getVirtualCores());
    assertEquals(getErrorMessage("steadyFairShare for resource: " +
            RESOURCE_NAME),
        0, steadyFairShare.getResourceValue(RESOURCE_NAME));
  }

  @Test
  public void testSetMinShare() {
    FSQueueMetrics metrics = setupMetrics(RESOURCE_NAME);

    Resource res = Resource.newInstance(2048L, 4, ImmutableMap.of(RESOURCE_NAME,
        20L));
    metrics.setMinShare(res);

    assertEquals(getErrorMessage("minShareMB"),
        2048L, metrics.getMinShareMB());
    assertEquals(getErrorMessage("minShareVcores"),
        4L, metrics.getMinShareVirtualCores());
    assertEquals(getErrorMessage("minShareMB"),
        2048L, metrics.getMinShare().getMemorySize());
    assertEquals(getErrorMessage("minShareVcores"),
        4L, metrics.getMinShare().getVirtualCores());
    assertEquals(getErrorMessage("minShare for resource: " + RESOURCE_NAME),
        20L, metrics.getMinShare().getResourceValue(RESOURCE_NAME));

    res = Resource.newInstance(2049L, 5);
    metrics.setMinShare(res);

    assertEquals(getErrorMessage("minShareMB"),
        2049L, metrics.getMinShareMB());
    assertEquals(getErrorMessage("minShareVcores"),
        5L, metrics.getMinShareVirtualCores());
    assertEquals(getErrorMessage("minShareMB"),
        2049L, metrics.getMinShare().getMemorySize());
    assertEquals(getErrorMessage("minShareVcores"),
        5L, metrics.getMinShare().getVirtualCores());
    assertEquals(getErrorMessage("minShare for resource: " + RESOURCE_NAME),
        0, metrics.getMinShare().getResourceValue(RESOURCE_NAME));
  }

  @Test
  public void testSetMaxShare() {
    FSQueueMetrics metrics = setupMetrics(RESOURCE_NAME);

    Resource res = Resource.newInstance(2048L, 4, ImmutableMap.of(RESOURCE_NAME,
        20L));
    metrics.setMaxShare(res);

    assertEquals(getErrorMessage("maxShareMB"),
        2048L, metrics.getMaxShareMB());
    assertEquals(getErrorMessage("maxShareVcores"),
        4L, metrics.getMaxShareVirtualCores());
    assertEquals(getErrorMessage("maxShareMB"),
        2048L, metrics.getMaxShare().getMemorySize());
    assertEquals(getErrorMessage("maxShareVcores"),
        4L, metrics.getMaxShare().getVirtualCores());
    assertEquals(getErrorMessage("maxShare for resource: " + RESOURCE_NAME),
        20L, metrics.getMaxShare().getResourceValue(RESOURCE_NAME));

    res = Resource.newInstance(2049L, 5);
    metrics.setMaxShare(res);

    assertEquals(getErrorMessage("maxShareMB"),
        2049L, metrics.getMaxShareMB());
    assertEquals(getErrorMessage("maxShareVcores"),
        5L, metrics.getMaxShareVirtualCores());
    assertEquals(getErrorMessage("maxShareMB"),
        2049L, metrics.getMaxShare().getMemorySize());
    assertEquals(getErrorMessage("maxShareVcores"),
        5L, metrics.getMaxShare().getVirtualCores());
    assertEquals(getErrorMessage("maxShare for resource: " + RESOURCE_NAME),
        0, metrics.getMaxShare().getResourceValue(RESOURCE_NAME));
  }

  @Test
  public void testSetMaxAMShare() {
    FSQueueMetrics metrics = setupMetrics(RESOURCE_NAME);

    Resource res = Resource.newInstance(2048L, 4, ImmutableMap.of(RESOURCE_NAME,
        20L));
    metrics.setMaxAMShare(res);

    assertEquals(getErrorMessage("maxAMShareMB"),
        2048L, metrics.getMaxAMShareMB());
    assertEquals(getErrorMessage("maxAMShareVcores"),
        4L, metrics.getMaxAMShareVCores());
    assertEquals(getErrorMessage("maxAMShareMB"),
        2048L, metrics.getMaxAMShare().getMemorySize());
    assertEquals(getErrorMessage("maxAMShareVcores"),
        4L, metrics.getMaxAMShare().getVirtualCores());
    assertEquals(getErrorMessage(
        "maxAMShare for resource: " + RESOURCE_NAME),
        20L, metrics.getMaxAMShare().getResourceValue(RESOURCE_NAME));

    res = Resource.newInstance(2049L, 5);
    metrics.setMaxAMShare(res);

    assertEquals(getErrorMessage("maxAMShareMB"),
        2049L, metrics.getMaxAMShareMB());
    assertEquals(getErrorMessage("maxAMShareVcores"),
        5L, metrics.getMaxAMShareVCores());
    assertEquals(getErrorMessage("maxAMShareMB"),
        2049L, metrics.getMaxAMShare().getMemorySize());
    assertEquals(getErrorMessage("maxAMShareVcores"),
        5L, metrics.getMaxAMShare().getVirtualCores());
    assertEquals(getErrorMessage(
        "maxAMShare for resource: " + RESOURCE_NAME),
        0, metrics.getMaxAMShare().getResourceValue(RESOURCE_NAME));
  }

  @Test
  public void testSetAMResourceUsage() {
    FSQueueMetrics metrics = setupMetrics(RESOURCE_NAME);

    Resource res = Resource.newInstance(2048L, 4, ImmutableMap.of(RESOURCE_NAME,
        20L));
    metrics.setAMResourceUsage(res);

    assertEquals(getErrorMessage("AMResourceUsageMB"),
        2048L, metrics.getAMResourceUsageMB());
    assertEquals(getErrorMessage("AMResourceUsageVcores"),
        4L, metrics.getAMResourceUsageVCores());

    Resource amResourceUsage = metrics.getAMResourceUsage();
    assertEquals(getErrorMessage("AMResourceUsageMB"),
        2048L, amResourceUsage.getMemorySize());
    assertEquals(getErrorMessage("AMResourceUsageVcores"),
        4L, amResourceUsage.getVirtualCores());
    assertEquals(getErrorMessage("AMResourceUsage for resource: " +
            RESOURCE_NAME),
        20L, amResourceUsage.getResourceValue(RESOURCE_NAME));

    res = Resource.newInstance(2049L, 5);
    metrics.setAMResourceUsage(res);

    assertEquals(getErrorMessage("AMResourceUsageMB"),
        2049L, metrics.getAMResourceUsageMB());
    assertEquals(getErrorMessage("AMResourceUsageVcores"),
        5L, metrics.getAMResourceUsageVCores());

    amResourceUsage = metrics.getAMResourceUsage();
    assertEquals(getErrorMessage("AMResourceUsageMB"),
        2049L, amResourceUsage.getMemorySize());
    assertEquals(getErrorMessage("AMResourceUsageVcores"),
        5L, amResourceUsage.getVirtualCores());
    assertEquals(getErrorMessage("AMResourceUsage for resource: " +
            RESOURCE_NAME),
        0, amResourceUsage.getResourceValue(RESOURCE_NAME));
  }

  @Test
  public void testSetMaxApps() {
    FSQueueMetrics metrics = setupMetrics(RESOURCE_NAME);
    metrics.setMaxApps(25);
    assertEquals(getErrorMessage("maxApps"), 25L, metrics.getMaxApps());
  }
}
