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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterNodeTracker;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB;
import static org.junit.Assert.assertEquals;

/**
 * Test case for custom resource container allocation.
 * for capacity scheduler
 * */
public class TestCSAllocateCustomResource {

  private YarnConfiguration conf;

  private RMNodeLabelsManager mgr;

  private File resourceTypesFile = null;

  private final int g = 1024;

  private ClusterNodeTracker<FiCaSchedulerNode> nodeTracker;
  private ClusterMetrics metrics;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @After
  public void tearDown() {
    if (resourceTypesFile != null && resourceTypesFile.exists()) {
      resourceTypesFile.delete();
    }
  }

  /**
   * Test containers request custom resource.
   * */
  @Test
  public void testCapacitySchedulerJobWhenConfigureCustomResourceType()
      throws Exception {
    // reset resource types
    ResourceUtils.resetResourceTypes();
    String resourceTypesFileName = "resource-types-test.xml";
    File source = new File(
        conf.getClassLoader().getResource(resourceTypesFileName).getFile());
    resourceTypesFile = new File(source.getParent(), "resource-types.xml");
    FileUtils.copyFile(source, resourceTypesFile);

    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    newConf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);
    newConf.set(CapacitySchedulerConfiguration.getQueuePrefix("root.a")
        + MAXIMUM_ALLOCATION_MB, "4096");
    // We must set this to false to avoid MockRM init configuration with
    // resource-types.xml by ResourceUtils.resetResourceTypes(conf);
    newConf.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES,
        false);
    //start RM
    MockRM rm = new MockRM(newConf);
    rm.start();

    //register node with custom resource
    String customResourceType = "yarn.io/gpu";
    Resource nodeResource = Resources.createResource(4 * g, 4);
    nodeResource.setResourceValue(customResourceType, 10);
    MockNM nm1 = rm.registerNode("h1:1234", nodeResource);

    // submit app
    Resource amResource = Resources.createResource(1 * g, 1);
    amResource.setResourceValue(customResourceType, 1);
    RMApp app1 = rm.submitApp(amResource, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // am request containers
    Resource cResource = Resources.createResource(1 * g, 1);
    amResource.setResourceValue(customResourceType, 1);
    am1.allocate("*", cResource, 2,
        new ArrayList<ContainerId>(), null);

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());

    // Do nm heartbeats 1 times, will allocate a container on nm1
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    rm.drainEvents();
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    rm.close();
  }

  /**
   * Test CS initialized with custom resource types loaded.
   * */
  @Test
  public void testCapacitySchedulerInitWithCustomResourceType()
      throws IOException {
    // reset resource types
    ResourceUtils.resetResourceTypes();
    String resourceTypesFileName = "resource-types-test.xml";
    File source = new File(
        conf.getClassLoader().getResource(resourceTypesFileName).getFile());
    resourceTypesFile = new File(source.getParent(), "resource-types.xml");
    FileUtils.copyFile(source, resourceTypesFile);

    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    newConf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);
    //start RM
    MockRM rm = new MockRM(newConf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Ensure the method can get custom resource type from
    // CapacitySchedulerConfiguration
    Assert.assertNotEquals(0,
        ResourceUtils
            .fetchMaximumAllocationFromConfig(cs.getConfiguration())
            .getResourceValue("yarn.io/gpu"));
    // Ensure custom resource type exists in queue's maximumAllocation
    Assert.assertNotEquals(0,
        cs.getMaximumResourceCapability("a")
            .getResourceValue("yarn.io/gpu"));
    rm.close();
  }

  @Test
  public void testClusterMetricsWithGPU()
      throws Exception {
    metrics = ClusterMetrics.getMetrics();
    // reset resource types
    ResourceUtils.resetResourceTypes();
    String resourceTypesFileName = "resource-types-test.xml";
    File source = new File(
        conf.getClassLoader().getResource(resourceTypesFileName).getFile());
    resourceTypesFile = new File(source.getParent(), "resource-types.xml");
    FileUtils.copyFile(source, resourceTypesFile);

    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    newConf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);
    //start RM
    MockRM rm = new MockRM(newConf);
    rm.start();

    nodeTracker = new ClusterNodeTracker<>();
    MockNodes.resetHostIds();
    Resource nodeResource = Resource.newInstance(4096, 4,
        Collections.singletonMap(GPU_URI, 4L));
    List<RMNode> rmNodes =
        MockNodes.newNodes(2, 4, nodeResource);
    for (RMNode rmNode : rmNodes) {
      nodeTracker.addNode(new FiCaSchedulerNode(rmNode, false));
    }

    // Check GPU inc related cluster metrics.
    assertEquals("Cluster Capability Memory incorrect",
        metrics.getCapabilityMB(), (4096 * 8));
    assertEquals("Cluster Capability Vcores incorrect",
        metrics.getCapabilityVirtualCores(), 4 * 8);
    assertEquals("Cluster Capability GPUs incorrect",
        metrics.getCapabilityGPUs(), 4 * 8);

    for (RMNode rmNode : rmNodes) {
      nodeTracker.removeNode(rmNode.getNodeID());
    }

    // Check GPU dec related cluster metrics.
    assertEquals("Cluster Capability Memory incorrect",
        metrics.getCapabilityMB(), 0);
    assertEquals("Cluster Capability Vcores incorrect",
        metrics.getCapabilityVirtualCores(), 0);
    assertEquals("Cluster Capability GPUs incorrect",
        metrics.getCapabilityGPUs(), 0);
    ClusterMetrics.destroy();
  }
}
