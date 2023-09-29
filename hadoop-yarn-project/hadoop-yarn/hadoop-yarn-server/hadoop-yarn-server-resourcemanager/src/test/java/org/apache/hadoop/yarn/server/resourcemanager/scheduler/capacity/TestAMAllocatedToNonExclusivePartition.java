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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

public class TestAMAllocatedToNonExclusivePartition {
  private Configuration conf;
  private RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }

  /**
   * Test that AM can be allocated to non-exclusive partition when the config
   * of {@code YarnConfiguration.AM_ALLOW_NON_EXCLUSIVE_ALLOCATION} is true.
   */
  @Test
  public void testAMAllowToNonExclusivePartition() throws Exception {
    conf.setBoolean(YarnConfiguration.AM_ALLOW_NON_EXCLUSIVE_ALLOCATION, true);

    mgr.addToCluserNodeLabels(
        Arrays.asList(NodeLabel.newInstance("x", false))
    );
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"))
    );

    conf = TestUtils.getConfigurationWithDefaultQueueLabels(conf);

    MockRM rm1 = new MockRM(conf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.getRMContext().setNodeLabelManager(mgr);
    Resource resource = Resource.newInstance(8000, 8);
    ((NullRMNodeLabelsManager)mgr).setResourceForLabel(CommonNodeLabelsManager.NO_LABEL, resource);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", resource); // label = x

    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData
            .Builder
            .createWithMemory(200, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c1")
            .withUnmanagedAM(false)
            .build();

    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    // Wait the AM allocated to non-partition node of h1
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    ContainerId containerId;

    // Request a container and it also should be allocated to non-partition node of h1
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm1, containerId, RMContainerState.ALLOCATED));

    rm1.close();
  }
}
