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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for checking Scheduling with allocationRequestId, i.e. mapping of
 * allocated containers to the original client {@code ResourceRequest}.
 */
public class TestSchedulingWithAllocationRequestId
    extends ParameterizedSchedulerTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSchedulingWithAllocationRequestId.class);
  private static final int GB = 1024;

  public TestSchedulingWithAllocationRequestId(SchedulerType type) throws IOException {
    super(type);
  }

  @Override
  public YarnConfiguration getConf() {
    YarnConfiguration conf = super.getConf();
    if (getSchedulerType().equals(SchedulerType.FAIR)) {
      // Some tests here rely on being able to assign multiple containers with
      // a single heartbeat
      conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    }
    return conf;
  }

  @Test (timeout = 10000)
  public void testMultipleAllocationRequestIds() throws Exception {
    YarnConfiguration conf = getConf();
    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * GB);
      MockNM nm2 = rm.registerNode("127.0.0.2:5678", 4 * GB);
      RMApp app1 = rm.submitApp(2048);
      // kick the scheduling
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      // send requests for containers with id 10 & 20
      am1.allocate(am1.createReq(
          new String[] {"127.0.0.1"}, 2 * GB, 1, 1, 10L), null);
      am1.allocate(am1.createReq(
          new String[] {"127.0.0.2"}, 2 * GB, 1, 2, 20L), null);

      // check if request id 10 is satisfied
      AllocateResponse allocResponse = waitForAllocResponse(rm, am1, nm1, 1);
      List<Container> allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(1, allocated.size());
      checkAllocatedContainer(allocated.get(0), 2 * GB, nm1.getNodeId(), 10);

      // check now if request id 20 is satisfied
      allocResponse = waitForAllocResponse(rm, am1, nm2, 2);
      allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(2, allocated.size());
      for (Container container : allocated) {
        checkAllocatedContainer(container, 2 * GB, nm2.getNodeId(), 20);
      }
    } finally {
      if (rm != null) {
        rm.stop();
      }
    }
  }

  @Test (timeout = 10000)
  public void testMultipleAllocationRequestDiffPriority() throws Exception {
    YarnConfiguration conf = getConf();
    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * GB);
      MockNM nm2 = rm.registerNode("127.0.0.2:5678", 4 * GB);
      RMApp app1 = rm.submitApp(2048);
      // kick the scheduling
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      // send requests for containers with id 10 & 20
      am1.allocate(am1.createReq(
          new String[] {"127.0.0.1"}, 2 * GB, 2, 1, 10L), null);
      am1.allocate(am1.createReq(
          new String[] {"127.0.0.2"}, 2 * GB, 1, 2, 20L), null);

      // check if request id 20 is satisfied first
      AllocateResponse allocResponse = waitForAllocResponse(rm, am1, nm2, 2);
      List<Container> allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(2, allocated.size());
      for (Container container : allocated) {
        checkAllocatedContainer(container, 2 * GB, nm2.getNodeId(), 20);
      }

      // check now if request id 10 is satisfied
      allocResponse = waitForAllocResponse(rm, am1, nm1, 1);
      allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(1, allocated.size());
      checkAllocatedContainer(allocated.get(0), 2 * GB, nm1.getNodeId(), 10);
    } finally {
      if (rm != null) {
        rm.stop();
      }
    }
  }

  private void checkAllocatedContainer(Container allocated, int memory,
      NodeId nodeId, long allocationRequestId) {
    Assert.assertEquals(memory, allocated.getResource().getMemorySize());
    Assert.assertEquals(nodeId, allocated.getNodeId());
    Assert.assertEquals(allocationRequestId,
        allocated.getAllocationRequestId());
  }

  @Test (timeout = 10000)
  public void testMultipleAppsWithAllocationReqId() throws Exception {
    YarnConfiguration conf = getConf();
    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      // Register node1
      String host0 = "host_0";
      String host1 = "host_1";
      MockNM nm1 =
          new MockNM(host0 + ":1234", 8 * GB, rm.getResourceTrackerService());
      nm1.registerNode();

      // Register node2
      MockNM nm2 =
          new MockNM(host1 + ":2351", 8 * GB, rm.getResourceTrackerService());
      nm2.registerNode();

      // submit 1st app
      RMApp app1 = rm.submitApp(1 * GB, "user_0", "a1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      // Submit app1 RR with allocationReqId = 5
      int numContainers = 1;
      am1.allocate(am1.createReq(
          new String[] {host0, host1}, 1 * GB, 1, numContainers, 5L), null);

      // wait for container to be allocated.
      AllocateResponse allocResponse = waitForAllocResponse(rm, am1, nm1, 1);
      List<Container> allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(1, allocated.size());
      checkAllocatedContainer(allocated.get(0), 1 * GB, nm1.getNodeId(), 5L);

      // Submit another application
      RMApp app2 = rm.submitApp(1 * GB, "user_1", "a2");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

      // Submit app2 RR with allocationReqId = 5
      am2.allocate(am1.createReq(
          new String[] {host0, host1}, 2 * GB, 1, numContainers, 5L), null);

      // wait for container to be allocated.
      allocResponse = waitForAllocResponse(rm, am2, nm2, 1);
      allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(1, allocated.size());
      checkAllocatedContainer(allocated.get(0), 2 * GB, nm2.getNodeId(), 5L);

      // Now submit app2 RR with allocationReqId = 10
      am2.allocate(am1.createReq(
          new String[] {host0, host1}, 3 * GB, 1, numContainers, 10L), null);

      // wait for container to be allocated.
      allocResponse = waitForAllocResponse(rm, am2, nm1, 1);
      allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(1, allocated.size());
      checkAllocatedContainer(allocated.get(0), 3 * GB, nm1.getNodeId(), 10L);

      // Now submit app1 RR with allocationReqId = 10
      am1.allocate(am1.createReq(
          new String[] {host0, host1}, 4 * GB, 1, numContainers, 10L), null);

      // wait for container to be allocated.
      allocResponse = waitForAllocResponse(rm, am1, nm2, 1);
      allocated = allocResponse.getAllocatedContainers();
      Assert.assertEquals(1, allocated.size());
      checkAllocatedContainer(allocated.get(0), 4 * GB, nm2.getNodeId(), 10L);
    } finally {
      if (rm != null) {
        rm.stop();
      }
    }
  }

  private AllocateResponse waitForAllocResponse(MockRM rm, MockAM am, MockNM nm,
      int size) throws Exception {
    AllocateResponse allocResponse = am.doHeartbeat();
    while (allocResponse.getAllocatedContainers().size() < size) {
      LOG.info("Waiting for containers to be created for app...");
      nm.nodeHeartbeat(true);
      ((AbstractYarnScheduler) rm.getResourceScheduler()).update();
      Thread.sleep(100);
      allocResponse = am.doHeartbeat();
    }
    return allocResponse;
  }

}
