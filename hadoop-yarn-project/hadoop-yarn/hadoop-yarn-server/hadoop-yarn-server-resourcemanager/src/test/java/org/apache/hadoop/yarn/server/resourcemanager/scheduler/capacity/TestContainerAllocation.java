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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMSecretManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.TestFifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.junit.Test;


public class TestContainerAllocation {

  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);

  private final int GB = 1024;

  @Test(timeout = 3000000)
  public void testExcessReservationThanNodeManagerCapacity() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 2 * GB, 4);
    MockNM nm2 = rm.registerNode("127.0.0.1:2234", 3 * GB, 4);

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(2, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    LOG.info("sending container requests ");
    am1.addRequests(new String[] {"*"}, 3 * GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    int waitCounter = 20;
    LOG.info("heartbeating nm1");
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());

    // No container should be allocated.
    // Internally it should not been reserved.
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 0);

    LOG.info("heartbeating nm2");
    waitCounter = 20;
    nm2.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 1);

    rm.stop();
  }

  // This is to test container tokens are generated when the containers are
  // acquired by the AM, not when the containers are allocated
  @Test
  public void testContainerTokenGeneratedOnPullRequest() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8000);
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    RMContainer container =
        rm1.getResourceScheduler().getRMContainer(containerId2);
    // no container token is generated.
    Assert.assertEquals(containerId2, container.getContainerId());
    Assert.assertNull(container.getContainer().getContainerToken());

    // acquire the container.
    List<Container> containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    Assert.assertEquals(containerId2, containers.get(0).getId());
    // container token is generated.
    Assert.assertNotNull(containers.get(0).getContainerToken());
    rm1.stop();
  }

  @Test
  public void testNormalContainerAllocationWhenDNSUnavailable() throws Exception{
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    // acquire the container.
    SecurityUtilTestHelper.setTokenServiceUseIp(true);
    List<Container> containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    // not able to fetch the container;
    Assert.assertEquals(0, containers.size());

    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    // should be able to fetch the container;
    Assert.assertEquals(1, containers.size());
  }

  private volatile int numRetries = 0;
  private class TestRMSecretManagerService extends RMSecretManagerService {

    public TestRMSecretManagerService(Configuration conf,
        RMContextImpl rmContext) {
      super(conf, rmContext);
    }
    @Override
    protected RMContainerTokenSecretManager createContainerTokenSecretManager(
        Configuration conf) {
      return new RMContainerTokenSecretManager(conf) {

        @Override
        public Token createContainerToken(ContainerId containerId,
            NodeId nodeId, String appSubmitter, Resource capability) {
          numRetries++;
          return super.createContainerToken(containerId, nodeId, appSubmitter,
            capability);
        }
      };
    }
  }

  // This is to test fetching AM container will be retried, if AM container is
  // not fetchable since DNS is unavailable causing container token/NMtoken
  // creation failure.
  @Test(timeout = 20000)
  public void testAMContainerAllocationWhenDNSUnavailable() throws Exception {
    final YarnConfiguration conf = new YarnConfiguration();
    MockRM rm1 = new MockRM(conf) {
      @Override
      protected RMSecretManagerService createRMSecretManagerService() {
        return new TestRMSecretManagerService(conf, rmContext);
      }
    };
    rm1.start();

    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    SecurityUtilTestHelper.setTokenServiceUseIp(true);
    RMApp app1 = rm1.submitApp(200);
    RMAppAttempt attempt = app1.getCurrentAppAttempt();
    nm1.nodeHeartbeat(true);

    // fetching am container will fail, keep retrying 5 times.
    while (numRetries <= 5) {
      nm1.nodeHeartbeat(true);
      Thread.sleep(1000);
      Assert.assertEquals(RMAppAttemptState.SCHEDULED,
        attempt.getAppAttemptState());
      System.out.println("Waiting for am container to be allocated.");
    }

    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    rm1.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.ALLOCATED);
    MockRM.launchAndRegisterAM(app1, rm1, nm1);
  }
}