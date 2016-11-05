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

package org.apache.hadoop.yarn.client.api.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates End2End Distributed Scheduling flow which includes the AM
 * specifying OPPORTUNISTIC containers in its resource requests,
 * the AMRMProxyService on the NM, the DistributedScheduler RequestInterceptor
 * on the NM and the DistributedSchedulingProtocol used by the framework to talk
 * to the OpportunisticContainerAllocatorAMService running on the RM.
 */
public class TestDistributedScheduling extends BaseAMRMProxyE2ETest {

  private static final Log LOG =
      LogFactory.getLog(TestDistributedScheduling.class);

  protected MiniYARNCluster cluster;
  protected YarnClient rmClient;
  protected ApplicationMasterProtocol client;
  protected Configuration conf;
  protected Configuration yarnConf;
  protected ApplicationAttemptId attemptId;
  protected ApplicationId appId;

  @Before
  public void doBefore() throws Exception {
    cluster = new MiniYARNCluster("testDistributedSchedulingE2E", 1, 1, 1);

    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.
        OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_CONTAINER_QUEUING_ENABLED, true);
    cluster.init(conf);
    cluster.start();
    yarnConf = cluster.getConfig();

    // the client has to connect to AMRMProxy
    yarnConf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS);
    rmClient = YarnClient.createYarnClient();
    rmClient.init(yarnConf);
    rmClient.start();

    // Submit application
    attemptId = createApp(rmClient, cluster, conf);
    appId = attemptId.getApplicationId();
    client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);
  }

  @After
  public void doAfter() throws Exception {
    if (client != null) {
      try {
        client.finishApplicationMaster(FinishApplicationMasterRequest
            .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));
        rmClient.killApplication(attemptId.getApplicationId());
        attemptId = null;
      } catch (Exception e) {
      }
    }
    if (rmClient != null) {
      try {
        rmClient.stop();
      } catch (Exception e) {
      }
    }
    if (cluster != null) {
      try {
        cluster.stop();
      } catch (Exception e) {
      }
    }
  }


  /**
   * Validates if Allocate Requests containing only OPPORTUNISTIC container
   * requests are satisfied instantly.
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testOpportunisticExecutionTypeRequestE2E() throws Exception {
    LOG.info("testDistributedSchedulingE2E - Register");

    RegisterApplicationMasterResponse responseRegister =
        client.registerApplicationMaster(RegisterApplicationMasterRequest
            .newInstance(NetUtils.getHostname(), 1024, ""));

    Assert.assertNotNull(responseRegister);
    Assert.assertNotNull(responseRegister.getQueue());
    Assert.assertNotNull(responseRegister.getApplicationACLs());
    Assert.assertNotNull(responseRegister.getClientToAMTokenMasterKey());
    Assert
        .assertNotNull(responseRegister.getContainersFromPreviousAttempts());
    Assert.assertNotNull(responseRegister.getSchedulerResourceTypes());
    Assert.assertNotNull(responseRegister.getMaximumResourceCapability());

    // Wait until the RM has been updated and verify
    Map<ApplicationId, RMApp> rmApps =
        cluster.getResourceManager().getRMContext().getRMApps();
    boolean rmUpdated = false;
    for (int i=0; i<10 && !rmUpdated; i++) {
      sleep(100);
      RMApp rmApp = rmApps.get(appId);
      if (rmApp.getState() == RMAppState.RUNNING) {
        rmUpdated = true;
      }
    }
    RMApp rmApp = rmApps.get(appId);
    Assert.assertEquals(RMAppState.RUNNING, rmApp.getState());

    LOG.info("testDistributedSchedulingE2E - Allocate");

    AllocateRequest request =
        createAllocateRequest(rmClient.getNodeReports(NodeState.RUNNING));

    // Replace 'ANY' requests with OPPORTUNISTIC aks and remove
    // everything else
    List<ResourceRequest> newAskList = new ArrayList<>();
    for (ResourceRequest rr : request.getAskList()) {
      if (ResourceRequest.ANY.equals(rr.getResourceName())) {
        ResourceRequest newRR = ResourceRequest.newInstance(rr
                .getPriority(), rr.getResourceName(),
            rr.getCapability(), rr.getNumContainers(), rr.getRelaxLocality(),
            rr.getNodeLabelExpression(),
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true));
        newAskList.add(newRR);
      }
    }
    request.setAskList(newAskList);

    AllocateResponse allocResponse = client.allocate(request);
    Assert.assertNotNull(allocResponse);

    // Ensure that all the requests are satisfied immediately
    Assert.assertEquals(2, allocResponse.getAllocatedContainers().size());

    // Verify that the allocated containers are OPPORTUNISTIC
    for (Container allocatedContainer : allocResponse
        .getAllocatedContainers()) {
      ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
          .newContainerTokenIdentifier(
              allocatedContainer.getContainerToken());
      Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
          containerTokenIdentifier.getExecutionType());
    }

    // Check that the RM sees OPPORTUNISTIC containers
    ResourceScheduler scheduler = cluster.getResourceManager()
        .getResourceScheduler();
    for (Container allocatedContainer : allocResponse
        .getAllocatedContainers()) {
      ContainerId containerId = allocatedContainer.getId();
      RMContainer rmContainer = scheduler.getRMContainer(containerId);
      Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
          rmContainer.getExecutionType());
    }

    LOG.info("testDistributedSchedulingE2E - Finish");
  }

  /**
   * Validates if Allocate Requests containing both GUARANTEED and OPPORTUNISTIC
   * container requests works as expected.
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testMixedExecutionTypeRequestE2E() throws Exception {
    LOG.info("testDistributedSchedulingE2E - Register");

    RegisterApplicationMasterResponse responseRegister =
        client.registerApplicationMaster(RegisterApplicationMasterRequest
            .newInstance(NetUtils.getHostname(), 1024, ""));

    Assert.assertNotNull(responseRegister);
    Assert.assertNotNull(responseRegister.getQueue());
    Assert.assertNotNull(responseRegister.getApplicationACLs());
    Assert.assertNotNull(responseRegister.getClientToAMTokenMasterKey());
    Assert
        .assertNotNull(responseRegister.getContainersFromPreviousAttempts());
    Assert.assertNotNull(responseRegister.getSchedulerResourceTypes());
    Assert.assertNotNull(responseRegister.getMaximumResourceCapability());

    RMApp rmApp =
        cluster.getResourceManager().getRMContext().getRMApps().get(appId);
    Assert.assertEquals(RMAppState.RUNNING, rmApp.getState());

    LOG.info("testDistributedSchedulingE2E - Allocate");

    AllocateRequest request =
        createAllocateRequest(rmClient.getNodeReports(NodeState.RUNNING));
    List<ResourceRequest> askList = request.getAskList();
    List<ResourceRequest> newAskList = new ArrayList<>(askList);

    // Duplicate all ANY requests marking them as opportunistic
    for (ResourceRequest rr : askList) {
      if (ResourceRequest.ANY.equals(rr.getResourceName())) {
        ResourceRequest newRR = ResourceRequest.newInstance(rr
                .getPriority(), rr.getResourceName(),
            rr.getCapability(), rr.getNumContainers(), rr.getRelaxLocality(),
            rr.getNodeLabelExpression(),
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true));
        newAskList.add(newRR);
      }
    }
    request.setAskList(newAskList);

    AllocateResponse allocResponse = client.allocate(request);
    Assert.assertNotNull(allocResponse);

    // Ensure that all the requests are satisfied immediately
    Assert.assertEquals(2, allocResponse.getAllocatedContainers().size());

    // Verify that the allocated containers are OPPORTUNISTIC
    for (Container allocatedContainer : allocResponse
        .getAllocatedContainers()) {
      ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
          .newContainerTokenIdentifier(
              allocatedContainer.getContainerToken());
      Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
          containerTokenIdentifier.getExecutionType());
    }

    request.setAskList(new ArrayList<ResourceRequest>());
    request.setResponseId(request.getResponseId() + 1);

    Thread.sleep(1000);

    // RM should allocate GUARANTEED containers within 2 calls to allocate()
    allocResponse = client.allocate(request);
    Assert.assertNotNull(allocResponse);
    Assert.assertEquals(2, allocResponse.getAllocatedContainers().size());

    // Verify that the allocated containers are GUARANTEED
    for (Container allocatedContainer : allocResponse
        .getAllocatedContainers()) {
      ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
          .newContainerTokenIdentifier(
              allocatedContainer.getContainerToken());
      Assert.assertEquals(ExecutionType.GUARANTEED,
          containerTokenIdentifier.getExecutionType());
    }

    LOG.info("testDistributedSchedulingE2E - Finish");
  }

  /**
   * Validates if AMRMClient can be used with Distributed Scheduling turned on.
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  @SuppressWarnings("unchecked")
  public void testAMRMClient() throws Exception {
    AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
    try {
      Priority priority = Priority.newInstance(1);
      Priority priority2 = Priority.newInstance(2);
      Resource capability = Resource.newInstance(1024, 1);

      List<NodeReport> nodeReports = rmClient.getNodeReports(NodeState.RUNNING);
      String node = nodeReports.get(0).getNodeId().getHost();
      String rack = nodeReports.get(0).getRackName();
      String[] nodes = new String[]{node};
      String[] racks = new String[]{rack};

      // start am rm client
      amClient = new AMRMClientImpl(client);
      amClient.init(yarnConf);
      amClient.start();
      amClient.registerApplicationMaster(NetUtils.getHostname(), 1024, "");

      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());

      amClient.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, null, null, priority2,
              0, true, null,
              ExecutionTypeRequest.newInstance(
                  ExecutionType.OPPORTUNISTIC, true)));
      amClient.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, null, null, priority2,
              0, true, null,
              ExecutionTypeRequest.newInstance(
                  ExecutionType.OPPORTUNISTIC, true)));

      amClient.removeContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.removeContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.removeContainerRequest(
          new AMRMClient.ContainerRequest(capability, null, null, priority2,
              0, true, null,
              ExecutionTypeRequest.newInstance(
                  ExecutionType.OPPORTUNISTIC, true)));

      RemoteRequestsTable<ContainerRequest> remoteRequestsTable =
          amClient.getTable(0);
      int containersRequestedNode = remoteRequestsTable.get(priority,
          node, ExecutionType.GUARANTEED, capability).remoteRequest
          .getNumContainers();
      int containersRequestedRack = remoteRequestsTable.get(priority,
          rack, ExecutionType.GUARANTEED, capability).remoteRequest
          .getNumContainers();
      int containersRequestedAny = remoteRequestsTable.get(priority,
          ResourceRequest.ANY, ExecutionType.GUARANTEED, capability)
          .remoteRequest.getNumContainers();
      int oppContainersRequestedAny =
          remoteRequestsTable.get(priority2, ResourceRequest.ANY,
              ExecutionType.OPPORTUNISTIC, capability).remoteRequest
              .getNumContainers();

      assertEquals(2, containersRequestedNode);
      assertEquals(2, containersRequestedRack);
      assertEquals(2, containersRequestedAny);
      assertEquals(1, oppContainersRequestedAny);

      assertEquals(4, amClient.ask.size());
      assertEquals(0, amClient.release.size());

      // RM should allocate container within 2 calls to allocate()
      int allocatedContainerCount = 0;
      int iterationsLeft = 10;
      Set<ContainerId> releases = new TreeSet<>();

      amClient.getNMTokenCache().clearCache();
      Assert.assertEquals(0,
          amClient.getNMTokenCache().numberOfTokensInCache());
      HashMap<String, Token> receivedNMTokens = new HashMap<>();

      while (allocatedContainerCount <
          (containersRequestedAny + oppContainersRequestedAny)
          && iterationsLeft-- > 0) {
        AllocateResponse allocResponse = amClient.allocate(0.1f);
        assertEquals(0, amClient.ask.size());
        assertEquals(0, amClient.release.size());

        allocatedContainerCount += allocResponse.getAllocatedContainers()
            .size();
        for (Container container : allocResponse.getAllocatedContainers()) {
          ContainerId rejectContainerId = container.getId();
          releases.add(rejectContainerId);
        }

        for (NMToken token : allocResponse.getNMTokens()) {
          String nodeID = token.getNodeId().toString();
          receivedNMTokens.put(nodeID, token.getToken());
        }

        if (allocatedContainerCount < containersRequestedAny) {
          // sleep to let NM's heartbeat to RM and trigger allocations
          sleep(100);
        }
      }

      assertEquals(allocatedContainerCount,
          containersRequestedAny + oppContainersRequestedAny);
      for (ContainerId rejectContainerId : releases) {
        amClient.releaseAssignedContainer(rejectContainerId);
      }
      assertEquals(3, amClient.release.size());
      assertEquals(0, amClient.ask.size());

      // need to tell the AMRMClient that we dont need these resources anymore
      amClient.removeContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.removeContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority));
      amClient.removeContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority2,
              0, true, null,
              ExecutionTypeRequest.newInstance(
                  ExecutionType.OPPORTUNISTIC, true)));
      assertEquals(4, amClient.ask.size());

      // test RPC exception handling
      amClient.addContainerRequest(new AMRMClient.ContainerRequest(capability,
          nodes, racks, priority));
      amClient.addContainerRequest(new AMRMClient.ContainerRequest(capability,
          nodes, racks, priority));
      amClient.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, priority2,
              0, true, null,
              ExecutionTypeRequest.newInstance(
                  ExecutionType.OPPORTUNISTIC, true)));

      final AMRMClient amc = amClient;
      ApplicationMasterProtocol realRM = amClient.rmClient;
      try {
        ApplicationMasterProtocol mockRM = mock(ApplicationMasterProtocol
            .class);
        when(mockRM.allocate(any(AllocateRequest.class))).thenAnswer(
            new Answer<AllocateResponse>() {
              public AllocateResponse answer(InvocationOnMock invocation)
                  throws Exception {
                amc.removeContainerRequest(
                    new AMRMClient.ContainerRequest(capability, nodes,
                        racks, priority));
                amc.removeContainerRequest(
                    new AMRMClient.ContainerRequest(capability, nodes, racks,
                        priority));
                amc.removeContainerRequest(
                    new AMRMClient.ContainerRequest(capability, null, null,
                        priority2, 0, true, null,
                        ExecutionTypeRequest.newInstance(
                            ExecutionType.OPPORTUNISTIC, true)));
                throw new Exception();
              }
            });
        amClient.rmClient = mockRM;
        amClient.allocate(0.1f);
      } catch (Exception ioe) {
      } finally {
        amClient.rmClient = realRM;
      }

      assertEquals(3, amClient.release.size());
      assertEquals(6, amClient.ask.size());

      iterationsLeft = 3;
      // do a few iterations to ensure RM is not going send new containers
      while (iterationsLeft-- > 0) {
        // inform RM of rejection
        AllocateResponse allocResponse = amClient.allocate(0.1f);
        // RM did not send new containers because AM does not need any
        assertEquals(0, allocResponse.getAllocatedContainers().size());
        if (allocResponse.getCompletedContainersStatuses().size() > 0) {
          for (ContainerStatus cStatus : allocResponse
              .getCompletedContainersStatuses()) {
            if (releases.contains(cStatus.getContainerId())) {
              assertEquals(cStatus.getState(), ContainerState.COMPLETE);
              assertEquals(-100, cStatus.getExitStatus());
              releases.remove(cStatus.getContainerId());
            }
          }
        }
        if (iterationsLeft > 0) {
          // sleep to make sure NM's heartbeat
          sleep(100);
        }
      }
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());

      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == Service.STATE
          .STARTED) {
        amClient.stop();
      }
    }
  }

  /**
   * Check if an AM can ask for opportunistic containers and get them.
   * @throws Exception
   */
  @Test
  public void testAMOpportunistic() throws Exception {
    // Basic container to request
    Resource capability = Resource.newInstance(1024, 1);
    Priority priority = Priority.newInstance(1);

    // Get the cluster topology
    List<NodeReport> nodeReports = rmClient.getNodeReports(NodeState.RUNNING);
    String node = nodeReports.get(0).getNodeId().getHost();
    String rack = nodeReports.get(0).getRackName();
    String[] nodes = new String[]{node};
    String[] racks = new String[]{rack};

    // Create an AM to request resources
    AMRMClient<AMRMClient.ContainerRequest> amClient = null;
    try {
      amClient = new AMRMClientImpl<AMRMClient.ContainerRequest>(client);
      amClient.init(yarnConf);
      amClient.start();
      amClient.registerApplicationMaster(NetUtils.getHostname(), 1024, "");

      // AM requests an opportunistic container
      ExecutionTypeRequest execTypeRequest =
          ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC, true);
      ContainerRequest containerRequest = new AMRMClient.ContainerRequest(
          capability, nodes, racks, priority, 0, true, null, execTypeRequest);
      amClient.addContainerRequest(containerRequest);

      // Wait until the container is allocated
      ContainerId opportunisticContainerId = null;
      for (int i=0; i<10 && opportunisticContainerId == null; i++) {
        AllocateResponse allocResponse = amClient.allocate(0.1f);
        List<Container> allocatedContainers =
            allocResponse.getAllocatedContainers();
        for (Container allocatedContainer : allocatedContainers) {
          // Check that this is the container we required
          assertEquals(ExecutionType.OPPORTUNISTIC,
              allocatedContainer.getExecutionType());
          opportunisticContainerId = allocatedContainer.getId();
        }
        sleep(100);
      }
      assertNotNull(opportunisticContainerId);

      // The RM sees the container as OPPORTUNISTIC
      ResourceScheduler scheduler = cluster.getResourceManager()
          .getResourceScheduler();
      RMContainer rmContainer = scheduler.getRMContainer(
          opportunisticContainerId);
      assertEquals(ExecutionType.OPPORTUNISTIC,
          rmContainer.getExecutionType());

      // Release the opportunistic container
      amClient.releaseAssignedContainer(opportunisticContainerId);
      // Wait for the release container to appear
      boolean released = false;
      for (int i=0; i<10 && !released; i++) {
        AllocateResponse allocResponse = amClient.allocate(0.1f);
        List<ContainerStatus> completedContainers =
            allocResponse.getCompletedContainersStatuses();
        for (ContainerStatus completedContainer : completedContainers) {
          ContainerId completedContainerId =
              completedContainer.getContainerId();
          assertEquals(completedContainerId, opportunisticContainerId);
          released = true;
        }
        if (!released) {
          sleep(100);
        }
      }
      assertTrue(released);

      // The RM shouldn't see the container anymore
      rmContainer = scheduler.getRMContainer(opportunisticContainerId);
      assertNull(rmContainer);

      // Clean the AM
      amClient.unregisterApplicationMaster(
          FinalApplicationStatus.SUCCEEDED, null, null);
    } finally {
      if (amClient != null &&
          amClient.getServiceState() == Service.STATE.STARTED) {
        amClient.close();
      }
    }
  }

  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
