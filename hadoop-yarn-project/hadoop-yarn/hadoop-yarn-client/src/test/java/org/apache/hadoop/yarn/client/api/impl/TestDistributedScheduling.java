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

package org.apache.hadoop.yarn.client.api.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates End2End Distributed Scheduling flow which includes the AM
 * specifying OPPORTUNISTIC containers in its resource requests,
 * the AMRMProxyService on the NM, the LocalScheduler RequestInterceptor on
 * the NM and the DistributedSchedulingProtocol used by the framework to talk
 * to the DistributedSchedulingService running on the RM.
 */
public class TestDistributedScheduling extends TestAMRMProxy {

  private static final Log LOG =
      LogFactory.getLog(TestDistributedScheduling.class);

  /**
   * Validates if Allocate Requests containing only OPPORTUNISTIC container
   * requests are satisfied instantly.
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testOpportunisticExecutionTypeRequestE2E() throws Exception {
    MiniYARNCluster cluster =
        new MiniYARNCluster("testDistributedSchedulingE2E", 1, 1, 1);
    YarnClient rmClient = null;
    ApplicationMasterProtocol client;

    try {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
      conf.setBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED, true);
      conf.setBoolean(YarnConfiguration.NM_CONTAINER_QUEUING_ENABLED, true);
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();

      // the client has to connect to AMRMProxy

      yarnConf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS);
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();

      // Submit application

      ApplicationId appId = createApp(rmClient, cluster);

      client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);

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

      // Replace 'ANY' requests with OPPORTUNISTIC aks and remove
      // everything else
      List<ResourceRequest> newAskList = new ArrayList<>();
      for (ResourceRequest rr : request.getAskList()) {
        if (ResourceRequest.ANY.equals(rr.getResourceName())) {
          ResourceRequest newRR = ResourceRequest.newInstance(rr
                  .getPriority(), rr.getResourceName(),
              rr.getCapability(), rr.getNumContainers(), rr.getRelaxLocality(),
              rr.getNodeLabelExpression(), ExecutionType.OPPORTUNISTIC);
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

      LOG.info("testDistributedSchedulingE2E - Finish");

      FinishApplicationMasterResponse responseFinish =
          client.finishApplicationMaster(FinishApplicationMasterRequest
              .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));

      Assert.assertNotNull(responseFinish);

    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  /**
   * Validates if Allocate Requests containing both GUARANTEED and OPPORTUNISTIC
   * container requests works as expected.
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testMixedExecutionTypeRequestE2E() throws Exception {
    MiniYARNCluster cluster =
        new MiniYARNCluster("testDistributedSchedulingE2E", 1, 1, 1);
    YarnClient rmClient = null;
    ApplicationMasterProtocol client;

    try {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
      conf.setBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED, true);
      conf.setBoolean(YarnConfiguration.NM_CONTAINER_QUEUING_ENABLED, true);
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();

      // the client has to connect to AMRMProxy

      yarnConf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS);
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();

      // Submit application

      ApplicationId appId = createApp(rmClient, cluster);

      client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);

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
              rr.getNodeLabelExpression(), ExecutionType.OPPORTUNISTIC);
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

      FinishApplicationMasterResponse responseFinish =
          client.finishApplicationMaster(FinishApplicationMasterRequest
              .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));

      Assert.assertNotNull(responseFinish);

    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  @Ignore
  @Override
  public void testAMRMProxyE2E() throws Exception { }

  @Ignore
  @Override
  public void testE2ETokenRenewal() throws Exception { }

  @Ignore
  @Override
  public void testE2ETokenSwap() throws Exception { }
}
