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

import java.io.IOException;
import java.util.ArrayList;

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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-End test cases for the AMRMProxy Service.
 */
public class TestAMRMProxy extends BaseAMRMProxyE2ETest {

  private static final Log LOG = LogFactory.getLog(TestAMRMProxy.class);

  /*
   * This test validates register, allocate and finish of an application through
   * the AMRMPRoxy.
   */
  @Test(timeout = 120000)
  public void testAMRMProxyE2E() throws Exception {
    ApplicationMasterProtocol client;

    try (MiniYARNCluster cluster = new MiniYARNCluster("testAMRMProxyE2E",
        1, 1, 1);
            YarnClient rmClient = YarnClient.createYarnClient()) {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();

      // the client has to connect to AMRMProxy

      yarnConf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS);
      rmClient.init(yarnConf);
      rmClient.start();

      // Submit application

      ApplicationAttemptId appAttmptId = createApp(rmClient, cluster, conf);
      ApplicationId appId = appAttmptId.getApplicationId();

      client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);

      LOG.info("testAMRMProxyE2E - Register Application Master");

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

      LOG.info("testAMRMProxyE2E - Allocate Resources Application Master");

      AllocateRequest request =
          createAllocateRequest(rmClient.getNodeReports(NodeState.RUNNING));

      AllocateResponse allocResponse = client.allocate(request);
      Assert.assertNotNull(allocResponse);
      Assert.assertEquals(0, allocResponse.getAllocatedContainers().size());

      request.setAskList(new ArrayList<ResourceRequest>());
      request.setResponseId(request.getResponseId() + 1);

      Thread.sleep(1000);

      // RM should allocate container within 2 calls to allocate()
      allocResponse = client.allocate(request);
      Assert.assertNotNull(allocResponse);
      Assert.assertEquals(2, allocResponse.getAllocatedContainers().size());

      LOG.info("testAMRMPRoxy - Finish Application Master");

      FinishApplicationMasterResponse responseFinish =
          client.finishApplicationMaster(FinishApplicationMasterRequest
              .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));

      Assert.assertNotNull(responseFinish);

      Thread.sleep(500);
      Assert.assertNotEquals(RMAppState.FINISHED, rmApp.getState());

    }
  }

  /*
   * This test validates the token renewal from the AMRMPRoxy. The test verifies
   * that the received token from AMRMProxy is different from the previous one
   * within 5 requests.
   */
  @Test(timeout = 120000)
  public void testAMRMProxyTokenRenewal() throws Exception {
    ApplicationMasterProtocol client;

    try (MiniYARNCluster cluster =
        new MiniYARNCluster("testE2ETokenRenewal", 1, 1, 1);
           YarnClient rmClient = YarnClient.createYarnClient()) {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
      conf.setInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 4500);
      conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 4500);
      conf.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 4500);
      // RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS should be at least
      // RM_AM_EXPIRY_INTERVAL_MS * 1.5 *3
      conf.setInt(
          YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS, 20);
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      yarnConf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS);
      rmClient.init(yarnConf);
      rmClient.start();

      // Submit

      ApplicationAttemptId appAttmptId = createApp(rmClient, cluster, conf);
      ApplicationId appId = appAttmptId.getApplicationId();

      client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);

      client.registerApplicationMaster(RegisterApplicationMasterRequest
          .newInstance(NetUtils.getHostname(), 1024, ""));

      LOG.info(
          "testAMRMProxyTokenRenewal - Allocate Resources Application Master");

      AllocateRequest request =
          createAllocateRequest(rmClient.getNodeReports(NodeState.RUNNING));

      Token lastToken = null;
      AllocateResponse response = null;

      for (int i = 0; i < 5; i++) {

        response = client.allocate(request);
        request.setResponseId(request.getResponseId() + 1);

        if (response.getAMRMToken() != null
            && !response.getAMRMToken().equals(lastToken)) {
          break;
        }

        lastToken = response.getAMRMToken();

        // Time slot to be sure the AMRMProxy renew the token
        Thread.sleep(4500);

      }

      Assert.assertFalse(response.getAMRMToken().equals(lastToken));

      LOG.info("testAMRMPRoxy - Finish Application Master");

      client.finishApplicationMaster(FinishApplicationMasterRequest
          .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));

    }
  }

  /*
   * This test validates that an AM cannot register directly to the RM, with the
   * token provided by the AMRMProxy.
   */
  @Test(timeout = 120000)
  public void testE2ETokenSwap() throws Exception {
    ApplicationMasterProtocol client;

    try (MiniYARNCluster cluster = new MiniYARNCluster("testE2ETokenSwap",
        1, 1, 1);
            YarnClient rmClient = YarnClient.createYarnClient()) {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
      cluster.init(conf);
      cluster.start();

      // the client will connect to the RM with the token provided by AMRMProxy
      final Configuration yarnConf = cluster.getConfig();
      rmClient.init(yarnConf);
      rmClient.start();

      ApplicationAttemptId appAttmptId = createApp(rmClient, cluster, conf);
      ApplicationId appId = appAttmptId.getApplicationId();

      client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);

      try {
        client.registerApplicationMaster(RegisterApplicationMasterRequest
            .newInstance(NetUtils.getHostname(), 1024, ""));
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(
            e.getMessage().startsWith("Invalid AMRMToken from appattempt_"));
      }

    }
  }
}
