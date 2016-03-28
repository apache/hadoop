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
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;

public class TestAMRMProxy {

  private static final Log LOG = LogFactory.getLog(TestAMRMProxy.class);

  /*
   * This test validates register, allocate and finish of an application through
   * the AMRMPRoxy.
   */
  @Test(timeout = 60000)
  public void testAMRMProxyE2E() throws Exception {
    MiniYARNCluster cluster = new MiniYARNCluster("testAMRMProxyE2E", 1, 1, 1);
    YarnClient rmClient = null;
    ApplicationMasterProtocol client;

    try {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
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

    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  /*
   * This test validates the token renewal from the AMRMPRoxy. The test verifies
   * that the received token it is different from the previous one within 5
   * requests.
   */
  @Test(timeout = 60000)
  public void testE2ETokenRenewal() throws Exception {
    MiniYARNCluster cluster =
        new MiniYARNCluster("testE2ETokenRenewal", 1, 1, 1);
    YarnClient rmClient = null;
    ApplicationMasterProtocol client;

    try {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
      conf.setInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 1500);
      conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 1500);
      conf.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 1500);
      // RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS should be at least
      // RM_AM_EXPIRY_INTERVAL_MS * 1.5 *3
      conf.setInt(
          YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS, 6);
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      yarnConf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS);
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();

      // Submit

      ApplicationId appId = createApp(rmClient, cluster);

      client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);

      client.registerApplicationMaster(RegisterApplicationMasterRequest
          .newInstance(NetUtils.getHostname(), 1024, ""));

      LOG.info("testAMRMPRoxy - Allocate Resources Application Master");

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

        // Time slot to be sure the RM renew the token
        Thread.sleep(1500);

      }

      Assert.assertFalse(response.getAMRMToken().equals(lastToken));

      LOG.info("testAMRMPRoxy - Finish Application Master");

      client.finishApplicationMaster(FinishApplicationMasterRequest
          .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));

    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  /*
   * This test validates that an AM cannot register directly to the RM, with the
   * token provided by the AMRMProxy.
   */
  @Test(timeout = 60000)
  public void testE2ETokenSwap() throws Exception {
    MiniYARNCluster cluster = new MiniYARNCluster("testE2ETokenSwap", 1, 1, 1);
    YarnClient rmClient = null;
    ApplicationMasterProtocol client;

    try {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
      cluster.init(conf);
      cluster.start();

      // the client will connect to the RM with the token provided by AMRMProxy
      final Configuration yarnConf = cluster.getConfig();
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();

      ApplicationId appId = createApp(rmClient, cluster);

      client = createAMRMProtocol(rmClient, appId, cluster, yarnConf);

      try {
        client.registerApplicationMaster(RegisterApplicationMasterRequest
            .newInstance(NetUtils.getHostname(), 1024, ""));
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(
            e.getMessage().startsWith("Invalid AMRMToken from appattempt_"));
      }

    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  private ApplicationMasterProtocol createAMRMProtocol(YarnClient rmClient,
      ApplicationId appId, MiniYARNCluster cluster,
      final Configuration yarnConf)
          throws IOException, InterruptedException, YarnException {

    UserGroupInformation user = null;

    // Get the AMRMToken from AMRMProxy

    ApplicationReport report = rmClient.getApplicationReport(appId);

    user = UserGroupInformation.createProxyUser(
        report.getCurrentApplicationAttemptId().toString(),
        UserGroupInformation.getCurrentUser());

    ContainerManagerImpl containerManager = (ContainerManagerImpl) cluster
        .getNodeManager(0).getNMContext().getContainerManager();

    AMRMProxyTokenSecretManager amrmTokenSecretManager =
        containerManager.getAMRMProxyService().getSecretManager();
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> token =
        amrmTokenSecretManager
            .createAndGetAMRMToken(report.getCurrentApplicationAttemptId());

    SecurityUtil.setTokenService(token,
        containerManager.getAMRMProxyService().getBindAddress());
    user.addToken(token);

    // Start Application Master

    return user
        .doAs(new PrivilegedExceptionAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() throws Exception {
            return ClientRMProxy.createRMProxy(yarnConf,
                ApplicationMasterProtocol.class);
          }
        });
  }

  private AllocateRequest createAllocateRequest(List<NodeReport> listNode) {
    // The test needs AMRMClient to create a real allocate request
    AMRMClientImpl<ContainerRequest> amClient =
        new AMRMClientImpl<ContainerRequest>();

    Resource capability = Resource.newInstance(1024, 2);
    Priority priority = Priority.newInstance(1);
    List<NodeReport> nodeReports = listNode;
    String node = nodeReports.get(0).getNodeId().getHost();
    String[] nodes = new String[] { node };

    ContainerRequest storedContainer1 =
        new ContainerRequest(capability, nodes, null, priority);
    amClient.addContainerRequest(storedContainer1);
    amClient.addContainerRequest(storedContainer1);

    List<ResourceRequest> resourceAsk = new ArrayList<ResourceRequest>();
    for (ResourceRequest rr : amClient.ask) {
      resourceAsk.add(rr);
    }

    ResourceBlacklistRequest resourceBlacklistRequest = ResourceBlacklistRequest
        .newInstance(new ArrayList<String>(), new ArrayList<String>());

    int responseId = 1;

    return AllocateRequest.newInstance(responseId, 0, resourceAsk,
        new ArrayList<ContainerId>(), resourceBlacklistRequest);
  }

  private ApplicationId createApp(YarnClient yarnClient,
      MiniYARNCluster yarnCluster) throws Exception {

    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    appContext.setApplicationName("Test");

    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    appContext.setPriority(pri);

    appContext.setQueue("default");

    ContainerLaunchContext amContainer = BuilderUtils.newContainerLaunchContext(
        Collections.<String, LocalResource> emptyMap(),
        new HashMap<String, String>(), Arrays.asList("sleep", "10000"),
        new HashMap<String, ByteBuffer>(), null,
        new HashMap<ApplicationAccessType, String>());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));

    SubmitApplicationRequest appRequest =
        Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);

    yarnClient.submitApplication(appContext);

    RMAppAttempt appAttempt = null;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport
          .getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        ApplicationAttemptId attemptId =
            appReport.getCurrentApplicationAttemptId();
        appAttempt = yarnCluster.getResourceManager().getRMContext().getRMApps()
            .get(attemptId.getApplicationId()).getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
    }
    Thread.sleep(1000);
    return appId;
  }
}
