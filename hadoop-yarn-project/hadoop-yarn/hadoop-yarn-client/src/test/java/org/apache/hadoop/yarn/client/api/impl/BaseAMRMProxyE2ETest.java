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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Base test case to be used for Testing frameworks that use AMRMProxy.
 */
public abstract class BaseAMRMProxyE2ETest {

  protected ApplicationMasterProtocol createAMRMProtocol(YarnClient rmClient,
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

  protected AllocateRequest createAllocateRequest(List<NodeReport> listNode) {
    // The test needs AMRMClient to create a real allocate request
    AMRMClientImpl<AMRMClient.ContainerRequest> amClient =
        new AMRMClientImpl<>();

    Resource capability = Resource.newInstance(1024, 2);
    Priority priority = Priority.newInstance(1);
    List<NodeReport> nodeReports = listNode;
    String node = nodeReports.get(0).getNodeId().getHost();
    String[] nodes = new String[] {node};

    AMRMClient.ContainerRequest storedContainer1 =
        new AMRMClient.ContainerRequest(capability, nodes, null, priority);
    amClient.addContainerRequest(storedContainer1);
    amClient.addContainerRequest(storedContainer1);

    List<ResourceRequest> resourceAsk = new ArrayList<>();
    for (ResourceRequest rr : amClient.ask) {
      resourceAsk.add(rr);
    }

    ResourceBlacklistRequest resourceBlacklistRequest = ResourceBlacklistRequest
        .newInstance(new ArrayList<>(), new ArrayList<>());

    int responseId = 1;

    return AllocateRequest.newInstance(responseId, 0, resourceAsk,
        new ArrayList<>(), resourceBlacklistRequest);
  }

  protected ApplicationAttemptId createApp(YarnClient yarnClient,
      MiniYARNCluster yarnCluster, Configuration conf) throws Exception {

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
    ApplicationAttemptId attemptId = null;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport
          .getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        attemptId =
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
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));

    // emulate RM setup of AMRM token in credentials by adding the token
    // *before* setting the token service
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    appAttempt.getAMRMToken().setService(
        ClientRMProxy.getAMRMTokenService(conf));
    return attemptId;
  }
}
