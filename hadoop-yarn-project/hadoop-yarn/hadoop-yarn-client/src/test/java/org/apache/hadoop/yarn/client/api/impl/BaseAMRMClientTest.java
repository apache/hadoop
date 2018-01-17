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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for testing AMRMClient.
 */
public class BaseAMRMClientTest {

  protected Configuration conf = null;
  protected MiniYARNCluster yarnCluster = null;
  protected YarnClient yarnClient = null;
  protected List<NodeReport> nodeReports = null;
  protected ApplicationAttemptId attemptId = null;

  protected String schedulerName = CapacityScheduler.class.getName();
  protected boolean autoUpdate = false;

  protected int nodeCount = 3;
  protected long amExpireMs = 4000;
  protected int rollingIntervalSec = 13;


  protected Resource capability;
  protected Priority priority;
  protected Priority priority2;
  protected String node;
  protected String rack;
  protected String[] nodes;
  protected String[] racks;

  @Before
  public void setup() throws Exception {
    conf = new YarnConfiguration();
    createClusterAndStartApplication(conf);
  }

  protected void createClusterAndStartApplication(Configuration conf)
      throws Exception {
    // start minicluster
    this.conf = conf;
    if (autoUpdate) {
      conf.setBoolean(YarnConfiguration.RM_AUTO_UPDATE_CONTAINERS, true);
    }
    conf.set(YarnConfiguration.RM_SCHEDULER, schedulerName);
    conf.setLong(
        YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        rollingIntervalSec);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, amExpireMs);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    // set the minimum allocation so that resource decrease can go under 1024
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH, 10);
    yarnCluster = new MiniYARNCluster(
        TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // get node info
    assertTrue("All node managers did not connect to the RM within the "
            + "allotted 5-second timeout",
        yarnCluster.waitForNodeManagersToConnect(5000L));
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    assertEquals("Not all node managers were reported running",
        nodeCount, nodeReports.size());

    priority = Priority.newInstance(1);
    priority2 = Priority.newInstance(2);
    capability = Resource.newInstance(1024, 1);

    node = nodeReports.get(0).getNodeId().getHost();
    rack = nodeReports.get(0).getRackName();
    nodes = new String[]{ node };
    racks = new String[]{ rack };

    // submit new app
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
        BuilderUtils.newContainerLaunchContext(
            Collections.<String, LocalResource> emptyMap(),
            new HashMap<String, String>(), Arrays.asList("sleep", "100"),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);

    // wait for app to start
    RMAppAttempt appAttempt = null;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() ==
          YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        appAttempt =
            yarnCluster.getResourceManager().getRMContext().getRMApps()
                .get(attemptId.getApplicationId()).getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
    }
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));

    // emulate RM setup of AMRM token in credentials by adding the token
    // *before* setting the token service
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    appAttempt.getAMRMToken().setService(
        ClientRMProxy.getAMRMTokenService(conf));
  }

  @After
  public void teardown() throws YarnException, IOException {
    yarnClient.killApplication(attemptId.getApplicationId());
    attemptId = null;

    if (yarnClient != null &&
        yarnClient.getServiceState() == Service.STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null &&
        yarnCluster.getServiceState() == Service.STATE.STARTED) {
      yarnCluster.stop();
    }
  }

}
