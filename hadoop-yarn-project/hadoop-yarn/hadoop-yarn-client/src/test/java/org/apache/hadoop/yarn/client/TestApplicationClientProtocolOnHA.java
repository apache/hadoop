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

package org.apache.hadoop.yarn.client;

import java.util.List;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestApplicationClientProtocolOnHA extends ProtocolHATestBase {
  private YarnClient client = null;

  @Before
  public void initiate() throws Exception {
    startHACluster(1, true, false, false);
    Configuration conf = new YarnConfiguration(this.conf);
    client = createAndStartYarnClient(conf);
  }

  @After
  public void shutDown() {
    if (client != null) {
      client.stop();
    }
  }

  @Rule
  public Timeout timeout = new Timeout(180, TimeUnit.SECONDS);

  @Test
  public void testGetApplicationReportOnHA() throws Exception {
    ApplicationReport report =
        client.getApplicationReport(cluster.createFakeAppId());
    Assert.assertTrue(report != null);
    Assert.assertEquals(cluster.createFakeAppReport(), report);
  }

  @Test
  public void testGetNewApplicationOnHA() throws Exception {
    ApplicationId appId =
        client.createApplication().getApplicationSubmissionContext()
            .getApplicationId();
    Assert.assertTrue(appId != null);
    Assert.assertEquals(cluster.createFakeAppId(), appId);
  }

  @Test
  public void testGetClusterMetricsOnHA() throws Exception {
    YarnClusterMetrics clusterMetrics =
        client.getYarnClusterMetrics();
    Assert.assertTrue(clusterMetrics != null);
    Assert.assertEquals(cluster.createFakeYarnClusterMetrics(),
        clusterMetrics);
  }

  @Test
  public void testGetApplicationsOnHA() throws Exception {
    List<ApplicationReport> reports =
        client.getApplications();
    Assert.assertTrue(reports != null);
    Assert.assertFalse(reports.isEmpty());
    Assert.assertEquals(cluster.createFakeAppReports(),
        reports);
  }

  @Test
  public void testGetClusterNodesOnHA() throws Exception {
    List<NodeReport> reports = client.getNodeReports(NodeState.RUNNING);
    Assert.assertTrue(reports != null);
    Assert.assertFalse(reports.isEmpty());
    Assert.assertEquals(cluster.createFakeNodeReports(),
        reports);
  }

  @Test
  public void testGetQueueInfoOnHA() throws Exception {
    QueueInfo queueInfo = client.getQueueInfo("root");
    Assert.assertTrue(queueInfo != null);
    Assert.assertEquals(cluster.createFakeQueueInfo(),
        queueInfo);
  }

  @Test
  public void testGetQueueUserAclsOnHA() throws Exception {
    List<QueueUserACLInfo> queueUserAclsList = client.getQueueAclsInfo();
    Assert.assertTrue(queueUserAclsList != null);
    Assert.assertFalse(queueUserAclsList.isEmpty());
    Assert.assertEquals(cluster.createFakeQueueUserACLInfoList(),
        queueUserAclsList);
  }

  @Test
  public void testGetApplicationAttemptReportOnHA() throws Exception {
    ApplicationAttemptReport report =
        client.getApplicationAttemptReport(cluster
            .createFakeApplicationAttemptId());
    Assert.assertTrue(report != null);
    Assert.assertEquals(cluster.createFakeApplicationAttemptReport(), report);
  }

  @Test
  public void testGetApplicationAttemptsOnHA() throws Exception {
    List<ApplicationAttemptReport> reports =
        client.getApplicationAttempts(cluster.createFakeAppId());
    Assert.assertTrue(reports != null);
    Assert.assertFalse(reports.isEmpty());
    Assert.assertEquals(cluster.createFakeApplicationAttemptReports(),
        reports);
  }

  @Test
  public void testGetContainerReportOnHA() throws Exception {
    ContainerReport report =
        client.getContainerReport(cluster.createFakeContainerId());
    Assert.assertTrue(report != null);
    Assert.assertEquals(cluster.createFakeContainerReport(), report);
  }

  @Test
  public void testGetContainersOnHA() throws Exception {
    List<ContainerReport> reports =
        client.getContainers(cluster.createFakeApplicationAttemptId());
    Assert.assertTrue(reports != null);
    Assert.assertFalse(reports.isEmpty());
    Assert.assertEquals(cluster.createFakeContainerReports(),
        reports);
  }

  @Test
  public void testSubmitApplicationOnHA() throws Exception {
    ApplicationSubmissionContext appContext =
        Records.newRecord(ApplicationSubmissionContext.class);
    appContext.setApplicationId(cluster.createFakeAppId());
    ContainerLaunchContext amContainer =
        Records.newRecord(ContainerLaunchContext.class);
    appContext.setAMContainerSpec(amContainer);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemorySize(10);
    capability.setVirtualCores(1);
    appContext.setResource(capability);
    ApplicationId appId = client.submitApplication(appContext);
    Assert.assertTrue(getActiveRM().getRMContext().getRMApps()
        .containsKey(appId));
  }

  @Test
  public void testMoveApplicationAcrossQueuesOnHA() throws Exception{
    client.moveApplicationAcrossQueues(cluster.createFakeAppId(), "root");
  }

  @Test
  public void testForceKillApplicationOnHA() throws Exception {
    client.killApplication(cluster.createFakeAppId());
  }

  @Test
  public void testGetDelegationTokenOnHA() throws Exception {
    Token token = client.getRMDelegationToken(new Text(" "));
    Assert.assertEquals(token, cluster.createFakeToken());
  }

  @Test
  public void testRenewDelegationTokenOnHA() throws Exception {
    RenewDelegationTokenRequest request =
        RenewDelegationTokenRequest.newInstance(cluster.createFakeToken());
    long newExpirationTime =
        ClientRMProxy.createRMProxy(this.conf, ApplicationClientProtocol.class)
            .renewDelegationToken(request).getNextExpirationTime();
    Assert.assertEquals(newExpirationTime, cluster.createNextExpirationTime());
  }

  @Test
  public void testCancelDelegationTokenOnHA() throws Exception {
    CancelDelegationTokenRequest request =
        CancelDelegationTokenRequest.newInstance(cluster.createFakeToken());
    ClientRMProxy.createRMProxy(this.conf, ApplicationClientProtocol.class)
        .cancelDelegationToken(request);
  }
}
