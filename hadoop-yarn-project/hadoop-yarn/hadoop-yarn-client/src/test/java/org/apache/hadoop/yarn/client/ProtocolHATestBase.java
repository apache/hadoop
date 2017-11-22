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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;


/**
 * Test Base for ResourceManager's Protocol on HA.
 *
 * Limited scope:
 * For all the test cases, we only test whether the method will be re-entered
 * when failover happens. Does not cover the entire logic of test.
 *
 * Test strategy:
 * Create a separate failover thread with a trigger flag,
 * override all APIs that are added trigger flag.
 * When the APIs are called, we will set trigger flag as true to kick off
 * the failover. So We can make sure the failover happens during process
 * of the method. If this API is marked as @Idempotent or @AtMostOnce,
 * the test cases will pass; otherwise, they will throw the exception.
 *
 */
public abstract class ProtocolHATestBase extends ClientBaseWithFixes {
  protected static final HAServiceProtocol.StateChangeRequestInfo req =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER);

  protected static final String RM1_NODE_ID = "rm1";
  protected static final int RM1_PORT_BASE = 10000;
  protected static final String RM2_NODE_ID = "rm2";
  protected static final int RM2_PORT_BASE = 20000;

  protected Configuration conf;
  protected MiniYARNClusterForHATesting cluster;

  protected Thread failoverThread = null;
  private volatile boolean keepRunning;

  @Before
  public void setup() throws IOException {
    failoverThread = null;
    keepRunning = true;
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setInt(YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, 5);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);

    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);
  }

  @After
  public void teardown() throws Exception {
    keepRunning = false;
    if (failoverThread != null) {
      failoverThread.interrupt();
      try {
        failoverThread.join();
      } catch (InterruptedException ex) {
        LOG.error("Error joining with failover thread", ex);
      }
    }
    cluster.stop();
  }

  protected AdminService getAdminService(int index) {
    return cluster.getResourceManager(index).getRMContext()
        .getRMAdminService();
  }

  protected void explicitFailover() throws IOException {
    int activeRMIndex = cluster.getActiveRMIndex();
    int newActiveRMIndex = (activeRMIndex + 1) % 2;
    getAdminService(activeRMIndex).transitionToStandby(req);
    getAdminService(newActiveRMIndex).transitionToActive(req);
    assertEquals("Failover failed", newActiveRMIndex,
        cluster.getActiveRMIndex());
  }

  protected YarnClient createAndStartYarnClient(Configuration conf) {
    Configuration configuration = new YarnConfiguration(conf);
    YarnClient client = YarnClient.createYarnClient();
    client.init(configuration);
    client.start();
    return client;
  }

  protected void verifyConnections() throws InterruptedException,
      YarnException {
    assertTrue("NMs failed to connect to the RM",
        cluster.waitForNodeManagersToConnect(5000));
    verifyClientConnection();
  }

  protected void verifyClientConnection() {
    int numRetries = 3;
    while(numRetries-- > 0) {
      Configuration conf = new YarnConfiguration(this.conf);
      YarnClient client = createAndStartYarnClient(conf);
      try {
        Thread.sleep(100);
        client.getApplications();
        return;
      } catch (Exception e) {
        LOG.error(e.getMessage());
      } finally {
        client.stop();
      }
    }
    fail("Client couldn't connect to the Active RM");
  }

  protected Thread createAndStartFailoverThread() {
    Thread failoverThread = new Thread() {
      public void run() {
        keepRunning = true;
        while (keepRunning) {
          if (cluster.getStartFailoverFlag()) {
            try {
              explicitFailover();
              keepRunning = false;
              cluster.resetFailoverTriggeredFlag(true);
            } catch (Exception e) {
              // Do Nothing
            } finally {
              keepRunning = false;
            }
          }
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            // DO NOTHING
          }
        }
      }
    };
    failoverThread.start();
    return failoverThread;
  }

  protected void startHACluster(int numOfNMs, boolean overrideClientRMService,
      boolean overrideRTS, boolean overrideApplicationMasterService)
      throws Exception {
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    cluster =
        new MiniYARNClusterForHATesting(TestRMFailover.class.getName(), 2,
            numOfNMs, 1, 1, false, overrideClientRMService, overrideRTS,
            overrideApplicationMasterService);
    cluster.resetStartFailoverFlag(false);
    cluster.init(conf);
    cluster.start();
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
    verifyConnections();

    // Do the failover
    explicitFailover();
    verifyConnections();

    failoverThread = createAndStartFailoverThread();

  }

  protected ResourceManager getActiveRM() {
    return cluster.getResourceManager(cluster.getActiveRMIndex());
  }

  public class MiniYARNClusterForHATesting extends MiniYARNCluster {

    private boolean overrideClientRMService;
    private boolean overrideRTS;
    private boolean overrideApplicationMasterService;
    private final AtomicBoolean startFailover = new AtomicBoolean(false);
    private final AtomicBoolean failoverTriggered = new AtomicBoolean(false);

    public MiniYARNClusterForHATesting(String testName,
        int numResourceManagers, int numNodeManagers, int numLocalDirs,
        int numLogDirs, boolean enableAHS, boolean overrideClientRMService,
        boolean overrideRTS, boolean overrideApplicationMasterService) {
      super(testName, numResourceManagers, numNodeManagers, numLocalDirs,
          numLogDirs, enableAHS);
      this.overrideClientRMService = overrideClientRMService;
      this.overrideRTS = overrideRTS;
      this.overrideApplicationMasterService = overrideApplicationMasterService;
    }

    public boolean getStartFailoverFlag() {
      return startFailover.get();
    }

    public void resetStartFailoverFlag(boolean flag) {
      startFailover.set(flag);
    }

    public void resetFailoverTriggeredFlag(boolean flag) {
      failoverTriggered.set(flag);
    }

    private boolean waittingForFailOver() {
      int maximumWaittingTime = 50;
      int count = 0;
      while (!failoverTriggered.get() && count <= maximumWaittingTime) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // DO NOTHING
        }
        count++;
      }
      if (count >= maximumWaittingTime && failoverThread != null) {
        return false;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // DO NOTHING
      }
      return true;
    }

    @Override
    protected ResourceManager createResourceManager() {
      return new ResourceManager() {
        @Override
        protected void doSecureLogin() throws IOException {
          // Don't try to login using keytab in the testcases.
        }
        @Override
        protected ClientRMService createClientRMService() {
          if (overrideClientRMService) {
            return new CustomedClientRMService(this.rmContext, this.scheduler,
                this.rmAppManager, this.applicationACLsManager,
                this.queueACLsManager,
                this.rmContext.getRMDelegationTokenSecretManager());
          }
          return super.createClientRMService();
        }
        @Override
        protected ResourceTrackerService createResourceTrackerService() {
          if (overrideRTS) {
            return new CustomedResourceTrackerService(this.rmContext,
                this.nodesListManager, this.nmLivelinessMonitor,
                this.rmContext.getContainerTokenSecretManager(),
                this.rmContext.getNMTokenSecretManager());
          }
          return super.createResourceTrackerService();
        }
        @Override
        protected ApplicationMasterService createApplicationMasterService() {
          if (overrideApplicationMasterService) {
            return new CustomedApplicationMasterService(this.rmContext,
                this.scheduler);
          }
          return super.createApplicationMasterService();
        }
      };
    }

    private class CustomedClientRMService extends ClientRMService {
      public CustomedClientRMService(RMContext rmContext,
          YarnScheduler scheduler, RMAppManager rmAppManager,
          ApplicationACLsManager applicationACLsManager,
          QueueACLsManager queueACLsManager,
          RMDelegationTokenSecretManager rmDTSecretManager) {
        super(rmContext, scheduler, rmAppManager, applicationACLsManager,
            queueACLsManager, rmDTSecretManager);
      }

      @Override
      public GetNewApplicationResponse getNewApplication(
          GetNewApplicationRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // create the GetNewApplicationResponse with fake applicationId
        GetNewApplicationResponse response =
            GetNewApplicationResponse.newInstance(
                createFakeAppId(), null, null);
        return response;
      }

      @Override
      public GetApplicationReportResponse getApplicationReport(
          GetApplicationReportRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // create a fake application report
        ApplicationReport report = createFakeAppReport();
        GetApplicationReportResponse response =
            GetApplicationReportResponse.newInstance(report);
        return response;
      }

      @Override
      public GetClusterMetricsResponse getClusterMetrics(
          GetClusterMetricsRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // create GetClusterMetricsResponse with fake YarnClusterMetrics
        GetClusterMetricsResponse response =
            GetClusterMetricsResponse.newInstance(
                createFakeYarnClusterMetrics());
        return response;
      }

      @Override
      public GetApplicationsResponse getApplications(
          GetApplicationsRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // create GetApplicationsResponse with fake applicationList
        GetApplicationsResponse response =
            GetApplicationsResponse.newInstance(createFakeAppReports());
        return response;
      }

      @Override
      public GetClusterNodesResponse getClusterNodes(
          GetClusterNodesRequest request)
          throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // create GetClusterNodesResponse with fake ClusterNodeLists
        GetClusterNodesResponse response =
            GetClusterNodesResponse.newInstance(createFakeNodeReports());
        return response;
      }

      @Override
      public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
          throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // return fake QueueInfo
        return GetQueueInfoResponse.newInstance(createFakeQueueInfo());
      }

      @Override
      public GetQueueUserAclsInfoResponse getQueueUserAcls(
          GetQueueUserAclsInfoRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // return fake queueUserAcls
        return GetQueueUserAclsInfoResponse
            .newInstance(createFakeQueueUserACLInfoList());
      }

      @Override
      public GetApplicationAttemptReportResponse getApplicationAttemptReport(
          GetApplicationAttemptReportRequest request) throws YarnException,
          IOException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // return fake ApplicationAttemptReport
        return GetApplicationAttemptReportResponse
            .newInstance(createFakeApplicationAttemptReport());
      }

      @Override
      public GetApplicationAttemptsResponse getApplicationAttempts(
          GetApplicationAttemptsRequest request) throws YarnException,
          IOException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // return fake ApplicationAttemptReports
        return GetApplicationAttemptsResponse
            .newInstance(createFakeApplicationAttemptReports());
      }

      @Override
      public GetContainerReportResponse getContainerReport(
          GetContainerReportRequest request) throws YarnException,
              IOException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // return fake containerReport
        return GetContainerReportResponse
            .newInstance(createFakeContainerReport());
      }

      @Override
      public GetContainersResponse getContainers(GetContainersRequest request)
          throws YarnException, IOException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        // return fake ContainerReports
        return GetContainersResponse.newInstance(createFakeContainerReports());
      }

      @Override
      public SubmitApplicationResponse submitApplication(
          SubmitApplicationRequest request) throws YarnException, IOException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        return super.submitApplication(request);
      }

      @Override
      public KillApplicationResponse forceKillApplication(
          KillApplicationRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        return KillApplicationResponse.newInstance(true);
      }

      @Override
      public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
          MoveApplicationAcrossQueuesRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        return Records.newRecord(MoveApplicationAcrossQueuesResponse.class);
      }

      @Override
      public GetDelegationTokenResponse getDelegationToken(
          GetDelegationTokenRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        return GetDelegationTokenResponse.newInstance(createFakeToken());
      }

      @Override
      public RenewDelegationTokenResponse renewDelegationToken(
          RenewDelegationTokenRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        return RenewDelegationTokenResponse
            .newInstance(createNextExpirationTime());
      }

      @Override
      public CancelDelegationTokenResponse cancelDelegationToken(
          CancelDelegationTokenRequest request) throws YarnException {
        resetStartFailoverFlag(true);

        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());

        return CancelDelegationTokenResponse.newInstance();
      }
    }

    public ApplicationReport createFakeAppReport() {
      ApplicationId appId = ApplicationId.newInstance(1000L, 1);
      ApplicationAttemptId attemptId =
          ApplicationAttemptId.newInstance(appId, 1);
      // create a fake application report
      ApplicationReport report =
          ApplicationReport.newInstance(appId, attemptId, "fakeUser",
              "fakeQueue", "fakeApplicationName", "localhost", 0, null,
              YarnApplicationState.FINISHED, "fake an application report", "",
              1000L, 1200L, FinalApplicationStatus.FAILED, null, "", 50f,
              "fakeApplicationType", null);
      return report;
    }

    public List<ApplicationReport> createFakeAppReports() {
      List<ApplicationReport> reports = new ArrayList<ApplicationReport>();
      reports.add(createFakeAppReport());
      return reports;
    }

    public ApplicationId createFakeAppId() {
      return ApplicationId.newInstance(1000L, 1);
    }

    public ApplicationAttemptId createFakeApplicationAttemptId() {
      return ApplicationAttemptId.newInstance(createFakeAppId(), 0);
    }

    public ContainerId createFakeContainerId() {
      return ContainerId.newContainerId(createFakeApplicationAttemptId(), 0);
    }

    public YarnClusterMetrics createFakeYarnClusterMetrics() {
      return YarnClusterMetrics.newInstance(1);
    }

    public List<NodeReport> createFakeNodeReports() {
      NodeId nodeId = NodeId.newInstance("localhost", 0);
      NodeReport report =
          NodeReport.newInstance(nodeId, NodeState.RUNNING, "localhost",
              "rack1", null, null, 4, null, 1000L);
      List<NodeReport> reports = new ArrayList<NodeReport>();
      reports.add(report);
      return reports;
    }

    public QueueInfo createFakeQueueInfo() {
      return QueueInfo.newInstance("root", 100f, 100f, 50f, null,
          createFakeAppReports(), QueueState.RUNNING, null, null, null, false);
    }

    public List<QueueUserACLInfo> createFakeQueueUserACLInfoList() {
      List<QueueACL> queueACL = new ArrayList<QueueACL>();
      queueACL.add(QueueACL.SUBMIT_APPLICATIONS);
      QueueUserACLInfo info = QueueUserACLInfo.newInstance("root", queueACL);
      List<QueueUserACLInfo> infos = new ArrayList<QueueUserACLInfo>();
      infos.add(info);
      return infos;
    }

    public ApplicationAttemptReport createFakeApplicationAttemptReport() {
      return ApplicationAttemptReport.newInstance(
          createFakeApplicationAttemptId(), "localhost", 0, "", "", "",
          YarnApplicationAttemptState.RUNNING, createFakeContainerId(), 1000L,
          1200L);
    }

    public List<ApplicationAttemptReport>
        createFakeApplicationAttemptReports() {
      List<ApplicationAttemptReport> reports =
          new ArrayList<ApplicationAttemptReport>();
      reports.add(createFakeApplicationAttemptReport());
      return reports;
    }

    public ContainerReport createFakeContainerReport() {
      return ContainerReport.newInstance(createFakeContainerId(), null,
          NodeId.newInstance("localhost", 0), null, 1000L, 1200L, "", "", 0,
          ContainerState.COMPLETE,
          "http://" + NodeId.newInstance("localhost", 0).toString());
    }

    public List<ContainerReport> createFakeContainerReports() {
      List<ContainerReport> reports =
          new ArrayList<ContainerReport>();
      reports.add(createFakeContainerReport());
      return reports;
    }

    public Token createFakeToken() {
      String identifier = "fake Token";
      String password = "fake token passwd";
      Token token = Token.newInstance(
          identifier.getBytes(), " ", password.getBytes(), " ");
      return token;
    }

    public long createNextExpirationTime() {
      return "fake Token".getBytes().length;
    }

    private class CustomedResourceTrackerService extends
        ResourceTrackerService {
      public CustomedResourceTrackerService(RMContext rmContext,
          NodesListManager nodesListManager,
          NMLivelinessMonitor nmLivelinessMonitor,
          RMContainerTokenSecretManager containerTokenSecretManager,
          NMTokenSecretManagerInRM nmTokenSecretManager) {
        super(rmContext, nodesListManager, nmLivelinessMonitor,
            containerTokenSecretManager, nmTokenSecretManager);
      }

      @Override
      public RegisterNodeManagerResponse registerNodeManager(
          RegisterNodeManagerRequest request) throws YarnException,
          IOException {
        resetStartFailoverFlag(true);
        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());
        return super.registerNodeManager(request);
      }

      @Override
      public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
          throws YarnException, IOException {
        resetStartFailoverFlag(true);
        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());
        return super.nodeHeartbeat(request);
      }
    }

    private class CustomedApplicationMasterService extends
        ApplicationMasterService {
      public CustomedApplicationMasterService(RMContext rmContext,
          YarnScheduler scheduler) {
        super(rmContext, scheduler);
      }

      @Override
      public AllocateResponse allocate(AllocateRequest request)
          throws YarnException, IOException {
        resetStartFailoverFlag(true);
        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());
        return createFakeAllocateResponse();
      }

      @Override
      public RegisterApplicationMasterResponse registerApplicationMaster(
          RegisterApplicationMasterRequest request) throws YarnException,
          IOException {
        resetStartFailoverFlag(true);
        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());
        return createFakeRegisterApplicationMasterResponse();
      }

      @Override
      public FinishApplicationMasterResponse finishApplicationMaster(
          FinishApplicationMasterRequest request) throws YarnException,
          IOException {
        resetStartFailoverFlag(true);
        // make sure failover has been triggered
        Assert.assertTrue(waittingForFailOver());
        return createFakeFinishApplicationMasterResponse();
      }
    }

    public RegisterApplicationMasterResponse
    createFakeRegisterApplicationMasterResponse() {
      Resource minCapability = Resource.newInstance(2048, 2);
      Resource maxCapability = Resource.newInstance(4096, 4);
      Map<ApplicationAccessType, String> acls =
          new HashMap<ApplicationAccessType, String>();
      acls.put(ApplicationAccessType.MODIFY_APP, "*");
      ByteBuffer key = ByteBuffer.wrap("fake_key".getBytes());
      return RegisterApplicationMasterResponse.newInstance(minCapability,
          maxCapability, acls, key, new ArrayList<Container>(), "root",
          new ArrayList<NMToken>());
    }

    public FinishApplicationMasterResponse
    createFakeFinishApplicationMasterResponse() {
      return FinishApplicationMasterResponse.newInstance(true);
    }

    public AllocateResponse createFakeAllocateResponse() {
      if (YarnConfiguration.timelineServiceV2Enabled(getConfig())) {
        return AllocateResponse.newInstance(-1,
            new ArrayList<ContainerStatus>(), new ArrayList<Container>(),
            new ArrayList<NodeReport>(), Resource.newInstance(1024, 2), null, 1,
            null, new ArrayList<NMToken>(), CollectorInfo.newInstance(
            "host:port", Token.newInstance(new byte[] {0}, "TIMELINE",
            new byte[] {0}, "rm")));
      } else {
        return AllocateResponse.newInstance(-1,
            new ArrayList<ContainerStatus>(),
            new ArrayList<Container>(), new ArrayList<NodeReport>(),
            Resource.newInstance(1024, 2), null, 1,
            null, new ArrayList<NMToken>());
      }
    }
  }

}
