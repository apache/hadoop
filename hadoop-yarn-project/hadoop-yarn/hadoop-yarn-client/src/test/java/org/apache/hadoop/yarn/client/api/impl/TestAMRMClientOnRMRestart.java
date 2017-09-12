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
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAMRMClientOnRMRestart {
  static Configuration conf = new Configuration();
  static final int rolling_interval_sec = 13;
  static final long am_expire_ms = 4000;

  @BeforeClass
  public static void setup() throws Exception {
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, false);
    SecurityUtil.setConfiguration(conf);

    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.setLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 0);
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "0.0.0.0:0");
  }

  @AfterClass
  public static void tearDown() {
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, true);
    SecurityUtil.setConfiguration(conf);
  }

  // Test does major 6 steps verification.
  // Step-1 : AMRMClient send allocate request for 3 container requests
  // Step-2 : 3 containers are allocated by RM.
  // Step-3 : AM Send 1 containerRequest(cRequest4) and 1 releaseRequests to
  // RM
  // Step-3.5 : AM Send 1 container resource increase request to RM
  // Step-4 : On RM restart, AM(does not know RM is restarted) sends additional
  // containerRequest(cRequest5) and blacklisted nodes.
  // Intern RM send resync command
  // Verify AM can recover increase request after resync
  // Step-5 : Allocater after resync command & new containerRequest(cRequest6)
  // Step-6 : RM allocates containers i.e cRequest4,cRequest5 and cRequest6
  @Test(timeout = 60000)
  public void testAMRMClientResendsRequestsOnRMRestart() throws Exception {

    UserGroupInformation.setLoginUser(null);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // Phase-1 Start 1st RM
    MyResourceManager rm1 = new MyResourceManager(conf, memStore);
    rm1.start();

    // Submit the application
    RMApp app = rm1.submitApp(1024);
    rm1.drainEvents();

    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true); // Node heartbeat
    rm1.drainEvents();

    ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    rm1.sendAMLaunched(appAttemptId);
    rm1.drainEvents();

    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> token =
        rm1.getRMContext().getRMApps().get(appAttemptId.getApplicationId())
            .getRMAppAttempt(appAttemptId).getAMRMToken();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.addTokenIdentifier(token.decodeIdentifier());

    // Step-1 : AMRMClient send allocate request for 3 ContainerRequest
    // cRequest1 = h1, cRequest2 = h1,h2 and cRequest3 = h1
    // blacklisted nodes = h2
    AMRMClient<ContainerRequest> amClient = new MyAMRMClientImpl(rm1);
    amClient.init(conf);
    amClient.start();

    amClient.registerApplicationMaster("Host", 10000, "");

    ContainerRequest cRequest1 = createReq(1, 1024, new String[] { "h1" });
    amClient.addContainerRequest(cRequest1);

    ContainerRequest cRequest2 =
        createReq(1, 1024, new String[] { "h1", "h2" });
    amClient.addContainerRequest(cRequest2);

    ContainerRequest cRequest3 = createReq(1, 1024, new String[] { "h1" });
    amClient.addContainerRequest(cRequest3);

    List<String> blacklistAdditions = new ArrayList<String>();
    List<String> blacklistRemoval = new ArrayList<String>();
    blacklistAdditions.add("h2");
    blacklistRemoval.add("h10");
    amClient.updateBlacklist(blacklistAdditions, blacklistRemoval);
    blacklistAdditions.remove("h2");// remove from local list

    AllocateResponse allocateResponse = amClient.allocate(0.1f);
    rm1.drainEvents();
    Assert.assertEquals("No of assignments must be 0", 0, allocateResponse
        .getAllocatedContainers().size());

    // Why 4 ask, why not 3 ask even h2 is blacklisted?
    // On blacklisting host,applicationmaster has to remove ask request from
    // remoterequest table.Here,test does not remove explicitely
    assertAsksAndReleases(4, 0, rm1);
    assertBlacklistAdditionsAndRemovals(1, 1, rm1);

    // Step-2 : NM heart beat is sent.
    // On 2nd AM allocate request, RM allocates 3 containers to AM
    nm1.nodeHeartbeat(true); // Node heartbeat
    rm1.drainEvents();

    allocateResponse = amClient.allocate(0.2f);
    rm1.drainEvents();
    // 3 containers are allocated i.e for cRequest1, cRequest2 and cRequest3.
    Assert.assertEquals("No of assignments must be 0", 3, allocateResponse
        .getAllocatedContainers().size());
    assertAsksAndReleases(0, 0, rm1);
    assertBlacklistAdditionsAndRemovals(0, 0, rm1);

    List<Container> allocatedContainers =
        allocateResponse.getAllocatedContainers();
    // removed allocated container requests
    amClient.removeContainerRequest(cRequest1);
    amClient.removeContainerRequest(cRequest2);
    amClient.removeContainerRequest(cRequest3);

    allocateResponse = amClient.allocate(0.2f);
    rm1.drainEvents();
    Assert.assertEquals("No of assignments must be 0", 0, allocateResponse
        .getAllocatedContainers().size());
    assertAsksAndReleases(4, 0, rm1);
    assertBlacklistAdditionsAndRemovals(0, 0, rm1);

    // Step-3 : Send 1 containerRequest and 1 releaseRequests to RM
    ContainerRequest cRequest4 = createReq(1, 1024, new String[] { "h1" });
    amClient.addContainerRequest(cRequest4);

    int pendingRelease = 0;
    Iterator<Container> it = allocatedContainers.iterator();
    while (it.hasNext()) {
      amClient.releaseAssignedContainer(it.next().getId());
      pendingRelease++;
      it.remove();
      break;// remove one container
    }

    // Step-3.5 : Send 1 container resource increase request to RM
    Container container = it.next();
    ContainerId containerId = container.getId();
    // Make sure that container is in RUNNING state before sending increase
    // request
    nm1.nodeHeartbeat(containerId.getApplicationAttemptId(),
        containerId.getContainerId(), ContainerState.RUNNING);
    rm1.drainEvents();
    amClient.requestContainerUpdate(
        container, UpdateContainerRequest.newInstance(
            container.getVersion(), container.getId(),
            ContainerUpdateType.INCREASE_RESOURCE,
            Resource.newInstance(2048, 1), null));
    it.remove();

    allocateResponse = amClient.allocate(0.3f);
    rm1.drainEvents();
    Assert.assertEquals("No of assignments must be 0", 0, allocateResponse
        .getAllocatedContainers().size());
    assertAsksAndReleases(3, pendingRelease, rm1);
    // Verify there is one increase and zero decrease
    assertChanges(1, 0, rm1);
    assertBlacklistAdditionsAndRemovals(0, 0, rm1);
    int completedContainer =
        allocateResponse.getCompletedContainersStatuses().size();
    pendingRelease -= completedContainer;

    // Phase-2 start 2nd RM is up
    MyResourceManager rm2 = new MyResourceManager(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    ((MyAMRMClientImpl) amClient).updateRMProxy(rm2);

    // NM should be rebooted on heartbeat, even first heartbeat for nm2
    NodeHeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());

    // new NM to represent NM re-register
    nm1 = new MockNM("h1:1234", 10240, rm2.getResourceTrackerService());
    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, 0, ContainerState.RUNNING,
            Resource.newInstance(1024, 1), "recover container", 0,
            Priority.newInstance(0), 0);
    nm1.registerNode(Collections.singletonList(containerReport),
        Collections.singletonList(
            containerId.getApplicationAttemptId().getApplicationId()));
    nm1.nodeHeartbeat(true);
    rm2.drainEvents();

    blacklistAdditions.add("h3");
    amClient.updateBlacklist(blacklistAdditions, null);
    blacklistAdditions.remove("h3");

    it = allocatedContainers.iterator();
    while (it.hasNext()) {
      amClient.releaseAssignedContainer(it.next().getId());
      pendingRelease++;
      it.remove();
    }

    ContainerRequest cRequest5 =
        createReq(1, 1024, new String[] { "h1", "h2" });
    amClient.addContainerRequest(cRequest5);

    // Step-4 : On RM restart, AM(does not know RM is restarted) sends
    // additional
    // containerRequest and blacklisted nodes.
    // Intern RM send resync command,AMRMClient resend allocate request
    allocateResponse = amClient.allocate(0.3f);
    rm2.drainEvents();

    completedContainer =
        allocateResponse.getCompletedContainersStatuses().size();
    pendingRelease -= completedContainer;

    assertAsksAndReleases(4, pendingRelease, rm2);
    // Verify there is one increase and zero decrease
    assertChanges(1, 0, rm2);
    assertBlacklistAdditionsAndRemovals(2, 0, rm2);

    ContainerRequest cRequest6 =
        createReq(1, 1024, new String[] { "h1", "h2", "h3" });
    amClient.addContainerRequest(cRequest6);

    // Step-5 : Allocater after resync command
    allocateResponse = amClient.allocate(0.5f);
    rm2.drainEvents();
    Assert.assertEquals("No of assignments must be 0", 0, allocateResponse
        .getAllocatedContainers().size());

    assertAsksAndReleases(5, 0, rm2);
    // Verify there is no increase or decrease requests any more
    assertChanges(0, 0, rm2);
    assertBlacklistAdditionsAndRemovals(0, 0, rm2);

    int noAssignedContainer = 0;
    int count = 5;
    while (count-- > 0) {
      nm1.nodeHeartbeat(true);
      rm2.drainEvents();

      allocateResponse = amClient.allocate(0.5f);
      rm2.drainEvents();
      noAssignedContainer += allocateResponse.getAllocatedContainers().size();
      if (noAssignedContainer == 3) {
        break;
      }
      Thread.sleep(1000);
    }

    // Step-6 : RM allocates containers i.e cRequest4,cRequest5 and cRequest6
    Assert.assertEquals("Number of container should be 3", 3,
        noAssignedContainer);

    amClient.stop();
    rm1.stop();
    rm2.stop();
  }

  // Test verify for
  // 1. AM try to unregister without registering
  // 2. AM register to RM, and try to unregister immediately after RM restart
  @Test(timeout = 60000)
  public void testAMRMClientForUnregisterAMOnRMRestart() throws Exception {

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // Phase-1 Start 1st RM
    MyResourceManager rm1 = new MyResourceManager(conf, memStore);
    rm1.start();

    // Submit the application
    RMApp app = rm1.submitApp(1024);
    rm1.drainEvents();

    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true); // Node heartbeat
    rm1.drainEvents();

    ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    rm1.sendAMLaunched(appAttemptId);
    rm1.drainEvents();

    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> token =
        rm1.getRMContext().getRMApps().get(appAttemptId.getApplicationId())
            .getRMAppAttempt(appAttemptId).getAMRMToken();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.addTokenIdentifier(token.decodeIdentifier());

    AMRMClient<ContainerRequest> amClient = new MyAMRMClientImpl(rm1);
    amClient.init(conf);
    amClient.start();

    amClient.registerApplicationMaster("h1", 10000, "");
    amClient.allocate(0.1f);

    // Phase-2 start 2nd RM is up
    MyResourceManager rm2 = new MyResourceManager(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    ((MyAMRMClientImpl) amClient).updateRMProxy(rm2);

    // NM should be rebooted on heartbeat, even first heartbeat for nm2
    NodeHeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());

    // new NM to represent NM re-register
    nm1 = new MockNM("h1:1234", 10240, rm2.getResourceTrackerService());

    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, 0, ContainerState.RUNNING,
            Resource.newInstance(1024, 1), "recover container", 0,
            Priority.newInstance(0), 0);
    nm1.registerNode(Arrays.asList(containerReport), null);
    nm1.nodeHeartbeat(true);
    rm2.drainEvents();

    amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
        null, null);
    rm2.waitForState(appAttemptId, RMAppAttemptState.FINISHING);
    nm1.nodeHeartbeat(appAttemptId, 1, ContainerState.COMPLETE);
    rm2.waitForState(appAttemptId, RMAppAttemptState.FINISHED);
    rm2.waitForState(app.getApplicationId(), RMAppState.FINISHED);

    amClient.stop();
    rm1.stop();
    rm2.stop();
  }


  // Test verify for AM issued with rolled-over AMRMToken
  // is still able to communicate with restarted RM.
  @Test(timeout = 30000)
  public void testAMRMClientOnAMRMTokenRollOverOnRMRestart() throws Exception {
    conf.setLong(
      YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
      rolling_interval_sec);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, am_expire_ms);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // start first RM
    MyResourceManager2 rm1 = new MyResourceManager2(conf, memStore);
    rm1.start();
    Long startTime = System.currentTimeMillis();
    // Submit the application
    RMApp app = rm1.submitApp(1024);
    rm1.drainEvents();

    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true); // Node heartbeat
    rm1.drainEvents();

    ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    rm1.sendAMLaunched(appAttemptId);
    rm1.drainEvents();

    AMRMTokenSecretManager amrmTokenSecretManagerForRM1 =
        rm1.getRMContext().getAMRMTokenSecretManager();
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> token =
        amrmTokenSecretManagerForRM1.createAndGetAMRMToken(appAttemptId);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.addTokenIdentifier(token.decodeIdentifier());

    AMRMClient<ContainerRequest> amClient = new MyAMRMClientImpl(rm1);
    amClient.init(conf);
    amClient.start();

    amClient.registerApplicationMaster("h1", 10000, "");
    amClient.allocate(0.1f);

    // Wait for enough time and make sure the roll_over happens
    // At mean time, the old AMRMToken should continue to work
    while (System.currentTimeMillis() - startTime < rolling_interval_sec * 1000) {
      amClient.allocate(0.1f);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // DO NOTHING
      }
    }
    Assert.assertTrue(amrmTokenSecretManagerForRM1.getMasterKey()
      .getMasterKey().getKeyId() != token.decodeIdentifier().getKeyId());

    amClient.allocate(0.1f);

    // active the nextMasterKey, and replace the currentMasterKey
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> newToken =
        amrmTokenSecretManagerForRM1.createAndGetAMRMToken(appAttemptId);
    int waitCount = 0;
    while (waitCount++ <= 50) {
      if (amrmTokenSecretManagerForRM1.getCurrnetMasterKeyData().getMasterKey()
        .getKeyId() != token.decodeIdentifier().getKeyId()) {
        break;
      }
      try {
        amClient.allocate(0.1f);
      } catch (Exception ex) {
        break;
      }
      Thread.sleep(500);
    }
    Assert
      .assertTrue(amrmTokenSecretManagerForRM1.getNextMasterKeyData() == null);
    Assert.assertTrue(amrmTokenSecretManagerForRM1.getCurrnetMasterKeyData()
      .getMasterKey().getKeyId() == newToken.decodeIdentifier().getKeyId());

    // start 2nd RM
    final MyResourceManager2 rm2 = new MyResourceManager2(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    ((MyAMRMClientImpl) amClient).updateRMProxy(rm2);

    AMRMTokenSecretManager amrmTokenSecretManagerForRM2 =
        rm2.getRMContext().getAMRMTokenSecretManager();
    Assert.assertTrue(amrmTokenSecretManagerForRM2.getCurrnetMasterKeyData()
      .getMasterKey().getKeyId() == newToken.decodeIdentifier().getKeyId());
    Assert
      .assertTrue(amrmTokenSecretManagerForRM2.getNextMasterKeyData() == null);

    try {
      UserGroupInformation testUser =
          UserGroupInformation.createRemoteUser("testUser");
      SecurityUtil.setTokenService(token, rm2.getApplicationMasterService()
        .getBindAddress());
      testUser.addToken(token);
      testUser.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
        @Override
        public ApplicationMasterProtocol run() {
          return (ApplicationMasterProtocol) YarnRPC.create(conf).getProxy(
            ApplicationMasterProtocol.class,
            rm2.getApplicationMasterService().getBindAddress(), conf);
        }
      }).allocate(Records.newRecord(AllocateRequest.class));
      Assert.fail("The old Token should not work");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof InvalidToken);
      Assert.assertTrue(ex.getMessage().contains(
        "Invalid AMRMToken from "
            + token.decodeIdentifier().getApplicationAttemptId()));
    }

    // make sure the recovered AMRMToken works for new RM
    amClient.allocate(0.1f);
    amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
      null, null);
    amClient.stop();
    rm1.stop();
    rm2.stop();
  }

  private static class MyFifoScheduler extends FifoScheduler {

    public MyFifoScheduler(RMContext rmContext) {
      super();
      try {
        Configuration conf = new Configuration();
        reinitialize(conf, rmContext);
      } catch (IOException ie) {
        assert (false);
      }
    }

    List<ResourceRequest> lastAsk = null;
    List<ContainerId> lastRelease = null;
    List<UpdateContainerRequest> lastIncrease = null;
    List<UpdateContainerRequest> lastDecrease = null;
    List<String> lastBlacklistAdditions;
    List<String> lastBlacklistRemovals;

    // override this to copy the objects otherwise FifoScheduler updates the
    // numContainers in same objects as kept by RMContainerAllocator
    @Override
    public synchronized Allocation allocate(
        ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
        List<ContainerId> release, List<String> blacklistAdditions,
        List<String> blacklistRemovals,
        ContainerUpdates updateRequests) {
      List<ResourceRequest> askCopy = new ArrayList<ResourceRequest>();
      for (ResourceRequest req : ask) {
        ResourceRequest reqCopy =
            ResourceRequest.newInstance(req.getPriority(),
                req.getResourceName(), req.getCapability(),
                req.getNumContainers(), req.getRelaxLocality());
        askCopy.add(reqCopy);
      }
      lastAsk = ask;
      lastRelease = release;
      lastIncrease = updateRequests.getIncreaseRequests();
      lastDecrease = updateRequests.getDecreaseRequests();
      lastBlacklistAdditions = blacklistAdditions;
      lastBlacklistRemovals = blacklistRemovals;
      return super.allocate(applicationAttemptId, askCopy, release,
          blacklistAdditions, blacklistRemovals, updateRequests);
    }
  }

  private static class MyResourceManager extends MockRM {

    private static long fakeClusterTimeStamp = System.currentTimeMillis();

    public MyResourceManager(Configuration conf, RMStateStore store) {
      super(conf, store);
    }

    @Override
    public void serviceStart() throws Exception {
      super.serviceStart();
      // Ensure that the application attempt IDs for all the tests are the same
      // The application attempt IDs will be used as the login user names
      MyResourceManager.setClusterTimeStamp(fakeClusterTimeStamp);
    }

    @Override
    protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
      // Dispatch inline for test sanity
      return new EventHandler<SchedulerEvent>() {
        @Override
        public void handle(SchedulerEvent event) {
          scheduler.handle(event);
        }
      };
    }

    @Override
    protected ResourceScheduler createScheduler() {
      return new MyFifoScheduler(this.getRMContext());
    }

    MyFifoScheduler getMyFifoScheduler() {
      return (MyFifoScheduler) scheduler;
    }
  }

  private static class MyResourceManager2 extends MyResourceManager {

    public MyResourceManager2(Configuration conf, RMStateStore store) {
      super(conf, store);
    }

    @Override
    protected ApplicationMasterService createApplicationMasterService() {
      return new ApplicationMasterService(getRMContext(), scheduler);
    }
  }

  private static class MyAMRMClientImpl extends
      AMRMClientImpl<ContainerRequest> {
    private MyResourceManager rm;

    public MyAMRMClientImpl(MyResourceManager rm) {
      this.rm = rm;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      this.rmClient = this.rm.getApplicationMasterService();
    }

    @Override
    protected void serviceStop() throws Exception {
      rmClient = null;
      super.serviceStop();
    }

    public void updateRMProxy(MyResourceManager rm) {
      rmClient = rm.getApplicationMasterService();
    }
  }

  private static void assertBlacklistAdditionsAndRemovals(
      int expectedAdditions, int expectedRemovals, MyResourceManager rm) {
    Assert.assertEquals(expectedAdditions,
        rm.getMyFifoScheduler().lastBlacklistAdditions.size());
    Assert.assertEquals(expectedRemovals,
        rm.getMyFifoScheduler().lastBlacklistRemovals.size());
  }

  private static void assertAsksAndReleases(int expectedAsk,
      int expectedRelease, MyResourceManager rm) {
    Assert.assertEquals(expectedAsk, rm.getMyFifoScheduler().lastAsk.size());
    Assert.assertEquals(expectedRelease,
        rm.getMyFifoScheduler().lastRelease.size());
  }

  private static void assertChanges(
      int expectedIncrease, int expectedDecrease, MyResourceManager rm) {
    Assert.assertEquals(
        expectedIncrease, rm.getMyFifoScheduler().lastIncrease.size());
    Assert.assertEquals(
        expectedDecrease, rm.getMyFifoScheduler().lastDecrease.size());
  }

  private ContainerRequest createReq(int priority, int memory, String[] hosts) {
    Resource capability = Resource.newInstance(memory, 1);
    Priority priorityOfContainer = Priority.newInstance(priority);
    return new ContainerRequest(capability, hosts,
        new String[] { NetworkTopology.DEFAULT_RACK }, priorityOfContainer);
  }

}
