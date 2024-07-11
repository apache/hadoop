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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.mockito.Mockito;
import org.junit.Assert;

public class TestCheckRemoveZKNodeRMStateStore extends RMStateStoreTestBase {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestCheckRemoveZKNodeRMStateStore.class);
  private TestingServer curatorTestingServer;
  private CuratorFramework curatorFramework;

  public static TestingServer setupCuratorServer() throws Exception {
    TestingServer curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    return curatorTestingServer;
  }

  public static CuratorFramework setupCuratorFramework(
      TestingServer curatorTestingServer) throws Exception {
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(curatorTestingServer.getConnectString())
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();
    return curatorFramework;
  }

  @Before
  public void setupCurator() throws Exception {
    curatorTestingServer = setupCuratorServer();
    curatorFramework = setupCuratorFramework(curatorTestingServer);
  }

  @After
  public void cleanupCuratorServer() throws IOException {
    curatorFramework.close();
    curatorTestingServer.stop();
  }

  class TestZKRMStateStoreTester implements RMStateStoreHelper {

    private TestZKRMStateStoreInternal store;
    private String workingZnode;

    class TestZKRMStateStoreInternal extends ZKRMStateStore {

      private ResourceManager resourceManager;
      private ZKCuratorManager zkCuratorManager;
      TestZKRMStateStoreInternal(Configuration conf, String workingZnode)
          throws Exception {
        resourceManager = Mockito.mock(ResourceManager.class);
        zkCuratorManager = Mockito.mock(ZKCuratorManager.class, Mockito.RETURNS_DEEP_STUBS);

        Mockito.when(resourceManager.getZKManager()).thenReturn(zkCuratorManager);
        Mockito.when(resourceManager.createAndStartZKManager(conf)).thenReturn(zkCuratorManager);
        Mockito.when(zkCuratorManager.exists(getAppNode("application_1708333280_0001")))
                .thenReturn(true);
        Mockito.when(zkCuratorManager.exists(getAppNode("application_1708334188_0001")))
                .thenReturn(true).thenReturn(false);
        Mockito.when(zkCuratorManager.exists(getDelegationTokenNode(0, 0)))
                .thenReturn(true).thenReturn(false);
        Mockito.when(zkCuratorManager.exists(getAppNode("application_1709705779_0001")))
                .thenReturn(true);
        Mockito.when(zkCuratorManager.exists(getAttemptNode("application_1709705779_0001",
                        "appattempt_1709705779_0001_000001")))
                .thenReturn(true);
        Mockito.doThrow(new KeeperException.NoNodeException()).when(zkCuratorManager)
                .safeDelete(Mockito.anyString(), Mockito.anyList(), Mockito.anyString());

        setResourceManager(resourceManager);
        init(conf);
        dispatcher.disableExitOnDispatchException();
        start();

        Assert.assertTrue(znodeWorkingPath.equals(workingZnode));
      }

      private String getVersionNode() {
        return znodeWorkingPath + "/" + ROOT_ZNODE_NAME + "/" + VERSION_NODE;
      }

      @Override
      public Version getCurrentVersion() {
        return CURRENT_VERSION_INFO;
      }

      private String getAppNode(String appId, int splitIdx) {
        String rootPath = workingZnode + "/" + ROOT_ZNODE_NAME + "/" +
            RM_APP_ROOT;
        String appPath = appId;
        if (splitIdx != 0) {
          int idx = appId.length() - splitIdx;
          appPath = appId.substring(0, idx) + "/" + appId.substring(idx);
          return rootPath + "/" + RM_APP_ROOT_HIERARCHIES + "/" +
              Integer.toString(splitIdx) + "/" + appPath;
        }
        return rootPath + "/" + appPath;
      }

      private String getAppNode(String appId) {
        return getAppNode(appId, 0);
      }

      private String getAttemptNode(String appId, String attemptId) {
        return getAppNode(appId) + "/" + attemptId;
      }

      private String getDelegationTokenNode(int rmDTSequenceNumber, int splitIdx) {
        String rootPath = workingZnode + "/" + ROOT_ZNODE_NAME + "/" +
            RM_DT_SECRET_MANAGER_ROOT + "/" +
            RMStateStore.RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME;
        String nodeName = DELEGATION_TOKEN_PREFIX;
        if (splitIdx == 0) {
          nodeName += rmDTSequenceNumber;
        } else {
          nodeName += String.format("%04d", rmDTSequenceNumber);
        }
        String path = nodeName;
        if (splitIdx != 0) {
          int idx = nodeName.length() - splitIdx;
          path = splitIdx + "/" + nodeName.substring(0, idx) + "/"
              + nodeName.substring(idx);
        }
        return rootPath + "/" + path;
      }
    }

    private RMStateStore createStore(Configuration conf) throws Exception {
      workingZnode = "/jira/issue/11626/rmstore";
      conf.set(CommonConfigurationKeys.ZK_ADDRESS,
          curatorTestingServer.getConnectString());
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
      conf.setLong(YarnConfiguration.RM_EPOCH, epoch);
      conf.setLong(YarnConfiguration.RM_EPOCH_RANGE, getEpochRange());
      this.store = new TestZKRMStateStoreInternal(conf, workingZnode);
      return this.store;
    }

    public RMStateStore getRMStateStore(Configuration conf) throws Exception {
      return createStore(conf);
    }

    @Override
    public RMStateStore getRMStateStore() throws Exception {
      YarnConfiguration conf = new YarnConfiguration();
      return createStore(conf);
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      return 1 ==
          curatorFramework.getChildren().forPath(store.znodeWorkingPath).size();
    }

    @Override
    public void writeVersion(Version version) throws Exception {
      curatorFramework.setData().withVersion(-1)
          .forPath(store.getVersionNode(),
              ((VersionPBImpl) version).getProto().toByteArray());
    }

    @Override
    public Version getCurrentVersion() throws Exception {
      return store.getCurrentVersion();
    }

    @Override
    public boolean appExists(RMApp app) throws Exception {
      String appIdPath = app.getApplicationId().toString();
      int split =
          store.getConfig().getInt(YarnConfiguration.ZK_APPID_NODE_SPLIT_INDEX,
          YarnConfiguration.DEFAULT_ZK_APPID_NODE_SPLIT_INDEX);
      return null != curatorFramework.checkExists()
          .forPath(store.getAppNode(appIdPath, split));
    }

    @Override
    public boolean attemptExists(RMAppAttempt attempt) throws Exception {
      ApplicationAttemptId attemptId = attempt.getAppAttemptId();
      return null != curatorFramework.checkExists()
          .forPath(store.getAttemptNode(
              attemptId.getApplicationId().toString(), attemptId.toString()));
    }
  }

  @Test (timeout = 60000)
  public void testSafeDeleteZKNode() throws Exception  {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    testRemoveAttempt(zkTester);
    testRemoveApplication(zkTester);
    testRemoveRMDelegationToken(zkTester);
    testRemoveRMDTMasterKeyState(zkTester);
    testRemoveReservationState(zkTester);
    testTransitionedToStandbyAfterCheckNode(zkTester);
  }

  public void testRemoveAttempt(RMStateStoreHelper stateStoreHelper) throws Exception  {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    ApplicationId appIdRemoved = ApplicationId.newInstance(1708333280, 1);
    storeApp(store, appIdRemoved, 123456, 654321);

    ApplicationAttemptId attemptIdRemoved = ApplicationAttemptId.newInstance(appIdRemoved, 1);
    storeAttempt(store, attemptIdRemoved,
            ContainerId.newContainerId(attemptIdRemoved, 1).toString(), null, null, dispatcher);

    try {
      store.removeApplicationAttemptInternal(attemptIdRemoved);
    } catch (KeeperException.NoNodeException nne) {
      Assert.fail("NoNodeException should not happen.");
    }

    // The verification method safeDelete is called once.
    Mockito.verify(store.resourceManager.getZKManager(), Mockito.times(1))
            .safeDelete(Mockito.anyString(), Mockito.anyList(), Mockito.anyString());

    store.close();
  }

  public void testRemoveApplication(RMStateStoreHelper stateStoreHelper) throws Exception  {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    ApplicationId appIdRemoved = ApplicationId.newInstance(1708334188, 1);
    storeApp(store, appIdRemoved, 123456, 654321);

    ApplicationAttemptId attemptIdRemoved = ApplicationAttemptId.newInstance(appIdRemoved, 1);
    storeAttempt(store, attemptIdRemoved,
            ContainerId.newContainerId(attemptIdRemoved, 1).toString(), null, null, dispatcher);

    ApplicationSubmissionContext context = new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appIdRemoved);

    ApplicationStateData appStateRemoved =
            ApplicationStateData.newInstance(
                    123456, 654321, context, "user1");
    appStateRemoved.attempts.put(attemptIdRemoved, null);

    try {
      // The occurrence of NoNodeException is induced by calling the safeDelete method.
      store.removeApplicationStateInternal(appStateRemoved);
    } catch (KeeperException.NoNodeException nne) {
      Assert.fail("NoNodeException should not happen.");
    }

    store.close();
  }

  public void testRemoveRMDelegationToken(RMStateStoreHelper stateStoreHelper) throws Exception{
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    RMDelegationTokenIdentifier tokenIdRemoved = new RMDelegationTokenIdentifier();

    try {
      store.removeRMDelegationTokenState(tokenIdRemoved);
    } catch (KeeperException.NoNodeException nne) {
      Assert.fail("NoNodeException should not happen.");
    }

    // The verification method safeDelete is called once.
    Mockito.verify(store.resourceManager.getZKManager(), Mockito.times(1))
            .safeDelete(Mockito.anyString(), Mockito.anyList(), Mockito.anyString());

    store.close();
  }

  public void testRemoveRMDTMasterKeyState(RMStateStoreHelper stateStoreHelper) throws Exception{
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    DelegationKey keyRemoved = new DelegationKey();

    try {
      store.removeRMDTMasterKeyState(keyRemoved);
    } catch (KeeperException.NoNodeException nne) {
      Assert.fail("NoNodeException should not happen.");
    }

    // The verification method safeDelete is called once.
    Mockito.verify(store.resourceManager.getZKManager(), Mockito.times(1))
            .safeDelete(Mockito.anyString(), Mockito.anyList(), Mockito.anyString());

    store.close();
  }

  public void testRemoveReservationState(RMStateStoreHelper stateStoreHelper) throws Exception{
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    String planName = "test-reservation";
    ReservationId reservationIdRemoved = ReservationId.newInstance(1708414427, 1);

    try {
      store.removeReservationState(planName, reservationIdRemoved.toString());
    } catch (KeeperException.NoNodeException nne) {
      Assert.fail("NoNodeException should not happen.");
    }

    // The verification method safeDelete is called once.
    Mockito.verify(store.resourceManager.getZKManager(), Mockito.times(1))
            .safeDelete(Mockito.anyString(), Mockito.anyList(), Mockito.anyString());

    store.close();
  }

  public void testTransitionedToStandbyAfterCheckNode(RMStateStoreHelper stateStoreHelper)
          throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();

    HAServiceProtocol.StateChangeRequestInfo req = new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    Configuration conf = new YarnConfiguration();
    ResourceManager rm = new MockRM(conf, store);
    rm.init(conf);
    rm.start();

    // Transition to active.
    rm.getRMContext().getRMAdminService().transitionToActive(req);
    Assert.assertEquals("RM with ZKStore didn't start",
            Service.STATE.STARTED, rm.getServiceState());
    Assert.assertEquals("RM should be Active",
            HAServiceProtocol.HAServiceState.ACTIVE,
            rm.getRMContext().getRMAdminService().getServiceStatus().getState());

    // Simulate throw NodeExistsException
    ZKRMStateStore zKStore = (ZKRMStateStore) rm.getRMContext().getStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    zKStore.setRMDispatcher(dispatcher);

    ApplicationId appIdRemoved = ApplicationId.newInstance(1709705779, 1);
    storeApp(zKStore, appIdRemoved, 123456, 654321);

    ApplicationAttemptId attemptIdRemoved = ApplicationAttemptId.newInstance(appIdRemoved, 1);
    storeAttempt(zKStore, attemptIdRemoved,
            ContainerId.newContainerId(attemptIdRemoved, 1).toString(), null, null, dispatcher);

    try {
      zKStore.removeApplicationAttemptInternal(attemptIdRemoved);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof KeeperException.NodeExistsException);
    }

    rm.close();
  }
}