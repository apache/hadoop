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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

public class TestZKRMStateStore extends RMStateStoreTestBase {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestZKRMStateStore.class);
  private static final int ZK_TIMEOUT_MS = 1000;
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

    TestZKRMStateStoreInternal store;
    String workingZnode;

    class TestZKRMStateStoreInternal extends ZKRMStateStore {

      TestZKRMStateStoreInternal(Configuration conf, String workingZnode)
          throws Exception {
        setResourceManager(new ResourceManager());
        init(conf);
        dispatcher.disableExitOnDispatchException();
        start();
        assertTrue(znodeWorkingPath.equals(workingZnode));
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

      /**
       * Emulating retrying createRootDir not to raise NodeExist exception
       * @throws Exception
       */
      private void testRetryingCreateRootDir() throws Exception {
        create(znodeWorkingPath);
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
      workingZnode = "/jira/issue/3077/rmstore";
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

    public boolean delegationTokenExists(RMDelegationTokenIdentifier token,
        int index) throws Exception {
      int rmDTSequenceNumber = token.getSequenceNumber();
      return curatorFramework.checkExists().forPath(
          store.getDelegationTokenNode(rmDTSequenceNumber, index)) != null;
    }

    public int getDelegationTokenNodeSplitIndex() {
      return store.delegationTokenNodeSplitIndex;
    }
  }

  @Test (timeout = 60000)
  public void testZKRMStateStoreRealZK() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    testRMAppStateStore(zkTester);
    testRMDTSecretManagerStateStore(zkTester);
    testCheckVersion(zkTester);
    testEpoch(zkTester);
    testAppDeletion(zkTester);
    testDeleteStore(zkTester);
    testRemoveApplication(zkTester);
    testRemoveAttempt(zkTester);
    testAMRMTokenSecretManagerStateStore(zkTester);
    testReservationStateStore(zkTester);
    ((TestZKRMStateStoreTester.TestZKRMStateStoreInternal)
        zkTester.getRMStateStore()).testRetryingCreateRootDir();
    testProxyCA(zkTester);
  }

  @Test
  public void testZKNodeLimit() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    long submitTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis() + 1234;
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_ZK_ZNODE_SIZE_LIMIT_BYTES, 1);
    RMStateStore store = zkTester.getRMStateStore(conf);
    TestAppRejDispatcher dispatcher = new TestAppRejDispatcher();
    store.setRMDispatcher(dispatcher);
    ApplicationId appId1 =
        ApplicationId.fromString("application_1352994193343_0001");
    storeApp(store, appId1, submitTime, startTime);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return dispatcher.appsavefailedEvnt;
      }
    }, 100, 5000);
  }

  static class TestAppRejDispatcher extends TestDispatcher {
    private boolean appsavefailedEvnt;

    @Override
    public void handle(Event event) {
      if (event instanceof RMAppEvent && event.getType()
          .equals(RMAppEventType.APP_SAVE_FAILED)) {
        appsavefailedEvnt = true;
      }
    }

    ;
  }

  @Test (timeout = 60000)
  public void testCheckMajorVersionChange() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester() {
      Version VERSION_INFO = Version.newInstance(Integer.MAX_VALUE, 0);

      @Override
      public Version getCurrentVersion() throws Exception {
        return VERSION_INFO;
      }

      @Override
      public RMStateStore getRMStateStore() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();
        workingZnode = "/jira/issue/3077/rmstore";
        conf.set(CommonConfigurationKeys.ZK_ADDRESS,
            curatorTestingServer.getConnectString());
        conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
        this.store = new TestZKRMStateStoreInternal(conf, workingZnode) {
          Version storedVersion = null;

          @Override
          public Version getCurrentVersion() {
            return VERSION_INFO;
          }

          @Override
          protected synchronized Version loadVersion() throws Exception {
            return storedVersion;
          }

          @Override
          protected synchronized void storeVersion() throws Exception {
            storedVersion = VERSION_INFO;
          }
        };
        return this.store;
      }

    };
    // default version
    RMStateStore store = zkTester.getRMStateStore();
    Version defaultVersion = zkTester.getCurrentVersion();
    store.checkVersion();
    assertEquals("Store had wrong version",
        defaultVersion, store.loadVersion());
  }

  public static Configuration createHARMConf(String rmIds, String rmId,
      int adminPort, boolean autoFailoverEnabled,
      TestingServer curatorTestServer) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, rmIds);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, ZKRMStateStore.class.getName());
    conf.set(CommonConfigurationKeys.ZK_ADDRESS,
        curatorTestServer.getConnectString());
    conf.setInt(CommonConfigurationKeys.ZK_TIMEOUT_MS, ZK_TIMEOUT_MS);
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");
    conf.setBoolean(
        YarnConfiguration.AUTO_FAILOVER_ENABLED, autoFailoverEnabled);
    for (String rpcAddress : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      for (String id : HAUtil.getRMHAIds(conf)) {
        conf.set(HAUtil.addSuffix(rpcAddress, id), "localhost:0");
      }
    }
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADMIN_ADDRESS, rmId),
        "localhost:" + adminPort);
    return conf;
  }

  private static boolean verifyZKACL(String id, String scheme, int perm,
      List<ACL> acls) {
    for (ACL acl : acls) {
      if (acl.getId().getScheme().equals(scheme) &&
          acl.getId().getId().startsWith(id) &&
          acl.getPerms() == perm) {
        return true;
      }
    }
    return false;
  }

  /**
   * Test if RM can successfully start in HA disabled mode if it was previously
   * running in HA enabled mode. And then start it in HA mode after running it
   * with HA disabled. NoAuth Exception should not be sent by zookeeper and RM
   * should start successfully.
   */
  @Test
  public void testZKRootPathAcls() throws Exception {
    StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    String parentPath = YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH;
    String rootPath = parentPath + "/" + ZKRMStateStore.ROOT_ZNODE_NAME;

    // Start RM with HA enabled
    Configuration conf =
        createHARMConf("rm1,rm2", "rm1", 1234, false, curatorTestingServer);
    conf.set(YarnConfiguration.RM_ZK_ACL, "world:anyone:rwca");
    int perm = 23;// rwca=1+2+4+16
    ResourceManager rm = new MockRM(conf);
    rm.start();
    rm.getRMContext().getRMAdminService().transitionToActive(req);
    ZKRMStateStore stateStore = (ZKRMStateStore) rm.getRMContext().getStateStore();
    List<ACL> acls = stateStore.getACL(rootPath);
    assertThat(acls).hasSize(2);
    // CREATE and DELETE permissions for root node based on RM ID
    verifyZKACL("digest", "localhost", Perms.CREATE | Perms.DELETE, acls);
    verifyZKACL(
        "world", "anyone", Perms.ALL ^ (Perms.CREATE | Perms.DELETE), acls);

    acls = stateStore.getACL(parentPath);
    assertThat(acls).hasSize(1);
    assertEquals(perm, acls.get(0).getPerms());
    rm.close();

    // Now start RM with HA disabled. NoAuth Exception should not be thrown.
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, false);
    conf.set(YarnConfiguration.RM_ZK_ACL, YarnConfiguration.DEFAULT_RM_ZK_ACL);
    rm = new MockRM(conf);
    rm.start();
    rm.getRMContext().getRMAdminService().transitionToActive(req);
    acls = stateStore.getACL(rootPath);
    assertEquals(acls.size(), 1);
    verifyZKACL("world", "anyone", Perms.ALL, acls);
    rm.close();

    // Start RM with HA enabled.
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    rm = new MockRM(conf);
    rm.start();
    rm.getRMContext().getRMAdminService().transitionToActive(req);
    acls = stateStore.getACL(rootPath);
    assertThat(acls).hasSize(2);
    verifyZKACL("digest", "localhost", Perms.CREATE | Perms.DELETE, acls);
    verifyZKACL(
        "world", "anyone", Perms.ALL ^ (Perms.CREATE | Perms.DELETE), acls);
    rm.close();
  }

  @Test
  public void testFencing() throws Exception {
    StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    Configuration conf1 =
        createHARMConf("rm1,rm2", "rm1", 1234, false, curatorTestingServer);
    ResourceManager rm1 = new MockRM(conf1);
    rm1.start();
    rm1.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm1.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm1.getRMContext().getRMAdminService().getServiceStatus().getState());

    Configuration conf2 =
        createHARMConf("rm1,rm2", "rm2", 5678, false, curatorTestingServer);
    ResourceManager rm2 = new MockRM(conf2);
    rm2.start();
    rm2.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm2.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());

    for (int i = 0; i < ZK_TIMEOUT_MS / 50; i++) {
      if (HAServiceProtocol.HAServiceState.ACTIVE ==
          rm1.getRMContext().getRMAdminService().getServiceStatus().getState()) {
        Thread.sleep(100);
      }
    }
    assertEquals("RM should have been fenced",
        HAServiceProtocol.HAServiceState.STANDBY,
        rm1.getRMContext().getRMAdminService().getServiceStatus().getState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());
    rm1.close();
    rm2.close();
  }
  
  @Test
  public void testFencedState() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    RMStateStore store = zkTester.getRMStateStore();

    // Move state to FENCED from ACTIVE
    store.updateFencedState();
    assertEquals("RMStateStore should have been in fenced state",
            true, store.isFencedState());    

    long submitTime = System.currentTimeMillis();
    long startTime = submitTime + 1000;

    // Add a new app
    RMApp mockApp = mock(RMApp.class);
    ApplicationSubmissionContext context =
      new ApplicationSubmissionContextPBImpl();
    when(mockApp.getSubmitTime()).thenReturn(submitTime);
    when(mockApp.getStartTime()).thenReturn(startTime);
    when(mockApp.getApplicationSubmissionContext()).thenReturn(context);
    when(mockApp.getUser()).thenReturn("test");
    store.storeNewApplication(mockApp);
    assertEquals("RMStateStore should have been in fenced state",
            true, store.isFencedState());

    // Add a new attempt
    ClientToAMTokenSecretManagerInRM clientToAMTokenMgr =
            new ClientToAMTokenSecretManagerInRM();
    ApplicationAttemptId attemptId = ApplicationAttemptId.fromString(
        "appattempt_1234567894321_0001_000001");
    SecretKey clientTokenMasterKey =
                clientToAMTokenMgr.createMasterKey(attemptId);
    RMAppAttemptMetrics mockRmAppAttemptMetrics = 
         mock(RMAppAttemptMetrics.class);
    Container container = new ContainerPBImpl();
    container.setId(ContainerId.fromString("container_1234567891234_0001_01_000001"));
    RMAppAttempt mockAttempt = mock(RMAppAttempt.class);
    when(mockAttempt.getAppAttemptId()).thenReturn(attemptId);
    when(mockAttempt.getMasterContainer()).thenReturn(container);
    when(mockAttempt.getClientTokenMasterKey())
        .thenReturn(clientTokenMasterKey);
    when(mockAttempt.getRMAppAttemptMetrics())
        .thenReturn(mockRmAppAttemptMetrics);
    when(mockRmAppAttemptMetrics.getAggregateAppResourceUsage())
        .thenReturn(new AggregateAppResourceUsage(new HashMap<>()));
    store.storeNewApplicationAttempt(mockAttempt);
    assertEquals("RMStateStore should have been in fenced state",
            true, store.isFencedState());

    long finishTime = submitTime + 1000;
    // Update attempt
    ApplicationAttemptStateData newAttemptState =
      ApplicationAttemptStateData.newInstance(attemptId, container,
            store.getCredentialsFromAppAttempt(mockAttempt),
            startTime, RMAppAttemptState.FINISHED, "testUrl", 
            "test", FinalApplicationStatus.SUCCEEDED, 100, 
            finishTime, new HashMap<>(), new HashMap<>());
    store.updateApplicationAttemptState(newAttemptState);
    assertEquals("RMStateStore should have been in fenced state",
            true, store.isFencedState());

    // Update app
    ApplicationStateData appState = ApplicationStateData.newInstance(submitTime, 
            startTime, context, "test");
    store.updateApplicationState(appState);
    assertEquals("RMStateStore should have been in fenced state",
            true, store.isFencedState());

    // Remove app
    store.removeApplication(mockApp);
    assertEquals("RMStateStore should have been in fenced state",
            true, store.isFencedState());

    // store RM delegation token;
    RMDelegationTokenIdentifier dtId1 =
        new RMDelegationTokenIdentifier(new Text("owner1"),
            new Text("renewer1"), new Text("realuser1"));
    Long renewDate1 = new Long(System.currentTimeMillis()); 
    dtId1.setSequenceNumber(1111);
    assertFalse("Token " + dtId1
        + " should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(dtId1, 0));
    store.storeRMDelegationToken(dtId1, renewDate1);
    assertFalse("Token " + dtId1
        + " should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(dtId1, 0));
    assertEquals("RMStateStore should have been in fenced state", true,
        store.isFencedState());

    store.updateRMDelegationToken(dtId1, renewDate1);
    assertFalse("Token " + dtId1
        + " should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(dtId1, 0));
    assertEquals("RMStateStore should have been in fenced state", true,
        store.isFencedState());

    // remove delegation key;
    store.removeRMDelegationToken(dtId1);
    assertEquals("RMStateStore should have been in fenced state", true,
        store.isFencedState());

    // store delegation master key;
    DelegationKey key = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    store.storeRMDTMasterKey(key);
    assertEquals("RMStateStore should have been in fenced state", true,
        store.isFencedState());

    // remove delegation master key;
    store.removeRMDTMasterKey(key);
    assertEquals("RMStateStore should have been in fenced state", true,
        store.isFencedState());

    // store or update AMRMToken;
    store.storeOrUpdateAMRMTokenSecretManager(null, false);
    assertEquals("RMStateStore should have been in fenced state", true,
        store.isFencedState());

    store.close();
  }

  @Test
  public void testDuplicateRMAppDeletion() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    long submitTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis() + 1234;
    RMStateStore store = zkTester.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    ApplicationAttemptId attemptIdRemoved = ApplicationAttemptId.fromString(
        "appattempt_1352994193343_0002_000001");
    ApplicationId appIdRemoved = attemptIdRemoved.getApplicationId();
    storeApp(store, appIdRemoved, submitTime, startTime);
    storeAttempt(store, attemptIdRemoved,
        "container_1352994193343_0002_01_000001", null, null, dispatcher);

    ApplicationSubmissionContext context =
        new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appIdRemoved);

    ApplicationStateData appStateRemoved =
        ApplicationStateData.newInstance(
            submitTime, startTime, context, "user1");
    appStateRemoved.attempts.put(attemptIdRemoved, null);
    store.removeApplicationStateInternal(appStateRemoved);
    try {
      store.removeApplicationStateInternal(appStateRemoved);
    } catch (KeeperException.NoNodeException nne) {
      fail("NoNodeException should not happen.");
    }
    store.close();
  }

  private static String createPath(String... parts) {
    return Joiner.on("/").join(parts);
  }

  private static Configuration createConfForAppNodeSplit(int splitIndex) {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.ZK_APPID_NODE_SPLIT_INDEX, splitIndex);
    return conf;
  }

  private static RMApp createMockAppForRemove(ApplicationId appId,
      ApplicationAttemptId... attemptIds) {
    RMApp app = mock(RMApp.class);
    ApplicationSubmissionContextPBImpl context =
        new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appId);
    when(app.getApplicationSubmissionContext()).thenReturn(context);
    when(app.getUser()).thenReturn("test");
    if (attemptIds.length > 0) {
      HashMap<ApplicationAttemptId, RMAppAttempt> attempts = new HashMap<>();
      for (ApplicationAttemptId attemptId : attemptIds) {
        RMAppAttempt appAttempt = mock(RMAppAttempt.class);
        when(appAttempt.getAppAttemptId()).thenReturn(attemptId);
        attempts.put(attemptId, appAttempt);
      }
      when(app.getAppAttempts()).thenReturn(attempts);
    }
    return app;
  }

  private static void verifyLoadedApp(ApplicationStateData appState,
      ApplicationId appId, String user, long submitTime, long startTime,
      RMAppState state, long finishTime, String diagnostics) {
    // Check if app is loaded correctly
    assertNotNull("App " + appId + " should have been loaded.", appState);
    assertEquals("App submit time in app state", submitTime,
        appState.getSubmitTime());
    assertEquals("App start time in app state", startTime,
        appState.getStartTime());
    assertEquals("App ID in app state", appId,
        appState.getApplicationSubmissionContext().getApplicationId());
    assertEquals("App state", state, appState.getState());
    assertEquals("Finish time in app state", finishTime,
        appState.getFinishTime());
    assertEquals("User in app state", user, appState.getUser());
    assertEquals("Diagnostics in app state", diagnostics,
        appState.getDiagnostics());
  }

  private static void verifyLoadedApp(RMState rmState,
      ApplicationId appId, long submitTime, long startTime, long finishTime,
      boolean isFinished, List<ApplicationAttemptId> attempts) {
    verifyLoadedApp(rmState, appId, submitTime, startTime, finishTime,
        isFinished, attempts, null, null);
  }

  private static void verifyLoadedApp(RMState rmState,
      ApplicationId appId, long submitTime, long startTime, long finishTime,
      boolean isFinished, List<ApplicationAttemptId> attempts,
      List<Integer> amExitStatuses,
      List<FinalApplicationStatus> finalStatuses) {
    Map<ApplicationId, ApplicationStateData> rmAppState =
        rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(appId);
    assertNotNull(appId + " is not there in loaded apps", appState);
    verifyLoadedApp(appState, appId, "test", submitTime, startTime,
        isFinished ? RMAppState.FINISHED : null, finishTime,
        isFinished ? "appDiagnostics" : "");
    // Check attempt state.
    if (attempts != null) {
      assertEquals("Attempts loaded for app " + appId, attempts.size(),
          appState.attempts.size());
      if (finalStatuses != null && amExitStatuses != null) {
        for (int i = 0; i < attempts.size(); i++) {
          if (finalStatuses.get(i) != null) {
            verifyLoadedAttempt(appState, attempts.get(i),
                amExitStatuses.get(i), true);
          } else {
            verifyLoadedAttempt(appState, attempts.get(i),
                amExitStatuses.get(i), false);
          }
        }
      }
    } else {
      assertEquals(
          "Attempts loaded for app " + appId, 0, appState.attempts.size());
    }
  }

  private static void verifyLoadedAttempt(ApplicationStateData appState,
      ApplicationAttemptId attemptId, int amExitStatus, boolean isFinished) {
    verifyLoadedAttempt(appState, attemptId, isFinished ? "myTrackingUrl" :
        "N/A", ContainerId.newContainerId(attemptId, 1), null,
        isFinished ? RMAppAttemptState.FINISHED : null, isFinished ?
        "attemptDiagnostics" : "", 0, amExitStatus,
        isFinished ? FinalApplicationStatus.SUCCEEDED : null);
  }

  private static void verifyLoadedAttempt(ApplicationStateData appState,
      ApplicationAttemptId attemptId, String trackingURL,
      ContainerId masterContainerId, SecretKey clientTokenKey,
      RMAppAttemptState state, String diagnostics, long finishTime,
      int amExitStatus, FinalApplicationStatus finalStatus) {
    ApplicationAttemptStateData attemptState = appState.getAttempt(attemptId);
    // Check if attempt is loaded correctly
    assertNotNull(
        "Attempt " + attemptId + " should have been loaded.", attemptState);
    assertEquals("Attempt Id in attempt state",
        attemptId, attemptState.getAttemptId());
    assertEquals("Master Container Id in attempt state",
        masterContainerId, attemptState.getMasterContainer().getId());
    if (null != clientTokenKey) {
      assertArrayEquals("Client token key in attempt state",
          clientTokenKey.getEncoded(), attemptState.getAppAttemptTokens().
          getSecretKey(RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME));
    }
    assertEquals("Attempt state", state, attemptState.getState());
    assertEquals("Finish time in attempt state", finishTime,
        attemptState.getFinishTime());
    assertEquals("Diagnostics in attempt state", diagnostics,
        attemptState.getDiagnostics());
    assertEquals("AM Container exit status in attempt state", amExitStatus,
        attemptState.getAMContainerExitStatus());
    assertEquals("Final app status in attempt state", finalStatus,
        attemptState.getFinalApplicationStatus());
    assertEquals("Tracking URL in attempt state", trackingURL,
        attemptState.getFinalTrackingUrl());
  }

  private static ApplicationStateData createAppState(
      ApplicationSubmissionContext ctxt, long submitTime, long startTime,
      long finishTime, boolean isFinished) {
    return ApplicationStateData.newInstance(submitTime, startTime, "test",
        ctxt, isFinished ? RMAppState.FINISHED : null, isFinished ?
        "appDiagnostics" : "", 0, isFinished ? finishTime : 0, null);
  }

  private static ApplicationAttemptStateData createFinishedAttempt(
      ApplicationAttemptId attemptId, Container container, long startTime,
      int amExitStatus) {
    Map<String, Long> resourceSecondsMap = new HashMap<>();
    Map<String, Long> preemptedResoureSecondsMap = new HashMap<>();
    resourceSecondsMap
        .put(ResourceInformation.MEMORY_MB.getName(), 0L);
    resourceSecondsMap
        .put(ResourceInformation.VCORES.getName(), 0L);
    preemptedResoureSecondsMap.put(ResourceInformation.MEMORY_MB.getName(),
        0L);
    preemptedResoureSecondsMap
        .put(ResourceInformation.VCORES.getName(), 0L);
    return ApplicationAttemptStateData.newInstance(attemptId,
        container, null, startTime, RMAppAttemptState.FINISHED,
        "myTrackingUrl", "attemptDiagnostics", FinalApplicationStatus.SUCCEEDED,
        amExitStatus, 0, resourceSecondsMap, preemptedResoureSecondsMap);
  }

  private ApplicationAttemptId storeAttempt(RMStateStore store,
      TestDispatcher dispatcher, String appAttemptIdStr,
      AMRMTokenSecretManager appTokenMgr,
      ClientToAMTokenSecretManagerInRM clientToAMTokenMgr,
      boolean createContainer) throws Exception {
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.fromString(appAttemptIdStr);
    Token<AMRMTokenIdentifier> appAttemptToken = null;
    if (appTokenMgr != null) {
      appAttemptToken = generateAMRMToken(attemptId, appTokenMgr);
    }
    SecretKey clientTokenKey = null;
    if (clientToAMTokenMgr != null) {
      clientTokenKey = clientToAMTokenMgr.createMasterKey(attemptId);
      Credentials attemptCred = new Credentials();
      attemptCred.addSecretKey(RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME,
          clientTokenKey.getEncoded());
    }
    ContainerId containerId = null;
    if (createContainer) {
      containerId = ContainerId.newContainerId(attemptId, 1);
    }
    storeAttempt(store, attemptId, containerId.toString(), appAttemptToken,
        clientTokenKey, dispatcher);
    return attemptId;
  }

  private void finishAppWithAttempts(RMState state, RMStateStore store,
      TestDispatcher dispatcher, ApplicationAttemptId attemptId,
      long submitTime, long startTime, int amExitStatus, long finishTime,
      boolean createNewApp) throws Exception {
    ApplicationId appId = attemptId.getApplicationId();
    ApplicationStateData appStateNew = null;
    if (createNewApp) {
      ApplicationSubmissionContext context =
          new ApplicationSubmissionContextPBImpl();
      context.setApplicationId(appId);
      context.setAMContainerSpec(new ContainerLaunchContextPBImpl());
      appStateNew = createAppState(context, submitTime, startTime, finishTime,
          true);
    } else {
      ApplicationStateData appState = state.getApplicationState().get(appId);
      appStateNew = createAppState(appState.getApplicationSubmissionContext(),
          submitTime, startTime, finishTime, true);
      appStateNew.attempts.putAll(appState.attempts);
    }
    store.updateApplicationState(appStateNew);
    waitNotify(dispatcher);
    Container container = new ContainerPBImpl();
    container.setId(ContainerId.newContainerId(attemptId, 1));
    ApplicationAttemptStateData newAttemptState =
        createFinishedAttempt(attemptId, container, startTime, amExitStatus);
    updateAttempt(store, dispatcher, newAttemptState);
  }

  private void storeAppWithAttempts(RMStateStore store,
      TestDispatcher dispatcher, ApplicationAttemptId attemptId,
      long submitTime, long startTime) throws Exception {
    storeAppWithAttempts(store, dispatcher, submitTime, startTime, null, null,
        attemptId);
  }

  private void storeApp(RMStateStore store, TestDispatcher dispatcher,
      ApplicationId appId, long submitTime, long startTime) throws Exception {
    storeApp(store, appId, submitTime, startTime);
    waitNotify(dispatcher);
  }

  private void storeAppWithAttempts(RMStateStore store,
      TestDispatcher dispatcher, long submitTime, long startTime,
      AMRMTokenSecretManager appTokenMgr,
      ClientToAMTokenSecretManagerInRM clientToAMTokenMgr,
      ApplicationAttemptId attemptId, ApplicationAttemptId... attemptIds)
      throws Exception {
    ApplicationId appId = attemptId.getApplicationId();
    storeApp(store, dispatcher, appId, submitTime, startTime);
    storeAttempt(store, dispatcher, attemptId.toString(), appTokenMgr,
        clientToAMTokenMgr, true);
    for (ApplicationAttemptId attempt : attemptIds) {
      storeAttempt(store, dispatcher, attempt.toString(), appTokenMgr,
          clientToAMTokenMgr, true);
    }
  }

  private static void removeApps(RMStateStore store,
      Map<ApplicationId, ApplicationAttemptId[]> appWithAttempts) {
    for (Map.Entry<ApplicationId, ApplicationAttemptId[]> entry :
        appWithAttempts.entrySet()) {
      RMApp mockApp = createMockAppForRemove(entry.getKey(), entry.getValue());
      store.removeApplication(mockApp);
    }
  }

  private static void verifyAppPathPath(RMStateStore store, ApplicationId appId,
        int splitIndex) throws Exception {
    String appIdStr = appId.toString();
    String appParent = appIdStr.substring(0, appIdStr.length() - splitIndex);
    String appPath = appIdStr.substring(appIdStr.length() - splitIndex);
    String path = createPath(((ZKRMStateStore)store).znodeWorkingPath,
        ZKRMStateStore.ROOT_ZNODE_NAME, ZKRMStateStore.RM_APP_ROOT,
        ZKRMStateStore.RM_APP_ROOT_HIERARCHIES, String.valueOf(splitIndex),
        appParent, appPath);
    assertTrue("Application with id " + appIdStr + " does not exist as per " +
        "split in state store.", ((ZKRMStateStore)store).exists(path));
  }

  private static void verifyAppInHierarchicalPath(RMStateStore store,
      String appId, int splitIdx) throws Exception {
    String path = createPath(((ZKRMStateStore)store).znodeWorkingPath,
        ZKRMStateStore.ROOT_ZNODE_NAME, ZKRMStateStore.RM_APP_ROOT);
    if (splitIdx != 0) {
      path = createPath(path, ZKRMStateStore.RM_APP_ROOT_HIERARCHIES,
          String.valueOf(splitIdx), appId.substring(0, appId.length() -
          splitIdx), appId.substring(appId.length() - splitIdx));
    } else {
      path = createPath(path, appId);
    }
    assertTrue(appId + " should exist in path " + path,
        ((ZKRMStateStore)store).exists(createPath(path)));
  }

  private static void assertHierarchicalPaths(RMStateStore store,
      Map<Integer, Integer> pathToApps) throws Exception {
    for (Map.Entry<Integer, Integer> entry : pathToApps.entrySet()) {
      String path = createPath(((ZKRMStateStore)store).znodeWorkingPath,
          ZKRMStateStore.ROOT_ZNODE_NAME, ZKRMStateStore.RM_APP_ROOT);
      if (entry.getKey() != 0) {
        path = createPath(path, ZKRMStateStore.RM_APP_ROOT_HIERARCHIES,
            String.valueOf(entry.getKey()));
      }
      assertEquals("Number of childrens for path " + path,
          (int) entry.getValue(),
          ((ZKRMStateStore)store).getChildren(path).size());
    }
  }

  // Test to verify storing of apps and app attempts in ZK state store with app
  // node split index configured more than 0.
  @Test
  public void testAppNodeSplit() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    long submitTime = System.currentTimeMillis();
    long startTime = submitTime + 1234;
    Configuration conf = new YarnConfiguration();

    // Get store with app node split config set as 1.
    RMStateStore store = zkTester.getRMStateStore(createConfForAppNodeSplit(1));
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    // Create RM Context and app token manager.
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getStateStore()).thenReturn(store);
    AMRMTokenSecretManager appTokenMgr =
        spy(new AMRMTokenSecretManager(conf, rmContext));
    MasterKeyData masterKeyData = appTokenMgr.createNewMasterKey();
    when(appTokenMgr.getMasterKey()).thenReturn(masterKeyData);
    ClientToAMTokenSecretManagerInRM clientToAMTokenMgr =
        new ClientToAMTokenSecretManagerInRM();

    // Store app1.
    ApplicationId appId1 = ApplicationId.newInstance(1352994193343L, 1);
    ApplicationAttemptId attemptId1 =
        ApplicationAttemptId.newInstance(appId1, 1);
    ApplicationAttemptId attemptId2 =
        ApplicationAttemptId.newInstance(appId1, 2);
    storeAppWithAttempts(store, dispatcher, submitTime, startTime,
        appTokenMgr, clientToAMTokenMgr, attemptId1, attemptId2);

    // Store app2 with app id application_1352994193343_120213.
    ApplicationId appId21 = ApplicationId.newInstance(1352994193343L, 120213);
    storeApp(store, appId21, submitTime, startTime);
    waitNotify(dispatcher);

    // Store another app which will be removed.
    ApplicationId appIdRemoved = ApplicationId.newInstance(1352994193343L, 2);
    ApplicationAttemptId attemptIdRemoved =
        ApplicationAttemptId.newInstance(appIdRemoved, 1);
    storeAppWithAttempts(store, dispatcher, submitTime, startTime,
        null, null, attemptIdRemoved);
    // Remove the app.
    RMApp mockRemovedApp =
        createMockAppForRemove(appIdRemoved, attemptIdRemoved);
    store.removeApplication(mockRemovedApp);
    // Close state store
    store.close();

    // Load state store
    store = zkTester.getRMStateStore(createConfForAppNodeSplit(1));
    store.setRMDispatcher(dispatcher);
    RMState state = store.loadState();
    // Check if application_1352994193343_120213 (i.e. app2) exists in state
    // store as per split index.
    verifyAppPathPath(store, appId21, 1);

    // Verify loaded apps and attempts based on the operations we did before
    // reloading the state store.
    verifyLoadedApp(state, appId1, submitTime, startTime, 0, false,
        Lists.newArrayList(attemptId1, attemptId2), Lists.newArrayList(-1000,
        -1000), Lists.newArrayList((FinalApplicationStatus) null, null));

    // Update app state for app1.
    finishAppWithAttempts(state, store, dispatcher, attemptId2, submitTime,
        startTime, 100, 1234, false);

    // Test updating app/attempt for app whose initial state is not saved
    ApplicationId dummyAppId = ApplicationId.newInstance(1234, 10);
    ApplicationAttemptId dummyAttemptId =
        ApplicationAttemptId.newInstance(dummyAppId, 6);
    finishAppWithAttempts(state, store, dispatcher, dummyAttemptId, submitTime,
        startTime, 111, 1234, true);
    // Close the store
    store.close();

    // Check updated application state.
    store = zkTester.getRMStateStore(createConfForAppNodeSplit(1));
    store.setRMDispatcher(dispatcher);
    RMState newRMState = store.loadState();
    verifyLoadedApp(newRMState, dummyAppId, submitTime, startTime, 1234, true,
        Lists.newArrayList(dummyAttemptId), Lists.newArrayList(111),
        Lists.newArrayList(FinalApplicationStatus.SUCCEEDED));
    verifyLoadedApp(newRMState, appId1, submitTime, startTime, 1234, true,
        Lists.newArrayList(attemptId1, attemptId2),
        Lists.newArrayList(-1000, 100), Lists.newArrayList(null,
        FinalApplicationStatus.SUCCEEDED));

    // assert store is in expected state after everything is cleaned
    assertTrue("Store is not in expected state", zkTester.isFinalStateValid());
    store.close();
  }

  // Test to verify storing of apps and app attempts in ZK state store with app
  // node split index config changing across restarts.
  @Test
  public void testAppNodeSplitChangeAcrossRestarts() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    long submitTime = System.currentTimeMillis();
    long startTime = submitTime + 1234;
    Configuration conf = new YarnConfiguration();

    // Create store with app node split set as 1.
    RMStateStore store = zkTester.getRMStateStore(createConfForAppNodeSplit(1));
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getStateStore()).thenReturn(store);
    AMRMTokenSecretManager appTokenMgr =
        spy(new AMRMTokenSecretManager(conf, rmContext));
    MasterKeyData masterKeyData = appTokenMgr.createNewMasterKey();
    when(appTokenMgr.getMasterKey()).thenReturn(masterKeyData);
    ClientToAMTokenSecretManagerInRM clientToAMTokenMgr =
        new ClientToAMTokenSecretManagerInRM();

    // Store app1 with 2 attempts.
    ApplicationId appId1 = ApplicationId.newInstance(1442994194053L, 1);
    ApplicationAttemptId attemptId1 =
        ApplicationAttemptId.newInstance(appId1, 1);
    ApplicationAttemptId attemptId2 =
        ApplicationAttemptId.newInstance(appId1, 2);
    storeAppWithAttempts(store, dispatcher, submitTime, startTime,
        appTokenMgr, clientToAMTokenMgr, attemptId1, attemptId2);

    // Store app2 and associated attempt.
    ApplicationId appId11 = ApplicationId.newInstance(1442994194053L, 2);
    ApplicationAttemptId attemptId11 =
        ApplicationAttemptId.newInstance(appId11, 1);
    storeAppWithAttempts(store, dispatcher, attemptId11, submitTime, startTime);
    // Close state store
    store.close();

    // Load state store with app node split config of 2.
    store = zkTester.getRMStateStore(createConfForAppNodeSplit(2));
    store.setRMDispatcher(dispatcher);
    RMState state = store.loadState();
    ApplicationId appId21 = ApplicationId.newInstance(1442994194053L, 120213);
    storeApp(store, dispatcher, appId21, submitTime, startTime);

    // Check if app is loaded correctly despite change in split index.
    verifyLoadedApp(state, appId1, submitTime, startTime, 0, false,
        Lists.newArrayList(attemptId1, attemptId2), Lists.newArrayList(-1000,
        -1000), Lists.newArrayList((FinalApplicationStatus) null, null));

    // Finish app/attempt state
    finishAppWithAttempts(state, store, dispatcher, attemptId2, submitTime,
        startTime, 100, 1234, false);

    // Test updating app/attempt for app whose initial state is not saved
    ApplicationId dummyAppId = ApplicationId.newInstance(1234, 10);
    ApplicationAttemptId dummyAttemptId =
        ApplicationAttemptId.newInstance(dummyAppId, 6);
    finishAppWithAttempts(state, store, dispatcher, dummyAttemptId, submitTime,
        startTime, 111, 1234, true);
    // Close the store
    store.close();

    // Load state store this time with split index of 0.
    store = zkTester.getRMStateStore(createConfForAppNodeSplit(0));
    store.setRMDispatcher(dispatcher);
    state = store.loadState();
    assertEquals("Number of Apps loaded should be 4.", 4,
        state.getApplicationState().size());
    verifyLoadedApp(state, appId1, submitTime, startTime, 1234, true,
        Lists.newArrayList(attemptId1, attemptId2), Lists.newArrayList(-1000,
        100), Lists.newArrayList(null, FinalApplicationStatus.SUCCEEDED));
    // Remove attempt1
    store.removeApplicationAttempt(attemptId1);
    ApplicationId appId31 = ApplicationId.newInstance(1442994195071L, 45);
    storeApp(store, dispatcher, appId31, submitTime, startTime);
    // Close state store.
    store.close();

    // Load state store with split index of 3.
    store = zkTester.getRMStateStore(createConfForAppNodeSplit(3));
    store.setRMDispatcher(dispatcher);
    state = store.loadState();
    assertEquals("Number of apps loaded should be 5.", 5,
        state.getApplicationState().size());
    verifyLoadedApp(state, dummyAppId, submitTime, startTime, 1234, true,
        Lists.newArrayList(dummyAttemptId), Lists.newArrayList(111),
        Lists.newArrayList(FinalApplicationStatus.SUCCEEDED));
    verifyLoadedApp(state, appId31, submitTime, startTime, 0, false, null);
    verifyLoadedApp(state, appId21, submitTime, startTime, 0, false, null);
    verifyLoadedApp(state, appId11, submitTime, startTime, 0, false,
        Lists.newArrayList(attemptId11), Lists.newArrayList(-1000),
        Lists.newArrayList((FinalApplicationStatus) null));
    verifyLoadedApp(state, appId1, submitTime, startTime, 1234, true,
        Lists.newArrayList(attemptId2), Lists.newArrayList(100),
        Lists.newArrayList(FinalApplicationStatus.SUCCEEDED));

    // Store another app.
    ApplicationId appId41 = ApplicationId.newInstance(1442994195087L, 1);
    storeApp(store, dispatcher, appId41, submitTime, startTime);
    // Check how many apps exist in each of the hierarchy based paths. 0 paths
    // should exist in "HIERARCHIES/4" path as app split index was never set
    // as 4 in tests above.
    assertHierarchicalPaths(store, ImmutableMap.of(0, 2, 1, 1, 2, 2,
        3, 1, 4, 0));
    verifyAppInHierarchicalPath(store, "application_1442994195087_0001", 3);

    ApplicationId appId71 = ApplicationId.newInstance(1442994195087L, 7);
    //storeApp(store, dispatcher, appId71, submitTime, startTime);
    storeApp(store, appId71, submitTime, startTime);
    waitNotify(dispatcher);
    ApplicationAttemptId attemptId71 =
        ApplicationAttemptId.newInstance(appId71, 1);
    storeAttempt(store, ApplicationAttemptId.newInstance(appId71, 1),
        ContainerId.newContainerId(attemptId71, 1).toString(), null, null,
        dispatcher);
    // Remove applications.
    removeApps(store, ImmutableMap.of(appId11, new ApplicationAttemptId[]
        {attemptId11}, appId71, new ApplicationAttemptId[] {attemptId71},
        appId41, new ApplicationAttemptId[0], appId31,
        new ApplicationAttemptId[0], appId21, new ApplicationAttemptId[0]));
    removeApps(store, ImmutableMap.of(dummyAppId,
        new ApplicationAttemptId[] {dummyAttemptId}, appId1,
        new ApplicationAttemptId[] {attemptId1, attemptId2}));
    store.close();

    // Load state store with split index of 3 again. As all apps have been
    // removed nothing should be loaded back.
    store = zkTester.getRMStateStore(createConfForAppNodeSplit(3));
    store.setRMDispatcher(dispatcher);
    state = store.loadState();
    assertEquals("Number of apps loaded should be 0.", 0,
        state.getApplicationState().size());
    // Close the state store.
    store.close();
  }

  private static Configuration createConfForDelegationTokenNodeSplit(
      int splitIndex) {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.ZK_DELEGATION_TOKEN_NODE_SPLIT_INDEX,
        splitIndex);
    return conf;
  }

  private void verifyDelegationTokensStateStore(
      TestZKRMStateStoreTester zkTester,
      Map<RMDelegationTokenIdentifier, Long> tokensWithRenewal,
      Map<RMDelegationTokenIdentifier, Integer> tokensWithIndex,
      int sequenceNumber) throws Exception {
    RMStateStore.RMDTSecretManagerState secretManagerState =
        zkTester.store.loadState().getRMDTSecretManagerState();
    assertEquals("Unexpected token state",
        tokensWithRenewal, secretManagerState.getTokenState());
    assertEquals("Unexpected sequence number", sequenceNumber,
        secretManagerState.getDTSequenceNumber());
    for (Map.Entry<RMDelegationTokenIdentifier, Integer> tokenEntry
        : tokensWithIndex.entrySet()) {
      assertTrue("Expected to find token " + tokenEntry.getKey()
          + " in zookeeper but did not",
          zkTester.delegationTokenExists(tokenEntry.getKey(),
          tokenEntry.getValue()));
    }
  }

  private void verifyDelegationTokenInStateStore(
      TestZKRMStateStoreTester zkTester, RMDelegationTokenIdentifier token,
      long renewDate, int index) throws Exception {
    RMStateStore.RMDTSecretManagerState secretManagerState =
        zkTester.store.loadState().getRMDTSecretManagerState();
    Map<RMDelegationTokenIdentifier, Long> tokenState =
        secretManagerState.getTokenState();
    assertTrue("token state does not contain " + token,
        tokenState.containsKey(token));
    assertTrue("token state does not contain a token with renewal " + renewDate,
        tokenState.containsValue(renewDate));
    assertTrue("Token " + token + "should exist but was not found in ZooKeeper",
        zkTester.delegationTokenExists(token, index));
  }

  private RMDelegationTokenIdentifier storeUpdateAndVerifyDelegationToken(
      TestZKRMStateStoreTester zkTester,
      Map<RMDelegationTokenIdentifier, Long> tokensWithRenewal,
      Map<RMDelegationTokenIdentifier, Integer> tokensWithIndex,
      int sequenceNumber, int split) throws Exception {
    // Store token
    RMDelegationTokenIdentifier token =
        new RMDelegationTokenIdentifier(new Text("owner"),
            new Text("renewer"), new Text("realuser"));
    assertFalse("Token should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(token, split));
    token.setSequenceNumber(sequenceNumber);
    Long renewDate = System.currentTimeMillis();
    zkTester.store.storeRMDelegationToken(token, renewDate);
    modifyRMDelegationTokenState();
    tokensWithRenewal.put(token, renewDate);
    tokensWithIndex.put(token, split);

    // Verify the token
    verifyDelegationTokensStateStore(zkTester, tokensWithRenewal,
        tokensWithIndex, sequenceNumber);

    // Update the token
    renewDate = System.currentTimeMillis();
    zkTester.store.updateRMDelegationToken(token, renewDate);
    tokensWithRenewal.put(token, renewDate);
    tokensWithIndex.put(token, split);

    // Verify updates
    verifyDelegationTokensStateStore(zkTester, tokensWithRenewal,
        tokensWithIndex, sequenceNumber);

    return token;
  }

  @Test
  public void testDelegationTokenSplitIndexConfig() throws Exception {
    // Valid values
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    zkTester.getRMStateStore(createConfForDelegationTokenNodeSplit(0)).close();
    assertEquals("Incorrect split index",
        0, zkTester.getDelegationTokenNodeSplitIndex());
    zkTester.getRMStateStore(createConfForDelegationTokenNodeSplit(1)).close();
    assertEquals("Incorrect split index",
        1, zkTester.getDelegationTokenNodeSplitIndex());
    zkTester.getRMStateStore(createConfForDelegationTokenNodeSplit(2)).close();
    assertEquals("Incorrect split index",
        2, zkTester.getDelegationTokenNodeSplitIndex());
    zkTester.getRMStateStore(createConfForDelegationTokenNodeSplit(3)).close();
    assertEquals("Incorrect split index",
        3, zkTester.getDelegationTokenNodeSplitIndex());
    zkTester.getRMStateStore(createConfForDelegationTokenNodeSplit(4)).close();
    assertEquals("Incorrect split index",
        4, zkTester.getDelegationTokenNodeSplitIndex());

    // Invalid values --> override to 0
    zkTester.getRMStateStore(createConfForDelegationTokenNodeSplit(-1)).close();
    assertEquals("Incorrect split index",
        0, zkTester.getDelegationTokenNodeSplitIndex());
    zkTester.getRMStateStore(createConfForDelegationTokenNodeSplit(5)).close();
    assertEquals("Incorrect split index",
        0, zkTester.getDelegationTokenNodeSplitIndex());
  }

  @Test
  public void testDelegationTokenNodeNoSplit() throws Exception {
    testDelegationTokenNode(0);
  }

  @Test
  public void testDelegationTokenNodeWithSplitOne() throws Exception {
    testDelegationTokenNode(1);
  }

  @Test
  public void testDelegationTokenNodeWithSplitTwo() throws Exception {
    testDelegationTokenNode(2);
  }

  @Test
  public void testDelegationTokenNodeWithSplitThree() throws Exception {
    testDelegationTokenNode(3);
  }

  @Test
  public void testDelegationTokenNodeWithSplitFour() throws Exception {
    testDelegationTokenNode(4);
  }

  public void testDelegationTokenNode(int split) throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    Configuration conf = createConfForDelegationTokenNodeSplit(split);
    RMStateStore store = zkTester.getRMStateStore(conf);

    // Store the token and verify
    Map<RMDelegationTokenIdentifier, Long> tokensWithRenewal = new HashMap<>();
    Map<RMDelegationTokenIdentifier, Integer> tokensWithIndex = new HashMap<>();
    int sequenceNumber = 0;
    RMDelegationTokenIdentifier token = storeUpdateAndVerifyDelegationToken(
        zkTester, tokensWithRenewal, tokensWithIndex, sequenceNumber, split);

    // Delete the token and verify
    store.removeRMDelegationToken(token);
    RMStateStore.RMDTSecretManagerState state =
        store.loadState().getRMDTSecretManagerState();
    tokensWithRenewal.clear();
    tokensWithIndex.clear();
    assertEquals("Unexpected token state",
        tokensWithRenewal, state.getTokenState());
    assertEquals("Unexpected sequence number",
        sequenceNumber, state.getDTSequenceNumber());
    assertFalse("Token should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(token, split));
    store.close();
  }

  @Test
  public void testDelegationTokenNodeWithSplitMultiple() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    Configuration conf = createConfForDelegationTokenNodeSplit(1);
    RMStateStore store = zkTester.getRMStateStore(conf);

    // With the split set to 1, we can store 10 tokens under a znode (i.e. 0-9)
    // Try to store more than 10
    Map<RMDelegationTokenIdentifier, Long> tokensWithRenewal = new HashMap<>();
    Map<RMDelegationTokenIdentifier, Integer> tokensWithIndex = new HashMap<>();
    Set<RMDelegationTokenIdentifier> tokensToDelete = new HashSet<>();
    int sequenceNumber = 0;
    for (int i = 0; i <= 12; i++) {
      RMDelegationTokenIdentifier token =
          new RMDelegationTokenIdentifier(new Text("owner" + i),
              new Text("renewer" + i), new Text("realuser" + i));
      sequenceNumber = i;
      token.setSequenceNumber(sequenceNumber);
      assertFalse("Token should not exist but was found in ZooKeeper",
          zkTester.delegationTokenExists(token, 1));
      Long renewDate = System.currentTimeMillis();
      store.storeRMDelegationToken(token, renewDate);
      modifyRMDelegationTokenState();
      tokensWithRenewal.put(token, renewDate);
      tokensWithIndex.put(token, 1);
      switch (i) {
        case 0:
        case 3:
        case 6:
        case 11:
          tokensToDelete.add(token);
          break;
        default:
          break;
      }
    }
    // Verify
    verifyDelegationTokensStateStore(zkTester, tokensWithRenewal,
        tokensWithIndex, sequenceNumber);

    // Try deleting some tokens and adding some new ones
    for (RMDelegationTokenIdentifier tokenToDelete : tokensToDelete) {
      store.removeRMDelegationToken(tokenToDelete);
      tokensWithRenewal.remove(tokenToDelete);
      tokensWithIndex.remove(tokenToDelete);
    }
    for (int i = 13; i <= 22; i++) {
      RMDelegationTokenIdentifier token =
          new RMDelegationTokenIdentifier(new Text("owner" + i),
              new Text("renewer" + i), new Text("realuser" + i));
      sequenceNumber = i;
      token.setSequenceNumber(sequenceNumber);
      Long renewDate = System.currentTimeMillis();
      store.storeRMDelegationToken(token, renewDate);
      modifyRMDelegationTokenState();
      tokensWithRenewal.put(token, renewDate);
      tokensWithIndex.put(token, 1);
    }
    // Verify
    verifyDelegationTokensStateStore(zkTester, tokensWithRenewal,
        tokensWithIndex, sequenceNumber);
    for (RMDelegationTokenIdentifier token : tokensToDelete) {
      assertFalse("Token " + token
              + " should not exist but was found in ZooKeeper",
          zkTester.delegationTokenExists(token, 1));
    }
    store.close();
  }

  @Test
  public void testDelegationTokenNodeWithSplitChangeAcrossRestarts()
      throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    Map<RMDelegationTokenIdentifier, Long> tokensWithRenewal = new HashMap<>();
    Map<RMDelegationTokenIdentifier, Integer> tokensWithIndex = new HashMap<>();
    int sequenceNumber = 0;

    // Start the store with index 1
    Configuration conf = createConfForDelegationTokenNodeSplit(1);
    RMStateStore store = zkTester.getRMStateStore(conf);
    // Store a token with index 1
    RMDelegationTokenIdentifier token1 = storeUpdateAndVerifyDelegationToken(
        zkTester, tokensWithRenewal, tokensWithIndex, sequenceNumber, 1);
    store.close();

    // Start the store with index 2
    conf = createConfForDelegationTokenNodeSplit(2);
    store = zkTester.getRMStateStore(conf);
    // Verify token1 is still there and under the /1/ znode
    verifyDelegationTokenInStateStore(
        zkTester, token1, tokensWithRenewal.get(token1), 1);
    // Store a token with index 2
    sequenceNumber++;
    RMDelegationTokenIdentifier token2 = storeUpdateAndVerifyDelegationToken(
        zkTester, tokensWithRenewal, tokensWithIndex, sequenceNumber, 2);
    // Update and verify token1
    long renewDate1 = System.currentTimeMillis();
    zkTester.store.updateRMDelegationToken(token1, renewDate1);
    tokensWithRenewal.put(token1, renewDate1);
    verifyDelegationTokenInStateStore(
        zkTester, token1, tokensWithRenewal.get(token1), 1);
    store.close();

    // Start the store with index 0
    conf = createConfForDelegationTokenNodeSplit(0);
    store = zkTester.getRMStateStore(conf);
    // Verify token1 is still there and under the /1/ znode
    verifyDelegationTokenInStateStore(
        zkTester, token1, tokensWithRenewal.get(token1), 1);
    // Verify token2 is still there and under the /2/ znode
    verifyDelegationTokenInStateStore(
        zkTester, token2, tokensWithRenewal.get(token2), 2);
    // Store a token with no index
    sequenceNumber++;
    RMDelegationTokenIdentifier token0 = storeUpdateAndVerifyDelegationToken(
        zkTester, tokensWithRenewal, tokensWithIndex, sequenceNumber, 0);
    store.close();

    // Start the store with index 3
    conf = createConfForDelegationTokenNodeSplit(3);
    store = zkTester.getRMStateStore(conf);
    // Verify token1 is still there and under the /1/ znode
    verifyDelegationTokenInStateStore(
        zkTester, token1, tokensWithRenewal.get(token1), 1);
    // Verify token2 is still there and under the /2/ znode
    verifyDelegationTokenInStateStore(
        zkTester, token2, tokensWithRenewal.get(token2), 2);
    // Verify token0 is still there and under the token root node
    verifyDelegationTokenInStateStore(
        zkTester, token0, tokensWithRenewal.get(token0), 0);
    // Delete all tokens and verify
    for (RMDelegationTokenIdentifier token : tokensWithRenewal.keySet()) {
      store.removeRMDelegationToken(token);
    }
    tokensWithRenewal.clear();
    tokensWithIndex.clear();
    verifyDelegationTokensStateStore(
        zkTester, tokensWithRenewal, tokensWithIndex, sequenceNumber);
    assertFalse("Token " + token1
            + " should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(token1, 1));
    assertFalse("Token " + token1
            + " should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(token2, 2));
    assertFalse("Token " + token1
            + " should not exist but was found in ZooKeeper",
        zkTester.delegationTokenExists(token0, 0));
    // Store a token with index 3
    sequenceNumber++;
    storeUpdateAndVerifyDelegationToken(zkTester, tokensWithRenewal,
        tokensWithIndex, sequenceNumber, 3);
    store.close();
  }

  @Test
  public void testAppSubmissionContextIsPrunedInFinalApplicationState()
      throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    ApplicationId appId = ApplicationId.fromString("application_1234_0010");

    Configuration conf = createConfForDelegationTokenNodeSplit(1);
    RMStateStore store = zkTester.getRMStateStore(conf);
    ApplicationSubmissionContext ctx =
        new ApplicationSubmissionContextPBImpl();
    ctx.setApplicationId(appId);
    ctx.setQueue("a_queue");
    ContainerLaunchContextPBImpl containerLaunchCtx =
        new ContainerLaunchContextPBImpl();
    containerLaunchCtx.setCommands(Collections.singletonList("a_command"));
    ctx.setAMContainerSpec(containerLaunchCtx);
    Resource resource = new ResourcePBImpl();
    resource.setMemorySize(17L);
    ctx.setResource(resource);
    Map<String, String> schedulingPropertiesMap =
        Collections.singletonMap("a_key", "a_value");
    ctx.setApplicationSchedulingPropertiesMap(schedulingPropertiesMap);
    ApplicationStateDataPBImpl appState = new ApplicationStateDataPBImpl();
    appState.setState(RMAppState.RUNNING);
    appState.setApplicationSubmissionContext(ctx);
    store.storeApplicationStateInternal(appId, appState);

    RMState rmState = store.loadState();
    assertEquals(1, rmState.getApplicationState().size());
    ctx = rmState.getApplicationState().get(appId)
        .getApplicationSubmissionContext();

    appState.setState(RMAppState.RUNNING);
    store.handleStoreEvent(new RMStateUpdateAppEvent(appState, false, null));

    rmState = store.loadState();
    ctx = rmState.getApplicationState().get(appId)
        .getApplicationSubmissionContext();

    assertEquals("ApplicationSchedulingPropertiesMap should not have been "
        + "pruned from the application submission context before the "
        + "FINISHED state",
        schedulingPropertiesMap, ctx.getApplicationSchedulingPropertiesMap());

    appState.setState(RMAppState.FINISHED);
    store.handleStoreEvent(new RMStateUpdateAppEvent(appState, false, null));

    rmState = store.loadState();
    ctx = rmState.getApplicationState().get(appId)
        .getApplicationSubmissionContext();

    assertEquals(appId, ctx.getApplicationId());
    assertEquals("a_queue", ctx.getQueue());
    assertNotNull(ctx.getAMContainerSpec());
    assertEquals(17L, ctx.getResource().getMemorySize());
    assertEquals("ApplicationSchedulingPropertiesMap should have been pruned"
        + " from the application submission context when in FINISHED STATE",
        Collections.emptyMap(), ctx.getApplicationSchedulingPropertiesMap());
    store.close();
  }
}
