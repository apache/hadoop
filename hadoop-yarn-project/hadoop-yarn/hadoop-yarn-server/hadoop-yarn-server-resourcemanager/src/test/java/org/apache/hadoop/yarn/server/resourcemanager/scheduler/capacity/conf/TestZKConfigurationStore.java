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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ZKConfigurationStore}.
 */
public class TestZKConfigurationStore extends ConfigurationStoreBaseTest {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestZKConfigurationStore.class);

  private static final int ZK_TIMEOUT_MS = 10000;
  private TestingServer curatorTestingServer;
  private CuratorFramework curatorFramework;
  private ResourceManager rm;

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
  public void setUp() throws Exception {
    super.setUp();
    curatorTestingServer = setupCuratorServer();
    curatorFramework = setupCuratorFramework(curatorTestingServer);

    conf.set(CommonConfigurationKeys.ZK_ADDRESS,
        curatorTestingServer.getConnectString());
    rm = new MockRM(conf);
    rm.start();
    rmContext = rm.getRMContext();
  }

  @After
  public void cleanup() throws IOException {
    rm.stop();
    curatorFramework.close();
    curatorTestingServer.stop();
  }

  @Test
  public void testVersioning() throws Exception {
    confStore.initialize(conf, schedConf, rmContext);
    assertNull(confStore.getConfStoreVersion());
    confStore.checkVersion();
    assertEquals(ZKConfigurationStore.CURRENT_VERSION_INFO,
        confStore.getConfStoreVersion());
  }

  @Test
  public void testPersistConfiguration() throws Exception {
    schedConf.set("key", "val");
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));

    // Create a new configuration store, and check for old configuration
    confStore = createConfStore();
    schedConf.set("key", "badVal");
    // Should ignore passed-in scheduler configuration.
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));
  }


  @Test
  public void testPersistUpdatedConfiguration() throws Exception {
    confStore.initialize(conf, schedConf, rmContext);
    assertNull(confStore.retrieve().get("key"));

    Map<String, String> update = new HashMap<>();
    update.put("key", "val");
    YarnConfigurationStore.LogMutation mutation =
        new YarnConfigurationStore.LogMutation(update, TEST_USER);
    confStore.logMutation(mutation);
    confStore.confirmMutation(true);
    assertEquals("val", confStore.retrieve().get("key"));

    // Create a new configuration store, and check for updated configuration
    confStore = createConfStore();
    schedConf.set("key", "badVal");
    // Should ignore passed-in scheduler configuration.
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));
  }

  @Test
  public void testMaxLogs() throws Exception {
    conf.setLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS, 2);
    confStore.initialize(conf, schedConf, rmContext);
    LinkedList<YarnConfigurationStore.LogMutation> logs =
        ((ZKConfigurationStore) confStore).getLogs();
    assertEquals(0, logs.size());

    Map<String, String> update1 = new HashMap<>();
    update1.put("key1", "val1");
    YarnConfigurationStore.LogMutation mutation =
        new YarnConfigurationStore.LogMutation(update1, TEST_USER);
    confStore.logMutation(mutation);
    logs = ((ZKConfigurationStore) confStore).getLogs();
    assertEquals(1, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    confStore.confirmMutation(true);
    assertEquals(1, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));

    Map<String, String> update2 = new HashMap<>();
    update2.put("key2", "val2");
    mutation = new YarnConfigurationStore.LogMutation(update2, TEST_USER);
    confStore.logMutation(mutation);
    logs = ((ZKConfigurationStore) confStore).getLogs();
    assertEquals(2, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    assertEquals("val2", logs.get(1).getUpdates().get("key2"));
    confStore.confirmMutation(true);
    assertEquals(2, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    assertEquals("val2", logs.get(1).getUpdates().get("key2"));

    // Next update should purge first update from logs.
    Map<String, String> update3 = new HashMap<>();
    update3.put("key3", "val3");
    mutation = new YarnConfigurationStore.LogMutation(update3, TEST_USER);
    confStore.logMutation(mutation);
    logs = ((ZKConfigurationStore) confStore).getLogs();
    assertEquals(2, logs.size());
    assertEquals("val2", logs.get(0).getUpdates().get("key2"));
    assertEquals("val3", logs.get(1).getUpdates().get("key3"));
    confStore.confirmMutation(true);
    assertEquals(2, logs.size());
    assertEquals("val2", logs.get(0).getUpdates().get("key2"));
    assertEquals("val3", logs.get(1).getUpdates().get("key3"));
  }

  public Configuration createRMHAConf(String rmIds, String rmId,
      int adminPort) {
    Configuration conf = new YarnConfiguration();
    this.conf.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, CapacityScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, rmIds);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.ZK_CONFIGURATION_STORE);
    conf.set(YarnConfiguration.RM_STORE, ZKRMStateStore.class.getName());
    conf.set(YarnConfiguration.RM_ZK_ADDRESS,
        curatorTestingServer.getConnectString());
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    for (String rpcAddress :
        YarnConfiguration.getServiceAddressConfKeys(conf)) {
      for (String id : HAUtil.getRMHAIds(conf)) {
        conf.set(HAUtil.addSuffix(rpcAddress, id), "localhost:0");
      }
    }
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADMIN_ADDRESS, rmId),
        "localhost:" + adminPort);
    return conf;
  }

  /**
   * When failing over, new active RM should read from current state of store,
   * including any updates when the new active RM was in standby.
   * @throws Exception
   */
  @Test
  public void testFailoverReadsFromUpdatedStore() throws Exception {
    HAServiceProtocol.StateChangeRequestInfo req =
        new HAServiceProtocol.StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    Configuration conf1 = createRMHAConf("rm1,rm2", "rm1", 1234);
    ResourceManager rm1 = new MockRM(conf1);
    rm1.start();
    rm1.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm1.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm1.getRMContext().getRMAdminService().getServiceStatus().getState());
    assertNull(((MutableConfScheduler) rm1.getResourceScheduler())
        .getConfiguration().get("key"));

    Configuration conf2 = createRMHAConf("rm1,rm2", "rm2", 5678);
    ResourceManager rm2 = new MockRM(conf2);
    rm2.start();
    assertEquals("RM should be Standby",
        HAServiceProtocol.HAServiceState.STANDBY,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());

    // Update configuration on RM1
    SchedConfUpdateInfo schedConfUpdateInfo = new SchedConfUpdateInfo();
    schedConfUpdateInfo.getGlobalParams().put("key", "val");
    MutableConfigurationProvider confProvider = ((MutableConfScheduler)
        rm1.getResourceScheduler()).getMutableConfProvider();
    UserGroupInformation user = UserGroupInformation
        .createUserForTesting(TEST_USER, new String[0]);
    confProvider.logAndApplyMutation(user, schedConfUpdateInfo);
    rm1.getResourceScheduler().reinitialize(conf1, rm1.getRMContext());
    assertEquals("val", ((MutableConfScheduler) rm1.getResourceScheduler())
        .getConfiguration().get("key"));
    confProvider.confirmPendingMutation(true);
    assertEquals("val", ((MutableCSConfigurationProvider) confProvider)
        .getConfStore().retrieve().get("key"));
    // Next update is not persisted, it should not be recovered
    schedConfUpdateInfo.getGlobalParams().put("key", "badVal");
    confProvider.logAndApplyMutation(user, schedConfUpdateInfo);

    // Start RM2 and verifies it starts with updated configuration
    rm2.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm2.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());

    for (int i = 0; i < ZK_TIMEOUT_MS / 50; i++) {
      if (HAServiceProtocol.HAServiceState.ACTIVE ==
          rm1.getRMContext().getRMAdminService().getServiceStatus()
              .getState()) {
        Thread.sleep(100);
      }
    }
    assertEquals("RM should have been fenced",
        HAServiceProtocol.HAServiceState.STANDBY,
        rm1.getRMContext().getRMAdminService().getServiceStatus().getState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());

    assertEquals("val", ((MutableCSConfigurationProvider) (
        (CapacityScheduler) rm2.getResourceScheduler())
        .getMutableConfProvider()).getConfStore().retrieve().get("key"));
    assertEquals("val", ((MutableConfScheduler) rm2.getResourceScheduler())
        .getConfiguration().get("key"));
    // Transition to standby will set RM's HA status and then reinitialize in
    // a separate thread. Despite asserting for STANDBY state, it's
    // possible for reinitialization to be unfinished. Wait here for it to
    // finish, otherwise closing rm1 will close zkManager and the unfinished
    // reinitialization will throw an exception.
    Thread.sleep(10000);
    rm1.close();
    rm2.close();
  }

  /**
   * When failing over, if RM1 stopped and removed a queue that RM2 has in
   * memory, failing over to RM2 should not throw an exception.
   * @throws Exception
   */
  @Test
  public void testFailoverAfterRemoveQueue() throws Exception {
    HAServiceProtocol.StateChangeRequestInfo req =
        new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    Configuration conf1 = createRMHAConf("rm1,rm2", "rm1", 1234);
    ResourceManager rm1 = new MockRM(conf1);
    rm1.start();
    rm1.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm1.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm1.getRMContext().getRMAdminService().getServiceStatus().getState());

    Configuration conf2 = createRMHAConf("rm1,rm2", "rm2", 5678);
    ResourceManager rm2 = new MockRM(conf2);
    rm2.start();
    assertEquals("RM should be Standby",
        HAServiceProtocol.HAServiceState.STANDBY,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());

    UserGroupInformation user = UserGroupInformation
        .createUserForTesting(TEST_USER, new String[0]);
    MutableConfigurationProvider confProvider = ((MutableConfScheduler)
        rm1.getResourceScheduler()).getMutableConfProvider();
    // Add root.a
    SchedConfUpdateInfo schedConfUpdateInfo = new SchedConfUpdateInfo();
    Map<String, String> addParams = new HashMap<>();
    addParams.put("capacity", "100");
    QueueConfigInfo addInfo = new QueueConfigInfo("root.a", addParams);
    schedConfUpdateInfo.getAddQueueInfo().add(addInfo);
    // Stop root.default
    Map<String, String> stopParams = new HashMap<>();
    stopParams.put("state", "STOPPED");
    stopParams.put("capacity", "0");
    QueueConfigInfo stopInfo = new QueueConfigInfo("root.default", stopParams);
    schedConfUpdateInfo.getUpdateQueueInfo().add(stopInfo);
    confProvider.logAndApplyMutation(user, schedConfUpdateInfo);
    rm1.getResourceScheduler().reinitialize(conf1, rm1.getRMContext());
    confProvider.confirmPendingMutation(true);
    assertTrue(Arrays.asList(((MutableConfScheduler) rm1.getResourceScheduler())
        .getConfiguration().get("yarn.scheduler.capacity.root.queues").split
            (",")).contains("a"));

    // Remove root.default
    schedConfUpdateInfo.getUpdateQueueInfo().clear();
    schedConfUpdateInfo.getAddQueueInfo().clear();
    schedConfUpdateInfo.getRemoveQueueInfo().add("root.default");
    confProvider.logAndApplyMutation(user, schedConfUpdateInfo);
    rm1.getResourceScheduler().reinitialize(conf1, rm1.getRMContext());
    confProvider.confirmPendingMutation(true);
    assertEquals("a", ((MutableConfScheduler) rm1.getResourceScheduler())
        .getConfiguration().get("yarn.scheduler.capacity.root.queues"));

    // Start RM2 and verifies it starts with updated configuration
    rm2.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm2.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());

    for (int i = 0; i < ZK_TIMEOUT_MS / 50; i++) {
      if (HAServiceProtocol.HAServiceState.ACTIVE ==
          rm1.getRMContext().getRMAdminService().getServiceStatus()
              .getState()) {
        Thread.sleep(100);
      }
    }
    assertEquals("RM should have been fenced",
        HAServiceProtocol.HAServiceState.STANDBY,
        rm1.getRMContext().getRMAdminService().getServiceStatus().getState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getRMContext().getRMAdminService().getServiceStatus().getState());

    assertEquals("a", ((MutableCSConfigurationProvider) (
        (CapacityScheduler) rm2.getResourceScheduler())
        .getMutableConfProvider()).getConfStore().retrieve()
        .get("yarn.scheduler.capacity.root.queues"));
    assertEquals("a", ((MutableConfScheduler) rm2.getResourceScheduler())
        .getConfiguration().get("yarn.scheduler.capacity.root.queues"));
    // Transition to standby will set RM's HA status and then reinitialize in
    // a separate thread. Despite asserting for STANDBY state, it's
    // possible for reinitialization to be unfinished. Wait here for it to
    // finish, otherwise closing rm1 will close zkManager and the unfinished
    // reinitialization will throw an exception.
    Thread.sleep(10000);
    rm1.close();
    rm2.close();
  }

  @Override
  public YarnConfigurationStore createConfStore() {
    return new ZKConfigurationStore();
  }
}
