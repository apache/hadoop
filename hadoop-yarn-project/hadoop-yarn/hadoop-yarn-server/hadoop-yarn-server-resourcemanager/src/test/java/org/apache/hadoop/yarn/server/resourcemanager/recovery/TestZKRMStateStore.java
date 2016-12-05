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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertFalse;

public class TestZKRMStateStore extends RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(TestZKRMStateStore.class);
  private static final int ZK_TIMEOUT_MS = 1000;

  class TestZKRMStateStoreTester implements RMStateStoreHelper {
    ZooKeeper client;
    TestZKRMStateStoreInternal store;
    String workingZnode;

    class TestZKRMStateStoreInternal extends ZKRMStateStore {

      public TestZKRMStateStoreInternal(Configuration conf, String workingZnode)
          throws Exception {
        init(conf);
        start();
        assertEquals(workingZnode, znodeWorkingPath);
      }

      @Override
      public ZooKeeper getNewZooKeeper() throws IOException {
        return client;
      }

      public String getVersionNode() {
        return znodeWorkingPath + "/" + ROOT_ZNODE_NAME + "/" + VERSION_NODE;
      }

      public Version getCurrentVersion() {
        return CURRENT_VERSION_INFO;
      }

      public String getAppNode(String appId) {
        return workingZnode + "/" + ROOT_ZNODE_NAME + "/" + RM_APP_ROOT + "/"
            + appId;
      }
    }

    public RMStateStore getRMStateStore() throws Exception {
      YarnConfiguration conf = new YarnConfiguration();
      workingZnode = "/Test";
      conf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
      this.client = createClient();
      this.store = new TestZKRMStateStoreInternal(conf, workingZnode);
      return this.store;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      List<String> nodes = client.getChildren(store.znodeWorkingPath, false);
      return nodes.size() == 1;
    }

    @Override
    public void writeVersion(Version version) throws Exception {
      client.setData(store.getVersionNode(), ((VersionPBImpl) version)
        .getProto().toByteArray(), -1);
    }

    @Override
    public Version getCurrentVersion() throws Exception {
      return store.getCurrentVersion();
    }

    public boolean appExists(RMApp app) throws Exception {
      Stat node =
          client.exists(store.getAppNode(app.getApplicationId().toString()),
            false);
      return node !=null;
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
    testAMRMTokenSecretManagerStateStore(zkTester);
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
        workingZnode = "/Test";
        conf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
        conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
        this.client = createClient();
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
    Assert.assertEquals(defaultVersion, store.loadVersion());
  }

  private Configuration createHARMConf(
      String rmIds, String rmId, int adminPort) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, rmIds);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, ZKRMStateStore.class.getName());
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
    conf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, ZK_TIMEOUT_MS);
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");

    for (String rpcAddress : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      for (String id : HAUtil.getRMHAIds(conf)) {
        conf.set(HAUtil.addSuffix(rpcAddress, id), "localhost:0");
      }
    }
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADMIN_ADDRESS, rmId),
        "localhost:" + adminPort);
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFencing() throws Exception {
    StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    Configuration conf1 = createHARMConf("rm1,rm2", "rm1", 1234);
    conf1.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    ResourceManager rm1 = new ResourceManager();
    rm1.init(conf1);
    rm1.start();
    rm1.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm1.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm1.getRMContext().getRMAdminService().getServiceStatus().getState());

    Configuration conf2 = createHARMConf("rm1,rm2", "rm2", 5678);
    conf2.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    ResourceManager rm2 = new ResourceManager();
    rm2.init(conf2);
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
  }

  @Test
  public void testTransitionWithUnreachableZK() throws Exception {
    final AtomicBoolean zkUnreachable = new AtomicBoolean(false);
    final CountDownLatch threadHung = new CountDownLatch(1);
    final Configuration conf = createHARMConf("rm1,rm2", "rm1", 1234);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);

    // Create a state store that can simulate losing contact with the ZK node
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester() {
      @Override
      public RMStateStore getRMStateStore() throws Exception {
        YarnConfiguration storeConf = new YarnConfiguration(conf);
        workingZnode = "/Test";
        storeConf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
        storeConf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH,
            workingZnode);
        storeConf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, 500);
        this.client = createClient();
        this.store = new TestZKRMStateStoreInternal(storeConf, workingZnode) {
          @Override
          synchronized void doMultiWithRetries(final List<Op> opList)
              throws Exception {
            if (zkUnreachable.get()) {
              // Let the test know that it can now proceed
              threadHung.countDown();

              // Take a long nap while holding the lock to simulate the ZK node
              // being unreachable. This behavior models what happens in
              // super.doStoreMultiWithRetries() when the ZK node it unreachble.
              // If that behavior changes, then this test should also change or
              // be phased out.
              Thread.sleep(60000);
            } else {
              // Business as usual
              super.doMultiWithRetries(opList);
            }
          }
        };
        return this.store;
      }
    };

    // Start with a single RM in HA mode
    final RMStateStore store = zkTester.getRMStateStore();
    final MockRM rm = new MockRM(conf, store);
    rm.start();

    // Make the RM active
    final StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    rm.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm.getRMContext().getRMAdminService().getServiceStatus().getState());

    // Simulate the ZK node going dark and wait for the
    // VerifyActiveStatusThread to hang
    zkUnreachable.set(true);

    assertTrue("Unable to perform test because Verify Active Status Thread "
        + "did not run", threadHung.await(2, TimeUnit.SECONDS));

    // Try to transition the RM to standby.  Give up after 2000ms.
    Thread standby = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          rm.getRMContext().getRMAdminService().transitionToStandby(req);
        } catch (IOException ex) {
          // OK to exit
        }
      }
    }, "Test Unreachable ZK Thread");

    standby.start();
    standby.join(2000);

    assertFalse("The thread initiating the transition to standby is hung",
        standby.isAlive());
    zkUnreachable.set(false);
  }
}
