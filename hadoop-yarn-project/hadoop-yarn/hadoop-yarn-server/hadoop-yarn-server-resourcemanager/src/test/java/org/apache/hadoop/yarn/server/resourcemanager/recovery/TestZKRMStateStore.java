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
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class TestZKRMStateStore extends RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(TestZKRMStateStore.class);

  class TestZKRMStateStoreTester implements RMStateStoreHelper {

    ZooKeeper client;
    ZKRMStateStore store;

    class TestZKRMStateStoreInternal extends ZKRMStateStore {

      public TestZKRMStateStoreInternal(Configuration conf, String workingZnode)
          throws Exception {
        init(conf);
        start();
        assertTrue(znodeWorkingPath.equals(workingZnode));
      }

      @Override
      public ZooKeeper getNewZooKeeper() throws IOException {
        return client;
      }
    }

    public RMStateStore getRMStateStore() throws Exception {
      String workingZnode = "/Test";
      Configuration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_ADDRESS, hostPort);
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
  }

  @Test
  public void testZKRMStateStoreRealZK() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    testRMAppStateStore(zkTester);
    testRMDTSecretManagerStateStore(zkTester);
  }

  private Configuration createHARMConf(
      String rmIds, String rmId, int adminPort) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, rmIds);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, ZKRMStateStore.class.getName());
    conf.set(YarnConfiguration.ZK_RM_STATE_STORE_ADDRESS, hostPort);
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
    for (String rpcAddress : HAUtil.RPC_ADDRESS_CONF_KEYS) {
      conf.set(HAUtil.addSuffix(rpcAddress, rmId), "localhost:0");
    }
    conf.set(YarnConfiguration.RM_HA_ADMIN_ADDRESS, "localhost:" + adminPort);
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFencing() throws Exception {
    StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    Configuration conf1 = createHARMConf("rm1,rm2", "rm1", 1234);
    ResourceManager rm1 = new ResourceManager();
    rm1.init(conf1);
    rm1.start();
    rm1.getHAService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm1.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm1.getHAService().getServiceStatus().getState());

    Configuration conf2 = createHARMConf("rm1,rm2", "rm2", 5678);
    ResourceManager rm2 = new ResourceManager();
    rm2.init(conf2);
    rm2.start();
    rm2.getHAService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm2.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getHAService().getServiceStatus().getState());

    // Submitting an application to RM1 to trigger a state store operation.
    // RM1 should realize that it got fenced and is not the Active RM anymore.
    Map mockMap = mock(Map.class);
    ApplicationSubmissionContext asc =
        ApplicationSubmissionContext.newInstance(
            ApplicationId.newInstance(1000, 1),
            "testApplication", // app Name
            "default", // queue name
            Priority.newInstance(0),
            ContainerLaunchContext.newInstance(mockMap, mockMap,
                new ArrayList<String>(), mockMap, mock(ByteBuffer.class),
                mockMap),
            false, // unmanaged AM
            true, // cancelTokens
            1, // max app attempts
            Resource.newInstance(1024, 1));
    ClientRMService rmService = rm1.getClientRMService();
    rmService.submitApplication(SubmitApplicationRequest.newInstance(asc));

    for (int i = 0; i < 30; i++) {
      if (HAServiceProtocol.HAServiceState.ACTIVE == rm1.getHAService()
          .getServiceStatus().getState()) {
        Thread.sleep(100);
      }
    }
    assertEquals("RM should have been fenced",
        HAServiceProtocol.HAServiceState.STANDBY,
        rm1.getHAService().getServiceStatus().getState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm2.getHAService().getServiceStatus().getState());
  }
}
