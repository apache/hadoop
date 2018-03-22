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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.TestZKRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore;
import org.junit.Test;

public class TestRMStoreCommands {

  @Test
  public void testFormatStateStoreCmdForZK() throws Exception {
    StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    try (TestingServer curatorTestingServer =
        TestZKRMStateStore.setupCuratorServer();
        CuratorFramework curatorFramework = TestZKRMStateStore.
            setupCuratorFramework(curatorTestingServer)) {
      Configuration conf = TestZKRMStateStore.createHARMConf("rm1,rm2", "rm1",
          1234, false, curatorTestingServer);
      ResourceManager rm = new MockRM(conf);
      rm.start();
      rm.getRMContext().getRMAdminService().transitionToActive(req);
      String zkStateRoot = ZKRMStateStore.ROOT_ZNODE_NAME;
      assertEquals("RM State store parent path should have a child node " +
          zkStateRoot, zkStateRoot, curatorFramework.getChildren().forPath(
              YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH).get(0));
      rm.close();
      try {
        ResourceManager.deleteRMStateStore(conf);
      } catch (Exception e) {
        fail("Exception should not be thrown during format rm state store" +
            " operation.");
      }
      assertTrue("After store format parent path should have no child nodes",
          curatorFramework.getChildren().forPath(
          YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH).isEmpty());
    }
  }

  @Test
  public void testRemoveApplicationFromStateStoreCmdForZK() throws Exception {
    StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    try (TestingServer curatorTestingServer =
        TestZKRMStateStore.setupCuratorServer();
        CuratorFramework curatorFramework = TestZKRMStateStore.
            setupCuratorFramework(curatorTestingServer)) {
      Configuration conf = TestZKRMStateStore.createHARMConf("rm1,rm2", "rm1",
          1234, false, curatorTestingServer);
      ResourceManager rm = new MockRM(conf);
      rm.start();
      rm.getRMContext().getRMAdminService().transitionToActive(req);
      rm.close();
      String appId = ApplicationId.newInstance(
          System.currentTimeMillis(), 1).toString();
      String appRootPath = YarnConfiguration.
          DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH + "/"+
          ZKRMStateStore.ROOT_ZNODE_NAME + "/" + RMStateStore.RM_APP_ROOT;
      String appIdPath = appRootPath + "/" + appId;
      curatorFramework.create().forPath(appIdPath);
      for (String path : curatorFramework.getChildren().forPath(appRootPath)) {
        if (path.equals(ZKRMStateStore.RM_APP_ROOT_HIERARCHIES)) {
          continue;
        }
        assertEquals("Application node for " + appId + " should exist",
            appId, path);
      }
      try {
        ResourceManager.removeApplication(conf, appId);
      } catch (Exception e) {
        fail("Exception should not be thrown while removing app from " +
            "rm state store.");
      }
      assertTrue("After remove app from store there should be no child nodes" +
          " for application in app root path",
          curatorFramework.getChildren().forPath(appRootPath).size() == 1 &&
          curatorFramework.getChildren().forPath(appRootPath).get(0).equals(
              ZKRMStateStore.RM_APP_ROOT_HIERARCHIES));
    }
  }
}