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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRMFailover {
  private static final Log LOG =
      LogFactory.getLog(TestRMFailover.class.getName());

  private static final String RM1_NODE_ID = "rm1";
  private static final int RM1_PORT_BASE = 10000;
  private static final String RM2_NODE_ID = "rm2";
  private static final int RM2_PORT_BASE = 20000;
  private static final HAServiceProtocol.StateChangeRequestInfo req =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED);

  private static Configuration conf;
  private static MiniYARNCluster cluster;

  private static void setConfForRM(String rmId, String prefix, String value) {
    conf.set(HAUtil.addSuffix(prefix, rmId), value);
  }

  private static void setRpcAddressForRM(String rmId, int base) {
    setConfForRM(rmId, YarnConfiguration.RM_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_SCHEDULER_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_ADMIN_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_ADMIN_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_WEBAPP_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_WEBAPP_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT));
  }

  private static AdminService getRMAdminService(int index) {
    return
        cluster.getResourceManager(index).getRMContext().getRMAdminService();
  }

  @BeforeClass
  public static void setup() throws IOException {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE);
    setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE);

    conf.setInt(YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, 100);
    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);
    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_MAX_MS, 1000L);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);

    cluster = new MiniYARNCluster(TestRMFailover.class.getName(), 2, 1, 1, 1);
    cluster.init(conf);
    cluster.start();

    cluster.getResourceManager(0).getRMContext().getRMAdminService()
        .transitionToActive(req);
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
  }

  @AfterClass
  public static void teardown() {
    cluster.stop();
  }

  private void verifyClientConnection() {
    int numRetries = 3;
    while(numRetries-- > 0) {
      Configuration conf = new YarnConfiguration(TestRMFailover.conf);
      YarnClient client = YarnClient.createYarnClient();
      client.init(conf);
      client.start();
      try {
        client.getApplications();
        return;
      } catch (Exception e) {
        LOG.error(e);
      } finally {
        client.stop();
      }
    }
    fail("Client couldn't connect to the Active RM");
  }

  @Test
  public void testExplicitFailover()
      throws YarnException, InterruptedException, IOException {
    assertTrue("NMs failed to connect to the RM",
        cluster.waitForNodeManagersToConnect(5000));
    verifyClientConnection();

    // Failover to the second RM
    getRMAdminService(0).transitionToStandby(req);
    getRMAdminService(1).transitionToActive(req);
    assertEquals("Wrong ResourceManager is active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        getRMAdminService(1).getServiceStatus().getState());
    assertTrue("NMs failed to connect to the RM",
        cluster.waitForNodeManagersToConnect(5000));
    verifyClientConnection();

    // Failover back to the first RM
    getRMAdminService(1).transitionToStandby(req);
    getRMAdminService(0).transitionToActive(req);
    assertEquals("Wrong ResourceManager is active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        getRMAdminService(0).getServiceStatus().getState());
    assertTrue("NMs failed to connect to the RM",
        cluster.waitForNodeManagersToConnect(5000));
    verifyClientConnection();
  }
}
