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

package org.apache.hadoop.yarn.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestMiniYarnCluster {

  @Test
  void testTimelineServiceStartInMiniCluster() throws Exception {
    Configuration conf = new YarnConfiguration();
    int numNodeManagers = 1;
    int numLocalDirs = 1;
    int numLogDirs = 1;
    boolean enableAHS;

    /*
     * Timeline service should not start if TIMELINE_SERVICE_ENABLED == false
     * and enableAHS flag == false
     */
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    enableAHS = false;
    try (MiniYARNCluster cluster =
        new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
            numNodeManagers, numLocalDirs, numLogDirs, numLogDirs,
            enableAHS)) {

      cluster.init(conf);
      cluster.start();

      //verify that the timeline service is not started.
      assertNull(cluster.getApplicationHistoryServer(),
          "Timeline Service should not have been started");
    }

    /*
     * Timeline service should start if TIMELINE_SERVICE_ENABLED == true
     * and enableAHS == false
     */
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    enableAHS = false;
    try (MiniYARNCluster cluster =
        new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
            numNodeManagers, numLocalDirs, numLogDirs, numLogDirs,
            enableAHS)) {
      cluster.init(conf);

      // Verify that the timeline-service starts on ephemeral ports by default
      String hostname = MiniYARNCluster.getHostname();
      assertEquals(hostname + ":0",
          conf.get(YarnConfiguration.TIMELINE_SERVICE_ADDRESS));

      cluster.start();

      //Timeline service may sometime take a while to get started
      int wait = 0;
      while (cluster.getApplicationHistoryServer() == null && wait < 20) {
        Thread.sleep(500);
        wait++;
      }
      //verify that the timeline service is started.
      assertNotNull(cluster.getApplicationHistoryServer(),
          "Timeline Service should have been started");
    }
    /*
     * Timeline service should start if TIMELINE_SERVICE_ENABLED == false
     * and enableAHS == true
     */
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    enableAHS = true;
    try (MiniYARNCluster cluster =
        new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
            numNodeManagers, numLocalDirs, numLogDirs, numLogDirs,
            enableAHS)) {
      cluster.init(conf);
      cluster.start();

      //Timeline service may sometime take a while to get started
      int wait = 0;
      while (cluster.getApplicationHistoryServer() == null && wait < 20) {
        Thread.sleep(500);
        wait++;
      }
      //verify that the timeline service is started.
      assertNotNull(cluster.getApplicationHistoryServer(),
          "Timeline Service should have been started");
    }
  }

  @Test
  void testMultiRMConf() throws IOException {
    String RM1_NODE_ID = "rm1", RM2_NODE_ID = "rm2";
    int RM1_PORT_BASE = 10000, RM2_PORT_BASE = 20000;
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "yarn-test-cluster");
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);

    try (MiniYARNCluster cluster =
        new MiniYARNCluster(TestMiniYarnCluster.class.getName(),
            2, 0, 1, 1)) {
      cluster.init(conf);
      Configuration conf1 = cluster.getResourceManager(0).getConfig(),
          conf2 = cluster.getResourceManager(1).getConfig();
      assertFalse(conf1 == conf2);
      assertEquals("0.0.0.0:18032",
          conf1.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS,
              RM1_NODE_ID)));
      assertEquals("0.0.0.0:28032",
          conf1.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS,
              RM2_NODE_ID)));
      assertEquals("rm1", conf1.get(YarnConfiguration.RM_HA_ID));

      assertEquals("0.0.0.0:18032",
          conf2.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS,
              RM1_NODE_ID)));
      assertEquals("0.0.0.0:28032",
          conf2.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS,
              RM2_NODE_ID)));
      assertEquals("rm2", conf2.get(YarnConfiguration.RM_HA_ID));
    }
  }

  @Test
  void testWebAppPortBinding() throws Exception {
    Configuration conf = new YarnConfiguration();
    int numNodeManagers = 1;
    int numLocalDirs = 1;
    int numLogDirs = 1;

    try (MiniYARNCluster cluster =
        new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
            numNodeManagers, numLocalDirs, numLogDirs, numLogDirs)) {

      cluster.init(conf);
      cluster.start();

      Configuration rmConf = cluster.getResourceManager(0).getConfig();
      Configuration nmConf = cluster.getNodeManager(0).getConfig();

      URL nmWebappURL =
        new URL("http://" + nmConf.get(YarnConfiguration.NM_WEBAPP_ADDRESS) + "/node");
      URL rmWebappURL =
        new URL("http://" + rmConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS) + "/cluster");

      HttpURLConnection nmConn = (HttpURLConnection) nmWebappURL.openConnection();
      nmConn.setRequestMethod("GET");
      nmConn.connect();
      assertEquals(nmConn.getResponseCode(), 200);

      // Check the ResourceManager after the NodeManager to make sure it wasn't
      // overwritten. Please see YARN-7747.
      HttpURLConnection rmConn = (HttpURLConnection) rmWebappURL.openConnection();
      rmConn.setRequestMethod("GET");
      rmConn.connect();
      assertEquals(rmConn.getResponseCode(), 200);
    }
  }
}
