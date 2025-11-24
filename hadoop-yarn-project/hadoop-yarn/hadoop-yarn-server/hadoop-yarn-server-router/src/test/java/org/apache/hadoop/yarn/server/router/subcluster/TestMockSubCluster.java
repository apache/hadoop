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
package org.apache.hadoop.yarn.server.router.subcluster;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.router.subcluster.TestFederationSubCluster.ZK_FEDERATION_STATESTORE;

public class TestMockSubCluster {

  private static final Logger LOG = LoggerFactory.getLogger(TestMockSubCluster.class);
  private Configuration conf;
  private String subClusterId;

  public TestMockSubCluster() {
  }

  public TestMockSubCluster(String pSubClusterId, Configuration pConf) {
    this.conf = pConf;
    this.subClusterId = pSubClusterId;
  }

  private static String getHostNameAndPort(int port) {
    return MiniYARNCluster.getHostname() + ":" + port;
  }

  public void startYarnSubCluster() {
    MiniYARNCluster yrCluster = new MiniYARNCluster(subClusterId, 3, 1, 1, false);
    yrCluster.init(conf);
    yrCluster.start();
  }

  public static void main(String[] args) {
    if (ArrayUtils.isEmpty(args)) {
      return;
    }

    // Step1. Parse the parameters.
    String[] params = args[0].split(",");
    int pRmAddressPort = Integer.parseInt(params[0]);
    int pRmSchedulerAddressPort = Integer.parseInt(params[1]);
    int pRmTrackerAddressPort = Integer.parseInt(params[2]);
    int pRmWebAddressPort = Integer.parseInt(params[3]);
    int pRmAdminAddressPort = Integer.parseInt(params[4]);
    String pSubClusterId = params[5];
    String pZkAddress = params[6];
    String schedulerType = params[7];

    // Step 2. Print the parameters.
    LOG.info("subClusterId = {}, rmAddressPort = {}, rmSchedulerAddressPort = {}, " +
        "rmTrackerAddressPort = {}, rmWebAddressPort = {}, rmAdminAddressPort = {}",
        pSubClusterId, pRmAddressPort, pRmSchedulerAddressPort, pRmTrackerAddressPort,
        pRmWebAddressPort, pRmAdminAddressPort);

    // Step 3. determine which scheduler to use.
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_ADDRESS, getHostNameAndPort(pRmAddressPort));
    conf.set(YarnConfiguration.RM_ADMIN_ADDRESS, getHostNameAndPort(pRmAdminAddressPort));
    conf.set(YarnConfiguration.RM_HOSTNAME, MiniYARNCluster.getHostname());
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, getHostNameAndPort(pRmSchedulerAddressPort));
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        getHostNameAndPort(pRmTrackerAddressPort));
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, getHostNameAndPort(pRmWebAddressPort));
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS, ZK_FEDERATION_STATESTORE);
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, pZkAddress);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, pSubClusterId);
    if (schedulerType.equals("fair-scheduler")) {
      conf.set("yarn.resourcemanager.scheduler.class",
          "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");
      conf.set("yarn.scheduler.fair.allocation.file", "fair-scheduler.xml");
    }

    // Step 4, start the mockSubCluster cluster.
    TestMockSubCluster sc = new TestMockSubCluster(pSubClusterId, conf);
    sc.startYarnSubCluster();
  }
}
