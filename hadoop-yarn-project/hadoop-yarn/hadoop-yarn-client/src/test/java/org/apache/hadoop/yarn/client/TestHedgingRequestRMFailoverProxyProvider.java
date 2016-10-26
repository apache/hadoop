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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestHedgingRequestRMFailoverProxyProvider {

  @Test
  public void testHedgingRequestProxyProvider() throws Exception {
    final MiniYARNCluster cluster =
        new MiniYARNCluster("testHedgingRequestProxyProvider", 5, 0, 1, 1);
    Configuration conf = new YarnConfiguration();

    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2,rm3,rm4,rm5");

    conf.set(YarnConfiguration.CLIENT_FAILOVER_PROXY_PROVIDER,
        RequestHedgingRMFailoverProxyProvider.class.getName());
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        2000);

    HATestUtil.setRpcAddressForRM("rm1", 10000, conf);
    HATestUtil.setRpcAddressForRM("rm2", 20000, conf);
    HATestUtil.setRpcAddressForRM("rm3", 30000, conf);
    HATestUtil.setRpcAddressForRM("rm4", 40000, conf);
    HATestUtil.setRpcAddressForRM("rm5", 50000, conf);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);

    cluster.init(conf);
    cluster.start();

    final YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();

    // Transition rm5 to active;
    long start = System.currentTimeMillis();
    makeRMActive(cluster, 4);

    validateActiveRM(client);

    long end = System.currentTimeMillis();
    System.out.println("Client call succeeded at " + end);
    // should return the response fast
    Assert.assertTrue(end - start <= 10000);

    // transition rm5 to standby
    cluster.getResourceManager(4).getRMContext().getRMAdminService()
        .transitionToStandby(new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER));

    makeRMActive(cluster, 2);

    validateActiveRM(client);

    cluster.stop();
  }

  private void validateActiveRM(YarnClient client) throws IOException {
    // first check if exception is thrown correctly;
    try {
      // client will retry until the rm becomes active.
      client.getApplicationReport(null);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e instanceof ApplicationNotFoundException);
    }
    // now make a valid call.
    try {
      client.getAllQueues();
    } catch (YarnException e) {
      Assert.fail(e.toString());
    }
  }

  private void makeRMActive(final MiniYARNCluster cluster, final int index) {
    Thread t = new Thread() {
      @Override public void run() {
        try {
          System.out.println("Transition rm" + index + " to active");
          cluster.getResourceManager(index).getRMContext().getRMAdminService()
              .transitionToActive(new HAServiceProtocol.StateChangeRequestInfo(
                  HAServiceProtocol.RequestSource.REQUEST_BY_USER));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
  }
}
