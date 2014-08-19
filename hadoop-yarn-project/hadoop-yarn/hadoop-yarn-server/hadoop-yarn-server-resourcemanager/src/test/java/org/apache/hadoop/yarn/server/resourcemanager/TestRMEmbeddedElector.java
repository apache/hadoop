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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRMEmbeddedElector extends ClientBaseWithFixes {
  private static final Log LOG =
      LogFactory.getLog(TestRMEmbeddedElector.class.getName());

  private static final String RM1_NODE_ID = "rm1";
  private static final int RM1_PORT_BASE = 10000;
  private static final String RM2_NODE_ID = "rm2";
  private static final int RM2_PORT_BASE = 20000;

  private Configuration conf;
  private AtomicBoolean callbackCalled;

  private void setConfForRM(String rmId, String prefix, String value) {
    conf.set(HAUtil.addSuffix(prefix, rmId), value);
  }

  private void setRpcAddressForRM(String rmId, int base) {
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

  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_EMBEDDED, true);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "yarn-test-cluster");
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
    conf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, 2000);

    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    conf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);
    setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE);
    setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);

    callbackCalled = new AtomicBoolean(false);
  }

  /**
   * Test that tries to see if there is a deadlock between
   * (a) the thread stopping the RM
   * (b) thread processing the ZK event asking RM to transition to active
   *
   * The test times out if there is a deadlock.
   */
  @Test (timeout = 10000)
  public void testDeadlockShutdownBecomeActive() throws InterruptedException {
    MockRM rm = new MockRMWithElector(conf, 1000);
    rm.start();
    LOG.info("Waiting for callback");
    while (!callbackCalled.get());
    LOG.info("Stopping RM");
    rm.stop();
    LOG.info("Stopped RM");
  }

  private class MockRMWithElector extends MockRM {
    private long delayMs = 0;

    MockRMWithElector(Configuration conf) {
      super(conf);
    }

    MockRMWithElector(Configuration conf, long delayMs) {
      this(conf);
      this.delayMs = delayMs;
    }

    @Override
    protected AdminService createAdminService() {
      return new AdminService(MockRMWithElector.this, getRMContext()) {
        @Override
        protected EmbeddedElectorService createEmbeddedElectorService() {
          return new EmbeddedElectorService(getRMContext()) {
            @Override
            public void becomeActive() throws
                ServiceFailedException {
              try {
                callbackCalled.set(true);
                LOG.info("Callback called. Sleeping now");
                Thread.sleep(delayMs);
                LOG.info("Sleep done");
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              super.becomeActive();
            }
          };
        }
      };
    }
  }
}
