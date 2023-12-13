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

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFencedException;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TestRMHAFatal {
  private Configuration configuration;
  private MockRM rm = null;
  private static final String STATE_ERR =
      "ResourceManager is in wrong HA state";
  private static final String RM1_ADDRESS = "1.1.1.1:1";
  private static final String RM1_NODE_ID = "rm1";

  private static final String RM2_ADDRESS = "0.0.0.0:0";
  private static final String RM2_NODE_ID = "rm2";

  private static final String RM3_ADDRESS = "2.2.2.2:2";
  private static final String RM3_NODE_ID = "rm3";

  @Before
  public void setUp() throws Exception {
    configuration = new Configuration();
    UserGroupInformation.setConfiguration(configuration);
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + ","
        + RM2_NODE_ID);
    for (String confKey : YarnConfiguration
        .getServiceAddressConfKeys(configuration)) {
      configuration.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS);
      configuration.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
      configuration.set(HAUtil.addSuffix(confKey, RM3_NODE_ID), RM3_ADDRESS);
    }

    // Enable webapp to test web-services also
    configuration.setBoolean(MockRM.ENABLE_WEBAPP, true);
    configuration.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    ClusterMetrics.destroy();
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();

    ExitUtil.disableSystemExit();
    ExitUtil.disableSystemHalt();
    ExitUtil.resetFirstExitException();
    ExitUtil.resetFirstHaltException();
  }

  @Test
  public void testHandleTransitionToStandByInNewThreadFatalExit()
      throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    Configuration conf = new YarnConfiguration(configuration);

    //1. Prepare a MockMemoryRMStateStore object
    MemoryRMStateStore memStore = new MockMemoryRMStateStore() {
      @Override
      public void updateApplicationState(ApplicationStateData appState) {
        notifyStoreOperationFailed(new StoreFencedException());
      }
    };
    memStore.init(conf);

    //2. start RM
    rm = new MockRM(conf, memStore);
    rm.init(conf);
    final HAServiceProtocol.StateChangeRequestInfo requestInfo =
        new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    assertEquals(STATE_ERR, HAServiceProtocol.HAServiceState.INITIALIZING,
        rm.adminService.getServiceStatus().getState());
    assertFalse("RM is ready to become active before being started",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());
    checkMonitorHealth();

    rm.start();
    checkMonitorHealth();

    //3. Transition to Active.
    rm.adminService.transitionToActive(requestInfo);
    //4. stop toActiveStandbyExecutor
    rm.stopToActiveStandbyExecutor();

    //5. STATE_STORE_FENCED to standby
    rm.getRMContext().getStateStore().updateApplicationState(null);

    GenericTestUtils.waitFor(
        new org.apache.hadoop.thirdparty.com.google.common.base.Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return (ExitUtil.terminateCalled());
          }
        }, 100, 3000);
    //6. Check Result
    Assert.assertTrue(
        ExitUtil.getFirstExitException().getMessage().contains("RejectedExecutionException"));
  }

  private void checkMonitorHealth() throws IOException {
    try {
      rm.adminService.monitorHealth();
    } catch (HealthCheckFailedException e) {
      fail("The RM is in bad health: it is Active, but the active services " +
          "are not running");
    }
  }

  @After
  public void after() {
    ExitUtil.resetFirstExitException();
    ExitUtil.resetFirstHaltException();
  }


}
