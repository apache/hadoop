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
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRMHA {
  private Log LOG = LogFactory.getLog(TestRMHA.class);
  private MockRM rm = null;
  private static final String STATE_ERR =
      "ResourceManager is in wrong HA state";

  private static final String RM1_ADDRESS = "0.0.0.0:0";
  private static final String RM1_NODE_ID = "rm1";

  @Before
  public void setUp() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID);
    for (String confKey : YarnConfiguration.RM_RPC_ADDRESS_CONF_KEYS) {
      conf.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS);
    }
    conf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);

    rm = new MockRM(conf);
    rm.init(conf);
  }

  private void checkMonitorHealth() throws IOException {
    try {
      rm.haService.monitorHealth();
    } catch (HealthCheckFailedException e) {
      fail("The RM is in bad health: it is Active, but the active services " +
          "are not running");
    }
  }

  private void checkStandbyRMFunctionality() throws IOException {
    assertEquals(STATE_ERR, HAServiceState.STANDBY,
        rm.haService.getServiceStatus().getState());
    assertFalse("Active RM services are started",
        rm.areActiveServicesRunning());
    assertTrue("RM is not ready to become active",
        rm.haService.getServiceStatus().isReadyToBecomeActive());
  }

  private void checkActiveRMFunctionality() throws IOException {
    assertEquals(STATE_ERR, HAServiceState.ACTIVE,
        rm.haService.getServiceStatus().getState());
    assertTrue("Active RM services aren't started",
        rm.areActiveServicesRunning());
    assertTrue("RM is not ready to become active",
        rm.haService.getServiceStatus().isReadyToBecomeActive());

    try {
      rm.getNewAppId();
      rm.registerNode("127.0.0.1:0", 2048);
      rm.submitApp(1024);
    } catch (Exception e) {
      fail("Unable to perform Active RM functions");
      LOG.error("ActiveRM check failed", e);
    }
  }

  /**
   * Test to verify the following RM HA transitions to the following states.
   * 1. Standby: Should be a no-op
   * 2. Active: Active services should start
   * 3. Active: Should be a no-op.
   *    While active, submit a couple of jobs
   * 4. Standby: Active services should stop
   * 5. Active: Active services should start
   * 6. Stop the RM: All services should stop and RM should not be ready to
   * become Active
   */
  @Test (timeout = 30000)
  public void testStartAndTransitions() throws IOException {
    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    assertEquals(STATE_ERR, HAServiceState.INITIALIZING,
        rm.haService.getServiceStatus().getState());
    assertFalse("RM is ready to become active before being started",
        rm.haService.getServiceStatus().isReadyToBecomeActive());
    checkMonitorHealth();

    rm.start();
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 1. Transition to Standby - must be a no-op
    rm.haService.transitionToStandby(requestInfo);
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 2. Transition to active
    rm.haService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();

    // 3. Transition to active - no-op
    rm.haService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();

    // 4. Transition to standby
    rm.haService.transitionToStandby(requestInfo);
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 5. Transition to active to check Active->Standby->Active works
    rm.haService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();

    // 6. Stop the RM. All services should stop and RM should not be ready to
    // become active
    rm.stop();
    assertEquals(STATE_ERR, HAServiceState.STOPPING,
        rm.haService.getServiceStatus().getState());
    assertFalse("RM is ready to become active even after it is stopped",
        rm.haService.getServiceStatus().isReadyToBecomeActive());
    assertFalse("Active RM services are started",
        rm.areActiveServicesRunning());
    checkMonitorHealth();
  }
}
