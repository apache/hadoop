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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.TestWorkPreservingRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for application life time monitor feature test.
 */
public class TestApplicationLifetimeMonitor {
  private YarnConfiguration conf;

  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    UserGroupInformation.setConfiguration(conf);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
        true);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setLong(YarnConfiguration.RM_APPLICATION_LIFETIME_MONITOR_INTERVAL_MS,
        3000L);
  }

  @Test(timeout = 90000)
  public void testApplicationLifetimeMonitor() throws Exception {
    MockRM rm = null;
    try {
      rm = new MockRM(conf);
      rm.start();
      Priority appPriority = Priority.newInstance(0);
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * 1024);

      Map<ApplicationTimeoutType, Long> timeouts =
          new HashMap<ApplicationTimeoutType, Long>();
      timeouts.put(ApplicationTimeoutType.LIFETIME, 10L);
      RMApp app1 = rm.submitApp(1024, appPriority, timeouts);
      nm1.nodeHeartbeat(true);
      // Send launch Event
      MockAM am1 =
          rm.sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
      am1.registerAppAttempt();
      rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
      Assert.assertTrue("Applicaiton killed before lifetime value",
          (System.currentTimeMillis() - app1.getSubmitTime()) > 10000);
    } finally {
      stopRM(rm);
    }
  }

  @SuppressWarnings("rawtypes")
  @Test(timeout = 180000)
  public void testApplicationLifetimeOnRMRestart() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true);

    long appLifetime = 60L;
    Map<ApplicationTimeoutType, Long> timeouts =
        new HashMap<ApplicationTimeoutType, Long>();
    timeouts.put(ApplicationTimeoutType.LIFETIME, appLifetime);
    RMApp app1 = rm1.submitApp(200, Priority.newInstance(0), timeouts);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Re-start RM
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    // recover app
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());

    NMContainerStatus amContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);
    NMContainerStatus runningContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);

    nm1.registerNode(Arrays.asList(amContainer, runningContainer), null);

    // Wait for RM to settle down on recovering containers;
    TestWorkPreservingRMRestart.waitForNumContainersToRecover(2, rm2,
        am1.getApplicationAttemptId());
    Set<ContainerId> launchedContainers =
        ((RMNodeImpl) rm2.getRMContext().getRMNodes().get(nm1.getNodeId()))
            .getLaunchedContainers();
    assertTrue(launchedContainers.contains(amContainer.getContainerId()));
    assertTrue(launchedContainers.contains(runningContainer.getContainerId()));

    // check RMContainers are re-recreated and the container state is correct.
    rm2.waitForState(nm1, amContainer.getContainerId(),
        RMContainerState.RUNNING);
    rm2.waitForState(nm1, runningContainer.getContainerId(),
        RMContainerState.RUNNING);

    // re register attempt to rm2
    rm2.waitForState(recoveredApp1.getApplicationId(), RMAppState.ACCEPTED);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt();
    rm2.waitForState(recoveredApp1.getApplicationId(), RMAppState.RUNNING);

    // wait for app life time and application to be in killed state.
    rm2.waitForState(recoveredApp1.getApplicationId(), RMAppState.KILLED);
    Assert.assertTrue("Applicaiton killed before lifetime value",
        (System.currentTimeMillis()
            - recoveredApp1.getSubmitTime()) > appLifetime);
  }

  private void stopRM(MockRM rm) {
    if (rm != null) {
      rm.stop();
    }
  }
}
