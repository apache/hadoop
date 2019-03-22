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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.TestWorkPreservingRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.Times;
import org.slf4j.event.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test class for application life time monitor feature test.
 */
@RunWith(Parameterized.class)
public class TestApplicationLifetimeMonitor {
  private final long maxLifetime = 30L;

  private YarnConfiguration conf;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{CapacityScheduler.class});
    params.add(new Object[]{FairScheduler.class});
    return params;
  }

  private Class scheduler;

  public TestApplicationLifetimeMonitor(Class schedulerParameter) {
    scheduler = schedulerParameter;
  }

  @Before
  public void setup() throws IOException {
    if (scheduler.equals(CapacityScheduler.class)) {
      // Since there is limited lifetime monitoring support in fair scheduler
      // it does not need queue setup
      long defaultLifetime = 15L;
      Configuration capacitySchedulerConfiguration =
          setUpCSQueue(maxLifetime, defaultLifetime);
      conf = new YarnConfiguration(capacitySchedulerConfiguration);
    } else {
      conf = new YarnConfiguration();
    }
    // Always run for CS, since other scheduler do not support this.
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        scheduler, ResourceScheduler.class);
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    UserGroupInformation.setConfiguration(conf);
    conf.setLong(YarnConfiguration.RM_APPLICATION_MONITOR_INTERVAL_MS,
        3000L);
  }

  @Test(timeout = 60000)
  public void testApplicationLifetimeMonitor()
      throws Exception {
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

      // 20L seconds
      timeouts.put(ApplicationTimeoutType.LIFETIME, 20L);
      RMApp app2 = rm.submitApp(1024, appPriority, timeouts);

      // user not set lifetime, so queue max lifetime will be considered.
      RMApp app3 = rm.submitApp(1024, appPriority, Collections.emptyMap());

      // asc lifetime exceeds queue max lifetime
      timeouts.put(ApplicationTimeoutType.LIFETIME, 40L);
      RMApp app4 = rm.submitApp(1024, appPriority, timeouts);

      nm1.nodeHeartbeat(true);
      // Send launch Event
      MockAM am1 =
          rm.sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
      am1.registerAppAttempt();
      rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
      Assert.assertTrue("Application killed before lifetime value",
          (System.currentTimeMillis() - app1.getSubmitTime()) > 10000);

      Map<ApplicationTimeoutType, String> updateTimeout =
          new HashMap<ApplicationTimeoutType, String>();
      long newLifetime = 40L;
      // update 30L seconds more to timeout which is greater than queue max
      // lifetime
      String formatISO8601 =
          Times.formatISO8601(System.currentTimeMillis() + newLifetime * 1000);
      updateTimeout.put(ApplicationTimeoutType.LIFETIME, formatISO8601);
      UpdateApplicationTimeoutsRequest request =
          UpdateApplicationTimeoutsRequest.newInstance(app2.getApplicationId(),
              updateTimeout);

      Map<ApplicationTimeoutType, Long> applicationTimeouts =
          app2.getApplicationTimeouts();
      // has old timeout time
      long beforeUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      // update app2 lifetime to new time i.e now + timeout
      rm.getRMContext().getClientRMService().updateApplicationTimeouts(request);

      applicationTimeouts =
          app2.getApplicationTimeouts();
      long afterUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      Assert.assertTrue("Application lifetime value not updated",
          afterUpdate > beforeUpdate);

      // verify for application report.
      RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);
      GetApplicationReportRequest appRequest =
          recordFactory.newRecordInstance(GetApplicationReportRequest.class);
      appRequest.setApplicationId(app2.getApplicationId());
      Map<ApplicationTimeoutType, ApplicationTimeout> appTimeouts = rm
          .getRMContext().getClientRMService().getApplicationReport(appRequest)
          .getApplicationReport().getApplicationTimeouts();
      Assert.assertTrue("Application Timeout are empty.",
          !appTimeouts.isEmpty());
      ApplicationTimeout timeout =
          appTimeouts.get(ApplicationTimeoutType.LIFETIME);
      Assert.assertTrue("Application remaining time is incorrect",
          timeout.getRemainingTime() > 0);

      rm.waitForState(app2.getApplicationId(), RMAppState.KILLED);
      // verify for app killed with updated lifetime
      Assert.assertTrue("Application killed before lifetime value",
          app2.getFinishTime() > afterUpdate);

      if (scheduler.equals(CapacityScheduler.class)) {
        // Supported only on capacity scheduler
        rm.waitForState(app3.getApplicationId(), RMAppState.KILLED);

        // app4 submitted exceeding queue max lifetime,
        // so killed after queue max lifetime.
        rm.waitForState(app4.getApplicationId(), RMAppState.KILLED);
        long totalTimeRun = app4.getFinishTime() - app4.getSubmitTime();
        Assert.assertTrue("Application killed before lifetime value",
            totalTimeRun > (maxLifetime * 1000));
        Assert.assertTrue(
            "Application killed before lifetime value " + totalTimeRun,
            totalTimeRun < ((maxLifetime + 10L) * 1000));
      }
    } finally {
      stopRM(rm);
    }
  }

  @Test(timeout = 180000)
  public void testApplicationLifetimeOnRMRestart() throws Exception {
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
        true);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());

    MockRM rm1 = new MockRM(conf);
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true);

    long appLifetime = 30L;
    Map<ApplicationTimeoutType, Long> timeouts =
        new HashMap<ApplicationTimeoutType, Long>();
    timeouts.put(ApplicationTimeoutType.LIFETIME, appLifetime);
    RMApp app1 = rm1.submitApp(200, Priority.newInstance(0), timeouts);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Re-start RM
    MockRM rm2 = new MockRM(conf, memStore);

    // make sure app has been unregistered with old RM else both will trigger
    // Expire event
    rm1.getRMContext().getRMAppLifetimeMonitor().unregisterApp(
        app1.getApplicationId(), ApplicationTimeoutType.LIFETIME);

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
    Assert.assertTrue("Application killed before lifetime value",
        recoveredApp1.getFinishTime() > (recoveredApp1.getSubmitTime()
            + appLifetime * 1000));
  }

  @Test(timeout = 60000)
  public void testUpdateApplicationTimeoutForStateStoreUpdateFail()
      throws Exception {
    MockRM rm1 = null;
    try {
      MemoryRMStateStore memStore = new MemoryRMStateStore() {
        private int count = 0;

        @Override
        public synchronized void updateApplicationStateInternal(
            ApplicationId appId, ApplicationStateData appState)
            throws Exception {
          // fail only 1 time.
          if (count++ == 0) {
            throw new Exception("State-store update failed");
          }
          super.updateApplicationStateInternal(appId, appState);
        }
      };
      memStore.init(conf);
      rm1 = new MockRM(conf, memStore);
      rm1.start();
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
      nm1.registerNode();
      nm1.nodeHeartbeat(true);

      long appLifetime = 30L;
      Map<ApplicationTimeoutType, Long> timeouts =
          new HashMap<ApplicationTimeoutType, Long>();
      timeouts.put(ApplicationTimeoutType.LIFETIME, appLifetime);
      RMApp app1 = rm1.submitApp(200, Priority.newInstance(0), timeouts);

      Map<ApplicationTimeoutType, String> updateTimeout =
          new HashMap<ApplicationTimeoutType, String>();
      long newLifetime = 10L;
      // update 10L seconds more to timeout i.e 30L seconds overall
      updateTimeout.put(ApplicationTimeoutType.LIFETIME,
          Times.formatISO8601(System.currentTimeMillis() + newLifetime * 1000));
      UpdateApplicationTimeoutsRequest request =
          UpdateApplicationTimeoutsRequest.newInstance(app1.getApplicationId(),
              updateTimeout);

      Map<ApplicationTimeoutType, Long> applicationTimeouts =
          app1.getApplicationTimeouts();
      // has old timeout time
      long beforeUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      try {
        // update app2 lifetime to new time i.e now + timeout
        rm1.getRMContext().getClientRMService()
            .updateApplicationTimeouts(request);
        fail("Update application should fail.");
      } catch (YarnException e) {
        // expected
        assertTrue("State-store exception does not containe appId",
            e.getMessage().contains(app1.getApplicationId().toString()));
      }

      applicationTimeouts = app1.getApplicationTimeouts();
      // has old timeout time
      long afterUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      Assert.assertEquals("Application timeout is updated", beforeUpdate,
          afterUpdate);
      rm1.waitForState(app1.getApplicationId(), RMAppState.KILLED);
      // verify for app killed with updated lifetime
      Assert.assertTrue("Application killed before lifetime value",
          app1.getFinishTime() > afterUpdate);
    } finally {
      stopRM(rm1);
    }
  }

  private CapacitySchedulerConfiguration setUpCSQueue(long maxLifetime,
      long defaultLifetime) {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"default"});
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".default", 100);
    csConf.setMaximumLifetimePerQueue(
        CapacitySchedulerConfiguration.ROOT + ".default", maxLifetime);
    csConf.setDefaultLifetimePerQueue(
        CapacitySchedulerConfiguration.ROOT + ".default", defaultLifetime);

    return csConf;
  }

  private void stopRM(MockRM rm) {
    if (rm != null) {
      rm.stop();
    }
  }
}
