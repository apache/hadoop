/*
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

import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAsyncScheduling;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRMHAForAsyncScheduler extends RMHATestBase {
  private TestCapacitySchedulerAsyncScheduling.NMHeartbeatThread
      nmHeartbeatThread = null;

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    confForRM1
        .setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
            DominantResourceCalculator.class, ResourceCalculator.class);
    confForRM1.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    confForRM1.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);

    confForRM2
        .setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
            DominantResourceCalculator.class, ResourceCalculator.class);
    confForRM2.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    confForRM2.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);
  }

  private void keepNMHeartbeat(List<MockNM> mockNMs, int interval) {
    if (nmHeartbeatThread != null) {
      nmHeartbeatThread.setShouldStop();
      nmHeartbeatThread = null;
    }
    nmHeartbeatThread =
        new TestCapacitySchedulerAsyncScheduling.NMHeartbeatThread(mockNMs,
            interval);
    nmHeartbeatThread.start();
  }

  private void pauseNMHeartbeat() {
    if (nmHeartbeatThread != null) {
      nmHeartbeatThread.setShouldStop();
      nmHeartbeatThread = null;
    }
  }

  @Test
  @Timeout(value = 60)
  public void testAsyncScheduleThreadStateAfterRMHATransit() throws Exception {
    // start two RMs, and transit rm1 to active, rm2 to standby
    startRMs();
    // register NM
    MockNM nm = rm1.registerNode("192.1.1.1:1234", 8192, 8);
    // submit app1 and check
    RMApp app1 = submitAppAndCheckLaunched(rm1);
    keepNMHeartbeat(Arrays.asList(nm), 1000);

    // failover RM1 to RM2
    explicitFailover();
    checkAsyncSchedulerThreads(Thread.currentThread());
    pauseNMHeartbeat();

    // register NM, kill app1
    nm = rm2.registerNode("192.1.1.1:1234", 8192, 8);
    keepNMHeartbeat(Arrays.asList(nm), 1000);

    rm2.waitForState(app1.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.LAUNCHED);
    rm2.killApp(app1.getApplicationId());
    // submit app3 and check
    RMApp app2 = submitAppAndCheckLaunched(rm2);
    pauseNMHeartbeat();

    // failover RM2 to RM1
    HAServiceProtocol.StateChangeRequestInfo requestInfo =
        new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    rm2.adminService.transitionToStandby(requestInfo);
    rm1.adminService.transitionToActive(requestInfo);
    assertTrue(rm2.getRMContext().getHAServiceState()
        == HAServiceProtocol.HAServiceState.STANDBY);
    assertTrue(rm1.getRMContext().getHAServiceState()
        == HAServiceProtocol.HAServiceState.ACTIVE);
    // check async schedule threads
    checkAsyncSchedulerThreads(Thread.currentThread());

    // register NM, kill app2
    nm = rm1.registerNode("192.1.1.1:1234", 8192, 8);
    keepNMHeartbeat(Arrays.asList(nm), 1000);

    rm1.waitForState(app2.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.LAUNCHED);
    rm1.killApp(app2.getApplicationId());
    // submit app3 and check
    submitAppAndCheckLaunched(rm1);
    pauseNMHeartbeat();

    rm1.stop();
    rm2.stop();
  }

  @Test
  @Timeout(value = 30)
  public void testAsyncScheduleThreadExit() throws Exception {
    // start two RMs, and transit rm1 to active, rm2 to standby
    startRMs();
    // register NM
    rm1.registerNode("192.1.1.1:1234", 8192, 8);
    rm1.drainEvents();

    // make sure async-scheduling thread is correct at beginning
    checkAsyncSchedulerThreads(Thread.currentThread());

    // test async-scheduling thread exit
    try{
      // set resource calculator to be null to simulate
      // NPE in async-scheduling thread
      CapacityScheduler cs =
          (CapacityScheduler) rm1.getRMContext().getScheduler();
      cs.setResourceCalculator(null);

      // wait for rm1 to be transitioned to standby
      GenericTestUtils.waitFor(() -> rm1.getRMContext().getHAServiceState()
          == HAServiceProtocol.HAServiceState.STANDBY, 100, 5000);

      // failover rm2 to rm1
      HAServiceProtocol.StateChangeRequestInfo requestInfo =
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_USER);
      rm2.adminService.transitionToStandby(requestInfo);
      GenericTestUtils.waitFor(() -> {
        try {
          // this call may fail when rm1 is still initializing
          // in StandByTransitionRunnable thread
          rm1.adminService.transitionToActive(requestInfo);
          return true;
        } catch (Exception e) {
          return false;
        }
      }, 100, 3000);

      // wait for rm1 to be transitioned to active again
      GenericTestUtils.waitFor(() -> rm1.getRMContext().getHAServiceState()
          == HAServiceProtocol.HAServiceState.ACTIVE, 100, 5000);

      // make sure async-scheduling thread is correct after failover
      checkAsyncSchedulerThreads(Thread.currentThread());
    } finally {
      rm1.stop();
      rm2.stop();
    }
  }

  private RMApp submitAppAndCheckLaunched(MockRM rm) throws Exception {
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
        .withAppName("")
        .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue("default")
            .withMaxAppAttempts(
                configuration.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS))
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(false)
        .withKeepContainers(false)
        .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(app.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.LAUNCHED);
    return app;
  }

  /**
   * Make sure the state of async-scheduler threads is correct
   * @param currentThread
   */
  private void checkAsyncSchedulerThreads(Thread currentThread){
    // Make sure AsyncScheduleThread is interrupted
    ThreadGroup threadGroup = currentThread.getThreadGroup();
    while (threadGroup.getParent() != null) {
      threadGroup = threadGroup.getParent();
    }
    Thread[] threads = new Thread[threadGroup.activeCount()];
    threadGroup.enumerate(threads);
    int numAsyncScheduleThread = 0;
    int numResourceCommitterService = 0;
    Thread asyncScheduleThread = null;
    Thread resourceCommitterService = null;
    for (Thread thread : threads) {
      StackTraceElement[] stackTrace = thread.getStackTrace();
      if(stackTrace.length>0){
        String stackBottom = stackTrace[stackTrace.length-1].toString();
        if(stackBottom.contains("AsyncScheduleThread.run")){
          numAsyncScheduleThread++;
          asyncScheduleThread = thread;
        }else if(stackBottom.contains("ResourceCommitterService.run")){
          numResourceCommitterService++;
          resourceCommitterService = thread;
        }
      }
    }
    assertEquals(1, numResourceCommitterService);
    assertEquals(1, numAsyncScheduleThread);
    assertNotNull(asyncScheduleThread);
    assertNotNull(resourceCommitterService);
  }

}