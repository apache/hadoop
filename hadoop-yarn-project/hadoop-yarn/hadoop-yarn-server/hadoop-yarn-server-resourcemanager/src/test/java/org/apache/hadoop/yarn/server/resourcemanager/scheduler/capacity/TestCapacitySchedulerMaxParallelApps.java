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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class TestCapacitySchedulerMaxParallelApps {
  private CapacitySchedulerConfiguration conf;
  private MockRM rm;
  private MockNM nm1;

  private RMApp app1;
  private MockAM am1;
  private RMApp app2;
  private MockAM am2;
  private RMApp app3;
  private RMAppAttempt attempt3;
  private RMApp app4;
  private RMAppAttempt attempt4;

  private ParentQueue rootQueue;
  private LeafQueue defaultQueue;

  @Before
  public void setUp() {
    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();
    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());

    conf = new CapacitySchedulerConfiguration(config);
  }

  @After
  public void after() {
    if (rm != null) {
      rm.stop();
    }
  }

  @Test(timeout = 30000)
  public void testMaxParallelAppsExceedsQueueSetting() throws Exception {
    conf.setInt("yarn.scheduler.capacity.root.default.max-parallel-apps", 2);
    executeCommonStepsAndChecks();
    testWhenSettingsExceeded();
  }

  @Test(timeout = 30000)
  public void testMaxParallelAppsExceedsDefaultQueueSetting()
      throws Exception {
    conf.setInt("yarn.scheduler.capacity.max-parallel-apps", 2);
    executeCommonStepsAndChecks();
    testWhenSettingsExceeded();
  }

  @Test(timeout = 30000)
  public void testMaxParallelAppsExceedsUserSetting() throws Exception {
    conf.setInt("yarn.scheduler.capacity.user.testuser.max-parallel-apps", 2);
    executeCommonStepsAndChecks();
    testWhenSettingsExceeded();
  }

  @Test(timeout = 30000)
  public void testMaxParallelAppsExceedsDefaultUserSetting() throws Exception {
    conf.setInt("yarn.scheduler.capacity.user.max-parallel-apps", 2);
    executeCommonStepsAndChecks();
    testWhenSettingsExceeded();
  }

  @Test(timeout = 30000)
  public void testMaxParallelAppsWhenReloadingConfig() throws Exception {
    conf.setInt("yarn.scheduler.capacity.root.default.max-parallel-apps", 2);

    executeCommonStepsAndChecks();

    RMContext rmContext = rm.getRMContext();
    // Disable parallel apps setting + max out AM percent
    conf.unset("yarn.scheduler.capacity.root.default.max-parallel-apps");
    conf.setFloat(PREFIX + "maximum-am-resource-percent", 1.0f);
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    cs.reinitialize(conf, rmContext);

    // Both app #3 and app #4 should transition to RUNNABLE
    launchAMandWaitForRunning(app3, attempt3, nm1);
    launchAMandWaitForRunning(app4, attempt4, nm1);
    verifyRunningAndAcceptedApps(4, 0);
  }

  @Test(timeout = 30000)
  public void testMaxAppsReachedWithNonRunnableApps() throws Exception {
    conf.setInt("yarn.scheduler.capacity.root.default.max-parallel-apps", 2);
    conf.setInt("yarn.scheduler.capacity.root.default.maximum-applications", 4);
    executeCommonStepsAndChecks();

    RMApp app5 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(512, rm)
        .withAppName("app5")
        .withUser("testuser")
        .withQueue("default")
        .withWaitForAppAcceptedState(false)
        .build());

    rm.waitForState(app5.getApplicationId(), RMAppState.FAILED);
  }

  private void executeCommonStepsAndChecks() throws Exception {
    rm = new MockRM(conf);
    rm.start();

    nm1 = rm.registerNode("h1:1234", 4096, 8);
    rm.registerNode("h2:1234", 4096, 8);
    rm.registerNode("h3:1234", 4096, 8);

    rm.drainEvents();

    app1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(512, rm)
        .withAppName("app1")
        .withUser("testuser")
        .withQueue("default")
        .build());

    am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    app2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(512, rm)
        .withAppName("app2")
        .withUser("testuser")
        .withQueue("default")
        .build());
    am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);

    app3 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(512, rm)
        .withAppName("app3")
        .withUser("testuser")
        .withQueue("default")
        .build());
    attempt3 = MockRM.waitForAttemptScheduled(app3, rm);

    app4 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(512, rm)
        .withAppName("app4")
        .withUser("testuser")
        .withQueue("default")
        .build());
    attempt4 = MockRM.waitForAttemptScheduled(app4, rm);

    // Check that app attempt #3 and #4 are non-runnable
    rootQueue = getRootQueue();
    defaultQueue = getDefaultQueue();
    Set<ApplicationAttemptId> nonRunnables =
        Sets.newHashSet(
            attempt3.getAppAttemptId(),
            attempt4.getAppAttemptId());
    verifyRunnableAppsInParent(rootQueue, 2);
    verifyRunnableAppsInLeaf(defaultQueue, 2, nonRunnables);
    verifyRunningAndAcceptedApps(2, 2);
  }

  private void testWhenSettingsExceeded() throws Exception {
    // Stop app #1
    unregisterAMandWaitForFinish(app1, am1, nm1);

    // Launch app #3
    launchAMandWaitForRunning(app3, attempt3, nm1);

    // Check that attempt #4 is still non-runnable
    verifyRunnableAppsInParent(rootQueue, 2);
    verifyRunnableAppsInLeaf(defaultQueue, 2,
        Collections.singleton(attempt4.getAppAttemptId()));
    verifyRunningAndAcceptedApps(2, 1);

    // Stop app #2
    unregisterAMandWaitForFinish(app2, am2, nm1);

    // Launch app #4
    launchAMandWaitForRunning(app4, attempt4, nm1);
    verifyRunnableAppsInParent(rootQueue, 2);
    verifyRunnableAppsInLeaf(defaultQueue, 2,
        Collections.emptySet());
    verifyRunningAndAcceptedApps(2, 0);
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  private LeafQueue getDefaultQueue() {
    CSQueue defaultQueue =
        ((CapacityScheduler) rm.getResourceScheduler()).getQueue("default");

    return (LeafQueue) defaultQueue;
  }

  private ParentQueue getRootQueue() {
    CSQueue root =
        ((CapacityScheduler) rm.getResourceScheduler()).getQueue("root");

    return (ParentQueue) root;
  }

  private void verifyRunnableAppsInParent(ParentQueue queue,
      int expectedRunnable) {
    assertEquals("Num of runnable apps", expectedRunnable,
        queue.getNumRunnableApps());
  }

  private void verifyRunnableAppsInLeaf(LeafQueue queue, int expectedRunnable,
      Set<ApplicationAttemptId> nonRunnableIds) {
    assertEquals("Num of runnable apps", expectedRunnable,
        queue.getNumRunnableApps());

    queue.getCopyOfNonRunnableAppSchedulables()
        .stream()
        .map(fca -> fca.getApplicationAttemptId())
        .forEach(id -> assertTrue(id + " not found as non-runnable",
          nonRunnableIds.contains(id)));
  }

  private void verifyRunningAndAcceptedApps(int expectedRunning,
      int expectedAccepted) throws YarnException {
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();

    GetApplicationsResponse resp =
        rm.getClientRMService().getApplications(request);

    List<ApplicationReport> apps = resp.getApplicationList();

    long runningCount = apps
        .stream()
        .filter(report ->
          report.getYarnApplicationState() == YarnApplicationState.RUNNING)
        .count();

    long acceptedCount = apps
        .stream()
        .filter(report ->
          report.getYarnApplicationState() == YarnApplicationState.ACCEPTED)
        .count();

    assertEquals("Running apps count", expectedRunning, runningCount);
    assertEquals("Accepted apps count", expectedAccepted, acceptedCount);
  }

  private void unregisterAMandWaitForFinish(RMApp app, MockAM am, MockNM nm)
      throws Exception {
    am.unregisterAppAttempt();
    nm.nodeHeartbeat(app.getCurrentAppAttempt().getAppAttemptId(), 1,
        ContainerState.COMPLETE);
    rm.waitForState(app.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.FINISHED);
  }

  @SuppressWarnings("rawtypes")
  private MockAM launchAMandWaitForRunning(RMApp app, RMAppAttempt attempt,
      MockNM nm) throws Exception {
    nm.nodeHeartbeat(true);
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    rm.drainEvents();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);

    return am;
  }
}
