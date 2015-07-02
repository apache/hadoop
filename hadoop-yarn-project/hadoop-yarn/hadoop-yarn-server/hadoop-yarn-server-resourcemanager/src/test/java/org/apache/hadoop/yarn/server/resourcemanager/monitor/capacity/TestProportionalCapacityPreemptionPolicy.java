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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.MAX_IGNORED_OVER_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.MONITORING_INTERVAL;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.NATURAL_TERMINATION_FACTOR;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.OBSERVE_ONLY;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.TOTAL_PREEMPTION_PER_ROUND;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.WAIT_TIME_BEFORE_KILL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType.KILL_CONTAINER;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType.PREEMPT_CONTAINER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

public class TestProportionalCapacityPreemptionPolicy {

  static final long TS = 3141592653L;

  int appAlloc = 0;
  boolean setAMContainer = false;
  boolean setLabeledContainer = false;
  float setAMResourcePercent = 0.0f;
  Random rand = null;
  Clock mClock = null;
  Configuration conf = null;
  CapacityScheduler mCS = null;
  RMContext rmContext = null;
  RMNodeLabelsManager lm = null;
  CapacitySchedulerConfiguration schedConf = null;
  EventHandler<SchedulerEvent> mDisp = null;
  ResourceCalculator rc = new DefaultResourceCalculator();
  Resource clusterResources = null;
  final ApplicationAttemptId appA = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 0), 0);
  final ApplicationAttemptId appB = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 1), 0);
  final ApplicationAttemptId appC = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 2), 0);
  final ApplicationAttemptId appD = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 3), 0);
  final ApplicationAttemptId appE = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 4), 0);
  final ApplicationAttemptId appF = ApplicationAttemptId.newInstance(
      ApplicationId.newInstance(TS, 4), 0);
  final ArgumentCaptor<ContainerPreemptEvent> evtCaptor =
    ArgumentCaptor.forClass(ContainerPreemptEvent.class);

  public enum priority {
    AMCONTAINER(0), CONTAINER(1), LABELEDCONTAINER(2);
    int value;

    private priority(int value) {
      this.value = value;
    }

    public int getValue() {
      return this.value;
    }
  };  

  @Rule public TestName name = new TestName();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    conf = new Configuration(false);
    conf.setLong(WAIT_TIME_BEFORE_KILL, 10000);
    conf.setLong(MONITORING_INTERVAL, 3000);
    // report "ideal" preempt
    conf.setFloat(TOTAL_PREEMPTION_PER_ROUND, (float) 1.0);
    conf.setFloat(NATURAL_TERMINATION_FACTOR, (float) 1.0);
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        ProportionalCapacityPreemptionPolicy.class.getCanonicalName());
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    // FairScheduler doesn't support this test,
    // Set CapacityScheduler as the scheduler for this test.
    conf.set("yarn.resourcemanager.scheduler.class",
        CapacityScheduler.class.getName());

    mClock = mock(Clock.class);
    mCS = mock(CapacityScheduler.class);
    when(mCS.getResourceCalculator()).thenReturn(rc);
    lm = mock(RMNodeLabelsManager.class);
    schedConf = new CapacitySchedulerConfiguration();
    when(mCS.getConfiguration()).thenReturn(schedConf);
    rmContext = mock(RMContext.class);
    when(mCS.getRMContext()).thenReturn(rmContext);
    when(rmContext.getNodeLabelManager()).thenReturn(lm);
    mDisp = mock(EventHandler.class);
    Dispatcher disp = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(disp);
    when(disp.getEventHandler()).thenReturn(mDisp);
    rand = new Random();
    long seed = rand.nextLong();
    System.out.println(name.getMethodName() + " SEED: " + seed);
    rand.setSeed(seed);
    appAlloc = 0;
  }

  @Test
  public void testIgnore() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  0, 60, 40 },  // used
      {   0,  0,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // don't correct imbalances without demand
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testProportionalPreemption() {
    int[][] qData = new int[][]{
      //  /   A   B   C  D
      { 100, 10, 40, 20, 30 },  // abs
      { 100, 100, 100, 100, 100 },  // maxCap
      { 100, 30, 60, 10,  0 },  // used
      {  45, 20,  5, 20,  0 },  // pending
      {   0,  0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1,  0 },  // apps
      {  -1,  1,  1,  1,  1 },  // req granularity
      {   4,  0,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    verify(mDisp, times(16)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }
  
  @Test
  public void testMaxCap() {
    int[][] qData = new int[][]{
        //  /   A   B   C
        { 100, 40, 40, 20 },  // abs
        { 100, 100, 45, 100 },  // maxCap
        { 100, 55, 45,  0 },  // used
        {  20, 10, 10,  0 },  // pending
        {   0,  0,  0,  0 },  // reserved
        {   2,  1,  1,  0 },  // apps
        {  -1,  1,  1,  0 },  // req granularity
        {   3,  0,  0,  0 },  // subqueues
      };
      ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
      policy.editSchedule();
      // despite the imbalance, since B is at maxCap, do not correct
      verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  
  @Test
  public void testPreemptCycle() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  0, 60, 40 },  // used
      {  10, 10,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // ensure all pending rsrc from A get preempted from other queues
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }

  @Test
  public void testExpireKill() {
    final long killTime = 10000L;
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  0, 60, 40 },  // used
      {  10, 10,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setLong(WAIT_TIME_BEFORE_KILL, killTime);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);

    // ensure all pending rsrc from A get preempted from other queues
    when(mClock.getTime()).thenReturn(0L);
    policy.editSchedule();
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // requests reiterated
    when(mClock.getTime()).thenReturn(killTime / 2);
    policy.editSchedule();
    verify(mDisp, times(20)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // kill req sent
    when(mClock.getTime()).thenReturn(killTime + 1);
    policy.editSchedule();
    verify(mDisp, times(30)).handle(evtCaptor.capture());
    List<ContainerPreemptEvent> events = evtCaptor.getAllValues();
    for (ContainerPreemptEvent e : events.subList(20, 30)) {
      assertEquals(appC, e.getAppId());
      assertEquals(KILL_CONTAINER, e.getType());
    }
  }

  @Test
  public void testDeadzone() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 39, 43, 21 },  // used
      {  10, 10,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setFloat(MAX_IGNORED_OVER_CAPACITY, (float) 0.1);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // ignore 10% overcapacity to avoid jitter
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testPerQueueDisablePreemption() {
    int[][] qData = new int[][]{
        //  /    A    B    C
        { 100,  55,  25,  20 },  // abs
        { 100, 100, 100, 100 },  // maxCap
        { 100,   0,  54,  46 },  // used
        {  10,  10,   0,   0 },  // pending
        {   0,   0,   0,   0 },  // reserved
       //     appA appB appC
        {   3,   1,   1,   1 },  // apps
        {  -1,   1,   1,   1 },  // req granularity
        {   3,   0,   0,  0 },  // subqueues
      };

    schedConf.setPreemptionDisabled("root.queueB", true);

    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // Since queueB is not preemptable, get resources from queueC
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appB)));

    // Since queueB is preemptable, resources will be preempted
    // from both queueB and queueC. Test must be reset so that the mDisp
    // event handler will count only events from the following test and not the
    // previous one.
    setup();
    schedConf.setPreemptionDisabled("root.queueB", false);
    ProportionalCapacityPreemptionPolicy policy2 = buildPolicy(qData);

    policy2.editSchedule();

    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appB)));
    verify(mDisp, times(6)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }

  @Test
  public void testPerQueueDisablePreemptionHierarchical() {
    int[][] qData = new int[][] {
      //  /    A              D
      //            B    C         E    F
      { 200, 100,  50,  50, 100,  10,  90 },  // abs
      { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
      { 200, 110,  60,  50,  90,  90,   0 },  // used
      {  10,   0,   0,   0,  10,   0,  10 },  // pending
      {   0,   0,   0,   0,   0,   0,   0 },  // reserved
      //          appA appB      appC appD
      {   4,   2,   1,   1,   2,   1,   1 },  // apps
      {  -1,  -1,   1,   1,  -1,   1,   1 },  // req granularity
      {   2,   2,   0,   0,   2,   0,   0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from queueB (appA), not queueE (appC) despite 
    // queueE being far over its absolute capacity because queueA (queueB's
    // parent) is over capacity and queueD (queueE's parent) is not.
    ApplicationAttemptId expectedAttemptOnQueueB = 
        ApplicationAttemptId.newInstance(
            appA.getApplicationId(), appA.getAttemptId());
    assertTrue("appA should be running on queueB",
        mCS.getAppsInQueue("queueB").contains(expectedAttemptOnQueueB));
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appA)));

    // Need to call setup() again to reset mDisp
    setup();
    // Turn off preemption for queueB and it's children
    schedConf.setPreemptionDisabled("root.queueA.queueB", true);
    ProportionalCapacityPreemptionPolicy policy2 = buildPolicy(qData);
    policy2.editSchedule();
    ApplicationAttemptId expectedAttemptOnQueueC = 
        ApplicationAttemptId.newInstance(
            appB.getApplicationId(), appB.getAttemptId());
    ApplicationAttemptId expectedAttemptOnQueueE = 
        ApplicationAttemptId.newInstance(
            appC.getApplicationId(), appC.getAttemptId());
    // Now, all of queueB's (appA) over capacity is not preemptable, so neither
    // is queueA's. Verify that capacity is taken from queueE (appC).
    assertTrue("appB should be running on queueC",
        mCS.getAppsInQueue("queueC").contains(expectedAttemptOnQueueC));
    assertTrue("appC should be running on queueE",
        mCS.getAppsInQueue("queueE").contains(expectedAttemptOnQueueE));
    // Resources should have come from queueE (appC) and neither of queueA's
    // children.
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }

  @Test
  public void testPerQueueDisablePreemptionBroadHierarchical() {
    int[][] qData = new int[][] {
        //  /    A              D              G    
        //            B    C         E    F         H    I
        {1000, 350, 150, 200, 400, 200, 200, 250, 100, 150 },  // abs
        {1000,1000,1000,1000,1000,1000,1000,1000,1000,1000 },  // maxCap
        {1000, 400, 200, 200, 400, 250, 150, 200, 150,  50 },  // used
        {  50,   0,   0,   0,  50,   0,  50,   0,   0,   0 },  // pending
        {   0,   0,   0,   0,   0,   0,   0,   0,   0,   0 },  // reserved
        //          appA appB      appC appD      appE appF
        {   6,   2,   1,   1,   2,   1,   1,   2,   1,   1 },  // apps
        {  -1,  -1,   1,   1,  -1,   1,   1,  -1,   1,   1 },  // req granulrity
        {   3,   2,   0,   0,   2,   0,   0,   2,   0,   0 },  // subqueues
      };

    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // queueF(appD) wants resources, Verify that resources come from queueE(appC)
    // because it's a sibling and queueB(appA) because queueA is over capacity.
    verify(mDisp, times(28)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(22)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // Need to call setup() again to reset mDisp
    setup();
    // Turn off preemption for queueB(appA)
    schedConf.setPreemptionDisabled("root.queueA.queueB", true);
    ProportionalCapacityPreemptionPolicy policy2 = buildPolicy(qData);
    policy2.editSchedule();
    // Now that queueB(appA) is not preemptable, verify that resources come
    // from queueE(appC)
    verify(mDisp, times(50)).handle(argThat(new IsPreemptionRequestFor(appC)));
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));

    setup();
    // Turn off preemption for two of the 3 queues with over-capacity.
    schedConf.setPreemptionDisabled("root.queueD.queueE", true);
    schedConf.setPreemptionDisabled("root.queueA.queueB", true);
    ProportionalCapacityPreemptionPolicy policy3 = buildPolicy(qData);
    policy3.editSchedule();

    // Verify that the request was starved out even though queueH(appE) is
    // over capacity. This is because queueG (queueH's parent) is NOT
    // overcapacity.
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA))); // queueB
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appB))); // queueC
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appC))); // queueE
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appE))); // queueH
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appF))); // queueI
  }

  @Test
  public void testPerQueueDisablePreemptionInheritParent() {
    int[][] qData = new int[][] {
        //  /    A                   E          
        //            B    C    D         F    G    H
        {1000, 500, 200, 200, 100, 500, 200, 200, 100 },  // abs (guar)
        {1000,1000,1000,1000,1000,1000,1000,1000,1000 },  // maxCap
        {1000, 700,   0, 350, 350, 300,   0, 200, 100 },  // used 
        { 200,   0,   0,   0,   0, 200, 200,   0,   0 },  // pending
        {   0,   0,   0,   0,   0,   0,   0,   0,   0 },  // reserved
        //               appA appB      appC appD appE 
        {   5,   2,   0,   1,   1,   3,   1,   1,   1 },  // apps 
        {  -1,  -1,   1,   1,   1,  -1,   1,   1,   1 },  // req granulrity
        {   2,   3,   0,   0,   0,   3,   0,   0,   0 },  // subqueues
      };

    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // With all queues preemptable, resources should be taken from queueC(appA)
    // and queueD(appB). Resources taken more from queueD(appB) than
    // queueC(appA) because it's over its capacity by a larger percentage.
    verify(mDisp, times(16)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(182)).handle(argThat(new IsPreemptionRequestFor(appB)));

    // Turn off preemption for queueA and it's children. queueF(appC)'s request
    // should starve.
    setup(); // Call setup() to reset mDisp
    schedConf.setPreemptionDisabled("root.queueA", true);
    ProportionalCapacityPreemptionPolicy policy2 = buildPolicy(qData);
    policy2.editSchedule();
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA))); // queueC
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appB))); // queueD
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appD))); // queueG
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appE))); // queueH
  }

  @Test
  public void testPerQueuePreemptionNotAllUntouchable() {
    int[][] qData = new int[][] {
      //  /      A                       E
      //               B     C     D           F     G     H
      { 2000, 1000,  800,  100,  100, 1000,  500,  300,  200 },  // abs
      { 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000 },  // maxCap
      { 2000, 1300,  300,  800,  200,  700,  500,    0,  200 },  // used
      {  300,    0,    0,    0,    0,  300,    0,  300,    0 },  // pending
      {    0,    0,    0,    0,    0,    0,    0,    0,    0 },  // reserved
      //             appA  appB  appC        appD  appE  appF
      {    6,    3,    1,    1,    1,    3,    1,    1,    1 },  // apps
      {   -1,   -1,    1,    1,    1,   -1,    1,    1,    1 },  // req granularity
      {    2,    3,    0,    0,    0,    3,    0,    0,    0 },  // subqueues
    };
    schedConf.setPreemptionDisabled("root.queueA.queueC", true);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // Although queueC(appB) is way over capacity and is untouchable,
    // queueD(appC) is preemptable. Request should be filled from queueD(appC).
    verify(mDisp, times(100)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }

  @Test
  public void testPerQueueDisablePreemptionRootDisablesAll() {
    int[][] qData = new int[][] {
        //  /    A              D              G    
        //            B    C         E    F         H    I
        {1000, 500, 250, 250, 250, 100, 150, 250, 100, 150 },  // abs
        {1000,1000,1000,1000,1000,1000,1000,1000,1000,1000 },  // maxCap
        {1000,  20,   0,  20, 490, 240, 250, 490, 240, 250 },  // used
        { 200, 200, 200,   0,   0,   0,   0,   0,   0,   0 },  // pending
        {   0,   0,   0,   0,   0,   0,   0,   0,   0,   0 },  // reserved
        //          appA appB      appC appD      appE appF
        {   6,   2,   1,   1,   2,   1,   1,   2,   1,   1 },  // apps
        {  -1,  -1,   1,   1,  -1,   1,   1,  -1,   1,   1 },  // req granulrity
        {   3,   2,   0,   0,   2,   0,   0,   2,   0,   0 },  // subqueues
   };

    schedConf.setPreemptionDisabled("root", true);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // All queues should be non-preemptable, so request should starve.
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appB))); // queueC
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appC))); // queueE
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appD))); // queueB
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appE))); // queueH
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appF))); // queueI
  }

  @Test
  public void testPerQueueDisablePreemptionOverAbsMaxCapacity() {
    int[][] qData = new int[][] {
        //  /    A              D
        //            B    C         E    F
        {1000, 725, 360, 365, 275,  17, 258 },  // absCap
        {1000,1000,1000,1000, 550, 109,1000 },  // absMaxCap
        {1000, 741, 396, 345, 259, 110, 149 },  // used
        {  40,  20,   0,  20,  20,  20,   0 },  // pending
        {   0,   0,   0,   0,   0,   0,   0 },  // reserved
        //          appA appB     appC appD
        {   4,   2,   1,   1,   2,   1,   1 },  // apps
        {  -1,  -1,   1,   1,  -1,   1,   1 },  // req granulrity
        {   2,   2,   0,   0,   2,   0,   0 },  // subqueues
    };
    // QueueE inherits non-preemption from QueueD
    schedConf.setPreemptionDisabled("root.queueD", true);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // appC is running on QueueE. QueueE is over absMaxCap, but is not
    // preemptable. Therefore, appC resources should not be preempted.
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appC)));
  }

  @Test
  public void testOverCapacityImbalance() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 55, 45,  0 },  // used
      {  20, 10, 10,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   2,  1,  1,  0 },  // apps
      {  -1,  1,  1,  0 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // correct imbalance between over-capacity queues
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
  public void testNaturalTermination() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 55, 45,  0 },  // used
      {  20, 10, 10,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   2,  1,  1,  0 },  // apps
      {  -1,  1,  1,  0 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setFloat(NATURAL_TERMINATION_FACTOR, (float) 0.1);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // ignore 10% imbalance between over-capacity queues
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testObserveOnly() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100, 90, 10,  0 },  // used
      {  80, 10, 20, 50 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   2,  1,  1,  0 },  // apps
      {  -1,  1,  1,  0 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues
    };
    conf.setBoolean(OBSERVE_ONLY, true);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify even severe imbalance not affected
    verify(mDisp, never()).handle(isA(ContainerPreemptEvent.class));
  }

  @Test
  public void testHierarchical() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F
      { 200, 100, 50, 50, 100, 10, 90 },  // abs
      { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
      { 200, 110, 60, 50,  90, 90,  0 },  // used
      {  10,   0,  0,  0,  10,  0, 10 },  // pending
      {   0,   0,  0,  0,   0,  0,  0 },  // reserved
      {   4,   2,  1,  1,   2,  1,  1 },  // apps
      {  -1,  -1,  1,  1,  -1,  1,  1 },  // req granularity
      {   2,   2,  0,  0,   2,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not B1 despite B1 being far over
    // its absolute guaranteed capacity
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
  public void testZeroGuar() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F
        { 200, 100, 0, 99, 100, 10, 90 },  // abs
        { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
        { 170,  80, 60, 20,  90, 90,  0 },  // used
        {  10,   0,  0,  0,  10,  0, 10 },  // pending
        {   0,   0,  0,  0,   0,  0,  0 },  // reserved
        {   4,   2,  1,  1,   2,  1,  1 },  // apps
        {  -1,  -1,  1,  1,  -1,  1,  1 },  // req granularity
        {   2,   2,  0,  0,   2,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not B1 despite B1 being far over
    // its absolute guaranteed capacity
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));
  }
  
  @Test
  public void testZeroGuarOverCap() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F
         { 200, 100, 0, 99, 0, 100, 100 },  // abs
        { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
        { 170,  170, 60, 20, 90, 0,  0 },  // used
        {  85,   50,  30,  10,  10,  20, 20 },  // pending
        {   0,   0,  0,  0,   0,  0,  0 },  // reserved
        {   4,   3,  1,  1,   1,  1,  1 },  // apps
        {  -1,  -1,  1,  1,  1,  -1,  1 },  // req granularity
        {   2,   3,  0,  0,   0,  1,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // we verify both that C has priority on B and D (has it has >0 guarantees)
    // and that B and D are force to share their over capacity fairly (as they
    // are both zero-guarantees) hence D sees some of its containers preempted
    verify(mDisp, times(14)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }
  
  
  
  @Test
  public void testHierarchicalLarge() {
    int[][] qData = new int[][] {
      //  /    A              D              G        
      //            B    C         E    F         H    I
      { 400, 200,  60, 140, 100,  70,  30, 100,  10,  90 },  // abs
      { 400, 400, 400, 400, 400, 400, 400, 400, 400, 400 },  // maxCap
      { 400, 210,  70, 140, 100,  50,  50,  90,  90,   0 },  // used
      {  15,   0,   0,   0,   0,   0,   0,   0,   0,  15 },  // pending
      {   0,   0,   0,   0,   0,   0,   0,   0,   0,   0 },  // reserved
      //          appA appB      appC appD      appE appF
      {   6,   2,   1,   1,   2,   1,   1,   2,   1,   1 },  // apps
      {  -1,  -1,   1,   1,  -1,   1,   1,  -1,   1,   1 },  // req granularity
      {   3,   2,   0,   0,   2,   0,   0,   2,   0,   0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not H1 despite H1 being far over
    // its absolute guaranteed capacity

    // XXX note: compensating for rounding error in Resources.multiplyTo
    // which is likely triggered since we use small numbers for readability
    verify(mDisp, times(7)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appE)));
  }

  @Test
  public void testContainerOrdering(){

    List<RMContainer> containers = new ArrayList<RMContainer>();

    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(TS, 10), 0);

    // create a set of containers
    RMContainer rm1 = mockContainer(appAttId, 5, mock(Resource.class), 3);
    RMContainer rm2 = mockContainer(appAttId, 3, mock(Resource.class), 3);
    RMContainer rm3 = mockContainer(appAttId, 2, mock(Resource.class), 2);
    RMContainer rm4 = mockContainer(appAttId, 1, mock(Resource.class), 2);
    RMContainer rm5 = mockContainer(appAttId, 4, mock(Resource.class), 1);

    // insert them in non-sorted order
    containers.add(rm3);
    containers.add(rm2);
    containers.add(rm1);
    containers.add(rm5);
    containers.add(rm4);

    // sort them
    ProportionalCapacityPreemptionPolicy.sortContainers(containers);

    // verify the "priority"-first, "reverse container-id"-second
    // ordering is enforced correctly
    assert containers.get(0).equals(rm1);
    assert containers.get(1).equals(rm2);
    assert containers.get(2).equals(rm3);
    assert containers.get(3).equals(rm4);
    assert containers.get(4).equals(rm5);

  }
  
  @Test
  public void testPolicyInitializeAfterSchedulerInitialized() {
    @SuppressWarnings("resource")
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    
    // ProportionalCapacityPreemptionPolicy should be initialized after
    // CapacityScheduler initialized. We will 
    // 1) find SchedulingMonitor from RMActiveService's service list, 
    // 2) check if ResourceCalculator in policy is null or not. 
    // If it's not null, we can come to a conclusion that policy initialized
    // after scheduler got initialized
    for (Service service : rm.getRMActiveService().getServices()) {
      if (service instanceof SchedulingMonitor) {
        ProportionalCapacityPreemptionPolicy policy =
            (ProportionalCapacityPreemptionPolicy) ((SchedulingMonitor) service)
                .getSchedulingEditPolicy();
        assertNotNull(policy.getResourceCalculator());
        return;
      }
    }
    
    fail("Failed to find SchedulingMonitor service, please check what happened");
  }
  
  @Test
  public void testSkipAMContainer() {
    int[][] qData = new int[][] {
        //  /   A   B
        { 100, 50, 50 }, // abs
        { 100, 100, 100 }, // maxcap
        { 100, 100, 0 }, // used
        { 70, 20, 50 }, // pending
        { 0, 0, 0 }, // reserved
        { 5, 4, 1 }, // apps
        { -1, 1, 1 }, // req granularity
        { 2, 0, 0 }, // subqueues
    };
    setAMContainer = true;
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    
    // By skipping AM Container, all other 24 containers of appD will be
    // preempted
    verify(mDisp, times(24)).handle(argThat(new IsPreemptionRequestFor(appD)));

    // By skipping AM Container, all other 24 containers of appC will be
    // preempted
    verify(mDisp, times(24)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // Since AM containers of appC and appD are saved, 2 containers from appB
    // has to be preempted.
    verify(mDisp, times(2)).handle(argThat(new IsPreemptionRequestFor(appB)));
    setAMContainer = false;
  }

  @Test
  public void testIdealAllocationForLabels() {
    int[][] qData = new int[][] {
    // / A B
        { 80, 40, 40 }, // abs
        { 80, 80, 80 }, // maxcap
        { 80, 80, 0 }, // used
        { 70, 20, 50 }, // pending
        { 0, 0, 0 }, // reserved
        { 5, 4, 1 }, // apps
        { -1, 1, 1 }, // req granularity
        { 2, 0, 0 }, // subqueues
    };
    setAMContainer = true;
    setLabeledContainer = true;
    Map<NodeId, Set<String>> labels = new HashMap<NodeId, Set<String>>();
    NodeId node = NodeId.newInstance("node1", 0);
    Set<String> labelSet = new HashSet<String>();
    labelSet.add("x");
    labels.put(node, labelSet);
    when(lm.getNodeLabels()).thenReturn(labels);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    // Subtracting Label X resources from cluster resources
    when(lm.getResourceByLabel(anyString(), any(Resource.class))).thenReturn(
        Resources.clone(Resource.newInstance(80, 0)));
    clusterResources.setMemory(100);
    policy.editSchedule();

    // By skipping AM Container and Labeled container, all other 18 containers
    // of appD will be
    // preempted
    verify(mDisp, times(19)).handle(argThat(new IsPreemptionRequestFor(appD)));

    // By skipping AM Container and Labeled container, all other 18 containers
    // of appC will be
    // preempted
    verify(mDisp, times(19)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // rest 4 containers from appB will be preempted
    verify(mDisp, times(2)).handle(argThat(new IsPreemptionRequestFor(appB)));
    setAMContainer = false;
    setLabeledContainer = false;
  }

  @Test
  public void testPreemptSkippedAMContainers() {
    int[][] qData = new int[][] {
        //  /   A   B
        { 100, 10, 90 }, // abs
        { 100, 100, 100 }, // maxcap
        { 100, 100, 0 }, // used
        { 70, 20, 90 }, // pending
        { 0, 0, 0 }, // reserved
        { 5, 4, 1 }, // apps
        { -1, 5, 5 }, // req granularity
        { 2, 0, 0 }, // subqueues
    };
    setAMContainer = true;
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    
    // All 5 containers of appD will be preempted including AM container.
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appD)));

    // All 5 containers of appC will be preempted including AM container.
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appC)));
    
    // By skipping AM Container, all other 4 containers of appB will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appB)));

    // By skipping AM Container, all other 4 containers of appA will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appA)));
    setAMContainer = false;
  }
  
  @Test
  public void testAMResourcePercentForSkippedAMContainers() {
    int[][] qData = new int[][] {
        //  /   A   B
        { 100, 10, 90 }, // abs
        { 100, 100, 100 }, // maxcap
        { 100, 100, 0 }, // used
        { 70, 20, 90 }, // pending
        { 0, 0, 0 }, // reserved
        { 5, 4, 1 }, // apps
        { -1, 5, 5 }, // req granularity
        { 2, 0, 0 }, // subqueues
    };
    setAMContainer = true;
    setAMResourcePercent = 0.5f;
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    
    // AMResoucePercent is 50% of cluster and maxAMCapacity will be 5Gb.
    // Total used AM container size is 20GB, hence 2 AM container has
    // to be preempted as Queue Capacity is 10Gb.
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appD)));

    // Including AM Container, all other 4 containers of appC will be
    // preempted
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appC)));
    
    // By skipping AM Container, all other 4 containers of appB will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appB)));

    // By skipping AM Container, all other 4 containers of appA will be
    // preempted
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appA)));
    setAMContainer = false;
  }

  static class IsPreemptionRequestFor
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
    private final SchedulerEventType type;
    IsPreemptionRequestFor(ApplicationAttemptId appAttId) {
      this(appAttId, PREEMPT_CONTAINER);
    }
    IsPreemptionRequestFor(ApplicationAttemptId appAttId,
        SchedulerEventType type) {
      this.appAttId = appAttId;
      this.type = type;
    }
    @Override
    public boolean matches(Object o) {
      return appAttId.equals(((ContainerPreemptEvent)o).getAppId())
          && type.equals(((ContainerPreemptEvent)o).getType());
    }
    @Override
    public String toString() {
      return appAttId.toString();
    }
  }

  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData) {
    ProportionalCapacityPreemptionPolicy policy =
      new ProportionalCapacityPreemptionPolicy(conf, rmContext, mCS, mClock);
    ParentQueue mRoot = buildMockRootQueue(rand, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);

    clusterResources =
      Resource.newInstance(leafAbsCapacities(qData[0], qData[7]), 0);
    when(mCS.getClusterResource()).thenReturn(clusterResources);
    return policy;
  }

  ParentQueue buildMockRootQueue(Random r, int[]... queueData) {
    int[] abs      = queueData[0];
    int[] maxCap   = queueData[1];
    int[] used     = queueData[2];
    int[] pending  = queueData[3];
    int[] reserved = queueData[4];
    int[] apps     = queueData[5];
    int[] gran     = queueData[6];
    int[] queues   = queueData[7];

    return mockNested(abs, maxCap, used, pending,  reserved, apps, gran, queues);
  }

  ParentQueue mockNested(int[] abs, int[] maxCap, int[] used,
      int[] pending, int[] reserved, int[] apps, int[] gran, int[] queues) {
    float tot = leafAbsCapacities(abs, queues);
    Deque<ParentQueue> pqs = new LinkedList<ParentQueue>();
    ParentQueue root = mockParentQueue(null, queues[0], pqs);
    when(root.getQueueName()).thenReturn("/");
    when(root.getAbsoluteUsedCapacity()).thenReturn(used[0] / tot);
    when(root.getAbsoluteCapacity()).thenReturn(abs[0] / tot);
    when(root.getAbsoluteMaximumCapacity()).thenReturn(maxCap[0] / tot);
    when(root.getQueuePath()).thenReturn("root");
    boolean preemptionDisabled = mockPreemptionStatus("root");
    when(root.getPreemptionDisabled()).thenReturn(preemptionDisabled);

    for (int i = 1; i < queues.length; ++i) {
      final CSQueue q;
      final ParentQueue p = pqs.removeLast();
      final String queueName = "queue" + ((char)('A' + i - 1));
      if (queues[i] > 0) {
        q = mockParentQueue(p, queues[i], pqs);
      } else {
        q = mockLeafQueue(p, tot, i, abs, used, pending, reserved, apps, gran);
      }
      when(q.getParent()).thenReturn(p);
      when(q.getQueueName()).thenReturn(queueName);
      when(q.getAbsoluteUsedCapacity()).thenReturn(used[i] / tot);
      when(q.getAbsoluteCapacity()).thenReturn(abs[i] / tot);
      when(q.getAbsoluteMaximumCapacity()).thenReturn(maxCap[i] / tot);
      String parentPathName = p.getQueuePath();
      parentPathName = (parentPathName == null) ? "root" : parentPathName;
      String queuePathName = (parentPathName+"."+queueName).replace("/","root");
      when(q.getQueuePath()).thenReturn(queuePathName);
      preemptionDisabled = mockPreemptionStatus(queuePathName);
      when(q.getPreemptionDisabled()).thenReturn(preemptionDisabled);
    }
    assert 0 == pqs.size();
    return root;
  }

  // Determine if any of the elements in the queupath have preemption disabled.
  // Also must handle the case where preemption disabled property is explicitly
  // set to something other than the default. Assumes system-wide preemption
  // property is true.
  private boolean mockPreemptionStatus(String queuePathName) {
    boolean preemptionDisabled = false;
    StringTokenizer tokenizer = new StringTokenizer(queuePathName, ".");
    String qName = "";
    while(tokenizer.hasMoreTokens()) {
      qName += tokenizer.nextToken();
      preemptionDisabled = schedConf.getPreemptionDisabled(qName, preemptionDisabled);
      qName += ".";
    }
    return preemptionDisabled;
  }

  ParentQueue mockParentQueue(ParentQueue p, int subqueues,
      Deque<ParentQueue> pqs) {
    ParentQueue pq = mock(ParentQueue.class);
    List<CSQueue> cqs = new ArrayList<CSQueue>();
    when(pq.getChildQueues()).thenReturn(cqs);
    for (int i = 0; i < subqueues; ++i) {
      pqs.add(pq);
    }
    if (p != null) {
      p.getChildQueues().add(pq);
    }
    return pq;
  }

  LeafQueue mockLeafQueue(ParentQueue p, float tot, int i, int[] abs, 
      int[] used, int[] pending, int[] reserved, int[] apps, int[] gran) {
    LeafQueue lq = mock(LeafQueue.class);
    List<ApplicationAttemptId> appAttemptIdList = 
        new ArrayList<ApplicationAttemptId>();
    when(lq.getTotalResourcePending()).thenReturn(
        Resource.newInstance(pending[i], 0));
    // consider moving where CapacityScheduler::comparator accessible
    NavigableSet<FiCaSchedulerApp> qApps = new TreeSet<FiCaSchedulerApp>(
      new Comparator<FiCaSchedulerApp>() {
        @Override
        public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
          return a1.getApplicationAttemptId()
              .compareTo(a2.getApplicationAttemptId());
        }
      });
    // applications are added in global L->R order in queues
    if (apps[i] != 0) {
      int aUsed    = used[i] / apps[i];
      int aPending = pending[i] / apps[i];
      int aReserve = reserved[i] / apps[i];
      for (int a = 0; a < apps[i]; ++a) {
        FiCaSchedulerApp mockFiCaApp =
            mockApp(i, appAlloc, aUsed, aPending, aReserve, gran[i]);
        qApps.add(mockFiCaApp);
        ++appAlloc;
        appAttemptIdList.add(mockFiCaApp.getApplicationAttemptId());
      }
      when(mCS.getAppsInQueue("queue" + (char)('A' + i - 1)))
              .thenReturn(appAttemptIdList);
    }
    when(lq.getApplications()).thenReturn(qApps);
    if(setAMResourcePercent != 0.0f){
      when(lq.getMaxAMResourcePerQueuePercent()).thenReturn(setAMResourcePercent);
    }
    p.getChildQueues().add(lq);
    return lq;
  }

  FiCaSchedulerApp mockApp(int qid, int id, int used, int pending, int reserved,
      int gran) {
    FiCaSchedulerApp app = mock(FiCaSchedulerApp.class);

    ApplicationId appId = ApplicationId.newInstance(TS, id);
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(appId, 0);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.getApplicationAttemptId()).thenReturn(appAttId);

    int cAlloc = 0;
    Resource unit = Resource.newInstance(gran, 0);
    List<RMContainer> cReserved = new ArrayList<RMContainer>();
    for (int i = 0; i < reserved; i += gran) {
      cReserved.add(mockContainer(appAttId, cAlloc, unit, priority.CONTAINER
          .getValue()));
      ++cAlloc;
    }
    when(app.getReservedContainers()).thenReturn(cReserved);

    List<RMContainer> cLive = new ArrayList<RMContainer>();
    for (int i = 0; i < used; i += gran) {
      if(setAMContainer && i == 0){
        cLive.add(mockContainer(appAttId, cAlloc, unit, priority.AMCONTAINER
            .getValue()));
      }else if(setLabeledContainer && i ==1){
        cLive.add(mockContainer(appAttId, cAlloc, unit,
            priority.LABELEDCONTAINER.getValue()));
        ++used;
      }
      else{
        cLive.add(mockContainer(appAttId, cAlloc, unit, priority.CONTAINER
            .getValue()));
      }
      ++cAlloc;
    }
    when(app.getLiveContainers()).thenReturn(cLive);
    return app;
  }

  RMContainer mockContainer(ApplicationAttemptId appAttId, int id,
      Resource r, int cpriority) {
    ContainerId cId = ContainerId.newContainerId(appAttId, id);
    Container c = mock(Container.class);
    when(c.getResource()).thenReturn(r);
    when(c.getPriority()).thenReturn(Priority.create(cpriority));
    RMContainer mC = mock(RMContainer.class);
    when(mC.getContainerId()).thenReturn(cId);
    when(mC.getContainer()).thenReturn(c);
    when(mC.getApplicationAttemptId()).thenReturn(appAttId);
    if (priority.AMCONTAINER.getValue() == cpriority) {
      when(mC.isAMContainer()).thenReturn(true);
    }
    if (priority.LABELEDCONTAINER.getValue() == cpriority) {
      when(mC.getAllocatedNode()).thenReturn(NodeId.newInstance("node1", 0));
    }
    return mC;
  }

  static int leafAbsCapacities(int[] abs, int[] subqueues) {
    int ret = 0;
    for (int i = 0; i < abs.length; ++i) {
      if (0 == subqueues[i]) {
        ret += abs[i];
      }
    }
    return ret;
  }

  void printString(CSQueue nq, String indent) {
    if (nq instanceof ParentQueue) {
      System.out.println(indent + nq.getQueueName()
          + " cur:" + nq.getAbsoluteUsedCapacity()
          + " guar:" + nq.getAbsoluteCapacity()
          );
      for (CSQueue q : ((ParentQueue)nq).getChildQueues()) {
        printString(q, indent + "  ");
      }
    } else {
      System.out.println(indent + nq.getQueueName()
          + " pen:" + ((LeafQueue) nq).getTotalResourcePending()
          + " cur:" + nq.getAbsoluteUsedCapacity()
          + " guar:" + nq.getAbsoluteCapacity()
          );
      for (FiCaSchedulerApp a : ((LeafQueue)nq).getApplications()) {
        System.out.println(indent + "  " + a.getApplicationId());
      }
    }
  }

}
