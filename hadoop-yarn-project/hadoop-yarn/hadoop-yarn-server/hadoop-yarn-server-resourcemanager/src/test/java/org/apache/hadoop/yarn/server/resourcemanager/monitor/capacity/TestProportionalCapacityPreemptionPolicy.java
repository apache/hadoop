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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeSet;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType.MARK_CONTAINER_FOR_PREEMPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestProportionalCapacityPreemptionPolicy {

  static final long TS = 3141592653L;

  int appAlloc = 0;
  boolean setAMContainer = false;
  boolean setLabeledContainer = false;
  float setAMResourcePercent = 0.0f;
  Random rand = null;
  Clock mClock = null;
  CapacitySchedulerConfiguration conf = null;
  CapacityScheduler mCS = null;
  RMContext rmContext = null;
  RMNodeLabelsManager lm = null;
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

    priority(int value) {
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
    conf = new CapacitySchedulerConfiguration(new Configuration(false));
    conf.setLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL, 10000);
    conf.setLong(CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        3000);
    // report "ideal" preempt
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        1.0f);
    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        1.0f);
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
    try {
      when(lm.isExclusiveNodeLabel(anyString())).thenReturn(true);
    } catch (IOException e) {
      // do nothing
    }
    when(mCS.getConfiguration()).thenReturn(conf);
    rmContext = mock(RMContext.class);
    when(mCS.getRMContext()).thenReturn(rmContext);
    when(mCS.getPreemptionManager()).thenReturn(new PreemptionManager());
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
    conf.setLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
        killTime);
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);

    // ensure all pending rsrc from A get preempted from other queues
    when(mClock.getTime()).thenReturn(0L);
    policy.editSchedule();
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // requests reiterated
    when(mClock.getTime()).thenReturn(killTime / 2);
    policy.editSchedule();
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));

    // kill req sent
    when(mClock.getTime()).thenReturn(killTime + 1);
    policy.editSchedule();
    verify(mDisp, times(20)).handle(evtCaptor.capture());
    List<ContainerPreemptEvent> events = evtCaptor.getAllValues();
    for (ContainerPreemptEvent e : events.subList(20, 20)) {
      assertEquals(appC, e.getAppId());
      assertEquals(MARK_CONTAINER_FOR_KILLABLE, e.getType());
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
    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_MAX_IGNORED_OVER_CAPACITY,
        (float) 0.1);
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

    conf.setPreemptionDisabled("root.queueB", true);

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
    conf.setPreemptionDisabled("root.queueB", false);
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
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appA)));

    // Need to call setup() again to reset mDisp
    setup();
    // Turn off preemption for queueB and it's children
    conf.setPreemptionDisabled("root.queueA.queueB", true);
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
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));
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
    conf.setPreemptionDisabled("root.queueA.queueB", true);
    ProportionalCapacityPreemptionPolicy policy2 = buildPolicy(qData);
    policy2.editSchedule();
    // Now that queueB(appA) is not preemptable, verify that resources come
    // from queueE(appC)
    verify(mDisp, times(50)).handle(argThat(new IsPreemptionRequestFor(appC)));
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));

    setup();
    // Turn off preemption for two of the 3 queues with over-capacity.
    conf.setPreemptionDisabled("root.queueD.queueE", true);
    conf.setPreemptionDisabled("root.queueA.queueB", true);
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
    verify(mDisp, times(17)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(183)).handle(argThat(new IsPreemptionRequestFor(appB)));

    // Turn off preemption for queueA and it's children. queueF(appC)'s request
    // should starve.
    setup(); // Call setup() to reset mDisp
    conf.setPreemptionDisabled("root.queueA", true);
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
    conf.setPreemptionDisabled("root.queueA.queueC", true);
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

    conf.setPreemptionDisabled("root", true);
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
    conf.setPreemptionDisabled("root.queueD", true);
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
    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        (float) 0.1);

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
    conf.setBoolean(CapacitySchedulerConfiguration.PREEMPTION_OBSERVE_ONLY,
        true);
    when(mCS.getConfiguration()).thenReturn(
        new CapacitySchedulerConfiguration(conf));
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
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
  public void testHierarchicalWithReserved() {
    int[][] qData = new int[][] {
        //  /    A   B   C    D   E   F
        { 200, 100, 50, 50, 100, 10, 90 },  // abs
        { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
        { 200, 110, 60, 50,  90, 90,  0 },  // used
        {  10,   0,  0,  0,  10,  0, 10 },  // pending
        {  40,  25,  15, 10, 15,  15,  0 },  // reserved
        {   4,   2,  1,  1,   2,  1,  1 },  // apps
        {  -1,  -1,  1,  1,  -1,  1,  1 },  // req granularity
        {   2,   2,  0,  0,   2,  0,  0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not B1 despite B1 being far over
    // its absolute guaranteed capacity
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appA)));
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
    verify(mDisp, times(15)).handle(argThat(new IsPreemptionRequestFor(appC)));
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
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(6)).handle(argThat(new IsPreemptionRequestFor(appE)));
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
    FifoCandidatesSelector.sortContainers(containers);

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

  @Test
  public void testPreemptionWithVCoreResource() {
    int[][] qData = new int[][]{
        // / A B
        {100, 100, 100}, // maxcap
        {5, 1, 1}, // apps
        {2, 0, 0}, // subqueues
    };

    // Resources can be set like memory:vcores
    String[][] resData = new String[][]{
        // / A B
        {"100:100", "50:50", "50:50"}, // abs
        {"10:100", "10:100", "0"}, // used
        {"70:20", "70:20", "10:100"}, // pending
        {"0", "0", "0"}, // reserved
        {"-1", "1:10", "1:10"}, // req granularity
    };

    // Passing last param as TRUE to use DominantResourceCalculator
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData, resData,
        true);
    policy.editSchedule();

    // 5 containers will be preempted here
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
  public void testHierarchicalLarge3Levels() {
    int[][] qData = new int[][] {
      //  /    A                      F               I
      //            B    C                  G    H          J    K
      //                    D    E
      { 400, 200,  60, 140, 100, 40,  100,  70,  30, 100,  10,  90 },  // abs
      { 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400 },  // maxCap
      { 400, 210,  60, 150, 100, 50,  100,  50,  50,  90,  10,   80 },  // used
      {  10,   0,   0,   0,  0,   0,   0,   0,   0,   0,   0,  10 },  // pending
      {   0,   0,   0,   0,  0,   0,   0,   0,   0,   0,   0,   0 },  // reserved
      //          appA     appB appC   appD appE      appF appG
      {   7,   3,   1,   2,   1,   1,  2,   1,   1,   2,   1,   1 },  // apps
      {  -1,  -1,   1,   -1,  1,   1,  -1,   1,   1,  -1,   1,   1 },  // req granularity
      {   3,   2,   0,   2,   0,   0,   2,   0,   0,   2,   0,   0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // XXX note: compensating for rounding error in Resources.multiplyTo
    // which is likely triggered since we use small numbers for readability
    //run with Logger.getRootLogger().setLevel(Level.DEBUG);
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appC)));
    assertEquals(10, policy.getQueuePartitions().get("queueE").get("").preemptableExtra.getMemorySize());
    //2nd level child(E) preempts 10, but parent A has only 9 extra
    //check the parent can prempt only the extra from > 2 level child
    TempQueuePerPartition tempQueueAPartition = policy.getQueuePartitions().get(
        "queueA").get("");
    assertEquals(0, tempQueueAPartition.untouchableExtra.getMemorySize());
    long extraForQueueA =
        tempQueueAPartition.getUsed().getMemorySize() - tempQueueAPartition
            .getGuaranteed().getMemorySize();
    assertEquals(extraForQueueA,
        tempQueueAPartition.preemptableExtra.getMemorySize());
  }

  @Test
  public void testHierarchicalLarge3LevelsWithReserved() {
    int[][] qData = new int[][] {
        //  /    A                      F               I
        //            B    C                  G    H          J    K
        //                    D    E
        { 400, 200,  60, 140, 100, 40,  100,  70,  30, 100,  10,  90 },  // abs
        { 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400 },  // maxCap
        { 400, 210,  60, 150, 100, 50,  100,  50,  50,  90,  10,   80 },  // used
        {  10,   0,   0,   0,  0,   0,   0,   0,   0,   0,   0,  10 },  // pending
        {  50,  30,  20,   10, 5,   5,   0,   0,   0,  10,  10,   0 },  // reserved
        //          appA     appB appC   appD appE      appF appG
        {   7,   3,   1,   2,   1,   1,  2,   1,   1,   2,   1,   1 },  // apps
        {  -1,  -1,   1,   -1,  1,   1,  -1,   1,   1,  -1,   1,   1 },  // req granularity
        {   3,   2,   0,   2,   0,   0,   2,   0,   0,   2,   0,   0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();

    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));
    assertEquals(10, policy.getQueuePartitions().get("queueE")
        .get("").preemptableExtra.getMemorySize());
    //2nd level child(E) preempts 10, but parent A has only 9 extra
    //check the parent can prempt only the extra from > 2 level child
    TempQueuePerPartition tempQueueAPartition = policy.getQueuePartitions().get(
        "queueA").get("");
    assertEquals(0, tempQueueAPartition.untouchableExtra.getMemorySize());
    long extraForQueueA =
        tempQueueAPartition.getUsed().getMemorySize() - tempQueueAPartition
            .getGuaranteed().getMemorySize();
    assertEquals(extraForQueueA,
        tempQueueAPartition.preemptableExtra.getMemorySize());
  }

  static class IsPreemptionRequestFor
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
    private final SchedulerEventType type;
    IsPreemptionRequestFor(ApplicationAttemptId appAttId) {
      this(appAttId, MARK_CONTAINER_FOR_PREEMPTION);
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
    ProportionalCapacityPreemptionPolicy policy = new ProportionalCapacityPreemptionPolicy(
        rmContext, mCS, mClock);
    clusterResources = Resource.newInstance(
        leafAbsCapacities(qData[0], qData[7]), 0);
    ParentQueue mRoot = buildMockRootQueue(rand, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);

    setResourceAndNodeDetails();
    return policy;
  }

  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData,
      String[][] resData, boolean useDominantResourceCalculator) {
    if (useDominantResourceCalculator) {
      when(mCS.getResourceCalculator()).thenReturn(
          new DominantResourceCalculator());
    }
    ProportionalCapacityPreemptionPolicy policy =
        new ProportionalCapacityPreemptionPolicy(rmContext, mCS, mClock);
    clusterResources = leafAbsCapacities(parseResourceDetails(resData[0]),
        qData[2]);
    ParentQueue mRoot = buildMockRootQueue(rand, resData, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);

    setResourceAndNodeDetails();
    return policy;
  }

  private void setResourceAndNodeDetails() {
    when(mCS.getClusterResource()).thenReturn(clusterResources);
    when(lm.getResourceByLabel(anyString(), any(Resource.class))).thenReturn(
        clusterResources);

    SchedulerNode mNode = mock(SchedulerNode.class);
    when(mNode.getPartition()).thenReturn(RMNodeLabelsManager.NO_LABEL);
    when(mCS.getSchedulerNode(any(NodeId.class))).thenReturn(mNode);
  }

  ParentQueue buildMockRootQueue(Random r, int[]... queueData) {
    Resource[] abs = generateResourceList(queueData[0]);
    Resource[] used = generateResourceList(queueData[2]);
    Resource[] pending = generateResourceList(queueData[3]);
    Resource[] reserved = generateResourceList(queueData[4]);
    Resource[] gran = generateResourceList(queueData[6]);
    int[] maxCap = queueData[1];
    int[] apps = queueData[5];
    int[] queues = queueData[7];

    return mockNested(abs, maxCap, used, pending, reserved, apps, gran, queues);
  }

  ParentQueue buildMockRootQueue(Random r, String[][] resData,
      int[]... queueData) {
    Resource[] abs = parseResourceDetails(resData[0]);
    Resource[] used = parseResourceDetails(resData[1]);
    Resource[] pending = parseResourceDetails(resData[2]);
    Resource[] reserved = parseResourceDetails(resData[3]);
    Resource[] gran = parseResourceDetails(resData[4]);
    int[] maxCap = queueData[0];
    int[] apps = queueData[1];
    int[] queues = queueData[2];

    return mockNested(abs, maxCap, used, pending, reserved, apps, gran, queues);
  }

  Resource[] parseResourceDetails(String[] resData) {
    List<Resource> resourceList = new ArrayList<Resource>();
    for (int i = 0; i < resData.length; i++) {
      String[] resource = resData[i].split(":");
      if (resource.length == 1) {
        resourceList.add(Resource.newInstance(Integer.parseInt(resource[0]), 0));
      } else {
        resourceList.add(Resource.newInstance(Integer.parseInt(resource[0]),
            Integer.parseInt(resource[1])));
      }
    }
    return resourceList.toArray(new Resource[resourceList.size()]);
  }

  Resource[] generateResourceList(int[] qData) {
    List<Resource> resourceList = new ArrayList<Resource>();
    for (int i = 0; i < qData.length; i++) {
      resourceList.add(Resource.newInstance(qData[i], 0));
    }
    return resourceList.toArray(new Resource[resourceList.size()]);
  }

  ParentQueue mockNested(Resource[] abs, int[] maxCap, Resource[] used,
      Resource[] pending, Resource[] reserved, int[] apps, Resource[] gran,
      int[] queues) {
    ResourceCalculator rc = mCS.getResourceCalculator();
    Resource tot = leafAbsCapacities(abs, queues);
    Deque<ParentQueue> pqs = new LinkedList<ParentQueue>();
    ParentQueue root = mockParentQueue(null, queues[0], pqs);
    ResourceUsage resUsage = new ResourceUsage();
    resUsage.setUsed(used[0]);
    resUsage.setReserved(reserved[0]);
    when(root.getQueueName()).thenReturn(CapacitySchedulerConfiguration.ROOT);
    when(root.getAbsoluteUsedCapacity()).thenReturn(
        Resources.divide(rc, tot, used[0], tot));
    when(root.getAbsoluteCapacity()).thenReturn(
        Resources.divide(rc, tot, abs[0], tot));
    when(root.getAbsoluteMaximumCapacity()).thenReturn(
        maxCap[0] / (float) tot.getMemorySize());
    when(root.getQueueResourceUsage()).thenReturn(resUsage);
    QueueCapacities rootQc = new QueueCapacities(true);
    rootQc.setAbsoluteUsedCapacity(Resources.divide(rc, tot, used[0], tot));
    rootQc.setAbsoluteCapacity(Resources.divide(rc, tot, abs[0], tot));
    rootQc.setAbsoluteMaximumCapacity(maxCap[0] / (float) tot.getMemorySize());
    when(root.getQueueCapacities()).thenReturn(rootQc);
    when(root.getQueuePath()).thenReturn(CapacitySchedulerConfiguration.ROOT);
    boolean preemptionDisabled = mockPreemptionStatus("root");
    when(root.getPreemptionDisabled()).thenReturn(preemptionDisabled);

    for (int i = 1; i < queues.length; ++i) {
      final CSQueue q;
      final ParentQueue p = pqs.removeLast();
      final String queueName = "queue" + ((char) ('A' + i - 1));
      if (queues[i] > 0) {
        q = mockParentQueue(p, queues[i], pqs);
        ResourceUsage resUsagePerQueue = new ResourceUsage();
        resUsagePerQueue.setUsed(used[i]);
        resUsagePerQueue.setReserved(reserved[i]);
        when(q.getQueueResourceUsage()).thenReturn(resUsagePerQueue);
      } else {
        q = mockLeafQueue(p, tot, i, abs, used, pending, reserved, apps, gran);
      }
      when(q.getParent()).thenReturn(p);
      when(q.getQueueName()).thenReturn(queueName);
      when(q.getAbsoluteUsedCapacity()).thenReturn(
          Resources.divide(rc, tot, used[i], tot));
      when(q.getAbsoluteCapacity()).thenReturn(
          Resources.divide(rc, tot, abs[i], tot));
      when(q.getAbsoluteMaximumCapacity()).thenReturn(
          maxCap[i] / (float) tot.getMemorySize());

      // We need to make these fields to QueueCapacities
      QueueCapacities qc = new QueueCapacities(false);
      qc.setAbsoluteUsedCapacity(Resources.divide(rc, tot, used[i], tot));
      qc.setAbsoluteCapacity(Resources.divide(rc, tot, abs[i], tot));
      qc.setAbsoluteMaximumCapacity(maxCap[i] / (float) tot.getMemorySize());
      when(q.getQueueCapacities()).thenReturn(qc);

      String parentPathName = p.getQueuePath();
      parentPathName = (parentPathName == null) ? "root" : parentPathName;
      String queuePathName = (parentPathName + "." + queueName).replace("/",
          "root");
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
      preemptionDisabled = conf.getPreemptionDisabled(qName, preemptionDisabled);
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

  @SuppressWarnings("rawtypes")
  LeafQueue mockLeafQueue(ParentQueue p, Resource tot, int i, Resource[] abs,
      Resource[] used, Resource[] pending, Resource[] reserved, int[] apps,
      Resource[] gran) {
    LeafQueue lq = mock(LeafQueue.class);
    ResourceCalculator rc = mCS.getResourceCalculator();
    List<ApplicationAttemptId> appAttemptIdList = 
        new ArrayList<ApplicationAttemptId>();
    when(lq.getTotalPendingResourcesConsideringUserLimit(isA(Resource.class),
        isA(String.class))).thenReturn(pending[i]);
    // need to set pending resource in resource usage as well
    ResourceUsage ru = new ResourceUsage();
    ru.setPending(pending[i]);
    ru.setUsed(used[i]);
    ru.setReserved(reserved[i]);
    when(lq.getQueueResourceUsage()).thenReturn(ru);
    // consider moving where CapacityScheduler::comparator accessible
    final NavigableSet<FiCaSchedulerApp> qApps = new TreeSet<FiCaSchedulerApp>(
      new Comparator<FiCaSchedulerApp>() {
        @Override
        public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
          return a1.getApplicationAttemptId()
              .compareTo(a2.getApplicationAttemptId());
        }
      });
    // applications are added in global L->R order in queues
    if (apps[i] != 0) {
      Resource aUsed = Resources.divideAndCeil(rc, used[i], apps[i]);
      Resource aPending = Resources.divideAndCeil(rc, pending[i], apps[i]);
      Resource aReserve = Resources.divideAndCeil(rc, reserved[i], apps[i]);
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
    @SuppressWarnings("unchecked")
    OrderingPolicy<FiCaSchedulerApp> so = mock(OrderingPolicy.class);
    when(so.getPreemptionIterator()).thenAnswer(new Answer() {
     public Object answer(InvocationOnMock invocation) {
         return qApps.descendingIterator();
       }
     });
    when(lq.getOrderingPolicy()).thenReturn(so);
    if(setAMResourcePercent != 0.0f){
      when(lq.getMaxAMResourcePerQueuePercent()).thenReturn(setAMResourcePercent);
    }
    p.getChildQueues().add(lq);
    return lq;
  }

  FiCaSchedulerApp mockApp(int qid, int id, Resource used, Resource pending,
      Resource reserved, Resource gran) {
    FiCaSchedulerApp app = mock(FiCaSchedulerApp.class);
    ResourceCalculator rc = mCS.getResourceCalculator();

    ApplicationId appId = ApplicationId.newInstance(TS, id);
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(appId, 0);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.getApplicationAttemptId()).thenReturn(appAttId);

    int cAlloc = 0;
    Resource unit = gran;
    List<RMContainer> cReserved = new ArrayList<RMContainer>();
    Resource resIter = Resource.newInstance(0, 0);
    for (; Resources.lessThan(rc, clusterResources, resIter, reserved); Resources
        .addTo(resIter, gran)) {
      cReserved.add(mockContainer(appAttId, cAlloc, unit,
          priority.CONTAINER.getValue()));
      ++cAlloc;
    }
    when(app.getReservedContainers()).thenReturn(cReserved);

    List<RMContainer> cLive = new ArrayList<RMContainer>();
    Resource usedIter = Resource.newInstance(0, 0);
    int i = 0;
    for (; Resources.lessThan(rc, clusterResources, usedIter, used); Resources
        .addTo(usedIter, gran)) {
      if (setAMContainer && i == 0) {
        cLive.add(mockContainer(appAttId, cAlloc, unit,
            priority.AMCONTAINER.getValue()));
      } else if (setLabeledContainer && i == 1) {
        cLive.add(mockContainer(appAttId, cAlloc, unit,
            priority.LABELEDCONTAINER.getValue()));
        Resources.addTo(used, Resource.newInstance(1, 1));
      } else {
        cLive.add(mockContainer(appAttId, cAlloc, unit,
            priority.CONTAINER.getValue()));
      }
      ++cAlloc;
      ++i;
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
    when(mC.getAllocatedResource()).thenReturn(r);
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

  static Resource leafAbsCapacities(Resource[] abs, int[] subqueues) {
    Resource ret = Resource.newInstance(0, 0);
    for (int i = 0; i < abs.length; ++i) {
      if (0 == subqueues[i]) {
        Resources.addTo(ret, abs[i]);
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
          + " pen:"
              + ((LeafQueue) nq).getTotalPendingResourcesConsideringUserLimit(
                                        isA(Resource.class), isA(String.class))
          + " cur:" + nq.getAbsoluteUsedCapacity()
          + " guar:" + nq.getAbsoluteCapacity()
          );
      for (FiCaSchedulerApp a : ((LeafQueue)nq).getApplications()) {
        System.out.println(indent + "  " + a.getApplicationId());
      }
    }
  }

}
