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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.MAX_IGNORED_OVER_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.MONITORING_INTERVAL;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.NATURAL_TERMINATION_FACTOR;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.OBSERVE_ONLY;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.TOTAL_PREEMPTION_PER_ROUND;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.WAIT_TIME_BEFORE_KILL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.KILL_CONTAINER;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.PREEMPT_CONTAINER;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestProportionalCapacityPreemptionPolicy {

  static final long TS = 3141592653L;

  int appAlloc = 0;
  Random rand = null;
  Clock mClock = null;
  Configuration conf = null;
  CapacityScheduler mCS = null;
  EventHandler<ContainerPreemptEvent> mDisp = null;
  ResourceCalculator rc = new DefaultResourceCalculator();
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
  final ArgumentCaptor<ContainerPreemptEvent> evtCaptor =
    ArgumentCaptor.forClass(ContainerPreemptEvent.class);

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

    mClock = mock(Clock.class);
    mCS = mock(CapacityScheduler.class);
    when(mCS.getResourceCalculator()).thenReturn(rc);
    mDisp = mock(EventHandler.class);
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
  public void testPreemptCycle() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
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
  public void testOverCapacityImbalance() {
    int[][] qData = new int[][]{
      //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
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
  public void testHierarchicalLarge() {
    int[][] qData = new int[][] {
      //  /    A   B   C    D   E   F    G   H   I
      { 400, 200, 60,140, 100, 70, 30, 100, 10, 90  },  // abs
      { 400, 210, 70,140, 100, 50, 50,  90, 90,  0  },  // used
      {  10,   0,  0,  0,   0,  0,  0,   0,  0, 15  },  // pending
      {   0,   0,  0,  0,   0,  0,  0,   0,  0,  0  },  // reserved
      {   6,   2,  1,  1,   2,  1,  1,   2,  1,  1  },  // apps
      {  -1,  -1,  1,  1,  -1,  1,  1,  -1,  1,  1  },  // req granularity
      {   3,   2,  0,  0,   2,  0,  0,   2,  0,  0  },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    // verify capacity taken from A1, not H1 despite H1 being far over
    // its absolute guaranteed capacity

    // XXX note: compensating for rounding error in Resources.multiplyTo
    // which is likely triggered since we use small numbers for readability
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(4)).handle(argThat(new IsPreemptionRequestFor(appE)));
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

  static class IsPreemptionRequestFor
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
    private final ContainerPreemptEventType type;
    IsPreemptionRequestFor(ApplicationAttemptId appAttId) {
      this(appAttId, PREEMPT_CONTAINER);
    }
    IsPreemptionRequestFor(ApplicationAttemptId appAttId,
        ContainerPreemptEventType type) {
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
      new ProportionalCapacityPreemptionPolicy(conf, mDisp, mCS, mClock);
    ParentQueue mRoot = buildMockRootQueue(rand, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);

    Resource clusterResources =
      Resource.newInstance(leafAbsCapacities(qData[0], qData[6]), 0);
    when(mCS.getClusterResources()).thenReturn(clusterResources);
    return policy;
  }

  ParentQueue buildMockRootQueue(Random r, int[]... queueData) {
    int[] abs      = queueData[0];
    int[] used     = queueData[1];
    int[] pending  = queueData[2];
    int[] reserved = queueData[3];
    int[] apps     = queueData[4];
    int[] gran     = queueData[5];
    int[] queues   = queueData[6];

    return mockNested(abs, used, pending, reserved, apps, gran, queues);
  }

  ParentQueue mockNested(int[] abs, int[] used,
      int[] pending, int[] reserved, int[] apps, int[] gran, int[] queues) {
    float tot = leafAbsCapacities(abs, queues);
    Deque<ParentQueue> pqs = new LinkedList<ParentQueue>();
    ParentQueue root = mockParentQueue(null, queues[0], pqs);
    when(root.getQueueName()).thenReturn("/");
    when(root.getAbsoluteUsedCapacity()).thenReturn(used[0] / tot);
    when(root.getAbsoluteCapacity()).thenReturn(abs[0] / tot);
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
    }
    assert 0 == pqs.size();
    return root;
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
        qApps.add(mockApp(i, appAlloc, aUsed, aPending, aReserve, gran[i]));
        ++appAlloc;
      }
    }
    when(lq.getApplications()).thenReturn(qApps);
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
      cReserved.add(mockContainer(appAttId, cAlloc, unit, 1));
      ++cAlloc;
    }
    when(app.getReservedContainers()).thenReturn(cReserved);

    List<RMContainer> cLive = new ArrayList<RMContainer>();
    for (int i = 0; i < used; i += gran) {
      cLive.add(mockContainer(appAttId, cAlloc, unit, 1));
      ++cAlloc;
    }
    when(app.getLiveContainers()).thenReturn(cLive);
    return app;
  }

  RMContainer mockContainer(ApplicationAttemptId appAttId, int id,
      Resource r, int priority) {
    ContainerId cId = ContainerId.newInstance(appAttId, id);
    Container c = mock(Container.class);
    when(c.getResource()).thenReturn(r);
    when(c.getPriority()).thenReturn(Priority.create(priority));
    RMContainer mC = mock(RMContainer.class);
    when(mC.getContainerId()).thenReturn(cId);
    when(mC.getContainer()).thenReturn(c);
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
