/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import junit.framework.TestCase;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import static org.apache.hadoop.mapred.CapacityTestUtils.*;

public class TestContainerQueue extends TestCase {

  CapacityTestUtils.FakeTaskTrackerManager taskTrackerManager = null;
  CapacityTaskScheduler scheduler = null;
  CapacityTestUtils.FakeClock clock = null;
  CapacityTestUtils.ControlledInitializationPoller
    controlledInitializationPoller;

  @Override
  protected void setUp() {
    setUp(2, 2, 1);
  }

  private void setUp(
    int numTaskTrackers, int numMapTasksPerTracker,
    int numReduceTasksPerTracker) {
    taskTrackerManager =
      new CapacityTestUtils.FakeTaskTrackerManager(
        numTaskTrackers, numMapTasksPerTracker,
        numReduceTasksPerTracker);
    clock = new CapacityTestUtils.FakeClock();
    scheduler = new CapacityTaskScheduler(clock);
    scheduler.setTaskTrackerManager(taskTrackerManager);

    // Don't let the JobInitializationPoller come in our way.
    controlledInitializationPoller =
        new CapacityTestUtils.ControlledInitializationPoller(
            scheduler.jobQueuesManager, taskTrackerManager);
    scheduler.setInitializationPoller(controlledInitializationPoller);
    scheduler.setConf(taskTrackerManager.defaultJobConf);
  }

  /**
   * check the minCapacity distribution across children
   * and grand children.
   * <p/>
   * Check for both capacity % and also actual no of slots allocated.
   */
  public void testMinCapacity() {

    AbstractQueue rt = QueueHierarchyBuilder.createRootAbstractQueue();

    //Simple check to make sure that capacity is properly distributed among
    // its children.
    //level 1 children
    QueueSchedulingContext a1 = new QueueSchedulingContext(
      "a", 25, -1, -1);
    QueueSchedulingContext a2 = new QueueSchedulingContext(
      "b", 25, -1, -1);

    AbstractQueue q = new ContainerQueue(rt, a1);
    AbstractQueue ql = new ContainerQueue(rt, a2);

    //level 2 children
    QueueSchedulingContext a = new QueueSchedulingContext(
      "aa", 50, -1, -1);
    QueueSchedulingContext b = new QueueSchedulingContext(
      "ab", 50, -1, -1);
    QueueSchedulingContext c = new QueueSchedulingContext(
      "ac", 50, -1, -1);
    QueueSchedulingContext d = new QueueSchedulingContext(
      "ad", 50, -1, -1);

    AbstractQueue q1 = new JobQueue(q, a);
    AbstractQueue q2 = new JobQueue(q, b);
    AbstractQueue q3 = new JobQueue(ql, c);
    AbstractQueue q4 = new JobQueue(ql, d);

    rt.update(1000, 1000);

    //share at level 0.
    // (1000 * 25) / 100
    assertEquals(q.getQueueSchedulingContext().getMapTSC().getCapacity(), 250);
    assertEquals(ql.getQueueSchedulingContext().getMapTSC().getCapacity(), 250);

    //share would be (1000 * 25 / 100 ) * (50 / 100)
    assertEquals(q1.getQueueSchedulingContext().getMapTSC().getCapacity(), 125);
    assertEquals(q2.getQueueSchedulingContext().getMapTSC().getCapacity(), 125);
    assertEquals(q3.getQueueSchedulingContext().getMapTSC().getCapacity(), 125);
    assertEquals(q4.getQueueSchedulingContext().getMapTSC().getCapacity(), 125);

    //
    rt.update(1, 1);
    assertEquals(q.getQueueSchedulingContext().getMapTSC().getCapacity(), 0);
    assertEquals(ql.getQueueSchedulingContext().getMapTSC().getCapacity(), 0);

    //share would be (1000 * 25 / 100 ) * (50 / 100)
    assertEquals(q1.getQueueSchedulingContext().getMapTSC().getCapacity(), 0);
    assertEquals(q2.getQueueSchedulingContext().getMapTSC().getCapacity(), 0);
    assertEquals(q3.getQueueSchedulingContext().getMapTSC().getCapacity(), 0);
    assertEquals(q4.getQueueSchedulingContext().getMapTSC().getCapacity(), 0);

  }

  public void testMaxCapacity() throws IOException {
    this.setUp(4, 1, 1);
    taskTrackerManager.addJobInProgressListener(scheduler.jobQueuesManager);

    AbstractQueue rt = QueueHierarchyBuilder.createRootAbstractQueue();

    QueueSchedulingContext a1 = new QueueSchedulingContext(
      "R.a", 25, 50, -1);
    QueueSchedulingContext a2 = new QueueSchedulingContext(
      "R.b", 25, 30, -1);
    QueueSchedulingContext a3 = new QueueSchedulingContext(
      "R.c", 50, -1, -1);


    //Test for max capacity
    AbstractQueue q = new JobQueue(rt, a1);
    AbstractQueue q1 = new JobQueue(rt, a2);
    AbstractQueue q2 = new JobQueue(rt, a3);
    scheduler.jobQueuesManager.addQueue((JobQueue) q);
    scheduler.jobQueuesManager.addQueue((JobQueue) q1);
    scheduler.jobQueuesManager.addQueue((JobQueue) q2);

    scheduler.setRoot(rt);
    rt.update(4, 4);

    scheduler.updateContextInfoForTests();

    // submit a job to the second queue
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 20, 20, "R.a", "u1");

    //Queue R.a should not more than 2 slots
    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP,
      "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE,
      "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP,
      "attempt_test_0001_m_000002_0 on tt2");
    expectedStrings.put(
      CapacityTestUtils.REDUCE,
      "attempt_test_0001_r_000002_0 on tt2");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);

    assertNull(scheduler.assignTasks(
      taskTrackerManager.getTaskTracker(
        "tt3")));

  }

  public void testDistributedUnConfiguredCapacity() {

    AbstractQueue rt = QueueHierarchyBuilder.createRootAbstractQueue();

    //generate Queuecontext for the children
    QueueSchedulingContext a1 = new QueueSchedulingContext(
      "a", 50, -1, -1);
    QueueSchedulingContext a2 = new QueueSchedulingContext(
      "b", -1, -1, -1);

    AbstractQueue rtChild1 = new ContainerQueue(rt, a1);
    AbstractQueue rtChild2 = new ContainerQueue(rt, a2);

    //Add further children to rtChild1.    
    QueueSchedulingContext b = new QueueSchedulingContext(
      "ab", 30, -1, -1);
    QueueSchedulingContext c = new QueueSchedulingContext(
      "ac", -1, -1, -1);
    QueueSchedulingContext d = new QueueSchedulingContext(
      "ad", 100, -1, -1);

    AbstractQueue q0 = new JobQueue(rtChild1, b);
    AbstractQueue q1 = new JobQueue(rtChild1, c);
    AbstractQueue q2 = new JobQueue(rtChild2, d);

    rt.distributeUnConfiguredCapacity();

    //after distribution the rtChild2 capacity should be 50.
    assertEquals(
      rtChild2.getQueueSchedulingContext().getCapacityPercent(), 50.0f);

    //Now check the capacity of q1. It should be 60%.
    assertTrue(q1.getQueueSchedulingContext().getCapacityPercent() == 70);
  }

  private Map setUpHierarchy() {
    return setUpHierarchy(60, 40, 80, 20);
  }

  /**
   * @param a capacity for sch q
   * @param b capacity for gta q
   * @param c capacity for sch.prod q
   * @param d capacity for sch.misc q
   * @return
   */
  private Map<String, AbstractQueue> setUpHierarchy(
                                    int a, int b, int c, int d) {
    HashMap<String, AbstractQueue> map = new HashMap<String, AbstractQueue>();

    AbstractQueue rt = QueueHierarchyBuilder.createRootAbstractQueue();
    map.put(rt.getName(), rt);
    scheduler.setRoot(rt);

    // Create 2 levels of hierarchy.

    //Firt level
    QueueSchedulingContext sch =
      new QueueSchedulingContext("rt.sch", a, -1, -1);
    QueueSchedulingContext gta =
      new QueueSchedulingContext("rt.gta", b, -1, -1);

    AbstractQueue schq = new ContainerQueue(rt, sch);

    //Cannot declare a ContainerQueue if no children.
    AbstractQueue gtaq = new JobQueue(rt, gta);
    map.put(schq.getName(), schq);
    map.put(gtaq.getName(), gtaq);
    scheduler.jobQueuesManager.addQueue((JobQueue) gtaq);

    //Create further children.
    QueueSchedulingContext prod =
      new QueueSchedulingContext("rt.sch.prod", c, -1, -1);
    QueueSchedulingContext misc =
      new QueueSchedulingContext("rt.sch.misc", d, -1, -1);

    AbstractQueue prodq = new JobQueue(schq, prod);
    AbstractQueue miscq = new JobQueue(schq, misc);
    map.put(prodq.getName(), prodq);
    map.put(miscq.getName(), miscq);
    scheduler.jobQueuesManager.addQueue(
      (JobQueue) prodq);
    scheduler.jobQueuesManager.addQueue((JobQueue) miscq);
    return map;
  }

  /**
   * Verifies that capacities are allocated properly in hierarchical queues.
   * 
   * The test case sets up a hierarchy of queues and submits jobs to 
   * all the queues. It verifies that computation of capacities, sorting,
   * etc are working correctly.
   * @throws Exception
   */
  public void testCapacityAllocationInHierarchicalQueues() throws Exception {
    this.setUp(9, 1, 1);
    taskTrackerManager.addJobInProgressListener(scheduler.jobQueuesManager);
    // set up some queues
    Map<String, AbstractQueue> map = setUpHierarchy();

    scheduler.updateContextInfoForTests();

    // verify initial capacity distribution
    TaskSchedulingContext mapTsc
      = map.get("rt.gta").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTsc.getCapacity(), 3);

    mapTsc = map.get("rt.sch").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTsc.getCapacity(), 5);

    mapTsc = map.get("rt.sch.prod").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTsc.getCapacity(), 4);

    mapTsc = map.get("rt.sch.misc").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTsc.getCapacity(), 1);

    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{0, 0, 0, 0});

    //Only Allow job submission to leaf queue
    taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, 4, 4, "rt.sch.prod",
      "u1");

    // submit a job to the second queue
    taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, 4, 4, "rt.sch.misc",
      "u1");

    //submit a job in gta level queue
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 4, 4, "rt.gta", "u1");

    int counter = 0;
    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{0, 1, 1, 0});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0003_m_000001_0 on tt2");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0003_r_000001_0 on tt2");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{1, 1, 1, 0});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000001_0 on tt3");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000001_0 on tt3");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt3",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{1, 2, 1, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0003_m_000002_0 on tt4");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0003_r_000002_0 on tt4");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt4",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{2, 2, 1, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000002_0 on tt5");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000002_0 on tt5");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt5",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{2, 3, 2, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000003_0 on tt6");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000003_0 on tt6");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt6",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{2, 4, 3, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0003_m_000003_0 on tt7");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0003_r_000003_0 on tt7");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt7",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{3, 4, 3, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000004_0 on tt8");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000004_0 on tt8");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt8",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{3, 5, 4, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000002_0 on tt9");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000002_0 on tt9");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt9",
      expectedStrings);
  }
  
  /**
   * Test to make sure that capacity is divided at each level properly.
   *
   * The test case sets up a two level hierarchy of queues as follows:
   *   - rt.sch
   *     - rt.sch.prod
   *     - rt.sch.misc
   *   - rt.gta  
   * Jobs are submitted to rt.sch.misc and rt.gta, and the test verifies 
   * that as long as rt.sch is below rt.gta's capacity, it still gets 
   * allocated slots even if rt.sch.misc is over its capacity. 
   * @throws IOException
   */
  public void testHierarchicalCapacityAllocation() throws IOException {
    this.setUp(8, 1, 1);
    taskTrackerManager.addJobInProgressListener(scheduler.jobQueuesManager);
    // set up some queues
    Map<String, AbstractQueue> map = setUpHierarchy(70, 30, 80, 20);

    scheduler.updateContextInfoForTests();

    // verify capacities as per the setup.
    TaskSchedulingContext mapTSC
      = map.get("rt.gta").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTSC.getCapacity(), 2);

    mapTSC = map.get("rt.sch").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTSC.getCapacity(), 5);

    mapTSC = map.get("rt.sch.prod").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTSC.getCapacity(), 4);

    mapTSC = map.get("rt.sch.misc").getQueueSchedulingContext().getMapTSC();
    assertEquals(mapTSC.getCapacity(), 1);

    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{0, 0, 0, 0});

    // submit a job to the second queue
    taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, 20, 20, "rt.sch.misc",
      "u1");

    //submit a job in gta level queue
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 20, 20, "rt.gta", "u1");
    int counter = 0;
    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.put(MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000001_0 on tt1");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{0, 1, 0, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0002_m_000001_0 on tt2");
    expectedStrings.put(REDUCE, "attempt_test_0002_r_000001_0 on tt2");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{1, 1, 0, 1});
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0001_m_000002_0 on tt3");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000002_0 on tt3");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt3",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{1, 2, 0, 2});
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0001_m_000003_0 on tt4");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000003_0 on tt4");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt4",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{1, 3, 0, 3});
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0002_m_000002_0 on tt5");
    expectedStrings.put(REDUCE, "attempt_test_0002_r_000002_0 on tt5");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt5",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{2, 3, 0, 3});
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0001_m_000004_0 on tt6");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000004_0 on tt6");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt6",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{2, 4, 0, 4});
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0001_m_000005_0 on tt7");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000005_0 on tt7");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt7",
      expectedStrings);
    assertUsedCapacity(
      map,
      new String[]{"rt.gta", "rt.sch", "rt.sch.prod", "rt.sch.misc"},
      new int[]{2, 5, 0, 5});
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0001_m_000006_0 on tt8");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000006_0 on tt8");


    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt8",
      expectedStrings);
  }

  // verify that the number of slots used for each queue
  // matches the expected value.
  private void assertUsedCapacity(Map<String, AbstractQueue> queueMap,
      String[] queueNames, int[] expectedUsedSlots) {
  
    scheduler.updateContextInfoForTests();
    assertEquals(queueNames.length, expectedUsedSlots.length);
    
    for (int i=0; i<queueNames.length; i++) {
      TaskSchedulingContext mapTSC = 
        queueMap.get(queueNames[i]).getQueueSchedulingContext().getMapTSC();
      assertEquals(mapTSC.getNumSlotsOccupied(), expectedUsedSlots[i]);
    }
  }
}
