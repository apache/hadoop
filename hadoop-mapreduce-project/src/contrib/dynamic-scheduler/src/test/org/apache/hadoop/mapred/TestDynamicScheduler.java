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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;


/**
 Test the dynamic scheduler.
 Use the System Property test.build.data to drive the test run
 */
public class TestDynamicScheduler extends BaseSchedulerTest {

  private DynamicPriorityScheduler scheduler;


  /**
   * Create the test queues
   * @throws Exception
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf.set(PrioritySchedulerOptions.DYNAMIC_SCHEDULER_SCHEDULER,
             "org.apache.hadoop.mapred.FakeDynamicScheduler");
    scheduler = new DynamicPriorityScheduler();
    scheduler.setTimer(timer); 
    scheduler.setConf(conf);
    scheduler.setTaskTrackerManager(taskTracker);
    taskTracker.addQueues(QUEUES);
    scheduler.start();
  }

  /**
   * Remove the queues
   * @throws Exception
   */
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    removeQueues(QUEUES);
  }
  

  private void setSpending(String queue, float spending) throws IOException {
    scheduler.allocations.setSpending(queue, spending);
  }

  private void setBudgets(String[] queue, float[] budget) throws IOException {
    for (int i = 0; i < queue.length; i++) {
      scheduler.allocations.addBudget(queue[i], budget[i]);
    }
  }

  private void addQueues(String[] queue) throws IOException {
    for (String aQueue : queue) {
      scheduler.allocations.addQueue(aQueue);
    }
  }
  private void removeQueues(String[] queue) throws IOException {
    for (String aQueue : queue) {
      scheduler.allocations.removeQueue(aQueue);
    }
  }


  public void testAllocation() throws IOException {
    addQueues(QUEUES);
    setSpending("queue1", 1.0f);
    setSpending("queue2", 2.0f);
    setBudgets(QUEUES, new float[] {100.0f, 100.0f});
    scheduler.allocations.setUsage("queue1",2,0);
    scheduler.allocations.setUsage("queue2",3,0);
    timer.runTask();
    assertNotNull(scheduler.allocations);
    assertNotNull(scheduler.allocations.allocation);
    assertNotNull(scheduler.allocations.allocation.get("queue1"));
    assertNotNull(scheduler.allocations.allocation.get("queue2"));
    Collection<BudgetQueue> budgetQueues =
      scheduler.allocations.store.getQueues();
    assertNotNull(budgetQueues);
    assertEquals(2, budgetQueues.size());
    BudgetQueue queue1Budget = null;
    BudgetQueue queue2Budget = null;
    for (BudgetQueue queue: budgetQueues) {
      if (queue.name.equals("queue1")) {
        queue1Budget = queue;
      } else {
        queue2Budget = queue;
      }
    }
    assertNotNull(queue1Budget);
    assertNotNull(queue2Budget);

    assertEquals(98.0f, queue1Budget.budget, 0.1f);
    assertEquals(94.0f, queue2Budget.budget, 0.1f);
    assertEquals(1.0f, queue1Budget.spending, 0.1f);
    assertEquals(2.0f, queue2Budget.spending, 0.1f);

    Map<String,QueueAllocation> shares = scheduler.allocations.getAllocation();
    assertNotNull(shares);
    assertEquals(2, shares.size());
    assertNotNull(shares.get("queue1")); 
    assertNotNull(shares.get("queue2")); 
    assertEquals(1.0f/3.0f, shares.get("queue1").getShare(), 0.1f);
    assertEquals(2.0f/3.0f, shares.get("queue2").getShare(), 0.1f);
  }

  public void testBudgetUpdate() throws IOException {
    addQueues(QUEUES);
    setSpending("queue1", 1.0f);
    setSpending("queue2", 2.0f);
    setBudgets(QUEUES, new float[] {100.0f, 200.0f});
    timer.runTask();
    Collection<BudgetQueue> budgetQueues =
      scheduler.allocations.store.getQueues();
    BudgetQueue queue1Budget = null;
    BudgetQueue queue2Budget = null;
    for (BudgetQueue queue: budgetQueues) {
      if (queue.name.equals("queue1")) {
        queue1Budget = queue;
      } else {
        queue2Budget = queue;
      }
    }
    assertNotNull(queue1Budget);
    assertNotNull(queue2Budget);
    assertEquals(100.0f, queue1Budget.budget, 0.1f);
    assertEquals(200.0f, queue2Budget.budget, 0.1f);
    setBudgets(QUEUES, new float[] {200.0f, 300.0f});
    timer.runTask();
    budgetQueues =  scheduler.allocations.store.getQueues();
    for (BudgetQueue queue: budgetQueues) {
      if (queue.name.equals("queue1")) {
        queue1Budget = queue;
      } else {
        queue2Budget = queue;
      }
    }
    assertEquals(300.0f, queue1Budget.budget, 0.1f);
    assertEquals(500.0f, queue2Budget.budget, 0.1f);
    removeQueues(QUEUES);
  }

  public void testSpendingUpdate() throws IOException {
    addQueues(QUEUES);
    setSpending("queue1", 1.0f);
    setSpending("queue2", 2.0f);
    setBudgets(QUEUES, new float[] {100.0f, 100.0f});
    scheduler.allocations.setUsage("queue1", 1, 0);
    scheduler.allocations.setUsage("queue2", 1, 0);
    timer.runTask();
    Map<String,QueueAllocation> shares =
      scheduler.allocations.getAllocation();
    assertEquals(1.0f/3.0f, shares.get("queue1").getShare(), 0.1f);
    assertEquals(2.0f/3.0f, shares.get("queue2").getShare(), 0.1f);
    setSpending("queue1", 5.0f);
    setSpending("queue2", 1.0f);
    timer.runTask();
    shares = scheduler.allocations.getAllocation();
    assertEquals(5.0f/6.0f, shares.get("queue1").getShare(), 0.1f);
    assertEquals(1.0f/6.0f, shares.get("queue2").getShare(), 0.1f);
  }
}
