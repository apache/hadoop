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

public class TestPriorityScheduler extends BaseSchedulerTest {

  private DynamicPriorityScheduler scheduler;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf.set(PrioritySchedulerOptions.DYNAMIC_SCHEDULER_SCHEDULER,
      "org.apache.hadoop.mapred.PriorityScheduler");
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

  public void testQueueAllocation() throws IOException {
    addQueues(QUEUES);
    setSpending("queue1", 1.0f);
    setSpending("queue2", 2.0f);
    setBudgets(QUEUES, new float[] {100.0f, 100.0f});
    scheduler.allocations.setUsage("queue1", 2,0);
    scheduler.allocations.setUsage("queue2", 3,0);
    timer.runTask();
    Map<String,PriorityScheduler.QueueQuota> queueQuota = 
      ((PriorityScheduler)scheduler.scheduler).
      getQueueQuota(100, 10, PriorityScheduler.MAP); 
    assertEquals(2, queueQuota.size());
    for (PriorityScheduler.QueueQuota quota: queueQuota.values()) {
      if (quota.name.equals("queue1")) {
        assertEquals(Math.round(100 * 1.0f/3.0f), quota.quota, 0.1f);
      } else {
        assertEquals(Math.round(100 * 2.0f/3.0f), quota.quota, 0.1f);
      }
      assertTrue(quota.mappers == quota.quota);
    }     
    queueQuota = ((PriorityScheduler)scheduler.scheduler).getQueueQuota(100, 10,
        PriorityScheduler.REDUCE); 
    assertEquals(2, queueQuota.size());
    for (PriorityScheduler.QueueQuota quota: queueQuota.values()) {
      if (quota.name.equals("queue1")) {
        assertEquals( Math.round(10 * 1.0f/3.0f), quota.quota, 0.1f);
      } else {
        assertEquals(Math.round(10 * 2.0f/3.0f), quota.quota, 0.1f);
      }
      assertTrue(quota.reducers == quota.quota);
    }     
  }

  public void testUsage() throws IOException {
    addQueues(QUEUES);
    setSpending("queue1", 1.0f);
    setSpending("queue2", 2.0f);
    setBudgets(QUEUES, new float[] {1000.0f, 1000.0f});
    scheduler.allocations.setUsage("queue1", 0, 1);
    scheduler.allocations.setUsage("queue2", 0, 1);
    timer.runTask();
    Map<String,PriorityScheduler.QueueQuota> queueQuota = 
      ((PriorityScheduler)scheduler.scheduler).getQueueQuota(100, 10,
          PriorityScheduler.MAP); 
    PriorityScheduler.QueueQuota quota1 = queueQuota.get("queue1");
    PriorityScheduler.QueueQuota quota2 = queueQuota.get("queue2");
    quota1.map_used = 10;
    quota2.map_used = 90; 
    ((PriorityScheduler)scheduler.scheduler).markIdle(queueQuota);
    timer.runTask();

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
    assertEquals("Budget incorrect", 990.0f, queue1Budget.budget, 0.1f);
    assertEquals("Budget incorrect", 866.0f, queue2Budget.budget, 0.1f);
  }
}
