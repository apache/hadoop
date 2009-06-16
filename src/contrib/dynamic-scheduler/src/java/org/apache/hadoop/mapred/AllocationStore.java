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

import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.HashMap;
import java.util.Collection;

/**
 * Abstract class for implementing a persistent store
 * of allocation information.
 */
public abstract class AllocationStore {
  Map<String,BudgetQueue> queueCache = new HashMap<String,BudgetQueue>();

  /**
   * Initializes configuration
   * @param conf MapReduce configuration
   */ 
  public abstract void init(Configuration conf);

  /**
   * Loads allocations from persistent store
   */ 
  public abstract void load();

  /**
   * Saves allocations to persistent store
   */ 
  public abstract void save();

  /**
   * Gets current remaining budget associated with queue.
   * @param queue name of queue
   * @return budget in credits
   */ 
  public float getBudget(String queue) {
    float budget = 0.0f;
    BudgetQueue budgetQueue = queueCache.get(queue);
    if (budgetQueue != null) {
      budget = budgetQueue.budget;
    }
    return budget;
  }

  /**
   * Gets current spending rate associated with queue.
   * @param queue name of queue
   * @return spending rate in credits per allocation interval to be
   * deducted from budget
   */ 
  public float getSpending(String queue) {
    float spending = 0;
    BudgetQueue budgetQueue = queueCache.get(queue);
    if (budgetQueue != null) {
      spending = budgetQueue.spending;
    }
    return spending;
  }

  /**
   * Adds budget to queue.
   * @param queue name of queue
   * @param budget in credits to be added to queue
   */
  public synchronized void addBudget(String queue, float budget) {
    BudgetQueue budgetQueue = queueCache.get(queue);
    if (budgetQueue == null) {
        return;
    }
    budgetQueue.addBudget(budget);
  }


  /**
   * Adds new queue.
   * @param queue name of queue
   */
  public synchronized void addQueue(String queue) {
    queueCache.put(queue, new BudgetQueue(queue,0.0f,0.0f));
  }

  /**
   * Gets queue info.
   * @param queue name of queue
   * @return xml representation of queue info as a string
   */
  public String getQueueInfo(String queue) {
    BudgetQueue budgetQueue = queueCache.get(queue);
    if (budgetQueue == null) {
        return "";
    }
    return "<budget>" + Float.toString(budgetQueue.budget) + "</budget>\n" +
        "<spending>" + Float.toString(budgetQueue.spending) + "</spending>\n" +
        "<used>" + Integer.toString(budgetQueue.used) + "</used>\n" +
        "<pending>" + budgetQueue.pending + "</pending>\n";
  }

  /**
   * Remove queue.
   * @param queue name of queue
   */
  public synchronized void removeQueue(String queue) {
    queueCache.remove(queue);
  }

  /**
   * Sets spending rate for queue.
   * @param queue name of queue
   * @param spending spending rate in credits per allocation interval to be
   * deducted from budget
   */ 
  public synchronized void setSpending(String queue, float spending) {
    BudgetQueue budgetQueue = queueCache.get(queue);
    if (budgetQueue == null) {
        return;
    }
    budgetQueue.spending = spending;
  }

  /**
   * Sets queue usage for accounting
   * @param queue name of queue
   * @param used slots currently in use
   * @param pending pending tasks
   */ 
  public synchronized void setUsage(String queue, int used, int pending) {
    BudgetQueue budgetQueue = queueCache.get(queue);
    if (budgetQueue == null) {
        return;
    }
    budgetQueue.used = used;
    budgetQueue.pending = pending;
  }

  /**
  * Gets queue status (budget, spending, usage)
  * @return collection of queue status objects
  */
  public Collection<BudgetQueue> getQueues() {
    return queueCache.values();
  }
}
