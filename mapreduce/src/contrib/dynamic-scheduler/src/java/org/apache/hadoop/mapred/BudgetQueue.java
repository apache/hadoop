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

/**
 * Class to hold accounting info about a queue
 * such as remaining budget, spending rate and
 * whether queue usage
 */
public class BudgetQueue {
  String name;
  volatile float budget;
  volatile float spending;
  volatile int used;
  volatile int pending;
  /**
   * @param name queue name
   * @param budget queue budget in credits
   * @param spending queue spending rate in credits per allocation interval
   * to deduct from budget
   */
  public BudgetQueue(String name, float budget, float spending) {
      this.name = name;
      this.budget = budget;
      this.spending = spending;
      this.used = 0;
      this.pending = 0;
  }
  /**
   * Thread safe addition of budget
   * @param newBudget budget to add
   */
  public synchronized void addBudget(float newBudget) {
    budget += newBudget;
  }
}
