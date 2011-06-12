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
 * Configuration Options used in the priority schedulers
 * -all in one place for ease of referencing in code.
 */
public class PrioritySchedulerOptions {
  /** {@value} */
  public static final String DYNAMIC_SCHEDULER_BUDGET_FILE = "mapred.dynamic-scheduler.budget-file";
  /** {@value} */
  public static final String DYNAMIC_SCHEDULER_STORE = "mapred.dynamic-scheduler.store";
  /** {@value} */
  public static final String MAPRED_QUEUE_NAMES = "mapred.queue.names";
  /** {@value} */
  public static final String DYNAMIC_SCHEDULER_SCHEDULER = "mapred.dynamic-scheduler.scheduler";
  /** {@value} */
  public static final String DYNAMIC_SCHEDULER_ALLOC_INTERVAL = "mapred.dynamic-scheduler.alloc-interval";
}
