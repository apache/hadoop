/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.utils;

import java.util.PriorityQueue;

/**
 * A priority queue that stores a number of {@link BackgroundTask}.
 */
public class BackgroundTaskQueue {

  private final PriorityQueue<BackgroundTask> tasks;

  public BackgroundTaskQueue() {
    tasks = new PriorityQueue<>((task1, task2)
        -> task1.getPriority() - task2.getPriority());
  }

  /**
   * @return the head task in this queue.
   */
  public synchronized BackgroundTask poll() {
    return tasks.poll();
  }

  /**
   * Add a {@link BackgroundTask} to the queue,
   * the task will be sorted by its priority.
   *
   * @param task
   */
  public synchronized void add(BackgroundTask task) {
    tasks.add(task);
  }

  /**
   * @return true if the queue contains no task, false otherwise.
   */
  public synchronized boolean isEmpty() {
    return tasks.isEmpty();
  }

  /**
   * @return the size of the queue.
   */
  public synchronized int size() {
    return tasks.size();
  }
}
