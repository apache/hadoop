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

package org.apache.hadoop.tools.rumen;

import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record the start of a task
 *
 */
public class TaskStartedEvent implements HistoryEvent {
  private TaskID taskId;
  private String splitLocations;
  private long startTime;
  private TaskType taskType;

  /**
   * Create an event to record start of a task
   * @param id Task Id
   * @param startTime Start time of the task
   * @param taskType Type of the task
   * @param splitLocations Split locations, applicable for map tasks
   */
  public TaskStartedEvent(TaskID id, long startTime, 
      TaskType taskType, String splitLocations) {
    this.taskId = id;
    this.splitLocations = splitLocations;
    this.startTime = startTime;
    this.taskType = taskType;
  }

  /** Get the task id */
  public TaskID getTaskId() { return taskId; }
  /** Get the split locations, applicable for map tasks */
  public String getSplitLocations() { return splitLocations; }
  /** Get the start time of the task */
  public long getStartTime() { return startTime; }
  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.TASK_STARTED;
  }

}
