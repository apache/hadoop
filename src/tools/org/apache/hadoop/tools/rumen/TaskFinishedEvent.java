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

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;


/**
 * Event to record the successful completion of a task
 *
 */
public class TaskFinishedEvent implements HistoryEvent {
  private TaskID taskId;
  private long finishTime;
  private JhCounters counters;
  private TaskType taskType;
  private String status;
  
  /**
   * Create an event to record the successful completion of a task
   * @param id Task ID
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param status Status string
   * @param counters Counters for the task
   */
  public TaskFinishedEvent(TaskID id, long finishTime,
                           TaskType taskType,
                           String status, Counters counters) {
    this.taskId = id;
    this.finishTime = finishTime;
    this.counters = new JhCounters(counters, "COUNTERS");
    this.taskType = taskType;
    this.status = status;
  }

  /** Get task id */
  public TaskID getTaskId() { return taskId; }
  /** Get the task finish time */
  public long getFinishTime() { return finishTime; }
  /** Get task counters */
  public JhCounters getCounters() { return counters; }
  /** Get task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get task status */
  public String getTaskStatus() { return status; }
  /** Get event type */
  public EventType getEventType() {
    return EventType.TASK_FINISHED;
  }

  
}
