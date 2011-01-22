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

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record the failure of a task
 *
 */
public class TaskFailedEvent implements HistoryEvent {
  private TaskID taskId;
  private String error;
  private long finishTime;
  private TaskType taskType;
  private TaskAttemptID failedDueToAttempt;
  private String status;

  /**
   * Create an event to record task failure
   * @param id Task ID
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param error Error String
   * @param status Status
   * @param failedDueToAttempt The attempt id due to which the task failed
   */
  public TaskFailedEvent(TaskID id, long finishTime, 
      TaskType taskType, String error, String status,
      TaskAttemptID failedDueToAttempt) {
    this.taskId = id;
    this.error = error;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.failedDueToAttempt = failedDueToAttempt;
    this.status = status;
  }

  /** Get the task id */
  public TaskID getTaskId() { return taskId; }
  /** Get the error string */
  public String getError() { return error; }
  /** Get the finish time of the attempt */
  public long getFinishTime() { return finishTime; }
  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the attempt id due to which the task failed */
  public TaskAttemptID getFailedAttemptID() {
    return failedDueToAttempt;
  }
  /** Get the task status */
  public String getTaskStatus() { return status; }
  /** Get the event type */
  public EventType getEventType() { return EventType.TASK_FAILED; }

  
}
