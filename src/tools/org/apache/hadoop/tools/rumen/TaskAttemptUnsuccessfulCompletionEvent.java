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
 * Event to record unsuccessful (Killed/Failed) completion of task attempts
 *
 */
public class TaskAttemptUnsuccessfulCompletionEvent implements HistoryEvent {
  private TaskID taskId;
  private TaskType taskType;
  private TaskAttemptID attemptId;
  private long finishTime;
  private String hostname;
  private String error;
  private String status;

  /** 
   * Create an event to record the unsuccessful completion of attempts
   * @param id Attempt ID
   * @param taskType Type of the task
   * @param status Status of the attempt
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param error Error string
   */
  public TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID id, 
      TaskType taskType,
      String status, long finishTime, 
      String hostname, String error) {
    this.taskId = id.getTaskID();
    this.taskType = taskType;
    this.attemptId = id;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.error = error;
    this.status = status;
  }

  /** Get the task id */
  public TaskID getTaskId() { return taskId; }
  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() {
    return attemptId;
  }
  /** Get the finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the name of the host where the attempt executed */
  public String getHostname() { return hostname; }
  /** Get the error string */
  public String getError() { return error; }
  /** Get the task status */
  public String getTaskStatus() { return status; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.MAP_ATTEMPT_KILLED;
  }

}
