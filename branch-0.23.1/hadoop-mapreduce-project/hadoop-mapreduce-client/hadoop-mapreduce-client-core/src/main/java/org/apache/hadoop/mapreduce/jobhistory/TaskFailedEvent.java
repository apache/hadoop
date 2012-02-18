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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

import org.apache.avro.util.Utf8;

/**
 * Event to record the failure of a task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskFailedEvent implements HistoryEvent {
  private TaskFailed datum = new TaskFailed();

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
    datum.taskid = new Utf8(id.toString());
    datum.error = new Utf8(error);
    datum.finishTime = finishTime;
    datum.taskType = new Utf8(taskType.name());
    datum.failedDueToAttempt = failedDueToAttempt == null
      ? null
      : new Utf8(failedDueToAttempt.toString());
    datum.status = new Utf8(status);
  }

  TaskFailedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) { this.datum = (TaskFailed)datum; }

  /** Get the task id */
  public TaskID getTaskId() { return TaskID.forName(datum.taskid.toString()); }
  /** Get the error string */
  public String getError() { return datum.error.toString(); }
  /** Get the finish time of the attempt */
  public long getFinishTime() { return datum.finishTime; }
  /** Get the task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.taskType.toString());
  }
  /** Get the attempt id due to which the task failed */
  public TaskAttemptID getFailedAttemptID() {
    return datum.failedDueToAttempt == null
      ? null
      : TaskAttemptID.forName(datum.failedDueToAttempt.toString());
  }
  /** Get the task status */
  public String getTaskStatus() { return datum.status.toString(); }
  /** Get the event type */
  public EventType getEventType() { return EventType.TASK_FAILED; }

  
}
