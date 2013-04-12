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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Counters;
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
  private TaskFailed datum = null;

  private TaskAttemptID failedDueToAttempt;
  private TaskID id;
  private TaskType taskType;
  private long finishTime;
  private String status;
  private String error;
  private Counters counters;

  private static final Counters EMPTY_COUNTERS = new Counters();

  /**
   * Create an event to record task failure
   * @param id Task ID
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param error Error String
   * @param status Status
   * @param failedDueToAttempt The attempt id due to which the task failed
   * @param counters Counters for the task
   */
  public TaskFailedEvent(TaskID id, long finishTime, 
      TaskType taskType, String error, String status,
      TaskAttemptID failedDueToAttempt, Counters counters) {
    this.id = id;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.error = error;
    this.status = status;
    this.failedDueToAttempt = failedDueToAttempt;
    this.counters = counters;
  }

  public TaskFailedEvent(TaskID id, long finishTime, 
	      TaskType taskType, String error, String status,
	      TaskAttemptID failedDueToAttempt) {
    this(id, finishTime, taskType, error, status,
        failedDueToAttempt, EMPTY_COUNTERS);
  }
  
  TaskFailedEvent() {}

  public Object getDatum() {
    if(datum == null) {
      datum = new TaskFailed();
      datum.taskid = new Utf8(id.toString());
      datum.error = new Utf8(error);
      datum.finishTime = finishTime;
      datum.taskType = new Utf8(taskType.name());
      datum.failedDueToAttempt =
          failedDueToAttempt == null
          ? null
          : new Utf8(failedDueToAttempt.toString());
      datum.status = new Utf8(status);
      datum.counters = EventWriter.toAvro(counters);
    }
    return datum;
  }
  
  public void setDatum(Object odatum) {
    this.datum = (TaskFailed)odatum;
    this.id =
        TaskID.forName(datum.taskid.toString());
    this.taskType =
        TaskType.valueOf(datum.taskType.toString());
    this.finishTime = datum.finishTime;
    this.error = datum.error.toString();
    this.failedDueToAttempt =
        datum.failedDueToAttempt == null
        ? null
        : TaskAttemptID.forName(
            datum.failedDueToAttempt.toString());
    this.status = datum.status.toString();
    this.counters =
        EventReader.fromAvro(datum.counters);
  }

  /** Get the task id */
  public TaskID getTaskId() { return id; }
  /** Get the error string */
  public String getError() { return error; }
  /** Get the finish time of the attempt */
  public long getFinishTime() {
    return finishTime;
  }
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
  /** Get task counters */
  public Counters getCounters() { return counters; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.TASK_FAILED;
  }

}
