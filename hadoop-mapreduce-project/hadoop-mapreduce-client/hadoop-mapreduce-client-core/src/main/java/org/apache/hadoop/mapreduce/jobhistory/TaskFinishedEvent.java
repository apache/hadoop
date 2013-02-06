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

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record the successful completion of a task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskFinishedEvent implements HistoryEvent {

  private TaskFinished datum = null;

  private TaskID taskid;
  private TaskAttemptID successfulAttemptId;
  private long finishTime;
  private TaskType taskType;
  private String status;
  private Counters counters;
  
  /**
   * Create an event to record the successful completion of a task
   * @param id Task ID
   * @param attemptId Task Attempt ID of the successful attempt for this task
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param status Status string
   * @param counters Counters for the task
   */
  public TaskFinishedEvent(TaskID id, TaskAttemptID attemptId, long finishTime,
                           TaskType taskType,
                           String status, Counters counters) {
    this.taskid = id;
    this.successfulAttemptId = attemptId;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.status = status;
    this.counters = counters;
  }
  
  TaskFinishedEvent() {}

  public Object getDatum() {
    if (datum == null) {
      datum = new TaskFinished();
      datum.taskid = new Utf8(taskid.toString());
      if(successfulAttemptId != null)
      {
        datum.successfulAttemptId = new Utf8(successfulAttemptId.toString());
      }
      datum.finishTime = finishTime;
      datum.counters = EventWriter.toAvro(counters);
      datum.taskType = new Utf8(taskType.name());
      datum.status = new Utf8(status);
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (TaskFinished)oDatum;
    this.taskid = TaskID.forName(datum.taskid.toString());
    if (datum.successfulAttemptId != null) {
      this.successfulAttemptId = TaskAttemptID
          .forName(datum.successfulAttemptId.toString());
    }
    this.finishTime = datum.finishTime;
    this.taskType = TaskType.valueOf(datum.taskType.toString());
    this.status = datum.status.toString();
    this.counters = EventReader.fromAvro(datum.counters);
  }

  /** Get task id */
  public TaskID getTaskId() { return taskid; }
  /** Get successful task attempt id */
  public TaskAttemptID getSuccessfulTaskAttemptId() {
    return successfulAttemptId;
  }
  /** Get the task finish time */
  public long getFinishTime() { return finishTime; }
  /** Get task counters */
  public Counters getCounters() { return counters; }
  /** Get task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get task status */
  public String getTaskStatus() { return status.toString(); }
  /** Get event type */
  public EventType getEventType() {
    return EventType.TASK_FINISHED;
  }

  
}
