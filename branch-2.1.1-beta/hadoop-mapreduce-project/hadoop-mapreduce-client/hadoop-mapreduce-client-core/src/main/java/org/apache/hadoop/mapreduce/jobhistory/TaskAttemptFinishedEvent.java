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
 * Event to record successful task completion
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptFinishedEvent  implements HistoryEvent {

  private TaskAttemptFinished datum = null;

  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long finishTime;
  private String rackName;
  private String hostname;
  private String state;
  private Counters counters;

  /**
   * Create an event to record successful finishes for setup and cleanup 
   * attempts
   * @param id Attempt ID
   * @param taskType Type of task
   * @param taskStatus Status of task
   * @param finishTime Finish time of attempt
   * @param hostname Host where the attempt executed
   * @param state State string
   * @param counters Counters for the attempt
   */
  public TaskAttemptFinishedEvent(TaskAttemptID id, 
      TaskType taskType, String taskStatus, 
      long finishTime, String rackName,
      String hostname, String state, Counters counters) {
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.finishTime = finishTime;
    this.rackName = rackName;
    this.hostname = hostname;
    this.state = state;
    this.counters = counters;
  }

  TaskAttemptFinishedEvent() {}

  public Object getDatum() {
    if (datum == null) {
      datum = new TaskAttemptFinished();
      datum.taskid = new Utf8(attemptId.getTaskID().toString());
      datum.attemptId = new Utf8(attemptId.toString());
      datum.taskType = new Utf8(taskType.name());
      datum.taskStatus = new Utf8(taskStatus);
      datum.finishTime = finishTime;
      if (rackName != null) {
        datum.rackname = new Utf8(rackName);
      }
      datum.hostname = new Utf8(hostname);
      datum.state = new Utf8(state);
      datum.counters = EventWriter.toAvro(counters);
    }
    return datum;
  }
  public void setDatum(Object oDatum) {
    this.datum = (TaskAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.attemptId.toString());
    this.taskType = TaskType.valueOf(datum.taskType.toString());
    this.taskStatus = datum.taskStatus.toString();
    this.finishTime = datum.finishTime;
    this.rackName = datum.rackname.toString();
    this.hostname = datum.hostname.toString();
    this.state = datum.state.toString();
    this.counters = EventReader.fromAvro(datum.counters);
  }

  /** Get the task ID */
  public TaskID getTaskId() { return attemptId.getTaskID(); }
  /** Get the task attempt id */
  public TaskAttemptID getAttemptId() {
    return attemptId;
  }
  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the task status */
  public String getTaskStatus() { return taskStatus.toString(); }
  /** Get the attempt finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the host where the attempt executed */
  public String getHostname() { return hostname.toString(); }
  
  /** Get the rackname where the attempt executed */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }
  
  /** Get the state string */
  public String getState() { return state.toString(); }
  /** Get the counters for the attempt */
  Counters getCounters() { return counters; }
  /** Get the event type */
  public EventType getEventType() {
    // Note that the task type can be setup/map/reduce/cleanup but the 
    // attempt-type can only be map/reduce.
    return getTaskId().getTaskType() == TaskType.MAP 
           ? EventType.MAP_ATTEMPT_FINISHED
           : EventType.REDUCE_ATTEMPT_FINISHED;
  }

}
