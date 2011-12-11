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
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

import org.apache.avro.util.Utf8;

/**
 * Event to record the successful completion of a task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskFinishedEvent implements HistoryEvent {
  private TaskFinished datum = new TaskFinished();
  
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
    datum.taskid = new Utf8(id.toString());
    datum.finishTime = finishTime;
    datum.counters = EventWriter.toAvro(counters);
    datum.taskType = new Utf8(taskType.name());
    datum.status = new Utf8(status);
  }
  
  TaskFinishedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (TaskFinished)datum;
  }

  /** Get task id */
  public TaskID getTaskId() { return TaskID.forName(datum.taskid.toString()); }
  /** Get the task finish time */
  public long getFinishTime() { return datum.finishTime; }
  /** Get task counters */
  Counters getCounters() { return EventReader.fromAvro(datum.counters); }
  /** Get task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.taskType.toString());
  }
  /** Get task status */
  public String getTaskStatus() { return datum.status.toString(); }
  /** Get event type */
  public EventType getEventType() {
    return EventType.TASK_FINISHED;
  }

  
}
