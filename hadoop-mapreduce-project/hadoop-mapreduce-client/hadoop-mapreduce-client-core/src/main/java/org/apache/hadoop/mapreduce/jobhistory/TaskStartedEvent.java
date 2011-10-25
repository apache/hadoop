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
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record the start of a task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskStartedEvent implements HistoryEvent {
  private TaskStarted datum = new TaskStarted();

  /**
   * Create an event to record start of a task
   * @param id Task Id
   * @param startTime Start time of the task
   * @param taskType Type of the task
   * @param splitLocations Split locations, applicable for map tasks
   */
  public TaskStartedEvent(TaskID id, long startTime, 
      TaskType taskType, String splitLocations) {
    datum.taskid = new Utf8(id.toString());
    datum.splitLocations = new Utf8(splitLocations);
    datum.startTime = startTime;
    datum.taskType = new Utf8(taskType.name());
  }

  TaskStartedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) { this.datum = (TaskStarted)datum; }

  /** Get the task id */
  public TaskID getTaskId() { return TaskID.forName(datum.taskid.toString()); }
  /** Get the split locations, applicable for map tasks */
  public String getSplitLocations() { return datum.splitLocations.toString(); }
  /** Get the start time of the task */
  public long getStartTime() { return datum.startTime; }
  /** Get the task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.taskType.toString());
  }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.TASK_STARTED;
  }

}
