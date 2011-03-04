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
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record successful completion of a map attempt
 *
 */
public class MapAttemptFinishedEvent  implements HistoryEvent {
  private TaskID taskId;
  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long mapFinishTime;
  private long finishTime;
  private String hostname;
  private String state;
  private JhCounters counters;
  
  /** 
   * Create an event for successful completion of map attempts
   * @param id Task Attempt ID
   * @param taskType Type of the task
   * @param taskStatus Status of the task
   * @param mapFinishTime Finish time of the map phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the map executed
   * @param state State string for the attempt
   * @param counters Counters for the attempt
   */
  public MapAttemptFinishedEvent(TaskAttemptID id, 
      TaskType taskType, String taskStatus, 
      long mapFinishTime, long finishTime,
      String hostname, String state, Counters counters) {
    this.taskId = id.getTaskID();
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.mapFinishTime = mapFinishTime;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.state = state;
    this.counters = new JhCounters(counters, "COUNTERS");
  }

  /** Get the task ID */
  public TaskID getTaskId() { return taskId; }
  /** Get the attempt id */
  public TaskAttemptID getAttemptId() {
    return attemptId;
  }
  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the task status */
  public String getTaskStatus() { return taskStatus; }
  /** Get the map phase finish time */
  public long getMapFinishTime() { return mapFinishTime; }
  /** Get the attempt finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the host name */
  public String getHostname() { return hostname; }
  /** Get the state string */
  public String getState() { return state; }
  /** Get the counters */
  public JhCounters getCounters() { return counters; }
  /** Get the event type */
   public EventType getEventType() {
    return EventType.MAP_ATTEMPT_FINISHED;
  }
  
}
