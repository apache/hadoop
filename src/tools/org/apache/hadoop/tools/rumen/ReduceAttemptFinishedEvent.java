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

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record successful completion of a reduce attempt
 *
 */
public class ReduceAttemptFinishedEvent  implements HistoryEvent {
  private TaskID taskId;
  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long shuffleFinishTime;
  private long sortFinishTime;
  private long finishTime;
  private String hostname;
  private String state;
  private JhCounters counters;

  /**
   * Create an event to record completion of a reduce attempt
   * @param id Attempt Id
   * @param taskType Type of task
   * @param taskStatus Status of the task
   * @param shuffleFinishTime Finish time of the shuffle phase
   * @param sortFinishTime Finish time of the sort phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param state State of the attempt
   * @param counters Counters for the attempt
   */
  public ReduceAttemptFinishedEvent(TaskAttemptID id, 
      TaskType taskType, String taskStatus, 
      long shuffleFinishTime, long sortFinishTime, 
      long finishTime,
      String hostname, String state, Counters counters) {
    this.taskId = id.getTaskID();
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.shuffleFinishTime = shuffleFinishTime;
    this.sortFinishTime = sortFinishTime;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.state = state;
    this.counters = new JhCounters(counters, "COUNTERS");
  }

  /** Get the Task ID */
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
  /** Get the finish time of the sort phase */
  public long getSortFinishTime() { return sortFinishTime; }
  /** Get the finish time of the shuffle phase */
  public long getShuffleFinishTime() { return shuffleFinishTime; }
  /** Get the finish time of the attempt */
  public long getFinishTime() { return finishTime; }
  /** Get the name of the host where the attempt ran */
  public String getHostname() { return hostname; }
  /** Get the state string */
  public String getState() { return state; }
  /** Get the counters for the attempt */
  public JhCounters getCounters() { return counters; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.REDUCE_ATTEMPT_FINISHED;
  }


}
