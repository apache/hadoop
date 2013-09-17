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
package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * A report on the state of a task. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TaskReport extends org.apache.hadoop.mapreduce.TaskReport {
  
  public TaskReport() {
    super();
  }
  
  /**
   * Creates a new TaskReport object
   * @param taskid
   * @param progress
   * @param state
   * @param diagnostics
   * @param startTime
   * @param finishTime
   * @param counters
   * @deprecated
   */
  @Deprecated
  TaskReport(TaskID taskid, float progress, String state,
      String[] diagnostics, long startTime, long finishTime,
      Counters counters) {
    this(taskid, progress, state, diagnostics, null, startTime, finishTime, 
        counters);
  }
  
  /**
   * Creates a new TaskReport object
   * @param taskid
   * @param progress
   * @param state
   * @param diagnostics
   * @param currentStatus
   * @param startTime
   * @param finishTime
   * @param counters
   */
  TaskReport(TaskID taskid, float progress, String state,
             String[] diagnostics, TIPStatus currentStatus, 
             long startTime, long finishTime,
             Counters counters) {
    super(taskid, progress, state, diagnostics, currentStatus, startTime,
      finishTime, new org.apache.hadoop.mapreduce.Counters(counters));
  }
  
  static TaskReport downgrade(
      org.apache.hadoop.mapreduce.TaskReport report) {
    return new TaskReport(TaskID.downgrade(report.getTaskID()),
      report.getProgress(), report.getState(), report.getDiagnostics(),
      report.getCurrentStatus(), report.getStartTime(), report.getFinishTime(),
      Counters.downgrade(report.getTaskCounters()));
  }
  
  static TaskReport[] downgradeArray(org.apache.hadoop.
      mapreduce.TaskReport[] reports) {
    List<TaskReport> ret = new ArrayList<TaskReport>();
    for (org.apache.hadoop.mapreduce.TaskReport report : reports) {
      ret.add(downgrade(report));
    }
    return ret.toArray(new TaskReport[0]);
  }
  
  /** The string of the task id. */
  public String getTaskId() {
    return TaskID.downgrade(super.getTaskID()).toString();
  }

  /** The id of the task. */
  public TaskID getTaskID() {
    return TaskID.downgrade(super.getTaskID());
  }

  public Counters getCounters() { 
    return Counters.downgrade(super.getTaskCounters()); 
  }
  
  /** 
   * set successful attempt ID of the task. 
   */ 
  public void setSuccessfulAttempt(TaskAttemptID t) {
    super.setSuccessfulAttemptId(t);
  }
  /**
   * Get the attempt ID that took this task to completion
   */
  public TaskAttemptID getSuccessfulTaskAttempt() {
    return TaskAttemptID.downgrade(super.getSuccessfulTaskAttemptId());
  }
  /** 
   * set running attempt(s) of the task. 
   */ 
  public void setRunningTaskAttempts(
      Collection<TaskAttemptID> runningAttempts) {
    Collection<org.apache.hadoop.mapreduce.TaskAttemptID> attempts = 
      new ArrayList<org.apache.hadoop.mapreduce.TaskAttemptID>();
    for (TaskAttemptID id : runningAttempts) {
      attempts.add(id);
    }
    super.setRunningTaskAttemptIds(attempts);
  }
  /**
   * Get the running task attempt IDs for this task
   */
  public Collection<TaskAttemptID> getRunningTaskAttempts() {
    Collection<TaskAttemptID> attempts = new ArrayList<TaskAttemptID>();
    for (org.apache.hadoop.mapreduce.TaskAttemptID id : 
         super.getRunningTaskAttemptIds()) {
      attempts.add(TaskAttemptID.downgrade(id));
    }
    return attempts;
  }
  
  /** 
   * set finish time of task. 
   * @param finishTime finish time of task. 
   */
  protected void setFinishTime(long finishTime) {
    super.setFinishTime(finishTime);
  }

  /** 
   * set start time of the task. 
   */ 
  protected void setStartTime(long startTime) {
    super.setStartTime(startTime);
  }

}
