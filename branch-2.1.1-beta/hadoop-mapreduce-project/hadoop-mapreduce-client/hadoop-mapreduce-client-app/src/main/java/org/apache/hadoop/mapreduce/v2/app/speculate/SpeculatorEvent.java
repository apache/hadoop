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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

public class SpeculatorEvent extends AbstractEvent<Speculator.EventType> {

  // valid for ATTEMPT_STATUS_UPDATE
  private TaskAttemptStatus reportedStatus;

  // valid for TASK_CONTAINER_NEED_UPDATE
  private TaskId taskID;
  private int containersNeededChange;
  
  // valid for CREATE_JOB
  private JobId jobID;

  public SpeculatorEvent(JobId jobID, long timestamp) {
    super(Speculator.EventType.JOB_CREATE, timestamp);
    this.jobID = jobID;
  }

  public SpeculatorEvent(TaskAttemptStatus reportedStatus, long timestamp) {
    super(Speculator.EventType.ATTEMPT_STATUS_UPDATE, timestamp);
    this.reportedStatus = reportedStatus;
  }

  public SpeculatorEvent(TaskAttemptId attemptID, boolean flag, long timestamp) {
    super(Speculator.EventType.ATTEMPT_START, timestamp);
    this.reportedStatus = new TaskAttemptStatus();
    this.reportedStatus.id = attemptID;
    this.taskID = attemptID.getTaskId();
  }

  /*
   * This c'tor creates a TASK_CONTAINER_NEED_UPDATE event .
   * We send a +1 event when a task enters a state where it wants a container,
   *  and a -1 event when it either gets one or withdraws the request.
   * The per job sum of all these events is the number of containers requested
   *  but not granted.  The intent is that we only do speculations when the
   *  speculation wouldn't compete for containers with tasks which need
   *  to be run.
   */
  public SpeculatorEvent(TaskId taskID, int containersNeededChange) {
    super(Speculator.EventType.TASK_CONTAINER_NEED_UPDATE);
    this.taskID = taskID;
    this.containersNeededChange = containersNeededChange;
  }

  public TaskAttemptStatus getReportedStatus() {
    return reportedStatus;
  }

  public int containersNeededChange() {
    return containersNeededChange;
  }

  public TaskId getTaskID() {
    return taskID;
  }
  
  public JobId getJobID() {
    return jobID;
  }
}