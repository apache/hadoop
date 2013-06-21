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

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record start of a task attempt
 *
 */
public class TaskAttemptStartedEvent implements HistoryEvent {
  private TaskID taskId;
  private TaskAttemptID attemptId;
  private long startTime;
  private TaskType taskType;
  private String trackerName;
  private int httpPort;
  private String locality;
  private String avataar;

  /**
   * Create an event to record the start of an attempt
   * @param attemptId Id of the attempt
   * @param taskType Type of task
   * @param startTime Start time of the attempt
   * @param trackerName Name of the Task Tracker where attempt is running
   * @param httpPort The port number of the tracker
   * @param locality the locality of the task attempt
   * @param avataar the avataar of the task attempt
   */
  public TaskAttemptStartedEvent( TaskAttemptID attemptId,  
      TaskType taskType, long startTime, String trackerName,
      int httpPort, String locality, String avataar) {
    this.taskId = attemptId.getTaskID();
    this.attemptId = attemptId;
    this.startTime = startTime;
    this.taskType = taskType;
    this.trackerName = trackerName;
    this.httpPort = httpPort;
    this.locality = locality;
    this.avataar = avataar;
  }

  /** Get the task id */
  public TaskID getTaskId() { return taskId; }
  /** Get the tracker name */
  public String getTrackerName() { return trackerName; }
  /** Get the start time */
  public long getStartTime() { return startTime; }
  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the HTTP port */
  public int getHttpPort() { return httpPort; }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() {
    return attemptId;
  }
  /** Get the locality of the task attempt */
  public String getLocality() {
    return locality;
  }
  /** Get the avataar of the task attempt */
  public String getAvataar() {
    return avataar;
  }
  /** Get the event type */
  public EventType getEventType() {
    if (taskType == TaskType.JOB_SETUP) {
      return EventType.SETUP_ATTEMPT_STARTED;
    } else if (taskType == TaskType.JOB_CLEANUP) {
      return EventType.CLEANUP_ATTEMPT_STARTED;
    }
    return attemptId.isMap() ? 
        EventType.MAP_ATTEMPT_STARTED : EventType.REDUCE_ATTEMPT_STARTED;
  }

}
