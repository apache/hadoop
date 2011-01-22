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

  /**
   * Create an event to record the start of an attempt
   * @param attemptId Id of the attempt
   * @param taskType Type of task
   * @param startTime Start time of the attempt
   * @param trackerName Name of the Task Tracker where attempt is running
   * @param httpPort The port number of the tracker
   */
  public TaskAttemptStartedEvent( TaskAttemptID attemptId,  
      TaskType taskType, long startTime, String trackerName,
      int httpPort) {
    this.taskId = attemptId.getTaskID();
    this.attemptId = attemptId;
    this.startTime = startTime;
    this.taskType = taskType;
    this.trackerName = trackerName;
    this.httpPort = httpPort;
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
  /** Get the event type */
  public EventType getEventType() {
    return EventType.MAP_ATTEMPT_STARTED;
  }

}
