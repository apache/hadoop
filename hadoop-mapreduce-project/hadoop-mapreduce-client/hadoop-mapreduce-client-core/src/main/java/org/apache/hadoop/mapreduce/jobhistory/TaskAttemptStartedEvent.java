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

import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Event to record start of a task attempt
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptStartedEvent implements HistoryEvent {
  private TaskAttemptStarted datum = new TaskAttemptStarted();

  /**
   * Create an event to record the start of an attempt
   * @param attemptId Id of the attempt
   * @param taskType Type of task
   * @param startTime Start time of the attempt
   * @param trackerName Name of the Task Tracker where attempt is running
   * @param httpPort The port number of the tracker
   * @param shufflePort The shuffle port number of the container
   * @param containerId The containerId for the task attempt.
   * @param locality The locality of the task attempt
   * @param avataar The avataar of the task attempt
   */
  public TaskAttemptStartedEvent( TaskAttemptID attemptId,  
      TaskType taskType, long startTime, String trackerName,
      int httpPort, int shufflePort, ContainerId containerId,
      String locality, String avataar) {
    datum.setAttemptId(new Utf8(attemptId.toString()));
    datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
    datum.setStartTime(startTime);
    datum.setTaskType(new Utf8(taskType.name()));
    datum.setTrackerName(new Utf8(trackerName));
    datum.setHttpPort(httpPort);
    datum.setShufflePort(shufflePort);
    datum.setContainerId(new Utf8(containerId.toString()));
    if (locality != null) {
      datum.setLocality(new Utf8(locality));
    }
    if (avataar != null) {
      datum.setAvataar(new Utf8(avataar));
    }
  }

  // TODO Remove after MrV1 is removed.
  // Using a dummy containerId to prevent jobHistory parse failures.
  public TaskAttemptStartedEvent(TaskAttemptID attemptId, TaskType taskType,
      long startTime, String trackerName, int httpPort, int shufflePort,
      String locality, String avataar) {
    this(attemptId, taskType, startTime, trackerName, httpPort, shufflePort,
        ContainerId.fromString("container_-1_-1_-1_-1"), locality,
            avataar);
  }

  TaskAttemptStartedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (TaskAttemptStarted)datum;
  }

  /** Get the task id */
  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }
  /** Get the tracker name */
  public String getTrackerName() { return datum.getTrackerName().toString(); }
  /** Get the start time */
  public long getStartTime() { return datum.getStartTime(); }
  /** Get the task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.getTaskType().toString());
  }
  /** Get the HTTP port */
  public int getHttpPort() { return datum.getHttpPort(); }
  /** Get the shuffle port */
  public int getShufflePort() { return datum.getShufflePort(); }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() {
    return TaskAttemptID.forName(datum.getAttemptId().toString());
  }
  /** Get the event type */
  public EventType getEventType() {
    // Note that the task type can be setup/map/reduce/cleanup but the 
    // attempt-type can only be map/reduce.
   return getTaskId().getTaskType() == TaskType.MAP 
           ? EventType.MAP_ATTEMPT_STARTED 
           : EventType.REDUCE_ATTEMPT_STARTED;
  }
  /** Get the ContainerId */
  public ContainerId getContainerId() {
    return ContainerId.fromString(datum.getContainerId().toString());
  }
  /** Get the locality */
  public String getLocality() {
    if (datum.getLocality() != null) {
      return datum.getLocality().toString();
    }
    return null;
  }
  /** Get the avataar */
  public String getAvataar() {
    if (datum.getAvataar() != null) {
      return datum.getAvataar().toString();
    }
    return null;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("TASK_ATTEMPT_ID",
        getTaskAttemptId().toString());
    tEvent.addInfo("START_TIME", getStartTime());
    tEvent.addInfo("HTTP_PORT", getHttpPort());
    tEvent.addInfo("TRACKER_NAME", getTrackerName());
    tEvent.addInfo("SHUFFLE_PORT", getShufflePort());
    tEvent.addInfo("CONTAINER_ID", getContainerId() == null ?
        "" : getContainerId().toString());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}
