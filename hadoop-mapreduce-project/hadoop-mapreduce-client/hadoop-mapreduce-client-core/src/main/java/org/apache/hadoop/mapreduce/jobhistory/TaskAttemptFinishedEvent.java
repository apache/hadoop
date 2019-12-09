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
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.util.JobHistoryEventUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.util.SystemClock;

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
  private long startTime;

  /**
   * Create an event to record successful finishes for setup and cleanup
   * attempts.
   * @param id Attempt ID
   * @param taskType Type of task
   * @param taskStatus Status of task
   * @param finishTime Finish time of attempt
   * @param hostname Host where the attempt executed
   * @param state State string
   * @param counters Counters for the attempt
   * @param startTs Task start time to be used for writing entity to ATSv2.
   */
  public TaskAttemptFinishedEvent(TaskAttemptID id, 
      TaskType taskType, String taskStatus, 
      long finishTime, String rackName,
      String hostname, String state, Counters counters, long startTs) {
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.finishTime = finishTime;
    this.rackName = rackName;
    this.hostname = hostname;
    this.state = state;
    this.counters = counters;
    this.startTime = startTs;
  }

  public TaskAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long finishTime, String rackName, String hostname,
      String state, Counters counters) {
    this(id, taskType, taskStatus, finishTime, rackName, hostname, state,
        counters, SystemClock.getInstance().getTime());
  }

  TaskAttemptFinishedEvent() {}

  public Object getDatum() {
    if (datum == null) {
      datum = new TaskAttemptFinished();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setTaskStatus(new Utf8(taskStatus));
      datum.setFinishTime(finishTime);
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setHostname(new Utf8(hostname));
      datum.setState(new Utf8(state));
      datum.setCounters(EventWriter.toAvro(counters));
    }
    return datum;
  }
  public void setDatum(Object oDatum) {
    this.datum = (TaskAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.taskStatus = datum.getTaskStatus().toString();
    this.finishTime = datum.getFinishTime();
    this.rackName = datum.getRackname().toString();
    this.hostname = datum.getHostname().toString();
    this.state = datum.getState().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
  }

  /** Gets the task ID. */
  public TaskID getTaskId() { return attemptId.getTaskID(); }
  /** Gets the task attempt id. */
  public TaskAttemptID getAttemptId() {
    return attemptId;
  }
  /** Gets the task type. */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Gets the task status. */
  public String getTaskStatus() { return taskStatus.toString(); }
  /** Gets the attempt finish time. */
  public long getFinishTime() { return finishTime; }
  /**
   * Gets the task attempt start time to be used while publishing to ATSv2.
   * @return task attempt start time.
   */
  public long getStartTime() {
    return startTime;
  }
  /** Gets the host where the attempt executed. */
  public String getHostname() { return hostname.toString(); }
  
  /** Gets the rackname where the attempt executed. */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }
  
  /**
   * Gets the state string.
   * @return task attempt state.
   */
  public String getState() { return state.toString(); }
  /** Gets the counters for the attempt. */
  Counters getCounters() { return counters; }
  /** Gets the event type. */
  public EventType getEventType() {
    // Note that the task type can be setup/map/reduce/cleanup but the 
    // attempt-type can only be map/reduce.
    return getTaskId().getTaskType() == TaskType.MAP 
           ? EventType.MAP_ATTEMPT_FINISHED
           : EventType.REDUCE_ATTEMPT_FINISHED;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("ATTEMPT_ID", getAttemptId() == null ?
        "" : getAttemptId().toString());
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("STATUS", getTaskStatus());
    tEvent.addInfo("STATE", getState());
    tEvent.addInfo("HOSTNAME", getHostname());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }
}
