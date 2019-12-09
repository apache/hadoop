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
import org.apache.hadoop.mapred.TaskStatus;
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
 * Event to record the failure of a task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskFailedEvent implements HistoryEvent {
  private TaskFailed datum = null;

  private TaskAttemptID failedDueToAttempt;
  private TaskID id;
  private TaskType taskType;
  private long finishTime;
  private String status;
  private String error;
  private Counters counters;
  private long startTime;

  private static final Counters EMPTY_COUNTERS = new Counters();

  /**
   * Create an event to record task failure.
   * @param id Task ID
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param error Error String
   * @param status Status
   * @param failedDueToAttempt The attempt id due to which the task failed
   * @param counters Counters for the task
   * @param startTs task start time.
   */
  public TaskFailedEvent(TaskID id, long finishTime, 
      TaskType taskType, String error, String status,
      TaskAttemptID failedDueToAttempt, Counters counters, long startTs) {
    this.id = id;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.error = error;
    this.status = status;
    this.failedDueToAttempt = failedDueToAttempt;
    this.counters = counters;
    this.startTime = startTs;
  }

  public TaskFailedEvent(TaskID id, long finishTime, TaskType taskType,
      String error, String status, TaskAttemptID failedDueToAttempt,
      Counters counters) {
    this(id, finishTime, taskType, error, status, failedDueToAttempt, counters,
        SystemClock.getInstance().getTime());
  }

  public TaskFailedEvent(TaskID id, long finishTime, 
      TaskType taskType, String error, String status,
      TaskAttemptID failedDueToAttempt) {
    this(id, finishTime, taskType, error, status, failedDueToAttempt,
        EMPTY_COUNTERS);
  }

  TaskFailedEvent() {}

  public Object getDatum() {
    if(datum == null) {
      datum = new TaskFailed();
      datum.setTaskid(new Utf8(id.toString()));
      datum.setError(new Utf8(error));
      datum.setFinishTime(finishTime);
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setFailedDueToAttempt(
          failedDueToAttempt == null
          ? null
          : new Utf8(failedDueToAttempt.toString()));
      datum.setStatus(new Utf8(status));
      datum.setCounters(EventWriter.toAvro(counters));
    }
    return datum;
  }
  
  public void setDatum(Object odatum) {
    this.datum = (TaskFailed)odatum;
    this.id =
        TaskID.forName(datum.getTaskid().toString());
    this.taskType =
        TaskType.valueOf(datum.getTaskType().toString());
    this.finishTime = datum.getFinishTime();
    this.error = datum.getError().toString();
    this.failedDueToAttempt =
        datum.getFailedDueToAttempt() == null
        ? null
        : TaskAttemptID.forName(
            datum.getFailedDueToAttempt().toString());
    this.status = datum.getStatus().toString();
    this.counters =
        EventReader.fromAvro(datum.getCounters());
  }

  /** Gets the task id. */
  public TaskID getTaskId() { return id; }
  /** Gets the error string. */
  public String getError() { return error; }
  /** Gets the finish time of the attempt. */
  public long getFinishTime() {
    return finishTime;
  }
  /**
   * Gets the task start time to be reported to ATSv2.
   * @return task start time.
   */
  public long getStartTime() {
    return startTime;
  }
  /** Gets the task type. */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Gets the attempt id due to which the task failed. */
  public TaskAttemptID getFailedAttemptID() {
    return failedDueToAttempt;
  }
  /**
   * Gets the task status.
   * @return task status
   */
  public String getTaskStatus() { return status; }
  /** Gets task counters. */
  public Counters getCounters() { return counters; }
  /** Gets the event type. */
  public EventType getEventType() {
    return EventType.TASK_FAILED;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("STATUS", TaskStatus.State.FAILED.toString());
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("ERROR", getError());
    tEvent.addInfo("FAILED_ATTEMPT_ID",
        getFailedAttemptID() == null ? "" : getFailedAttemptID().toString());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }
}
