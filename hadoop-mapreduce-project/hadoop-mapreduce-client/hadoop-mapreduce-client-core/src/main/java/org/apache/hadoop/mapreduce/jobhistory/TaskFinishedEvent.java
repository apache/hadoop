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
 * Event to record the successful completion of a task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskFinishedEvent implements HistoryEvent {

  private TaskFinished datum = null;

  private TaskID taskid;
  private TaskAttemptID successfulAttemptId;
  private long finishTime;
  private TaskType taskType;
  private String status;
  private Counters counters;
  private long startTime;

  /**
   * Create an event to record the successful completion of a task.
   * @param id Task ID
   * @param attemptId Task Attempt ID of the successful attempt for this task
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param status Status string
   * @param counters Counters for the task
   * @param startTs task start time
   */
  public TaskFinishedEvent(TaskID id, TaskAttemptID attemptId, long finishTime,
                           TaskType taskType,
                           String status, Counters counters, long startTs) {
    this.taskid = id;
    this.successfulAttemptId = attemptId;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.status = status;
    this.counters = counters;
    this.startTime = startTs;
  }

  public TaskFinishedEvent(TaskID id, TaskAttemptID attemptId, long finishTime,
          TaskType taskType, String status, Counters counters) {
    this(id, attemptId, finishTime, taskType, status, counters,
        SystemClock.getInstance().getTime());
  }

  TaskFinishedEvent() {}

  public Object getDatum() {
    if (datum == null) {
      datum = new TaskFinished();
      datum.setTaskid(new Utf8(taskid.toString()));
      if(successfulAttemptId != null)
      {
        datum.setSuccessfulAttemptId(new Utf8(successfulAttemptId.toString()));
      }
      datum.setFinishTime(finishTime);
      datum.setCounters(EventWriter.toAvro(counters));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setStatus(new Utf8(status));
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (TaskFinished)oDatum;
    this.taskid = TaskID.forName(datum.getTaskid().toString());
    if (datum.getSuccessfulAttemptId() != null) {
      this.successfulAttemptId = TaskAttemptID
          .forName(datum.getSuccessfulAttemptId().toString());
    }
    this.finishTime = datum.getFinishTime();
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.status = datum.getStatus().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
  }

  /** Gets task id. */
  public TaskID getTaskId() { return taskid; }
  /** Gets successful task attempt id. */
  public TaskAttemptID getSuccessfulTaskAttemptId() {
    return successfulAttemptId;
  }
  /** Gets the task finish time. */
  public long getFinishTime() { return finishTime; }
  /**
   * Gets the task start time to be reported to ATSv2.
   * @return task start time
   */
  public long getStartTime() {
    return startTime;
  }
  /** Gets task counters. */
  public Counters getCounters() { return counters; }
  /** Gets task type. */
  public TaskType getTaskType() {
    return taskType;
  }
  /**
   * Gets task status.
   * @return task status
   */
  public String getTaskStatus() { return status.toString(); }
  /** Gets event type. */
  public EventType getEventType() {
    return EventType.TASK_FINISHED;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("STATUS", TaskStatus.State.SUCCEEDED.toString());
    tEvent.addInfo("SUCCESSFUL_TASK_ATTEMPT_ID",
        getSuccessfulTaskAttemptId() == null ? "" :
            getSuccessfulTaskAttemptId().toString());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> jobMetrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return jobMetrics;
  }
}
