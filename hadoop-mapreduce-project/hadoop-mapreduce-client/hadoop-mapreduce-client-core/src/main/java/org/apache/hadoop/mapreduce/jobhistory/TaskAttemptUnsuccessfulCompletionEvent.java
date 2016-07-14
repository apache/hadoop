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
import org.apache.hadoop.mapred.ProgressSplitsBlock;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.util.JobHistoryEventUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * Event to record unsuccessful (Killed/Failed) completion of task attempts
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptUnsuccessfulCompletionEvent implements HistoryEvent {

  private TaskAttemptUnsuccessfulCompletion datum = null;

  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String status;
  private long finishTime;
  private String hostname;
  private int port;
  private String rackName;
  private String error;
  private Counters counters;
  int[][] allSplits;
  int[] clockSplits;
  int[] cpuUsages;
  int[] vMemKbytes;
  int[] physMemKbytes;
  private static final Counters EMPTY_COUNTERS = new Counters();

  /**
   * Create an event to record the unsuccessful completion of attempts
   * @param id Attempt ID
   * @param taskType Type of the task
   * @param status Status of the attempt
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param port rpc port for for the tracker
   * @param rackName Name of the rack where the attempt executed
   * @param error Error string
   * @param counters Counters for the attempt
   * @param allSplits the "splits", or a pixelated graph of various
   *        measurable worker node state variables against progress.
   *        Currently there are four; wallclock time, CPU time,
   *        virtual memory and physical memory.
   */
  public TaskAttemptUnsuccessfulCompletionEvent
       (TaskAttemptID id, TaskType taskType,
        String status, long finishTime,
        String hostname, int port, String rackName,
        String error, Counters counters, int[][] allSplits) {
    this.attemptId = id;
    this.taskType = taskType;
    this.status = status;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.port = port;
    this.rackName = rackName;
    this.error = error;
    this.counters = counters;
    this.allSplits = allSplits;
    this.clockSplits =
        ProgressSplitsBlock.arrayGetWallclockTime(allSplits);
    this.cpuUsages =
        ProgressSplitsBlock.arrayGetCPUTime(allSplits);
    this.vMemKbytes =
        ProgressSplitsBlock.arrayGetVMemKbytes(allSplits);
    this.physMemKbytes =
        ProgressSplitsBlock.arrayGetPhysMemKbytes(allSplits);
  }

  /**
   * @deprecated please use the constructor with an additional
   *              argument, an array of splits arrays instead.  See
   *              {@link org.apache.hadoop.mapred.ProgressSplitsBlock}
   *              for an explanation of the meaning of that parameter.
   *
   * Create an event to record the unsuccessful completion of attempts
   * @param id Attempt ID
   * @param taskType Type of the task
   * @param status Status of the attempt
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param error Error string
   */
  public TaskAttemptUnsuccessfulCompletionEvent
       (TaskAttemptID id, TaskType taskType,
        String status, long finishTime,
        String hostname, String error) {
    this(id, taskType, status, finishTime, hostname, -1, "",
        error, EMPTY_COUNTERS, null);
  }

  public TaskAttemptUnsuccessfulCompletionEvent
      (TaskAttemptID id, TaskType taskType,
       String status, long finishTime,
       String hostname, int port, String rackName,
       String error, int[][] allSplits) {
    this(id, taskType, status, finishTime, hostname, port,
        rackName, error, EMPTY_COUNTERS, allSplits);
  }

  TaskAttemptUnsuccessfulCompletionEvent() {}

  public Object getDatum() {
    if(datum == null) {
      datum = new TaskAttemptUnsuccessfulCompletion();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setFinishTime(finishTime);
      datum.setHostname(new Utf8(hostname));
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setPort(port);
      datum.setError(new Utf8(error));
      datum.setStatus(new Utf8(status));

      datum.setCounters(EventWriter.toAvro(counters));

      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }



  public void setDatum(Object odatum) {
    this.datum =
        (TaskAttemptUnsuccessfulCompletion)odatum;
    this.attemptId =
        TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType =
        TaskType.valueOf(datum.getTaskType().toString());
    this.finishTime = datum.getFinishTime();
    this.hostname = datum.getHostname().toString();
    this.rackName = datum.getRackname().toString();
    this.port = datum.getPort();
    this.status = datum.getStatus().toString();
    this.error = datum.getError().toString();
    this.counters =
        EventReader.fromAvro(datum.getCounters());
    this.clockSplits =
        AvroArrayUtils.fromAvro(datum.getClockSplits());
    this.cpuUsages =
        AvroArrayUtils.fromAvro(datum.getCpuUsages());
    this.vMemKbytes =
        AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    this.physMemKbytes =
        AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }

  /** Get the task id */
  public TaskID getTaskId() {
    return attemptId.getTaskID();
  }
  /** Get the task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(taskType.toString());
  }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() {
    return attemptId;
  }
  /** Get the finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the name of the host where the attempt executed */
  public String getHostname() { return hostname; }
  /** Get the rpc port for the host where the attempt executed */
  public int getPort() { return port; }

  /** Get the rack name of the node where the attempt ran */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }

  /** Get the error string */
  public String getError() { return error.toString(); }
  /** Get the task status */
  public String getTaskStatus() {
    return status.toString();
  }
  /** Get the counters */
  Counters getCounters() { return counters; }
  /** Get the event type */
  public EventType getEventType() {
    // Note that the task type can be setup/map/reduce/cleanup but the
    // attempt-type can only be map/reduce.
    // find out if the task failed or got killed
    boolean failed = TaskStatus.State.FAILED.toString().equals(getTaskStatus());
    return getTaskId().getTaskType() == TaskType.MAP
           ? (failed
              ? EventType.MAP_ATTEMPT_FAILED
              : EventType.MAP_ATTEMPT_KILLED)
           : (failed
              ? EventType.REDUCE_ATTEMPT_FAILED
              : EventType.REDUCE_ATTEMPT_KILLED);
  }



  public int[] getClockSplits() {
    return clockSplits;
  }
  public int[] getCpuUsages() {
    return cpuUsages;
  }
  public int[] getVMemKbytes() {
    return vMemKbytes;
  }
  public int[] getPhysMemKbytes() {
    return physMemKbytes;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("TASK_ATTEMPT_ID", getTaskAttemptId() == null ?
        "" : getTaskAttemptId().toString());
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("ERROR", getError());
    tEvent.addInfo("STATUS", getTaskStatus());
    tEvent.addInfo("HOSTNAME", getHostname());
    tEvent.addInfo("PORT", getPort());
    tEvent.addInfo("RACK_NAME", getRackName());
    tEvent.addInfo("SHUFFLE_FINISH_TIME", getFinishTime());
    tEvent.addInfo("SORT_FINISH_TIME", getFinishTime());
    tEvent.addInfo("MAP_FINISH_TIME", getFinishTime());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }
}
