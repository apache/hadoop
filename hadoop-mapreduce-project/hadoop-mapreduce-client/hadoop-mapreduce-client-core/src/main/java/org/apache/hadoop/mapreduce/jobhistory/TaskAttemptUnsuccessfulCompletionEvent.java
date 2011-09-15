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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

import org.apache.hadoop.mapred.ProgressSplitsBlock;

import org.apache.avro.util.Utf8;

/**
 * Event to record unsuccessful (Killed/Failed) completion of task attempts
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptUnsuccessfulCompletionEvent implements HistoryEvent {
  private TaskAttemptUnsuccessfulCompletion datum =
    new TaskAttemptUnsuccessfulCompletion();

  /** 
   * Create an event to record the unsuccessful completion of attempts
   * @param id Attempt ID
   * @param taskType Type of the task
   * @param status Status of the attempt
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param error Error string
   * @param allSplits the "splits", or a pixelated graph of various
   *        measurable worker node state variables against progress.
   *        Currently there are four; wallclock time, CPU time,
   *        virtual memory and physical memory.  
   */
  public TaskAttemptUnsuccessfulCompletionEvent
       (TaskAttemptID id, TaskType taskType,
        String status, long finishTime, 
        String hostname, String error,
        int[][] allSplits) {
    datum.taskid = new Utf8(id.getTaskID().toString());
    datum.taskType = new Utf8(taskType.name());
    datum.attemptId = new Utf8(id.toString());
    datum.finishTime = finishTime;
    datum.hostname = new Utf8(hostname);
    datum.error = new Utf8(error);
    datum.status = new Utf8(status);

    datum.clockSplits 
      = AvroArrayUtils.toAvro
           (ProgressSplitsBlock.arrayGetWallclockTime(allSplits));
    datum.cpuUsages 
      = AvroArrayUtils.toAvro
           (ProgressSplitsBlock.arrayGetCPUTime(allSplits));
    datum.vMemKbytes 
      = AvroArrayUtils.toAvro
           (ProgressSplitsBlock.arrayGetVMemKbytes(allSplits));
    datum.physMemKbytes 
      = AvroArrayUtils.toAvro
           (ProgressSplitsBlock.arrayGetPhysMemKbytes(allSplits));
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
    this(id, taskType, status, finishTime, hostname, error, null);
  }

  TaskAttemptUnsuccessfulCompletionEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (TaskAttemptUnsuccessfulCompletion)datum;
  }

  /** Get the task id */
  public TaskID getTaskId() { return TaskID.forName(datum.taskid.toString()); }
  /** Get the task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.taskType.toString());
  }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() {
    return TaskAttemptID.forName(datum.attemptId.toString());
  }
  /** Get the finish time */
  public long getFinishTime() { return datum.finishTime; }
  /** Get the name of the host where the attempt executed */
  public String getHostname() { return datum.hostname.toString(); }
  /** Get the error string */
  public String getError() { return datum.error.toString(); }
  /** Get the task status */
  public String getTaskStatus() { return datum.status.toString(); }
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
    return AvroArrayUtils.fromAvro(datum.clockSplits);
  }
  public int[] getCpuUsages() {
    return AvroArrayUtils.fromAvro(datum.cpuUsages);
  }
  public int[] getVMemKbytes() {
    return AvroArrayUtils.fromAvro(datum.vMemKbytes);
  }
  public int[] getPhysMemKbytes() {
    return AvroArrayUtils.fromAvro(datum.physMemKbytes);
  }

}
