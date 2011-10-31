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

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.ProgressSplitsBlock;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record successful completion of a reduce attempt
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceAttemptFinishedEvent  implements HistoryEvent {
  private ReduceAttemptFinished datum =
    new ReduceAttemptFinished();

  /**
   * Create an event to record completion of a reduce attempt
   * @param id Attempt Id
   * @param taskType Type of task
   * @param taskStatus Status of the task
   * @param shuffleFinishTime Finish time of the shuffle phase
   * @param sortFinishTime Finish time of the sort phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param rackName Name of the rack where the attempt executed
   * @param state State of the attempt
   * @param counters Counters for the attempt
   * @param allSplits the "splits", or a pixelated graph of various
   *        measurable worker node state variables against progress.
   *        Currently there are four; wallclock time, CPU time,
   *        virtual memory and physical memory.  
   */
  public ReduceAttemptFinishedEvent
    (TaskAttemptID id, TaskType taskType, String taskStatus, 
     long shuffleFinishTime, long sortFinishTime, long finishTime,
     String hostname, String rackName, String state, Counters counters,
     int[][] allSplits) {
    datum.taskid = new Utf8(id.getTaskID().toString());
    datum.attemptId = new Utf8(id.toString());
    datum.taskType = new Utf8(taskType.name());
    datum.taskStatus = new Utf8(taskStatus);
    datum.shuffleFinishTime = shuffleFinishTime;
    datum.sortFinishTime = sortFinishTime;
    datum.finishTime = finishTime;
    datum.hostname = new Utf8(hostname);
    datum.rackname = new Utf8(rackName);
    datum.state = new Utf8(state);
    datum.counters = EventWriter.toAvro(counters);

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
   * Create an event to record completion of a reduce attempt
   * @param id Attempt Id
   * @param taskType Type of task
   * @param taskStatus Status of the task
   * @param shuffleFinishTime Finish time of the shuffle phase
   * @param sortFinishTime Finish time of the sort phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param state State of the attempt
   * @param counters Counters for the attempt
   */
  public ReduceAttemptFinishedEvent
    (TaskAttemptID id, TaskType taskType, String taskStatus, 
     long shuffleFinishTime, long sortFinishTime, long finishTime,
     String hostname, String state, Counters counters) {
    this(id, taskType, taskStatus,
         shuffleFinishTime, sortFinishTime, finishTime,
         hostname, null, state, counters, null);
  }

  ReduceAttemptFinishedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (ReduceAttemptFinished)datum;
  }

  /** Get the Task ID */
  public TaskID getTaskId() { return TaskID.forName(datum.taskid.toString()); }
  /** Get the attempt id */
  public TaskAttemptID getAttemptId() {
    return TaskAttemptID.forName(datum.attemptId.toString());
  }
  /** Get the task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.taskType.toString());
  }
  /** Get the task status */
  public String getTaskStatus() { return datum.taskStatus.toString(); }
  /** Get the finish time of the sort phase */
  public long getSortFinishTime() { return datum.sortFinishTime; }
  /** Get the finish time of the shuffle phase */
  public long getShuffleFinishTime() { return datum.shuffleFinishTime; }
  /** Get the finish time of the attempt */
  public long getFinishTime() { return datum.finishTime; }
  /** Get the name of the host where the attempt ran */
  public String getHostname() { return datum.hostname.toString(); }
  /** Get the rack name of the node where the attempt ran */
  public String getRackName() { return datum.rackname.toString(); }
  /** Get the state string */
  public String getState() { return datum.state.toString(); }
  /** Get the counters for the attempt */
  Counters getCounters() { return EventReader.fromAvro(datum.counters); }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.REDUCE_ATTEMPT_FINISHED;
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
