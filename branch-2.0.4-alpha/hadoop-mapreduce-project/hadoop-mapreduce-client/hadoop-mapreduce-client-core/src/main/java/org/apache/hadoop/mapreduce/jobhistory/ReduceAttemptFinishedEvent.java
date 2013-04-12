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

  private ReduceAttemptFinished datum = null;

  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long shuffleFinishTime;
  private long sortFinishTime;
  private long finishTime;
  private String hostname;
  private String rackName;
  private int port;
  private String state;
  private Counters counters;
  int[][] allSplits;
  int[] clockSplits;
  int[] cpuUsages;
  int[] vMemKbytes;
  int[] physMemKbytes;

  /**
   * Create an event to record completion of a reduce attempt
   * @param id Attempt Id
   * @param taskType Type of task
   * @param taskStatus Status of the task
   * @param shuffleFinishTime Finish time of the shuffle phase
   * @param sortFinishTime Finish time of the sort phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param port RPC port for the tracker host.
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
     String hostname, int port,  String rackName, String state, 
     Counters counters, int[][] allSplits) {
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.shuffleFinishTime = shuffleFinishTime;
    this.sortFinishTime = sortFinishTime;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.rackName = rackName;
    this.port = port;
    this.state = state;
    this.counters = counters;
    this.allSplits = allSplits;
    this.clockSplits = ProgressSplitsBlock.arrayGetWallclockTime(allSplits);
    this.cpuUsages = ProgressSplitsBlock.arrayGetCPUTime(allSplits);
    this.vMemKbytes = ProgressSplitsBlock.arrayGetVMemKbytes(allSplits);
    this.physMemKbytes = ProgressSplitsBlock.arrayGetPhysMemKbytes(allSplits);
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
         hostname, -1, "", state, counters, null);
  }

  ReduceAttemptFinishedEvent() {}

  public Object getDatum() {
    if (datum == null) {
      datum = new ReduceAttemptFinished();
      datum.taskid = new Utf8(attemptId.getTaskID().toString());
      datum.attemptId = new Utf8(attemptId.toString());
      datum.taskType = new Utf8(taskType.name());
      datum.taskStatus = new Utf8(taskStatus);
      datum.shuffleFinishTime = shuffleFinishTime;
      datum.sortFinishTime = sortFinishTime;
      datum.finishTime = finishTime;
      datum.hostname = new Utf8(hostname);
      datum.port = port;
      if (rackName != null) {
        datum.rackname = new Utf8(rackName);
      }
      datum.state = new Utf8(state);
      datum.counters = EventWriter.toAvro(counters);

      datum.clockSplits = AvroArrayUtils.toAvro(ProgressSplitsBlock
        .arrayGetWallclockTime(allSplits));
      datum.cpuUsages = AvroArrayUtils.toAvro(ProgressSplitsBlock
        .arrayGetCPUTime(allSplits));
      datum.vMemKbytes = AvroArrayUtils.toAvro(ProgressSplitsBlock
        .arrayGetVMemKbytes(allSplits));
      datum.physMemKbytes = AvroArrayUtils.toAvro(ProgressSplitsBlock
        .arrayGetPhysMemKbytes(allSplits));
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (ReduceAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.attemptId.toString());
    this.taskType = TaskType.valueOf(datum.taskType.toString());
    this.taskStatus = datum.taskStatus.toString();
    this.shuffleFinishTime = datum.shuffleFinishTime;
    this.sortFinishTime = datum.sortFinishTime;
    this.finishTime = datum.finishTime;
    this.hostname = datum.hostname.toString();
    this.rackName = datum.rackname.toString();
    this.port = datum.port;
    this.state = datum.state.toString();
    this.counters = EventReader.fromAvro(datum.counters);
    this.clockSplits = AvroArrayUtils.fromAvro(datum.clockSplits);
    this.cpuUsages = AvroArrayUtils.fromAvro(datum.cpuUsages);
    this.vMemKbytes = AvroArrayUtils.fromAvro(datum.vMemKbytes);
    this.physMemKbytes = AvroArrayUtils.fromAvro(datum.physMemKbytes);
  }

  /** Get the Task ID */
  public TaskID getTaskId() { return attemptId.getTaskID(); }
  /** Get the attempt id */
  public TaskAttemptID getAttemptId() {
    return attemptId;
  }
  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the task status */
  public String getTaskStatus() { return taskStatus.toString(); }
  /** Get the finish time of the sort phase */
  public long getSortFinishTime() { return sortFinishTime; }
  /** Get the finish time of the shuffle phase */
  public long getShuffleFinishTime() { return shuffleFinishTime; }
  /** Get the finish time of the attempt */
  public long getFinishTime() { return finishTime; }
  /** Get the name of the host where the attempt ran */
  public String getHostname() { return hostname.toString(); }
  /** Get the tracker rpc port */
  public int getPort() { return port; }
  
  /** Get the rack name of the node where the attempt ran */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }
  
  /** Get the state string */
  public String getState() { return state.toString(); }
  /** Get the counters for the attempt */
  Counters getCounters() { return counters; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.REDUCE_ATTEMPT_FINISHED;
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

}
