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

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;

/**
 * Event to record successful completion of job
 *
 */
public class JobFinishedEvent  implements HistoryEvent {
  private JobID jobId;
  private long finishTime;
  private int finishedMaps;
  private int finishedReduces;
  private int failedMaps;
  private int failedReduces;
  private Counters mapCounters;
  private Counters reduceCounters;
  private Counters totalCounters;

  /** 
   * Create an event to record successful job completion
   * @param id Job ID
   * @param finishTime Finish time of the job
   * @param finishedMaps The number of finished maps
   * @param finishedReduces The number of finished reduces
   * @param failedMaps The number of failed maps
   * @param failedReduces The number of failed reduces
   * @param mapCounters Map Counters for the job
   * @param reduceCounters Reduce Counters for the job
   * @param totalCounters Total Counters for the job
   */
  public JobFinishedEvent(JobID id, long finishTime,
      int finishedMaps, int finishedReduces,
      int failedMaps, int failedReduces,
      Counters mapCounters, Counters reduceCounters,
      Counters totalCounters) {
    this.jobId = id;
    this.finishTime = finishTime;
    this.finishedMaps = finishedMaps;
    this.finishedReduces = finishedReduces;
    this.failedMaps = failedMaps;
    this.failedReduces = failedReduces;
    this.mapCounters = mapCounters;
    this.reduceCounters = reduceCounters;
    this.totalCounters = totalCounters;
  }

  public EventType getEventType() {
    return EventType.JOB_FINISHED;
  }

  /** Get the Job ID */
  public JobID getJobid() { return jobId; }
  /** Get the job finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the number of finished maps for the job */
  public int getFinishedMaps() { return finishedMaps; }
  /** Get the number of finished reducers for the job */
  public int getFinishedReduces() { return finishedReduces; }
  /** Get the number of failed maps for the job */
  public int getFailedMaps() { return failedMaps; }
  /** Get the number of failed reducers for the job */
  public int getFailedReduces() { return failedReduces; }
  /** Get the counters for the job */
  public Counters getTotalCounters() {
    return totalCounters;
  }
  /** Get the Map counters for the job */
  public Counters getMapCounters() {
    return mapCounters;
  }
  /** Get the reduce counters for the job */
  public Counters getReduceCounters() {
    return reduceCounters;
  }
}
