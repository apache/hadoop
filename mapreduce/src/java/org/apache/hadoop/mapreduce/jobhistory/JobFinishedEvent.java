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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;

import org.apache.avro.util.Utf8;

/**
 * Event to record successful completion of job
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobFinishedEvent  implements HistoryEvent {
  private JobFinished datum = new JobFinished();

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
    datum.jobid = new Utf8(id.toString());
    datum.finishTime = finishTime;
    datum.finishedMaps = finishedMaps;
    datum.finishedReduces = finishedReduces;
    datum.failedMaps = failedMaps;
    datum.failedReduces = failedReduces;
    datum.mapCounters =
      EventWriter.toAvro(mapCounters, "MAP_COUNTERS");
    datum.reduceCounters =
      EventWriter.toAvro(reduceCounters, "REDUCE_COUNTERS");
    datum.totalCounters =
      EventWriter.toAvro(totalCounters, "TOTAL_COUNTERS");
  }

  JobFinishedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) { this.datum = (JobFinished)datum; }
  public EventType getEventType() {
    return EventType.JOB_FINISHED;
  }

  /** Get the Job ID */
  public JobID getJobid() { return JobID.forName(datum.jobid.toString()); }
  /** Get the job finish time */
  public long getFinishTime() { return datum.finishTime; }
  /** Get the number of finished maps for the job */
  public int getFinishedMaps() { return datum.finishedMaps; }
  /** Get the number of finished reducers for the job */
  public int getFinishedReduces() { return datum.finishedReduces; }
  /** Get the number of failed maps for the job */
  public int getFailedMaps() { return datum.failedMaps; }
  /** Get the number of failed reducers for the job */
  public int getFailedReduces() { return datum.failedReduces; }
  /** Get the counters for the job */
  public Counters getTotalCounters() {
    return EventReader.fromAvro(datum.totalCounters);
  }
  /** Get the Map counters for the job */
  public Counters getMapCounters() {
    return EventReader.fromAvro(datum.mapCounters);
  }
  /** Get the reduce counters for the job */
  public Counters getReduceCounters() {
    return EventReader.fromAvro(datum.reduceCounters);
  }
}
