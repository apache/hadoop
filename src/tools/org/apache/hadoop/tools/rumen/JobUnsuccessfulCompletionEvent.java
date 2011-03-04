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

import org.apache.hadoop.mapreduce.JobID;

/**
 * Event to record Failed and Killed completion of jobs
 *
 */
public class JobUnsuccessfulCompletionEvent implements HistoryEvent {
  private JobID jobId;
  private long finishTime;
  private int finishedMaps;
  private int finishedReduces;
  private String jobStatus;

  /**
   * Create an event to record unsuccessful completion (killed/failed) of jobs
   * @param id Job ID
   * @param finishTime Finish time of the job
   * @param finishedMaps Number of finished maps
   * @param finishedReduces Number of finished reduces
   * @param status Status of the job
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int finishedMaps,
      int finishedReduces, String status) {
    this.jobId = id;
    this.finishTime = finishTime;
    this.finishedMaps = finishedMaps;
    this.finishedReduces = finishedReduces;
    this.jobStatus = status;
  }

  /** Get the Job ID */
  public JobID getJobId() { return jobId; }
  /** Get the job finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the number of finished maps */
  public int getFinishedMaps() { return finishedMaps; }
  /** Get the number of finished reduces */
  public int getFinishedReduces() { return finishedReduces; }
  /** Get the status */
  public String getStatus() { return jobStatus; }
  /** Get the event type */
  public EventType getEventType() {
    if ("FAILED".equals(getStatus())) {
      return EventType.JOB_FAILED;
    } else
      return EventType.JOB_KILLED;
  }

}
