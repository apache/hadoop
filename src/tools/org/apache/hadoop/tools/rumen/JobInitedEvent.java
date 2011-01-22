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
 * Event to record the initialization of a job
 *
 */
public class JobInitedEvent implements HistoryEvent {
  private JobID jobId;
  private long launchTime;
  private int totalMaps;
  private int totalReduces;
  private String jobStatus;

  /**
   * Create an event to record job initialization
   * @param id
   * @param launchTime
   * @param totalMaps
   * @param totalReduces
   * @param jobStatus
   */
  public JobInitedEvent(JobID id, long launchTime, int totalMaps,
                        int totalReduces, String jobStatus) {
    this.jobId = id;
    this.launchTime = launchTime;
    this.totalMaps = totalMaps;
    this.totalReduces = totalReduces;
    this.jobStatus = jobStatus;
  }

  /** Get the job ID */
  public JobID getJobId() { return jobId; }
  /** Get the launch time */
  public long getLaunchTime() { return launchTime; }
  /** Get the total number of maps */
  public int getTotalMaps() { return totalMaps; }
  /** Get the total number of reduces */
  public int getTotalReduces() { return totalReduces; }
  /** Get the status */
  public String getStatus() { return jobStatus; }
 /** Get the event type */
  public EventType getEventType() {
    return EventType.JOB_INITED;
  }

}
