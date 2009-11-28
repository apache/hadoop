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

import org.apache.hadoop.mapreduce.JobID;

import org.apache.avro.util.Utf8;

/**
 * Event to record the submission of a job
 *
 */
public class JobSubmittedEvent implements HistoryEvent {
  private JobSubmitted datum = new JobSubmitted();

  /**
   * Create an event to record job submission
   * @param id The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath) {
    datum.jobid = new Utf8(id.toString());
    datum.jobName = new Utf8(jobName);
    datum.userName = new Utf8(userName);
    datum.submitTime = submitTime;
    datum.jobConfPath = new Utf8(jobConfPath);
  }

  JobSubmittedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobSubmitted)datum;
  }

  /** Get the Job Id */
  public JobID getJobId() { return JobID.forName(datum.jobid.toString()); }
  /** Get the Job name */
  public String getJobName() { return datum.jobName.toString(); }
  /** Get the user name */
  public String getUserName() { return datum.userName.toString(); }
  /** Get the submit time */
  public long getSubmitTime() { return datum.submitTime; }
  /** Get the Path for the Job Configuration file */
  public String getJobConfPath() { return datum.jobConfPath.toString(); }
  /** Get the event type */
  public EventType getEventType() { return EventType.JOB_SUBMITTED; }

}
