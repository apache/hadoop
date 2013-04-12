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
import org.apache.hadoop.mapreduce.JobID;

import org.apache.avro.util.Utf8;

/**
 * Event to record the change of status for a job
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobStatusChangedEvent implements HistoryEvent {
  private JobStatusChanged datum = new JobStatusChanged();

  /**
   * Create an event to record the change in the Job Status
   * @param id Job ID
   * @param jobStatus The new job status
   */
  public JobStatusChangedEvent(JobID id, String jobStatus) {
    datum.jobid = new Utf8(id.toString());
    datum.jobStatus = new Utf8(jobStatus);
  }

  JobStatusChangedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobStatusChanged)datum;
  }

  /** Get the Job Id */
  public JobID getJobId() { return JobID.forName(datum.jobid.toString()); }
  /** Get the event status */
  public String getStatus() { return datum.jobStatus.toString(); }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.JOB_STATUS_CHANGED;
  }

}
