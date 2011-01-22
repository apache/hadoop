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
 * Event to record the change of status for a job
 *
 */
public class JobStatusChangedEvent implements HistoryEvent {
  private JobID jobId;
  private String jobStatus;

  /**
   * Create an event to record the change in the Job Status
   * @param id Job ID
   * @param jobStatus The new job status
   */
  public JobStatusChangedEvent(JobID id, String jobStatus) {
    this.jobId = id;
    this.jobStatus = jobStatus;
  }

  /** Get the Job Id */
  public JobID getJobId() { return jobId; }
  /** Get the event status */
  public String getStatus() { return jobStatus; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.JOB_STATUS_CHANGED;
  }

}
