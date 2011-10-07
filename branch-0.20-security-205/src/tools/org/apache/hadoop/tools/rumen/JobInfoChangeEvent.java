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
import org.apache.hadoop.mapreduce.JobID;

/**
 * Event to record changes in the submit and launch time of
 * a job
 */
public class JobInfoChangeEvent implements HistoryEvent {
  private JobID jobId;
  private long submitTime;
  private long launchTime;

  /** 
   * Create a event to record the submit and launch time of a job
   * @param id Job Id 
   * @param submitTime Submit time of the job
   * @param launchTime Launch time of the job
   */
  public JobInfoChangeEvent(JobID id, long submitTime, long launchTime) {
    this.jobId = id;
    this.submitTime = submitTime;
    this.launchTime = launchTime;
  }

  /** Get the Job ID */
  public JobID getJobId() { return jobId; }
  /** Get the Job submit time */
  public long getSubmitTime() { return submitTime; }
  /** Get the Job launch time */
  public long getLaunchTime() { return launchTime; }

  public EventType getEventType() {
    return EventType.JOB_INFO_CHANGED;
  }

}
