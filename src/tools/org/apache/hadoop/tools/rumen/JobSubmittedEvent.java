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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Event to record the submission of a job
 *
 */
public class JobSubmittedEvent implements HistoryEvent {
  private JobID jobId;
  private String jobName;
  private String userName;
  private long submitTime;
  private String jobConfPath;
  private Map<JobACL, AccessControlList> jobAcls;

  /**
   * @deprecated Use
   *             {@link #JobSubmittedEvent(JobID, String, String, long, String, Map)}
   *             instead.
   */
  @Deprecated
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath) {
    this(id, jobName, userName, submitTime, jobConfPath,
        new HashMap<JobACL, AccessControlList>());
  }

  /**
   * Create an event to record job submission
   * @param id The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   * @param jobACLs The configured acls for the job.
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs) {
    this.jobId = id;
    this.jobName = jobName;
    this.userName = userName;
    this.submitTime = submitTime;
    this.jobConfPath = jobConfPath;
    this.jobAcls = jobACLs;
  }

  /** Get the Job Id */
  public JobID getJobId() { return jobId; }
  /** Get the Job name */
  public String getJobName() { return jobName; }
  /** Get the user name */
  public String getUserName() { return userName; }
  /** Get the submit time */
  public long getSubmitTime() { return submitTime; }
  /** Get the Path for the Job Configuration file */
  public String getJobConfPath() { return jobConfPath; }
  /** Get the acls configured for the job **/
  public Map<JobACL, AccessControlList> getJobAcls() {
    return jobAcls;
  }
  
  /** Get the event type */
  public EventType getEventType() { return EventType.JOB_SUBMITTED; }

}
