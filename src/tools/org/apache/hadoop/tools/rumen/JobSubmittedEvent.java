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
  private String queue;
  private String workflowId;
  private String workflowName;
  private String workflowNodeName;
  private String workflowAdjacencies;
  private String workflowTags;

  /**
   * @deprecated Use
   *             {@link #JobSubmittedEvent(JobID, String, String, long, String,
   *             Map, String, String, String, String, String, String)}
   *             instead.
   */
  @Deprecated
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath) {
    this(id, jobName, userName, submitTime, jobConfPath,
        new HashMap<JobACL, AccessControlList>(), null, "", "", "", "");
  }

  /**
   * @deprecated Use
   *             {@link #JobSubmittedEvent(JobID, String, String, long, String,
   *             Map, String, String, String, String, String, String)}
   *             instead.
   */
  @Deprecated
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs, null,
        "", "", "", "");
  }

  /**
   * @deprecated Use
   *             {@link #JobSubmittedEvent(JobID, String, String, long, String,
   *             Map, String, String, String, String, String, String)}
   *             instead.
   */
  @Deprecated
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String queue,
      String workflowId, String workflowName, String workflowNodeName,
      String workflowAdjacencies) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs, queue,
        workflowId, workflowName, workflowNodeName, workflowAdjacencies, "");
  }

  /**
   * Create an event to record job submission
   * @param id The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   * @param jobACLs The configured acls for the job.
   * @param queue job queue name
   * @param workflowId the workflow Id
   * @param workflowName the workflow name
   * @param workflowNodeName the workflow node name
   * @param workflowAdjacencies the workflow adjacencies
   * @param workflowTags Comma-separated workflow tags
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String queue,
      String workflowId, String workflowName, String workflowNodeName,
      String workflowAdjacencies, String workflowTags) {
    this.jobId = id;
    this.jobName = jobName;
    this.userName = userName;
    this.submitTime = submitTime;
    this.jobConfPath = jobConfPath;
    this.jobAcls = jobACLs;
    this.queue = queue;
    this.workflowId = workflowId;
    this.workflowName = workflowName;
    this.workflowNodeName = workflowNodeName;
    this.workflowAdjacencies = workflowAdjacencies;
    this.workflowTags = workflowTags;
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
  /** Get the acls configured for the job **/
  public String getJobQueueName() {
    return queue;
  }
  /** Get the workflow Id */
  public String getWorkflowId() {
    return workflowId;
  }
  /** Get the workflow name */
  public String getWorkflowName() {
    return workflowName;
  }
  /** Get the workflow node name */
  public String getWorkflowNodeName() {
    return workflowNodeName;
  }
  /** Get the workflow adjacencies */
  public String getWorkflowAdjacencies() {
    return workflowAdjacencies;
  }
  /** Get the workflow tags */
  public String getWorkflowTags() {
    return workflowTags;
  }

  /** Get the event type */
  public EventType getEventType() { return EventType.JOB_SUBMITTED; }

}
