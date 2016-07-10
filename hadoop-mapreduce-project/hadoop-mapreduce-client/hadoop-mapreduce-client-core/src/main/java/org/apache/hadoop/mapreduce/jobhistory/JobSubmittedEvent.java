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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * Event to record the submission of a job
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobSubmittedEvent implements HistoryEvent {
  private JobSubmitted datum = new JobSubmitted();
  private JobConf jobConf = null;

  /**
   * Create an event to record job submission
   * @param id The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   * @param jobACLs The configured acls for the job.
   * @param jobQueueName The job-queue to which this job was submitted to
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String jobQueueName) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs,
        jobQueueName, "", "", "", "");
  }

  /**
   * Create an event to record job submission
   * @param id The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   * @param jobACLs The configured acls for the job.
   * @param jobQueueName The job-queue to which this job was submitted to
   * @param workflowId The Id of the workflow
   * @param workflowName The name of the workflow
   * @param workflowNodeName The node name of the workflow
   * @param workflowAdjacencies The adjacencies of the workflow
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String jobQueueName,
      String workflowId, String workflowName, String workflowNodeName,
      String workflowAdjacencies) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs,
        jobQueueName, workflowId, workflowName, workflowNodeName,
        workflowAdjacencies, "");
  }

  /**
   * Create an event to record job submission.
   * @param id The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   * @param jobACLs The configured acls for the job.
   * @param jobQueueName The job-queue to which this job was submitted to
   * @param workflowId The Id of the workflow
   * @param workflowName The name of the workflow
   * @param workflowNodeName The node name of the workflow
   * @param workflowAdjacencies The adjacencies of the workflow
   * @param workflowTags Comma-separated tags for the workflow
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String jobQueueName,
      String workflowId, String workflowName, String workflowNodeName,
      String workflowAdjacencies, String workflowTags) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs,
        jobQueueName, workflowId, workflowName, workflowNodeName,
        workflowAdjacencies, workflowTags, null);
  }

    /**
     * Create an event to record job submission
     * @param id The job Id of the job
     * @param jobName Name of the job
     * @param userName Name of the user who submitted the job
     * @param submitTime Time of submission
     * @param jobConfPath Path of the Job Configuration file
     * @param jobACLs The configured acls for the job.
     * @param jobQueueName The job-queue to which this job was submitted to
     * @param workflowId The Id of the workflow
     * @param workflowName The name of the workflow
     * @param workflowNodeName The node name of the workflow
     * @param workflowAdjacencies The adjacencies of the workflow
     * @param workflowTags Comma-separated tags for the workflow
     * @param conf Job configuration
     */
    public JobSubmittedEvent(JobID id, String jobName, String userName,
        long submitTime, String jobConfPath,
        Map<JobACL, AccessControlList> jobACLs, String jobQueueName,
        String workflowId, String workflowName, String workflowNodeName,
        String workflowAdjacencies, String workflowTags, JobConf conf) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setJobName(new Utf8(jobName));
    datum.setUserName(new Utf8(userName));
    datum.setSubmitTime(submitTime);
    datum.setJobConfPath(new Utf8(jobConfPath));
    Map<CharSequence, CharSequence> jobAcls = new HashMap<CharSequence, CharSequence>();
    for (Entry<JobACL, AccessControlList> entry : jobACLs.entrySet()) {
      jobAcls.put(new Utf8(entry.getKey().getAclName()), new Utf8(
          entry.getValue().getAclString()));
    }
    datum.setAcls(jobAcls);
    if (jobQueueName != null) {
      datum.setJobQueueName(new Utf8(jobQueueName));
    }
    if (workflowId != null) {
      datum.setWorkflowId(new Utf8(workflowId));
    }
    if (workflowName != null) {
      datum.setWorkflowName(new Utf8(workflowName));
    }
    if (workflowNodeName != null) {
      datum.setWorkflowNodeName(new Utf8(workflowNodeName));
    }
    if (workflowAdjacencies != null) {
      datum.setWorkflowAdjacencies(new Utf8(workflowAdjacencies));
    }
    if (workflowTags != null) {
      datum.setWorkflowTags(new Utf8(workflowTags));
    }
    jobConf = conf;
  }

  JobSubmittedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobSubmitted)datum;
  }

  /** Get the Job Id */
  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  /** Get the Job name */
  public String getJobName() { return datum.getJobName().toString(); }
  /** Get the Job queue name */
  public String getJobQueueName() {
    if (datum.getJobQueueName() != null) {
      return datum.getJobQueueName().toString();
    }
    return null;
  }
  /** Get the user name */
  public String getUserName() { return datum.getUserName().toString(); }
  /** Get the submit time */
  public long getSubmitTime() { return datum.getSubmitTime(); }
  /** Get the Path for the Job Configuration file */
  public String getJobConfPath() { return datum.getJobConfPath().toString(); }
  /** Get the acls configured for the job **/
  public Map<JobACL, AccessControlList> getJobAcls() {
    Map<JobACL, AccessControlList> jobAcls =
        new HashMap<JobACL, AccessControlList>();
    for (JobACL jobACL : JobACL.values()) {
      Utf8 jobACLsUtf8 = new Utf8(jobACL.getAclName());
      if (datum.getAcls().containsKey(jobACLsUtf8)) {
        jobAcls.put(jobACL, new AccessControlList(datum.getAcls().get(
            jobACLsUtf8).toString()));
      }
    }
    return jobAcls;
  }
  /** Get the id of the workflow */
  public String getWorkflowId() {
    if (datum.getWorkflowId() != null) {
      return datum.getWorkflowId().toString();
    }
    return null;
  }
  /** Get the name of the workflow */
  public String getWorkflowName() {
    if (datum.getWorkflowName() != null) {
      return datum.getWorkflowName().toString();
    }
    return null;
  }
  /** Get the node name of the workflow */
  public String getWorkflowNodeName() {
    if (datum.getWorkflowNodeName() != null) {
      return datum.getWorkflowNodeName().toString();
    }
    return null;
  }
  /** Get the adjacencies of the workflow */
  public String getWorkflowAdjacencies() {
    if (datum.getWorkflowAdjacencies() != null) {
      return datum.getWorkflowAdjacencies().toString();
    }
    return null;
  }
  /** Get the workflow tags */
  public String getWorkflowTags() {
    if (datum.getWorkflowTags() != null) {
      return datum.getWorkflowTags().toString();
    }
    return null;
  }
  /** Get the event type */
  public EventType getEventType() { return EventType.JOB_SUBMITTED; }

  public JobConf getJobConf() {
    return jobConf;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("SUBMIT_TIME", getSubmitTime());
    tEvent.addInfo("QUEUE_NAME", getJobQueueName());
    tEvent.addInfo("JOB_NAME", getJobName());
    tEvent.addInfo("USER_NAME", getUserName());
    tEvent.addInfo("JOB_CONF_PATH", getJobConfPath());
    tEvent.addInfo("ACLS", getJobAcls());
    tEvent.addInfo("JOB_QUEUE_NAME", getJobQueueName());
    tEvent.addInfo("WORKLFOW_ID", getWorkflowId());
    tEvent.addInfo("WORKFLOW_NAME", getWorkflowName());
    tEvent.addInfo("WORKFLOW_NODE_NAME", getWorkflowNodeName());
    tEvent.addInfo("WORKFLOW_ADJACENCIES",
        getWorkflowAdjacencies());
    tEvent.addInfo("WORKFLOW_TAGS", getWorkflowTags());

    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}
