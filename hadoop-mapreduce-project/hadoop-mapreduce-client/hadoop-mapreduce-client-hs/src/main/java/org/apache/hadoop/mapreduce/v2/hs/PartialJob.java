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

package org.apache.hadoop.mapreduce.v2.hs;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;


public class PartialJob implements org.apache.hadoop.mapreduce.v2.app.job.Job {
  private static final Log LOG = LogFactory.getLog(PartialJob.class);

  private JobIndexInfo jobIndexInfo = null;
  private JobId jobId = null;
  private JobReport jobReport = null;
  
  public PartialJob(JobIndexInfo jobIndexInfo, JobId jobId) {
    this.jobIndexInfo = jobIndexInfo;
    this.jobId = jobId;
    jobReport = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobReport.class);
    jobReport.setSubmitTime(jobIndexInfo.getSubmitTime());
    jobReport.setStartTime(jobIndexInfo.getJobStartTime());
    jobReport.setFinishTime(jobIndexInfo.getFinishTime());
    jobReport.setJobState(getState());
  }
  
  @Override
  public JobId getID() {
//    return jobIndexInfo.getJobId();
    return this.jobId;
  }

  @Override
  public String getName() {
    return jobIndexInfo.getJobName();
  }

  @Override
  public String getQueueName() {
    return jobIndexInfo.getQueueName();
  }

  @Override
  public JobState getState() {
    JobState js = null;
    try {
      js = JobState.valueOf(jobIndexInfo.getJobStatus());
    } catch (Exception e) {
      // Meant for use by the display UI. Exception would prevent it from being
      // rendered.e Defaulting to KILLED
      LOG.warn("Exception while parsing job state. Defaulting to KILLED", e);
      js = JobState.KILLED;
    }
    return js;
  }

  @Override
  public JobReport getReport() {
    return jobReport;
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }

  @Override
  public Counters getAllCounters() {
    return null;
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    return null;
  }

  @Override
  public Map<TaskId, Task> getTasks(TaskType taskType) {
    return null;
  }

  @Override
  public Task getTask(TaskId taskID) {
    return null;
  }

  @Override
  public List<String> getDiagnostics() {
    return null;
  }

  @Override
  public int getTotalMaps() {
    return jobIndexInfo.getNumMaps();
  }

  @Override
  public int getTotalReduces() {
    return jobIndexInfo.getNumReduces();
  }

  @Override
  public int getCompletedMaps() {
    return jobIndexInfo.getNumMaps();
  }

  @Override
  public int getCompletedReduces() {
    return jobIndexInfo.getNumReduces();
  }

  @Override
  public boolean isUber() {
    return false;
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    return null;
  }

  @Override
  public TaskCompletionEvent[] getMapAttemptCompletionEvents(
      int startIndex, int maxEvents) {
    return null;
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation) {
    return true;
  }
  
  @Override
  public String getUserName() {
    return jobIndexInfo.getUser();
  }

  @Override
  public Path getConfFile() {
    throw new IllegalStateException("Not implemented yet");
  }
  
  @Override
  public Configuration loadConfFile() {
    throw new IllegalStateException("Not implemented yet");
  }

  @Override
  public Map<JobACL, AccessControlList> getJobACLs() {
    throw new IllegalStateException("Not implemented yet");
  }

  @Override
  public List<AMInfo> getAMInfos() {
    return null;
  }
  
  @Override
  public void setQueueName(String queueName) {
    throw new UnsupportedOperationException("Can't set job's queue name in history");
  }

  @Override
  public void setJobPriority(Priority priority) {
    throw new UnsupportedOperationException(
        "Can't set job's priority in history");
  }

  @Override
  public int getFailedMaps() {
    return -1;
  }

  @Override
  public int getFailedReduces() {
    return -1;
  }

  @Override
  public int getKilledMaps() {
    return -1;
  }

  @Override
  public int getKilledReduces() {
    return -1;
  }
}
