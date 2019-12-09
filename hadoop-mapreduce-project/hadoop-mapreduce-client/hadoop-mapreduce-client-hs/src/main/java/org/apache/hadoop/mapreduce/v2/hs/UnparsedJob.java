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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.*;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

/**
 * A job that has too many tasks associated with it, of which we do not parse
 * its job history file, to prevent the Job History Server from hanging on
 * parsing the file. It is meant to be used only by JHS to indicate if the
 * history file of a job is fully parsed or not.
 */
public class UnparsedJob implements org.apache.hadoop.mapreduce.v2.app.job.Job {
  private final JobIndexInfo jobIndexInfo;
  private final int maxTasksAllowed;
  private JobReport jobReport;
  private final HistoryFileManager.HistoryFileInfo jhfInfo;

  public UnparsedJob(int maxTasksAllowed, JobIndexInfo jobIndexInfo,
      HistoryFileManager.HistoryFileInfo jhfInfo) throws IOException {
    this.jobIndexInfo = jobIndexInfo;
    this.jhfInfo = jhfInfo;
    this.maxTasksAllowed = maxTasksAllowed;
  }

  public int getMaxTasksAllowed() {
    return maxTasksAllowed;
  }

  @Override
  public JobId getID() {
    return jobIndexInfo.getJobId();
  }

  @Override
  public String getName() {
    return jobIndexInfo.getJobName();
  }

  @Override
  public JobState getState() {
    return JobState.valueOf(jobIndexInfo.getJobStatus());
  }

  @Override
  public synchronized JobReport getReport() {
    if(jobReport == null) {
      jobReport = constructJobReport();
    }
    return jobReport;
  }

  public JobReport constructJobReport() {
    JobReport report = Records.newRecord(JobReport.class);
    report.setJobId(getID());
    report.setJobState(getState());
    report.setSubmitTime(jobIndexInfo.getSubmitTime());
    report.setStartTime(jobIndexInfo.getJobStartTime());
    report.setFinishTime(jobIndexInfo.getFinishTime());
    report.setJobName(jobIndexInfo.getJobName());
    report.setUser(jobIndexInfo.getUser());
    report.setJobFile(getConfFile().toString());
    report.setHistoryFile(jhfInfo.getHistoryFile().toString());
    return report;
  }

  @Override
  public Counters getAllCounters() {
    return new Counters();
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    return new HashMap<>();
  }

  @Override
  public Map<TaskId, Task> getTasks(TaskType taskType) {
    return new HashMap<>();
  }

  @Override
  public Task getTask(TaskId taskID) {
    return null;
  }

  @Override
  public List<String> getDiagnostics() {
    return new ArrayList<>();
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
    return -1;
  }

  @Override
  public int getCompletedReduces() {
    return -1;
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }

  @Override
  public boolean isUber() {
    return false;
  }

  @Override
  public String getUserName() {
    return jobIndexInfo.getUser();
  }

  @Override
  public String getQueueName() {
    return jobIndexInfo.getQueueName();
  }

  @Override
  public Path getConfFile() {
    return jhfInfo.getConfFile();
  }

  @Override
  public Configuration loadConfFile() throws IOException {
    return jhfInfo.loadConfFile();
  }

  @Override
  public Map<JobACL, AccessControlList> getJobACLs() {
    return new HashMap<>();
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    return new TaskAttemptCompletionEvent[0];
  }

  @Override
  public TaskCompletionEvent[] getMapAttemptCompletionEvents(
      int startIndex, int maxEvents) {
    return new TaskCompletionEvent[0];
  }

  @Override
  public List<AMInfo> getAMInfos() {
    return new ArrayList<>();
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI,
      JobACL jobOperation) {
    return true;
  }

  @Override
  public void setQueueName(String queueName) {
    throw new UnsupportedOperationException("Can't set job's " +
        "queue name in history");
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
