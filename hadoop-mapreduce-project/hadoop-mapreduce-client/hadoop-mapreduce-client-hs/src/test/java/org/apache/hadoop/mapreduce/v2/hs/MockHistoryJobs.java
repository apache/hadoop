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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.collect.Maps;

public class MockHistoryJobs extends MockJobs {

  public static class JobsPair {
    public Map<JobId, Job> partial;
    public Map<JobId, Job> full;
  }
  
  public static JobsPair newHistoryJobs(int numJobs, int numTasksPerJob,
      int numAttemptsPerTask) throws IOException {
    Map<JobId, Job> mocked = newJobs(numJobs, numTasksPerJob, numAttemptsPerTask);
    return split(mocked);
  }
  
  public static JobsPair newHistoryJobs(ApplicationId appID, int numJobsPerApp,
      int numTasksPerJob, int numAttemptsPerTask) throws IOException {
    Map<JobId, Job> mocked = newJobs(appID, numJobsPerApp, numTasksPerJob, 
        numAttemptsPerTask);
    return split(mocked);
  }
  
  public static JobsPair newHistoryJobs(ApplicationId appID, int numJobsPerApp,
      int numTasksPerJob, int numAttemptsPerTask, boolean hasFailedTasks)
      throws IOException {
    Map<JobId, Job> mocked = newJobs(appID, numJobsPerApp, numTasksPerJob,
        numAttemptsPerTask, hasFailedTasks);
    return split(mocked);
  }

  private static JobsPair split(Map<JobId, Job> mocked) throws IOException {
    JobsPair ret = new JobsPair();
    ret.full = Maps.newHashMap();
    ret.partial = Maps.newHashMap();
    for(Map.Entry<JobId, Job> entry: mocked.entrySet()) {
      JobId id = entry.getKey();
      Job j = entry.getValue();
      ret.full.put(id, new MockCompletedJob(j));
      JobReport report = j.getReport();
      JobIndexInfo info = new JobIndexInfo(report.getStartTime(), 
          report.getFinishTime(), j.getUserName(), j.getName(), id, 
          j.getCompletedMaps(), j.getCompletedReduces(), String.valueOf(j.getState()));
      info.setQueueName(j.getQueueName());
      ret.partial.put(id, new PartialJob(info, id));
    }
    return ret;
  }

  private static class MockCompletedJob extends CompletedJob {
    private Job job;
    
    public MockCompletedJob(Job job) throws IOException {
      super(new Configuration(), job.getID(), null, true, job.getUserName(),
          null, null);
      this.job = job;
    }

    @Override
    public int getCompletedMaps() {
      return job.getCompletedMaps();
    }

    @Override
    public int getCompletedReduces() {
      return job.getCompletedReduces();
    }

    @Override
    public org.apache.hadoop.mapreduce.Counters getAllCounters() {
      return job.getAllCounters();
    }

    @Override
    public JobId getID() {
      return job.getID();
    }

    @Override
    public JobReport getReport() {
      return job.getReport();
    }

    @Override
    public float getProgress() {
      return job.getProgress();
    }

    @Override
    public JobState getState() {
      return job.getState();
    }

    @Override
    public Task getTask(TaskId taskId) {
      return job.getTask(taskId);
    }

    @Override
    public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
        int fromEventId, int maxEvents) {
      return job.getTaskAttemptCompletionEvents(fromEventId, maxEvents);
    }

    @Override
    public TaskCompletionEvent[] getMapAttemptCompletionEvents(
        int startIndex, int maxEvents) {
      return job.getMapAttemptCompletionEvents(startIndex, maxEvents);
    }

    @Override
    public Map<TaskId, Task> getTasks() {
      return job.getTasks();
    }

    @Override
    protected void loadFullHistoryData(boolean loadTasks,
        Path historyFileAbsolute) throws IOException {
      //Empty
    }

    @Override
    public List<String> getDiagnostics() {
      return job.getDiagnostics();
    }

    @Override
    public String getName() {
      return job.getName();
    }

    @Override
    public String getQueueName() {
      return job.getQueueName();
    }

    @Override
    public int getTotalMaps() {
      return job.getTotalMaps();
    }

    @Override
    public int getTotalReduces() {
      return job.getTotalReduces();
    }

    @Override
    public boolean isUber() {
      return job.isUber();
    }

    @Override
    public Map<TaskId, Task> getTasks(TaskType taskType) {
      return job.getTasks();
    }

    @Override
    public
        boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation) {
      return job.checkAccess(callerUGI, jobOperation);
    }
    
    @Override
    public  Map<JobACL, AccessControlList> getJobACLs() {
      return job.getJobACLs();
    }
    
    @Override
    public String getUserName() {
      return job.getUserName();
    }

    @Override
    public Path getConfFile() {
      return job.getConfFile();
    }

    @Override
    public List<AMInfo> getAMInfos() {
      return job.getAMInfos();
    }
  }
}
