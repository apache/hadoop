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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;


/**
 * Loads the basic job level data upfront.
 * Data from job history file is loaded lazily.
 */
public class CompletedJob implements org.apache.hadoop.mapreduce.v2.app.job.Job {
  
  static final Log LOG = LogFactory.getLog(CompletedJob.class);
  private final Counters counters;
  private final Configuration conf;
  private final JobId jobId;
  private final List<String> diagnostics = new ArrayList<String>();
  private final JobReport report;
  private final Map<TaskId, Task> tasks = new HashMap<TaskId, Task>();
  private final Map<TaskId, Task> mapTasks = new HashMap<TaskId, Task>();
  private final Map<TaskId, Task> reduceTasks = new HashMap<TaskId, Task>();
  
  private TaskAttemptCompletionEvent[] completionEvents;
  private JobInfo jobInfo;

  public CompletedJob(Configuration conf, JobId jobId, Path historyFile, boolean loadTasks) throws IOException {
    this.conf = conf;
    this.jobId = jobId;
    
    //TODO: load the data lazily. for now load the full data upfront
    loadFullHistoryData(loadTasks, historyFile);

    counters = TypeConverter.toYarn(jobInfo.getTotalCounters());
    diagnostics.add(jobInfo.getErrorInfo());
    report = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobReport.class);
    report.setJobId(jobId);
    report.setJobState(JobState.valueOf(jobInfo.getJobStatus()));
    report.setStartTime(jobInfo.getLaunchTime());
    report.setFinishTime(jobInfo.getFinishTime());
  }

  @Override
  public int getCompletedMaps() {
    return jobInfo.getFinishedMaps();
  }

  @Override
  public int getCompletedReduces() {
    return jobInfo.getFinishedReduces();
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  @Override
  public JobReport getReport() {
    return report;
  }

  @Override
  public JobState getState() {
    return report.getJobState();
  }

  @Override
  public Task getTask(TaskId taskId) {
    return tasks.get(taskId);
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    return completionEvents;
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    return tasks;
  }

  //History data is leisurely loaded when task level data is requested
  private synchronized void loadFullHistoryData(boolean loadTasks, Path historyFileAbsolute) throws IOException {
    if (jobInfo != null) {
      return; //data already loaded
    }
    
    if (historyFileAbsolute != null) {
      try {
      JobHistoryParser parser = new JobHistoryParser(historyFileAbsolute.getFileSystem(conf), historyFileAbsolute);
      jobInfo = parser.parse();
      } catch (IOException e) {
        throw new YarnException("Could not load history file " + historyFileAbsolute,
            e);
      }
    } else {
      throw new IOException("History file not found");
    }
    
    if (loadTasks) {
    // populate the tasks
    for (Map.Entry<org.apache.hadoop.mapreduce.TaskID, TaskInfo> entry : jobInfo
        .getAllTasks().entrySet()) {
      TaskId yarnTaskID = TypeConverter.toYarn(entry.getKey());
      TaskInfo taskInfo = entry.getValue();
      Task task = new CompletedTask(yarnTaskID, taskInfo);
      tasks.put(yarnTaskID, task);
      if (task.getType() == TaskType.MAP) {
        mapTasks.put(task.getID(), task);
      } else if (task.getType() == TaskType.REDUCE) {
        reduceTasks.put(task.getID(), task);
      }
    }
    }
    
    // TODO: populate the TaskAttemptCompletionEvent
    completionEvents = new TaskAttemptCompletionEvent[0];
    LOG.info("TaskInfo loaded");
  }

  @Override
  public List<String> getDiagnostics() {
    return diagnostics;
  }

  @Override
  public String getName() {
    return jobInfo.getJobname();
  }

  @Override
  public int getTotalMaps() {
    return jobInfo.getTotalMaps();
  }

  @Override
  public int getTotalReduces() {
    return jobInfo.getTotalReduces();
  }

  @Override
  public boolean isUber() {
    return false;
  }

  @Override
  public Map<TaskId, Task> getTasks(TaskType taskType) {
    if (TaskType.MAP.equals(taskType)) {
      return mapTasks;
    } else {//we have only two types of tasks
      return reduceTasks;
    }
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return true;
    }
    Map<JobACL, AccessControlList> jobACLs = jobInfo.getJobACLs();
    AccessControlList jobACL = jobACLs.get(jobOperation);
    JobACLsManager aclsMgr = new JobACLsManager(conf);
    return aclsMgr.checkAccess(callerUGI, jobOperation, 
        jobInfo.getUsername(), jobACL);
  }
}
