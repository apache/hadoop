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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
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
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
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
  
  private List<TaskAttemptCompletionEvent> completionEvents = null;
  private JobInfo jobInfo;

  public CompletedJob(Configuration conf, JobId jobId, Path historyFile, boolean loadTasks) throws IOException {
    LOG.info("Loading job: " + jobId + " from file: " + historyFile);
    this.conf = conf;
    this.jobId = jobId;
    
    loadFullHistoryData(loadTasks, historyFile);

    counters = TypeConverter.toYarn(jobInfo.getTotalCounters());
    diagnostics.add(jobInfo.getErrorInfo());
    report = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobReport.class);
    report.setJobId(jobId);
    report.setJobState(JobState.valueOf(jobInfo.getJobStatus()));
    report.setStartTime(jobInfo.getLaunchTime());
    report.setFinishTime(jobInfo.getFinishTime());
    //TOODO Possibly populate job progress. Never used.
    //report.setMapProgress(progress) 
    //report.setReduceProgress(progress)
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
    if (completionEvents == null) {
      constructTaskAttemptCompletionEvents();
    }
    TaskAttemptCompletionEvent[] events = new TaskAttemptCompletionEvent[0];
    if (completionEvents.size() > fromEventId) {
      int actualMax = Math.min(maxEvents,
          (completionEvents.size() - fromEventId));
      events = completionEvents.subList(fromEventId, actualMax + fromEventId)
          .toArray(events);
    }
    return events;
  }

  private void constructTaskAttemptCompletionEvents() {
    completionEvents = new LinkedList<TaskAttemptCompletionEvent>();
    List<TaskAttempt> allTaskAttempts = new LinkedList<TaskAttempt>();
    for (TaskId taskId : tasks.keySet()) {
      Task task = tasks.get(taskId);
      for (TaskAttemptId taskAttemptId : task.getAttempts().keySet()) {
        TaskAttempt taskAttempt = task.getAttempts().get(taskAttemptId);
        allTaskAttempts.add(taskAttempt);
      }
    }
    Collections.sort(allTaskAttempts, new Comparator<TaskAttempt>() {

      @Override
      public int compare(TaskAttempt o1, TaskAttempt o2) {
        if (o1.getFinishTime() == 0 || o2.getFinishTime() == 0) {
          if (o1.getFinishTime() == 0 && o2.getFinishTime() == 0) {
            if (o1.getLaunchTime() == 0 || o2.getLaunchTime() == 0) {
              if (o1.getLaunchTime() == 0 && o2.getLaunchTime() == 0) {
                return 0;
              } else {
                long res = o1.getLaunchTime() - o2.getLaunchTime();
                return res > 0 ? -1 : 1;
              }
            } else {
              return (int) (o1.getLaunchTime() - o2.getLaunchTime());
            }
          } else {
            long res = o1.getFinishTime() - o2.getFinishTime();
            return res > 0 ? -1 : 1;
          }
        } else {
          return (int) (o1.getFinishTime() - o2.getFinishTime());
        }
      }
    });

    int eventId = 0;
    for (TaskAttempt taskAttempt : allTaskAttempts) {

      TaskAttemptCompletionEvent tace = RecordFactoryProvider.getRecordFactory(
          null).newRecordInstance(TaskAttemptCompletionEvent.class);

      int attemptRunTime = -1;
      if (taskAttempt.getLaunchTime() != 0 && taskAttempt.getFinishTime() != 0) {
        attemptRunTime = (int) (taskAttempt.getFinishTime() - taskAttempt
            .getLaunchTime());
      }
      // Default to KILLED
      TaskAttemptCompletionEventStatus taceStatus = TaskAttemptCompletionEventStatus.KILLED;
      String taStateString = taskAttempt.getState().toString();
      try {
        taceStatus = TaskAttemptCompletionEventStatus.valueOf(taStateString);
      } catch (Exception e) {
        LOG.warn("Cannot constuct TACEStatus from TaskAtemptState: ["
            + taStateString + "] for taskAttemptId: [" + taskAttempt.getID()
            + "]. Defaulting to KILLED");
      }

      tace.setAttemptId(taskAttempt.getID());
      tace.setAttemptRunTime(attemptRunTime);
      tace.setEventId(eventId++);
      tace.setMapOutputServerAddress(taskAttempt
          .getAssignedContainerMgrAddress());
      tace.setStatus(taceStatus);
      completionEvents.add(tace);
    }
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    return tasks;
  }

  //History data is leisurely loaded when task level data is requested
  private synchronized void loadFullHistoryData(boolean loadTasks, Path historyFileAbsolute) throws IOException {
    LOG.info("Loading history file: [" + historyFileAbsolute + "]");
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
    return jobInfo.getIsUber();
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
