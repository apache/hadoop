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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
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
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Records;


/**
 * Loads the basic job level data upfront.
 * Data from job history file is loaded lazily.
 */
public class CompletedJob implements org.apache.hadoop.mapreduce.v2.app.job.Job {
  // Backward compatibility: if the failed or killed map/reduce
  // count is -1, that means the value was not recorded
  // so we count it as 0
  private static final int UNDEFINED_VALUE = -1;

  static final Log LOG = LogFactory.getLog(CompletedJob.class);
  private final Configuration conf;
  private final JobId jobId; //Can be picked from JobInfo with a conversion.
  private final String user; //Can be picked up from JobInfo
  private final HistoryFileInfo info;
  private JobInfo jobInfo;
  private JobReport report;
  AtomicBoolean tasksLoaded = new AtomicBoolean(false);
  private Lock tasksLock = new ReentrantLock();
  private Map<TaskId, Task> tasks = new HashMap<TaskId, Task>();
  private Map<TaskId, Task> mapTasks = new HashMap<TaskId, Task>();
  private Map<TaskId, Task> reduceTasks = new HashMap<TaskId, Task>();
  private List<TaskAttemptCompletionEvent> completionEvents = null;
  private List<TaskAttemptCompletionEvent> mapCompletionEvents = null;
  private JobACLsManager aclsMgr;
  
  
  public CompletedJob(Configuration conf, JobId jobId, Path historyFile, 
      boolean loadTasks, String userName, HistoryFileInfo info,
      JobACLsManager aclsMgr) 
          throws IOException {
    LOG.info("Loading job: " + jobId + " from file: " + historyFile);
    this.conf = conf;
    this.jobId = jobId;
    this.user = userName;
    this.info = info;
    this.aclsMgr = aclsMgr;
    loadFullHistoryData(loadTasks, historyFile);
  }

  @Override
  public int getCompletedMaps() {
    int killedMaps = (int) jobInfo.getKilledMaps();
    int failedMaps = (int) jobInfo.getFailedMaps();

    if (killedMaps == UNDEFINED_VALUE) {
      killedMaps = 0;
    }

    if (failedMaps == UNDEFINED_VALUE) {
      failedMaps = 0;
    }

    return (int) (jobInfo.getSucceededMaps() +
        killedMaps + failedMaps);
  }

  @Override
  public int getCompletedReduces() {
    int killedReduces = (int) jobInfo.getKilledReduces();
    int failedReduces = (int) jobInfo.getFailedReduces();

    if (killedReduces == UNDEFINED_VALUE) {
      killedReduces = 0;
    }

    if (failedReduces == UNDEFINED_VALUE) {
      failedReduces = 0;
    }

    return (int) (jobInfo.getSucceededReduces() +
        killedReduces + failedReduces);
  }

  @Override
  public Counters getAllCounters() {
    return jobInfo.getTotalCounters();
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  @Override
  public synchronized JobReport getReport() {
    if (report == null) {
      constructJobReport();
    }
    return report;
  }

  private void constructJobReport() {
    report = Records.newRecord(JobReport.class);
    report.setJobId(jobId);
    report.setJobState(JobState.valueOf(jobInfo.getJobStatus()));
    report.setSubmitTime(jobInfo.getSubmitTime());
    report.setStartTime(jobInfo.getLaunchTime());
    report.setFinishTime(jobInfo.getFinishTime());
    report.setJobName(jobInfo.getJobname());
    report.setUser(jobInfo.getUsername());
    report.setDiagnostics(jobInfo.getErrorInfo());

    if ( getTotalMaps() == 0 ) {
      report.setMapProgress(1.0f);
    } else {
      report.setMapProgress((float) getCompletedMaps() / getTotalMaps());
    }
    if ( getTotalReduces() == 0 ) {
      report.setReduceProgress(1.0f);
    } else {
      report.setReduceProgress((float) getCompletedReduces() / getTotalReduces());
    }

    report.setJobFile(getConfFile().toString());
    String historyUrl = "N/A";
    try {
      historyUrl =
          MRWebAppUtil.getApplicationWebURLOnJHSWithScheme(conf,
              jobId.getAppId());
    } catch (UnknownHostException e) {
        LOG.error("Problem determining local host: " + e.getMessage());
    }
    report.setTrackingUrl(historyUrl);
    report.setAMInfos(getAMInfos());
    report.setIsUber(isUber());
    report.setHistoryFile(info.getHistoryFile().toString());
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }

  @Override
  public JobState getState() {
    return JobState.valueOf(jobInfo.getJobStatus());
  }

  @Override
  public Task getTask(TaskId taskId) {
    if (tasksLoaded.get()) {
      return tasks.get(taskId);
    } else {
      TaskID oldTaskId = TypeConverter.fromYarn(taskId);
      CompletedTask completedTask =
          new CompletedTask(taskId, jobInfo.getAllTasks().get(oldTaskId));
      return completedTask;
    }
  }

  @Override
  public synchronized TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    if (completionEvents == null) {
      constructTaskAttemptCompletionEvents();
    }
    return getAttemptCompletionEvents(completionEvents,
        fromEventId, maxEvents);
  }

  @Override
  public synchronized TaskCompletionEvent[] getMapAttemptCompletionEvents(
      int startIndex, int maxEvents) {
    if (mapCompletionEvents == null) {
      constructTaskAttemptCompletionEvents();
    }
    return TypeConverter.fromYarn(getAttemptCompletionEvents(
        mapCompletionEvents, startIndex, maxEvents));
  }

  private static TaskAttemptCompletionEvent[] getAttemptCompletionEvents(
      List<TaskAttemptCompletionEvent> eventList,
      int startIndex, int maxEvents) {
    TaskAttemptCompletionEvent[] events = new TaskAttemptCompletionEvent[0];
    if (eventList.size() > startIndex) {
      int actualMax = Math.min(maxEvents,
          (eventList.size() - startIndex));
      events = eventList.subList(startIndex, actualMax + startIndex)
          .toArray(events);
    }
    return events;
  }

  private void constructTaskAttemptCompletionEvents() {
    loadAllTasks();
    completionEvents = new LinkedList<TaskAttemptCompletionEvent>();
    List<TaskAttempt> allTaskAttempts = new LinkedList<TaskAttempt>();
    int numMapAttempts = 0;
    for (Map.Entry<TaskId,Task> taskEntry : tasks.entrySet()) {
      Task task = taskEntry.getValue();
      for (Map.Entry<TaskAttemptId,TaskAttempt> taskAttemptEntry : task.getAttempts().entrySet()) {
        TaskAttempt taskAttempt = taskAttemptEntry.getValue();
        allTaskAttempts.add(taskAttempt);
        if (task.getType() == TaskType.MAP) {
          ++numMapAttempts;
        }
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

    mapCompletionEvents =
        new ArrayList<TaskAttemptCompletionEvent>(numMapAttempts);
    int eventId = 0;
    for (TaskAttempt taskAttempt : allTaskAttempts) {

      TaskAttemptCompletionEvent tace =
          Records.newRecord(TaskAttemptCompletionEvent.class);

      int attemptRunTime = -1;
      if (taskAttempt.getLaunchTime() != 0 && taskAttempt.getFinishTime() != 0) {
        attemptRunTime =
            (int) (taskAttempt.getFinishTime() - taskAttempt.getLaunchTime());
      }
      // Default to KILLED
      TaskAttemptCompletionEventStatus taceStatus =
          TaskAttemptCompletionEventStatus.KILLED;
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
      if (taskAttempt.getID().getTaskId().getTaskType() == TaskType.MAP) {
        mapCompletionEvents.add(tace);
      }
    }
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    loadAllTasks();
    return tasks;
  }

  private void loadAllTasks() {
    if (tasksLoaded.get()) {
      return;
    }
    tasksLock.lock();
    try {
      if (tasksLoaded.get()) {
        return;
      }
      for (Map.Entry<TaskID, TaskInfo> entry : jobInfo.getAllTasks().entrySet()) {
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
      tasksLoaded.set(true);
    } finally {
      tasksLock.unlock();
    }
  }

  protected JobHistoryParser createJobHistoryParser(Path historyFileAbsolute)
      throws IOException {
    return new JobHistoryParser(historyFileAbsolute.getFileSystem(conf),
                historyFileAbsolute);
  }

  //History data is leisurely loaded when task level data is requested
  protected synchronized void loadFullHistoryData(boolean loadTasks,
      Path historyFileAbsolute) throws IOException {
    LOG.info("Loading history file: [" + historyFileAbsolute + "]");
    if (this.jobInfo != null) {
      return;
    }
    
    if (historyFileAbsolute != null) {
      JobHistoryParser parser = null;
      try {
        parser = createJobHistoryParser(historyFileAbsolute);
        this.jobInfo = parser.parse();
      } catch (IOException e) {
        throw new YarnRuntimeException("Could not load history file "
            + historyFileAbsolute, e);
      }
      IOException parseException = parser.getParseException(); 
      if (parseException != null) {
        throw new YarnRuntimeException(
            "Could not parse history file " + historyFileAbsolute, 
            parseException);
      }
    } else {
      throw new IOException("History file not found");
    }
    if (loadTasks) {
      loadAllTasks();
      LOG.info("TaskInfo loaded");
    }    
  }

  @Override
  public List<String> getDiagnostics() {
    return Collections.singletonList(jobInfo.getErrorInfo());
  }

  @Override
  public String getName() {
    return jobInfo.getJobname();
  }

  @Override
  public String getQueueName() {
    return jobInfo.getJobQueueName();
  }

  @Override
  public int getTotalMaps() {
    return (int) jobInfo.getTotalMaps();
  }

  @Override
  public int getTotalReduces() {
    return (int) jobInfo.getTotalReduces();
  }

  @Override
  public boolean isUber() {
    return jobInfo.getUberized();
  }

  @Override
  public Map<TaskId, Task> getTasks(TaskType taskType) {
    loadAllTasks();
    if (TaskType.MAP.equals(taskType)) {
      return mapTasks;
    } else {//we have only two types of tasks
      return reduceTasks;
    }
  }

  @Override
  public
      boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation) {
    Map<JobACL, AccessControlList> jobACLs = jobInfo.getJobACLs();
    AccessControlList jobACL = jobACLs.get(jobOperation);
    if (jobACL == null) {
      return true;
    }
    return aclsMgr.checkAccess(callerUGI, jobOperation, 
        jobInfo.getUsername(), jobACL);
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.job.Job#getJobACLs()
   */
  @Override
  public  Map<JobACL, AccessControlList> getJobACLs() {
    return jobInfo.getJobACLs();
  }
  
  @Override
  public String getUserName() {
    return user;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.job.Job#getConfFile()
   */
  @Override
  public Path getConfFile() {
    return info.getConfFile();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.job.Job#loadConfFile()
   */
  @Override
  public Configuration loadConfFile() throws IOException {
    return info.loadConfFile();
  }

  @Override
  public List<AMInfo> getAMInfos() {
    List<AMInfo> amInfos = new LinkedList<AMInfo>();
    for (org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.AMInfo jhAmInfo : jobInfo
        .getAMInfos()) {
      AMInfo amInfo =
          MRBuilderUtils.newAMInfo(jhAmInfo.getAppAttemptId(),
              jhAmInfo.getStartTime(), jhAmInfo.getContainerId(),
              jhAmInfo.getNodeManagerHost(), jhAmInfo.getNodeManagerPort(),
              jhAmInfo.getNodeManagerHttpPort());
   
      amInfos.add(amInfo);
    }
    return amInfos;
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
    return (int) jobInfo.getFailedMaps();
  }

  @Override
  public int getFailedReduces() {
    return (int) jobInfo.getFailedReduces();
  }

  @Override
  public int getKilledMaps() {
    return (int) jobInfo.getKilledMaps();
  }

  @Override
  public int getKilledReduces() {
    return (int) jobInfo.getKilledReduces();
  }
}
