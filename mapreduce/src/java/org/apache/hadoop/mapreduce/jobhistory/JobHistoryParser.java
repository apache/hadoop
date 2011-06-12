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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Default Parser for the JobHistory files. Typical usage is
 * JobHistoryParser parser = new JobHistoryParser(fs, historyFile);
 * job = parser.parse();
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobHistoryParser {

  private final FSDataInputStream in;
  JobInfo info = null;

  /**
   * Create a job history parser for the given history file using the 
   * given file system
   * @param fs
   * @param file
   * @throws IOException
   */
  public JobHistoryParser(FileSystem fs, String file) throws IOException {
    this(fs, new Path(file));
  }
  
  /**
   * Create the job history parser for the given history file using the 
   * given file system
   * @param fs
   * @param historyFile
   * @throws IOException
   */
  public JobHistoryParser(FileSystem fs, Path historyFile) 
  throws IOException {
    this(fs.open(historyFile));
  }
  
  /**
   * Create the history parser based on the input stream
   * @param in
   */
  public JobHistoryParser(FSDataInputStream in) {
    this.in = in;
  }
  
  /**
   * Parse the entire history file and populate the JobInfo object
   * The first invocation will populate the object, subsequent calls
   * will return the already parsed object. 
   * The input stream is closed on return 
   * @return The populated jobInfo object
   * @throws IOException
   */
  public synchronized JobInfo parse() throws IOException {

    if (info != null) {
      return info;
    }

    EventReader reader = new EventReader(in);

    HistoryEvent event;
    info = new JobInfo();
    try {
      while ((event = reader.getNextEvent()) != null) {
        handleEvent(event);
      }
    } finally {
      in.close();
    }
    return info;
  }
  
  private void handleEvent(HistoryEvent event) throws IOException { 
    EventType type = event.getEventType();

    switch (type) {
    case JOB_SUBMITTED:
      handleJobSubmittedEvent((JobSubmittedEvent)event);
      break;
    case JOB_STATUS_CHANGED:
      break;
    case JOB_INFO_CHANGED:
      handleJobInfoChangeEvent((JobInfoChangeEvent) event);
      break;
    case JOB_INITED:
      handleJobInitedEvent((JobInitedEvent) event);
      break;
    case JOB_PRIORITY_CHANGED:
      handleJobPriorityChangeEvent((JobPriorityChangeEvent) event);
      break;
    case JOB_FAILED:
    case JOB_KILLED:
      handleJobFailedEvent((JobUnsuccessfulCompletionEvent) event);
      break;
    case JOB_FINISHED:
      handleJobFinishedEvent((JobFinishedEvent)event);
      break;
    case TASK_STARTED:
      handleTaskStartedEvent((TaskStartedEvent) event);
      break;
    case TASK_FAILED:
      handleTaskFailedEvent((TaskFailedEvent) event);
      break;
    case TASK_UPDATED:
      handleTaskUpdatedEvent((TaskUpdatedEvent) event);
      break;
    case TASK_FINISHED:
      handleTaskFinishedEvent((TaskFinishedEvent) event);
      break;
    case MAP_ATTEMPT_STARTED:
    case CLEANUP_ATTEMPT_STARTED:
    case REDUCE_ATTEMPT_STARTED:
    case SETUP_ATTEMPT_STARTED:
      handleTaskAttemptStartedEvent((TaskAttemptStartedEvent) event);
      break;
    case MAP_ATTEMPT_FAILED:
    case CLEANUP_ATTEMPT_FAILED:
    case REDUCE_ATTEMPT_FAILED:
    case SETUP_ATTEMPT_FAILED:
    case MAP_ATTEMPT_KILLED:
    case CLEANUP_ATTEMPT_KILLED:
    case REDUCE_ATTEMPT_KILLED:
    case SETUP_ATTEMPT_KILLED:
      handleTaskAttemptFailedEvent(
          (TaskAttemptUnsuccessfulCompletionEvent) event);
      break;
    case MAP_ATTEMPT_FINISHED:
      handleMapAttemptFinishedEvent((MapAttemptFinishedEvent) event);
      break;
    case REDUCE_ATTEMPT_FINISHED:
      handleReduceAttemptFinishedEvent((ReduceAttemptFinishedEvent) event);
      break;
    case SETUP_ATTEMPT_FINISHED:
    case CLEANUP_ATTEMPT_FINISHED:
      handleTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) event);
      break;
    default:
      break;
    }
  }
  
  private void handleTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = event.getTaskStatus();
    attemptInfo.state = event.getState();
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = event.getHostname();
  }

  private void handleReduceAttemptFinishedEvent
  (ReduceAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = event.getTaskStatus();
    attemptInfo.state = event.getState();
    attemptInfo.shuffleFinishTime = event.getShuffleFinishTime();
    attemptInfo.sortFinishTime = event.getSortFinishTime();
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = event.getHostname();
  }

  private void handleMapAttemptFinishedEvent(MapAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = event.getTaskStatus();
    attemptInfo.state = event.getState();
    attemptInfo.mapFinishTime = event.getMapFinishTime();
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = event.getHostname();
  }

  private void handleTaskAttemptFailedEvent(
      TaskAttemptUnsuccessfulCompletionEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getTaskAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.error = event.getError();
    attemptInfo.status = event.getTaskStatus();
    attemptInfo.hostname = event.getHostname();
    attemptInfo.shuffleFinishTime = event.getFinishTime();
    attemptInfo.sortFinishTime = event.getFinishTime();
    attemptInfo.mapFinishTime = event.getFinishTime();
  }

  private void handleTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    TaskAttemptID attemptId = event.getTaskAttemptId();
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    
    TaskAttemptInfo attemptInfo = new TaskAttemptInfo();
    attemptInfo.startTime = event.getStartTime();
    attemptInfo.attemptId = event.getTaskAttemptId();
    attemptInfo.httpPort = event.getHttpPort();
    attemptInfo.trackerName = event.getTrackerName();
    attemptInfo.taskType = event.getTaskType();
    
    taskInfo.attemptsMap.put(attemptId, attemptInfo);
  }

  private void handleTaskFinishedEvent(TaskFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.counters = event.getCounters();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.status = TaskStatus.State.SUCCEEDED.toString();
  }

  private void handleTaskUpdatedEvent(TaskUpdatedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.finishTime = event.getFinishTime();
  }

  private void handleTaskFailedEvent(TaskFailedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.status = TaskStatus.State.FAILED.toString();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.error = event.getError();
    taskInfo.failedDueToAttemptId = event.getFailedAttemptID();
  }

  private void handleTaskStartedEvent(TaskStartedEvent event) {
    TaskInfo taskInfo = new TaskInfo();
    taskInfo.taskId = event.getTaskId();
    taskInfo.startTime = event.getStartTime();
    taskInfo.taskType = event.getTaskType();
    taskInfo.splitLocations = event.getSplitLocations();
    info.tasksMap.put(event.getTaskId(), taskInfo);
  }

  private void handleJobFailedEvent(JobUnsuccessfulCompletionEvent event) {
    info.finishTime = event.getFinishTime();
    info.finishedMaps = event.getFinishedMaps();
    info.finishedReduces = event.getFinishedReduces();
    info.jobStatus = event.getStatus();
  }

  private void handleJobFinishedEvent(JobFinishedEvent event) {
    info.finishTime = event.getFinishTime();
    info.finishedMaps = event.getFinishedMaps();
    info.finishedReduces = event.getFinishedReduces();
    info.failedMaps = event.getFailedMaps();
    info.failedReduces = event.getFailedReduces();
    info.totalCounters = event.getTotalCounters();
    info.mapCounters = event.getMapCounters();
    info.reduceCounters = event.getReduceCounters();
    info.jobStatus = JobStatus.getJobRunState(JobStatus.SUCCEEDED);
  }

  private void handleJobPriorityChangeEvent(JobPriorityChangeEvent event) {
    info.priority = event.getPriority();
  }

  private void handleJobInitedEvent(JobInitedEvent event) {
    info.launchTime = event.getLaunchTime();
    info.totalMaps = event.getTotalMaps();
    info.totalReduces = event.getTotalReduces();
  }

  private void handleJobInfoChangeEvent(JobInfoChangeEvent event) {
    info.submitTime = event.getSubmitTime();
    info.launchTime = event.getLaunchTime();
  }

  private void handleJobSubmittedEvent(JobSubmittedEvent event) {
    info.jobid = event.getJobId();
    info.jobname = event.getJobName();
    info.username = event.getUserName();
    info.submitTime = event.getSubmitTime();
    info.jobConfPath = event.getJobConfPath();
    info.jobACLs = event.getJobAcls();
  }

  /**
   * The class where job information is aggregated into after parsing
   */
  public static class JobInfo {
    long submitTime;
    long finishTime;
    JobID jobid;
    String username;
    String jobname;
    String jobConfPath;
    long launchTime;
    int totalMaps;
    int totalReduces;
    int failedMaps;
    int failedReduces;
    int finishedMaps;
    int finishedReduces;
    String jobStatus;
    Counters totalCounters;
    Counters mapCounters;
    Counters reduceCounters;
    JobPriority priority;
    Map<JobACL, AccessControlList> jobACLs;
    
    Map<TaskID, TaskInfo> tasksMap;
    
    /** Create a job info object where job information will be stored
     * after a parse
     */
    public JobInfo() {
      submitTime = launchTime = finishTime = -1;
      totalMaps = totalReduces = failedMaps = failedReduces = 0;
      finishedMaps = finishedReduces = 0;
      username = jobname = jobConfPath = "";
      tasksMap = new HashMap<TaskID, TaskInfo>();
      jobACLs = new HashMap<JobACL, AccessControlList>();
    }
    
    /** Print all the job information */
    public void printAll() {
      System.out.println("JOBNAME: " + jobname);
      System.out.println("USERNAME: " + username);
      System.out.println("SUBMIT_TIME" + submitTime);
      System.out.println("LAUNCH_TIME: " + launchTime);
      System.out.println("JOB_STATUS: " + jobStatus);
      System.out.println("PRIORITY: " + priority);
      System.out.println("TOTAL_MAPS: " + totalMaps);
      System.out.println("TOTAL_REDUCES: " + totalReduces);
      System.out.println("MAP_COUNTERS:" + mapCounters.toString());
      System.out.println("REDUCE_COUNTERS:" + reduceCounters.toString());
      System.out.println("TOTAL_COUNTERS: " + totalCounters.toString());
      
      for (TaskInfo ti: tasksMap.values()) {
        ti.printAll();
      }
    }

    /** Get the job submit time */
    public long getSubmitTime() { return submitTime; }
    /** Get the job finish time */
    public long getFinishTime() { return finishTime; }
    /** Get the job id */
    public JobID getJobId() { return jobid; }
    /** Get the user name */
    public String getUsername() { return username; }
    /** Get the job name */
    public String getJobname() { return jobname; }
    /** Get the path for the job configuration file */
    public String getJobConfPath() { return jobConfPath; }
    /** Get the job launch time */
    public long getLaunchTime() { return launchTime; }
    /** Get the total number of maps */
    public long getTotalMaps() { return totalMaps; }
    /** Get the total number of reduces */
    public long getTotalReduces() { return totalReduces; }
    /** Get the total number of failed maps */
    public long getFailedMaps() { return failedMaps; }
    /** Get the number of failed reduces */
    public long getFailedReduces() { return failedReduces; }
    /** Get the number of finished maps */
    public long getFinishedMaps() { return finishedMaps; }
    /** Get the number of finished reduces */
    public long getFinishedReduces() { return finishedReduces; }
    /** Get the job status */
    public String getJobStatus() { return jobStatus; }
    /** Get the counters for the job */
    public Counters getTotalCounters() { return totalCounters; }
    /** Get the map counters for the job */
    public Counters getMapCounters() { return mapCounters; }
    /** Get the reduce counters for the job */
    public Counters getReduceCounters() { return reduceCounters; }
    /** Get the map of all tasks in this job */
    public Map<TaskID, TaskInfo> getAllTasks() { return tasksMap; }
    /** Get the priority of this job */
    public String getPriority() { return priority.toString(); }
    public Map<JobACL, AccessControlList> getJobACLs() { return jobACLs; }
  }
  
  /**
   * TaskInformation is aggregated in this class after parsing
   */
  public static class TaskInfo {
    TaskID taskId;
    long startTime;
    long finishTime;
    TaskType taskType;
    String splitLocations;
    Counters counters;
    String status;
    String error;
    TaskAttemptID failedDueToAttemptId;
    Map<TaskAttemptID, TaskAttemptInfo> attemptsMap;

    public TaskInfo() {
      startTime = finishTime = -1;
      error = splitLocations = "";
      attemptsMap = new HashMap<TaskAttemptID, TaskAttemptInfo>();
    }
    
    public void printAll() {
      System.out.println("TASK_ID:" + taskId.toString());
      System.out.println("START_TIME: " + startTime);
      System.out.println("FINISH_TIME:" + finishTime);
      System.out.println("TASK_TYPE:" + taskType);
      if (counters != null) {
        System.out.println("COUNTERS:" + counters.toString());
      }
      
      for (TaskAttemptInfo tinfo: attemptsMap.values()) {
        tinfo.printAll();
      }
    }
    
    /** Get the Task ID */
    public TaskID getTaskId() { return taskId; }
    /** Get the start time of this task */
    public long getStartTime() { return startTime; }
    /** Get the finish time of this task */
    public long getFinishTime() { return finishTime; }
    /** Get the task type */
    public TaskType getTaskType() { return taskType; }
    /** Get the split locations */
    public String getSplitLocations() { return splitLocations; }
    /** Get the counters for this task */
    public Counters getCounters() { return counters; }
    /** Get the task status */
    public String getTaskStatus() { return status; }
    /** Get the attempt Id that caused this task to fail */
    public TaskAttemptID getFailedDueToAttemptId() {
      return failedDueToAttemptId;
    }
    /** Get the error */
    public String getError() { return error; }
    /** Get the map of all attempts for this task */
    public Map<TaskAttemptID, TaskAttemptInfo> getAllTaskAttempts() {
      return attemptsMap;
    }
  }
  
  /**
   * Task Attempt Information is aggregated in this class after parsing
   */
  public static class TaskAttemptInfo {
    TaskAttemptID attemptId;
    long startTime;
    long finishTime;
    long shuffleFinishTime;
    long sortFinishTime;
    long mapFinishTime;
    String error;
    String status;
    String state;
    TaskType taskType;
    String trackerName;
    Counters counters;
    int httpPort;
    String hostname;

    /** Create a Task Attempt Info which will store attempt level information
     * on a history parse.
     */
    public TaskAttemptInfo() {
      startTime = finishTime = shuffleFinishTime = sortFinishTime = 
        mapFinishTime = -1;
      error =  state =  trackerName = hostname = "";
      httpPort = -1;
    }
    /**
     * Print all the information about this attempt.
     */
    public void printAll() {
      System.out.println("ATTEMPT_ID:" + attemptId.toString());
      System.out.println("START_TIME: " + startTime);
      System.out.println("FINISH_TIME:" + finishTime);
      System.out.println("ERROR:" + error);
      System.out.println("TASK_STATUS:" + status);
      System.out.println("STATE:" + state);
      System.out.println("TASK_TYPE:" + taskType);
      System.out.println("TRACKER_NAME:" + trackerName);
      System.out.println("HTTP_PORT:" + httpPort);
      if (counters != null) {
        System.out.println("COUNTERS:" + counters.toString());
      }
    }

    /** Get the attempt Id */
    public TaskAttemptID getAttemptId() { return attemptId; }
    /** Get the start time of the attempt */
    public long getStartTime() { return startTime; }
    /** Get the finish time of the attempt */
    public long getFinishTime() { return finishTime; }
    /** Get the shuffle finish time. Applicable only for reduce attempts */
    public long getShuffleFinishTime() { return shuffleFinishTime; }
    /** Get the sort finish time. Applicable only for reduce attempts */
    public long getSortFinishTime() { return sortFinishTime; }
    /** Get the map finish time. Applicable only for map attempts */
    public long getMapFinishTime() { return mapFinishTime; }
    /** Get the error string */
    public String getError() { return error; }
    /** Get the state */
    public String getState() { return state; }
    /** Get the task status */
    public String getTaskStatus() { return status; }
    /** Get the task type */
    public TaskType getTaskType() { return taskType; }
    /** Get the tracker name where the attempt executed */
    public String getTrackerName() { return trackerName; }
    /** Get the host name */
    public String getHostname() { return hostname; }
    /** Get the counters for the attempt */
    public Counters getCounters() { return counters; }
    /** Get the HTTP port for the tracker */
    public int getHttpPort() { return httpPort; }
  }
}
