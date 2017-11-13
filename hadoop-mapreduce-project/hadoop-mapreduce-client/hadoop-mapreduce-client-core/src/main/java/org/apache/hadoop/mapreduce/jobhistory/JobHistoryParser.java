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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
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
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Parser for the JobHistory files. Typical usage is
 * JobHistoryParser parser = new JobHistoryParser(fs, historyFile);
 * job = parser.parse();
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobHistoryParser implements HistoryEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryParser.class);
  
  private final FSDataInputStream in;
  private JobInfo info = null;

  private IOException parseException = null;
  
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
  
  public synchronized void parse(HistoryEventHandler handler) 
    throws IOException {
    parse(new EventReader(in), handler);
  }
  
  /**
   * Only used for unit tests.
   */
  @Private
  public synchronized void parse(EventReader reader, HistoryEventHandler handler)
    throws IOException {
    int eventCtr = 0;
    HistoryEvent event;
    try {
      while ((event = reader.getNextEvent()) != null) {
        handler.handleEvent(event);
        ++eventCtr;
      } 
    } catch (IOException ioe) {
      LOG.info("Caught exception parsing history file after " + eventCtr + 
          " events", ioe);
      parseException = ioe;
    } finally {
      in.close();
    }
  }
  
  
  /**
   * Parse the entire history file and populate the JobInfo object
   * The first invocation will populate the object, subsequent calls
   * will return the already parsed object. 
   * The input stream is closed on return 
   * 
   * This api ignores partial records and stops parsing on encountering one.
   * {@link #getParseException()} can be used to fetch the exception, if any.
   * 
   * @return The populated jobInfo object
   * @throws IOException
   * @see #getParseException()
   */
  public synchronized JobInfo parse() throws IOException {
    return parse(new EventReader(in)); 
  }

  /**
   * Only used for unit tests.
   */
  @Private
  public synchronized JobInfo parse(EventReader reader) throws IOException {

    if (info != null) {
      return info;
    }

    info = new JobInfo();
    parse(reader, this);
    return info;
  }
  
  /**
   * Get the parse exception, if any.
   * 
   * @return the parse exception, if any
   * @see #parse()
   */
  public synchronized IOException getParseException() {
    return parseException;
  }
  
  @Override
  public void handleEvent(HistoryEvent event)  { 
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
    case JOB_QUEUE_CHANGED:
      handleJobQueueChangeEvent((JobQueueChangeEvent) event);
      break;
    case JOB_FAILED:
    case JOB_KILLED:
    case JOB_ERROR:
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
    case AM_STARTED:
      handleAMStartedEvent((AMStartedEvent) event);
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
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleReduceAttemptFinishedEvent
  (ReduceAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.shuffleFinishTime = event.getShuffleFinishTime();
    attemptInfo.sortFinishTime = event.getSortFinishTime();
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleMapAttemptFinishedEvent(MapAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.mapFinishTime = event.getMapFinishTime();
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleTaskAttemptFailedEvent(
      TaskAttemptUnsuccessfulCompletionEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    if(taskInfo == null) {
      LOG.warn("TaskInfo is null for TaskAttemptUnsuccessfulCompletionEvent"
          + " taskId:  " + event.getTaskId().toString());
      return;
    }
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getTaskAttemptId());
    if(attemptInfo == null) {
      LOG.warn("AttemptInfo is null for TaskAttemptUnsuccessfulCompletionEvent"
          + " taskAttemptId:  " + event.getTaskAttemptId().toString());
      return;
    }
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.error = StringInterner.weakIntern(event.getError());
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    attemptInfo.shuffleFinishTime = event.getFinishTime();
    attemptInfo.sortFinishTime = event.getFinishTime();
    attemptInfo.mapFinishTime = event.getFinishTime();
    attemptInfo.counters = event.getCounters();
    if(TaskStatus.State.SUCCEEDED.toString().equals(taskInfo.status))
    {
      //this is a successful task
      if(attemptInfo.getAttemptId().equals(taskInfo.getSuccessfulAttemptId()))
      {
        // the failed attempt is the one that made this task successful
        // so its no longer successful. Reset fields set in
        // handleTaskFinishedEvent()
        taskInfo.counters = null;
        taskInfo.finishTime = -1;
        taskInfo.status = null;
        taskInfo.successfulAttemptId = null;
      }
    }
    info.completedTaskAttemptsMap.put(event.getTaskAttemptId(), attemptInfo);
  }

  private void handleTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    TaskAttemptID attemptId = event.getTaskAttemptId();
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    
    TaskAttemptInfo attemptInfo = new TaskAttemptInfo();
    attemptInfo.startTime = event.getStartTime();
    attemptInfo.attemptId = event.getTaskAttemptId();
    attemptInfo.httpPort = event.getHttpPort();
    attemptInfo.trackerName = StringInterner.weakIntern(event.getTrackerName());
    attemptInfo.taskType = event.getTaskType();
    attemptInfo.shufflePort = event.getShufflePort();
    attemptInfo.containerId = event.getContainerId();
    
    taskInfo.attemptsMap.put(attemptId, attemptInfo);
  }

  private void handleTaskFinishedEvent(TaskFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.counters = event.getCounters();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.status = TaskStatus.State.SUCCEEDED.toString();
    taskInfo.successfulAttemptId = event.getSuccessfulTaskAttemptId();
  }

  private void handleTaskUpdatedEvent(TaskUpdatedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.finishTime = event.getFinishTime();
  }

  private void handleTaskFailedEvent(TaskFailedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.status = TaskStatus.State.FAILED.toString();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.error = StringInterner.weakIntern(event.getError());
    taskInfo.failedDueToAttemptId = event.getFailedAttemptID();
    taskInfo.counters = event.getCounters();
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
    info.succeededMaps = event.getSucceededMaps();
    info.succeededReduces = event.getSucceededReduces();
    info.failedMaps = event.getFailedMaps();
    info.failedReduces = event.getFailedReduces();
    info.killedMaps = event.getKilledMaps();
    info.killedReduces = event.getKilledReduces();
    info.jobStatus = StringInterner.weakIntern(event.getStatus());
    info.errorInfo = StringInterner.weakIntern(event.getDiagnostics());
  }

  private void handleJobFinishedEvent(JobFinishedEvent event) {
    info.finishTime = event.getFinishTime();
    info.succeededMaps = event.getSucceededMaps();
    info.succeededReduces = event.getSucceededReduces();
    info.failedMaps = event.getFailedMaps();
    info.failedReduces = event.getFailedReduces();
    info.killedMaps = event.getKilledMaps();
    info.killedReduces = event.getKilledReduces();
    info.totalCounters = event.getTotalCounters();
    info.mapCounters = event.getMapCounters();
    info.reduceCounters = event.getReduceCounters();
    info.jobStatus = JobStatus.getJobRunState(JobStatus.SUCCEEDED);
  }

  private void handleJobPriorityChangeEvent(JobPriorityChangeEvent event) {
    info.priority = event.getPriority();
  }
  
  private void handleJobQueueChangeEvent(JobQueueChangeEvent event) {
    info.jobQueueName = event.getJobQueueName();
  }

  private void handleJobInitedEvent(JobInitedEvent event) {
    info.launchTime = event.getLaunchTime();
    info.totalMaps = event.getTotalMaps();
    info.totalReduces = event.getTotalReduces();
    info.uberized = event.getUberized();
  }
  
  private void handleAMStartedEvent(AMStartedEvent event) {
    AMInfo amInfo = new AMInfo();
    amInfo.appAttemptId = event.getAppAttemptId();
    amInfo.startTime = event.getStartTime();
    amInfo.containerId = event.getContainerId();
    amInfo.nodeManagerHost = StringInterner.weakIntern(event.getNodeManagerHost());
    amInfo.nodeManagerPort = event.getNodeManagerPort();
    amInfo.nodeManagerHttpPort = event.getNodeManagerHttpPort();
    if (info.amInfos == null) {
      info.amInfos = new LinkedList<AMInfo>();
    }
    info.amInfos.add(amInfo);
    info.latestAmInfo = amInfo;
  }

  private void handleJobInfoChangeEvent(JobInfoChangeEvent event) {
    info.submitTime = event.getSubmitTime();
    info.launchTime = event.getLaunchTime();
  }

  private void handleJobSubmittedEvent(JobSubmittedEvent event) {
    info.jobid = event.getJobId();
    info.jobname = event.getJobName();
    info.username = StringInterner.weakIntern(event.getUserName());
    info.submitTime = event.getSubmitTime();
    info.jobConfPath = event.getJobConfPath();
    info.jobACLs = event.getJobAcls();
    info.jobQueueName = StringInterner.weakIntern(event.getJobQueueName());
  }

  /**
   * The class where job information is aggregated into after parsing
   */
  public static class JobInfo {
    String errorInfo = "";
    long submitTime;
    long finishTime;
    JobID jobid;
    String username;
    String jobname;
    String jobQueueName;
    String jobConfPath;
    long launchTime;
    int totalMaps;
    int totalReduces;
    int failedMaps;
    int failedReduces;
    int succeededMaps;
    int succeededReduces;
    int killedMaps;
    int killedReduces;
    String jobStatus;
    Counters totalCounters;
    Counters mapCounters;
    Counters reduceCounters;
    JobPriority priority;
    Map<JobACL, AccessControlList> jobACLs;
    
    Map<TaskID, TaskInfo> tasksMap;
    Map<TaskAttemptID, TaskAttemptInfo> completedTaskAttemptsMap;
    List<AMInfo> amInfos;
    AMInfo latestAmInfo;
    boolean uberized;
    
    /** Create a job info object where job information will be stored
     * after a parse
     */
    public JobInfo() {
      submitTime = launchTime = finishTime = -1;
      totalMaps = totalReduces = failedMaps = failedReduces = 0;
      succeededMaps = succeededReduces = 0;
      username = jobname = jobConfPath = jobQueueName = "";
      tasksMap = new HashMap<TaskID, TaskInfo>();
      completedTaskAttemptsMap = new HashMap<TaskAttemptID, TaskAttemptInfo>();
      jobACLs = new HashMap<JobACL, AccessControlList>();
      priority = JobPriority.NORMAL;
    }
    
    /** Print all the job information */
    public void printAll() {
      System.out.println("JOBNAME: " + jobname);
      System.out.println("USERNAME: " + username);
      System.out.println("JOB_QUEUE_NAME: " + jobQueueName);
      System.out.println("SUBMIT_TIME" + submitTime);
      System.out.println("LAUNCH_TIME: " + launchTime);
      System.out.println("JOB_STATUS: " + jobStatus);
      System.out.println("PRIORITY: " + priority);
      System.out.println("TOTAL_MAPS: " + totalMaps);
      System.out.println("TOTAL_REDUCES: " + totalReduces);
      if (mapCounters != null) {
        System.out.println("MAP_COUNTERS:" + mapCounters.toString());
      }
      if (reduceCounters != null) {
        System.out.println("REDUCE_COUNTERS:" + reduceCounters.toString());
      }
      if (totalCounters != null) {
        System.out.println("TOTAL_COUNTERS: " + totalCounters.toString());
      }
      System.out.println("UBERIZED: " + uberized);
      if (amInfos != null) {
        for (AMInfo amInfo : amInfos) {
          amInfo.printAll();
        }
      }
      for (TaskInfo ti: tasksMap.values()) {
        ti.printAll();
      }
    }

    /** @return the job submit time */
    public long getSubmitTime() { return submitTime; }
    /** @return the job finish time */
    public long getFinishTime() { return finishTime; }
    /** @return the job id */
    public JobID getJobId() { return jobid; }
    /** @return the user name */
    public String getUsername() { return username; }
    /** @return the job name */
    public String getJobname() { return jobname; }
    /** @return the job queue name */
    public String getJobQueueName() { return jobQueueName; }
    /** @return the path for the job configuration file */
    public String getJobConfPath() { return jobConfPath; }
    /** @return the job launch time */
    public long getLaunchTime() { return launchTime; }
    /** @return the total number of maps */
    public long getTotalMaps() { return totalMaps; }
    /** @return the total number of reduces */
    public long getTotalReduces() { return totalReduces; }
    /** @return the total number of failed maps */
    public long getFailedMaps() { return failedMaps; }
    /** @return the number of failed reduces */
    public long getFailedReduces() { return failedReduces; }
    /** @return the number of killed maps */
    public long getKilledMaps() { return killedMaps; }
    /** @return the number of killed reduces */
    public long getKilledReduces() { return killedReduces; }
    /** @return the number of succeeded maps */
    public long getSucceededMaps() { return succeededMaps; }
    /** @return the number of succeeded reduces */
    public long getSucceededReduces() { return succeededReduces; }
    /** @return the job status */
    public String getJobStatus() { return jobStatus; }
    public String getErrorInfo() { return errorInfo; }
    /** @return the counters for the job */
    public Counters getTotalCounters() { return totalCounters; }
    /** @return the map counters for the job */
    public Counters getMapCounters() { return mapCounters; }
    /** @return the reduce counters for the job */
    public Counters getReduceCounters() { return reduceCounters; }
    /** @return the map of all tasks in this job */
    public Map<TaskID, TaskInfo> getAllTasks() { return tasksMap; }
    /** @return the map of all completed task attempts in this job */
    public Map<TaskAttemptID, TaskAttemptInfo> getAllCompletedTaskAttempts() { return completedTaskAttemptsMap; }
    /** @return the priority of this job */
    public String getPriority() { return priority.toString(); }
    public Map<JobACL, AccessControlList> getJobACLs() { return jobACLs; }
    /** @return the uberized status of this job */
    public boolean getUberized() { return uberized; }
    /** @return the AMInfo for the job's AppMaster */
    public List<AMInfo> getAMInfos() { return amInfos; }
    /** @return the AMInfo for the newest AppMaster */
    public AMInfo getLatestAMInfo() { return latestAmInfo; }
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
    TaskAttemptID successfulAttemptId;
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
    
    /** @return the Task ID */
    public TaskID getTaskId() { return taskId; }
    /** @return the start time of this task */
    public long getStartTime() { return startTime; }
    /** @return the finish time of this task */
    public long getFinishTime() { return finishTime; }
    /** @return the task type */
    public TaskType getTaskType() { return taskType; }
    /** @return the split locations */
    public String getSplitLocations() { return splitLocations; }
    /** @return the counters for this task */
    public Counters getCounters() { return counters; }
    /** @return the task status */
    public String getTaskStatus() { return status; }
    /** @return the attempt Id that caused this task to fail */
    public TaskAttemptID getFailedDueToAttemptId() {
      return failedDueToAttemptId;
    }
    /** @return the attempt Id that caused this task to succeed */
    public TaskAttemptID getSuccessfulAttemptId() {
      return successfulAttemptId;
    }
    /** @return the error */
    public String getError() { return error; }
    /** @return the map of all attempts for this task */
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
    int shufflePort;
    String hostname;
    int port;
    String rackname;
    ContainerId containerId;

    /** Create a Task Attempt Info which will store attempt level information
     * on a history parse.
     */
    public TaskAttemptInfo() {
      startTime = finishTime = shuffleFinishTime = sortFinishTime = 
        mapFinishTime = -1;
      error =  state =  trackerName = hostname = rackname = "";
      port = -1;
      httpPort = -1;
      shufflePort = -1;
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
      System.out.println("SHUFFLE_PORT:" + shufflePort);
      System.out.println("CONTIANER_ID:" + containerId);
      if (counters != null) {
        System.out.println("COUNTERS:" + counters.toString());
      }
    }

    /** @return the attempt Id */
    public TaskAttemptID getAttemptId() { return attemptId; }
    /** @return the start time of the attempt */
    public long getStartTime() { return startTime; }
    /** @return the finish time of the attempt */
    public long getFinishTime() { return finishTime; }
    /** @return the shuffle finish time. Applicable only for reduce attempts */
    public long getShuffleFinishTime() { return shuffleFinishTime; }
    /** @return the sort finish time. Applicable only for reduce attempts */
    public long getSortFinishTime() { return sortFinishTime; }
    /** @return the map finish time. Applicable only for map attempts */
    public long getMapFinishTime() { return mapFinishTime; }
    /** @return the error string */
    public String getError() { return error; }
    /** @return the state */
    public String getState() { return state; }
    /** @return the task status */
    public String getTaskStatus() { return status; }
    /** @return the task type */
    public TaskType getTaskType() { return taskType; }
    /** @return the tracker name where the attempt executed */
    public String getTrackerName() { return trackerName; }
    /** @return the host name */
    public String getHostname() { return hostname; }
    /** @return the port */
    public int getPort() { return port; }
    /** @return the rack name */
    public String getRackname() { return rackname; }
    /** @return the counters for the attempt */
    public Counters getCounters() { return counters; }
    /** @return the HTTP port for the tracker */
    public int getHttpPort() { return httpPort; }
    /** @return the Shuffle port for the tracker */
    public int getShufflePort() { return shufflePort; }
    /** @return the ContainerId for the tracker */
    public ContainerId getContainerId() { return containerId; }
  }

  /**
   * Stores AM information
   */
  public static class AMInfo {
    ApplicationAttemptId appAttemptId;
    long startTime;
    ContainerId containerId;
    String nodeManagerHost;
    int nodeManagerPort;
    int nodeManagerHttpPort;

    /**
     * Create a AM Info which will store AM level information on a history
     * parse.
     */
    public AMInfo() {
      startTime = -1;
      nodeManagerHost = "";
      nodeManagerHttpPort = -1;
    }

    public AMInfo(ApplicationAttemptId appAttemptId, long startTime,
        ContainerId containerId, String nodeManagerHost, int nodeManagerPort,
        int nodeManagerHttpPort) {
      this.appAttemptId = appAttemptId;
      this.startTime = startTime;
      this.containerId = containerId;
      this.nodeManagerHost = nodeManagerHost;
      this.nodeManagerPort = nodeManagerPort;
      this.nodeManagerHttpPort = nodeManagerHttpPort;
    }

    /**
     * Print all the information about this AM.
     */
    public void printAll() {
      System.out.println("APPLICATION_ATTEMPT_ID:" + appAttemptId.toString());
      System.out.println("START_TIME: " + startTime);
      System.out.println("CONTAINER_ID: " + containerId.toString());
      System.out.println("NODE_MANAGER_HOST: " + nodeManagerHost);
      System.out.println("NODE_MANAGER_PORT: " + nodeManagerPort);
      System.out.println("NODE_MANAGER_HTTP_PORT: " + nodeManagerHttpPort);
    }

    /** @return the ApplicationAttemptId */
    public ApplicationAttemptId getAppAttemptId() {
      return appAttemptId;
    }

    /** @return the start time of the AM */
    public long getStartTime() {
      return startTime;
    }

    /** @return the container id for the AM */
    public ContainerId getContainerId() {
      return containerId;
    }

    /** @return the host name for the node manager on which the AM is running */
    public String getNodeManagerHost() {
      return nodeManagerHost;
    }

    /** @return the port for the node manager running the AM */
    public int getNodeManagerPort() {
      return nodeManagerPort;
    }

    /** @return the http port for the node manager running the AM */
    public int getNodeManagerHttpPort() {
      return nodeManagerHttpPort;
    }
  }

}
