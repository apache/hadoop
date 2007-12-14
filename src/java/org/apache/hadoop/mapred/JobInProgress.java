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
package org.apache.hadoop.mapred;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapred.JobTracker.JobTrackerMetrics;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;

/*************************************************************
 * JobInProgress maintains all the info for keeping
 * a Job on the straight and narrow.  It keeps its JobProfile
 * and its latest JobStatus, plus a set of tables for 
 * doing bookkeeping of its Tasks.
 * ***********************************************************
 */
class JobInProgress {
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.JobInProgress");
    
  JobProfile profile;
  JobStatus status;
  Path localJobFile = null;
  Path localJarFile = null;

  TaskInProgress maps[] = new TaskInProgress[0];
  TaskInProgress reduces[] = new TaskInProgress[0];
  int numMapTasks = 0;
  int numReduceTasks = 0;
  int runningMapTasks = 0;
  int runningReduceTasks = 0;
  int finishedMapTasks = 0;
  int finishedReduceTasks = 0;
  int failedMapTasks = 0; 
  int failedReduceTasks = 0;
  
  int mapFailuresPercent = 0;
  int reduceFailuresPercent = 0;
  int failedMapTIPs = 0;
  int failedReduceTIPs = 0;

  JobPriority priority = JobPriority.NORMAL;
  JobTracker jobtracker = null;
  Map<String,List<TaskInProgress>> hostToMaps =
    new HashMap<String,List<TaskInProgress>>();
  private int taskCompletionEventTracker = 0; 
  List<TaskCompletionEvent> taskCompletionEvents;
    
  // The maximum percentage of trackers in cluster added to the 'blacklist'.
  private static final double CLUSTER_BLACKLIST_PERCENT = 0.25;
  
  // No. of tasktrackers in the cluster
  private volatile int clusterSize = 0;
  
  // The no. of tasktrackers where >= conf.getMaxTaskFailuresPerTracker()
  // tasks have failed
  private volatile int flakyTaskTrackers = 0;
  // Map of trackerHostName -> no. of task failures
  private Map<String, Integer> trackerToFailuresMap = 
    new TreeMap<String, Integer>();
    
  long startTime;
  long finishTime;

  private JobConf conf;
  boolean tasksInited = false;

  private LocalFileSystem localFs;
  private String jobId;

  // Per-job counters
  public static enum Counter { 
    NUM_FAILED_MAPS, 
    NUM_FAILED_REDUCES,
    TOTAL_LAUNCHED_MAPS,
    TOTAL_LAUNCHED_REDUCES,
    DATA_LOCAL_MAPS
  }
  private Counters jobCounters = new Counters();
  
  private MetricsRecord jobMetrics;
  
  // Maximum no. of fetch-failure notifications after which
  // the map task is killed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;
  
  // Map of mapTaskId -> no. of fetch failures
  private Map<String, Integer> mapTaskIdToFetchFailuresMap =
    new TreeMap<String, Integer>();
  
  /**
   * Create a JobInProgress with the given job file, plus a handle
   * to the tracker.
   */
  public JobInProgress(String jobid, JobTracker jobtracker, 
                       JobConf default_conf) throws IOException {
    this.jobId = jobid;
    String url = "http://" + jobtracker.getJobTrackerMachine() + ":" 
        + jobtracker.getInfoPort() + "/jobdetails.jsp?jobid=" + jobid;
    this.jobtracker = jobtracker;
    this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.PREP);
    this.startTime = System.currentTimeMillis();
    this.localFs = FileSystem.getLocal(default_conf);

    JobConf default_job_conf = new JobConf(default_conf);
    this.localJobFile = default_job_conf.getLocalPath(JobTracker.SUBDIR 
                                                      +"/"+jobid + ".xml");
    this.localJarFile = default_job_conf.getLocalPath(JobTracker.SUBDIR
                                                      +"/"+ jobid + ".jar");
    FileSystem fs = FileSystem.get(default_conf);
    Path jobFile = new Path(default_conf.getSystemDir(), jobid + "/job.xml");
    fs.copyToLocalFile(jobFile, localJobFile);
    conf = new JobConf(localJobFile);
    this.priority = conf.getJobPriority();
    this.profile = new JobProfile(conf.getUser(), jobid, 
                                  jobFile.toString(), url, conf.getJobName());
    String jarFile = conf.getJar();
    if (jarFile != null) {
      fs.copyToLocalFile(new Path(jarFile), localJarFile);
      conf.setJar(localJarFile.toString());
    }

    this.numMapTasks = conf.getNumMapTasks();
    this.numReduceTasks = conf.getNumReduceTasks();
    this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>
       (numMapTasks + numReduceTasks + 10);

    this.mapFailuresPercent = conf.getMaxMapTaskFailuresPercent();
    this.reduceFailuresPercent = conf.getMaxReduceTaskFailuresPercent();
        
    JobHistory.JobInfo.logSubmitted(jobid, conf, jobFile.toString(), 
                                    System.currentTimeMillis()); 
        
    MetricsContext metricsContext = MetricsUtil.getContext("mapred");
    this.jobMetrics = MetricsUtil.createRecord(metricsContext, "job");
    this.jobMetrics.setTag("user", conf.getUser());
    this.jobMetrics.setTag("sessionId", conf.getSessionId());
    this.jobMetrics.setTag("jobName", conf.getJobName());
    this.jobMetrics.setTag("jobId", jobid);
  }

  /**
   * Called periodically by JobTrackerMetrics to update the metrics for
   * this job.
   */
  public void updateMetrics() {
    Counters counters = getCounters();
    for (String groupName : counters.getGroupNames()) {
      Counters.Group group = counters.getGroup(groupName);
      jobMetrics.setTag("group", group.getDisplayName());
          
      for (String counter : group.getCounterNames()) {
        long value = group.getCounter(counter);
        jobMetrics.setTag("counter", group.getDisplayName(counter));
        jobMetrics.setMetric("value", (float) value);
        jobMetrics.update();
      }
    }
  }
    
  /**
   * Called when the job is complete
   */
  public void cleanUpMetrics() {
    // Deletes all metric data for this job (in internal table in metrics package).
    // This frees up RAM and possibly saves network bandwidth, since otherwise
    // the metrics package implementation might continue to send these job metrics
    // after the job has finished.
    jobMetrics.removeTag("group");
    jobMetrics.removeTag("counter");
    jobMetrics.remove();
  }
  
    
  /**
   * Construct the splits, etc.  This is invoked from an async
   * thread so that split-computation doesn't block anyone.
   */
  public synchronized void initTasks() throws IOException {
    if (tasksInited) {
      return;
    }

    //
    // read input splits and create a map per a split
    //
    String jobFile = profile.getJobFile();

    FileSystem fs = FileSystem.get(conf);
    DataInputStream splitFile =
      fs.open(new Path(conf.get("mapred.job.split.file")));
    JobClient.RawSplit[] splits;
    try {
      splits = JobClient.readSplitFile(splitFile);
    } finally {
      splitFile.close();
    }
    numMapTasks = splits.length;
    maps = new TaskInProgress[numMapTasks];
    for(int i=0; i < numMapTasks; ++i) {
      maps[i] = new TaskInProgress(jobId, jobFile, 
                                   splits[i].getClassName(),
                                   splits[i].getBytes(), 
                                   jobtracker, conf, this, i);
      for(String host: splits[i].getLocations()) {
        List<TaskInProgress> hostMaps = hostToMaps.get(host);
        if (hostMaps == null) {
          hostMaps = new ArrayList<TaskInProgress>();
          hostToMaps.put(host, hostMaps);
        }
        hostMaps.add(maps[i]);              
      }
    }
        
    // if no split is returned, job is considered completed and successful
    if (numMapTasks == 0) {
      // Finished time need to be setted here to prevent this job to be retired
      // from the job tracker jobs at the next retire iteration.
      this.finishTime = System.currentTimeMillis();
      status.setMapProgress(1.0f);
      status.setReduceProgress(1.0f);
      status.setRunState(JobStatus.SUCCEEDED);
      tasksInited = true;
      JobHistory.JobInfo.logStarted(profile.getJobId(), 
                                    System.currentTimeMillis(), 0, 0);
      JobHistory.JobInfo.logFinished(profile.getJobId(), 
                                     System.currentTimeMillis(), 0, 0, 0, 0,
                                     getCounters());
      // Special case because the Job is not queued
      JobEndNotifier.registerNotification(this.getJobConf(), this.getStatus());

      return;
    }

    //
    // Create reduce tasks
    //
    this.reduces = new TaskInProgress[numReduceTasks];
    for (int i = 0; i < numReduceTasks; i++) {
      reduces[i] = new TaskInProgress(jobId, jobFile, 
                                      numMapTasks, i, 
                                      jobtracker, conf, this);
    }

    this.status = new JobStatus(status.getJobId(), 0.0f, 0.0f, JobStatus.RUNNING);
    tasksInited = true;
        
    JobHistory.JobInfo.logStarted(profile.getJobId(), System.currentTimeMillis(), numMapTasks, numReduceTasks);
  }

  /////////////////////////////////////////////////////
  // Accessors for the JobInProgress
  /////////////////////////////////////////////////////
  public JobProfile getProfile() {
    return profile;
  }
  public JobStatus getStatus() {
    return status;
  }
  public long getStartTime() {
    return startTime;
  }
  public long getFinishTime() {
    return finishTime;
  }
  public int desiredMaps() {
    return numMapTasks;
  }
  public synchronized int finishedMaps() {
    return finishedMapTasks;
  }
  public int desiredReduces() {
    return numReduceTasks;
  }
  public synchronized int runningMaps() {
    return runningMapTasks;
  }
  public synchronized int runningReduces() {
    return runningReduceTasks;
  }
  public synchronized int finishedReduces() {
    return finishedReduceTasks;
  }
  public JobPriority getPriority() {
    return this.priority;
  }
  public void setPriority(JobPriority priority) {
    if(priority == null) {
      this.priority = JobPriority.NORMAL;
    } else {
      this.priority = priority;
    }
  }
 
  /**
   * Get the list of map tasks
   * @return the raw array of maps for this job
   */
  TaskInProgress[] getMapTasks() {
    return maps;
  }
    
  /**
   * Get the list of reduce tasks
   * @return the raw array of reduce tasks for this job
   */
  TaskInProgress[] getReduceTasks() {
    return reduces;
  }
    
  /**
   * Get the job configuration
   * @return the job's configuration
   */
  JobConf getJobConf() {
    return conf;
  }
    
  /**
   * Return a vector of completed TaskInProgress objects
   */
  public Vector<TaskInProgress> reportTasksInProgress(boolean shouldBeMap,
                                                      boolean shouldBeComplete) {
    
    Vector<TaskInProgress> results = new Vector<TaskInProgress>();
    TaskInProgress tips[] = null;
    if (shouldBeMap) {
      tips = maps;
    } else {
      tips = reduces;
    }
    for (int i = 0; i < tips.length; i++) {
      if (tips[i].isComplete() == shouldBeComplete) {
        results.add(tips[i]);
      }
    }
    return results;
  }

  ////////////////////////////////////////////////////
  // Status update methods
  ////////////////////////////////////////////////////
  public synchronized void updateTaskStatus(TaskInProgress tip, 
                                            TaskStatus status,
                                            JobTrackerMetrics metrics) {

    double oldProgress = tip.getProgress();   // save old progress
    boolean wasRunning = tip.isRunning();
    boolean wasComplete = tip.isComplete();
    boolean change = tip.updateStatus(status);
    if (change) {
      TaskStatus.State state = status.getRunState();
      TaskTrackerStatus ttStatus = 
        this.jobtracker.getTaskTracker(status.getTaskTracker());
      String httpTaskLogLocation = null; 

      if (state == TaskStatus.State.COMMIT_PENDING ||
          state == TaskStatus.State.FAILED ||
          state == TaskStatus.State.KILLED) {
        JobWithTaskContext j = new JobWithTaskContext(this, tip, 
                                                      status.getTaskId(),
                                                      metrics);
        jobtracker.addToCommitQueue(j);
      }

      if (null != ttStatus){
        httpTaskLogLocation = "http://" + ttStatus.getHost() + ":" + 
          ttStatus.getHttpPort() + "/tasklog?plaintext=true&taskid=" +
          status.getTaskId();
      }

      TaskCompletionEvent taskEvent = null;
      if (state == TaskStatus.State.SUCCEEDED) {
        taskEvent = new TaskCompletionEvent(
                                            taskCompletionEventTracker, 
                                            status.getTaskId(),
                                            tip.idWithinJob(),
                                            status.getIsMap(),
                                            TaskCompletionEvent.Status.SUCCEEDED,
                                            httpTaskLogLocation 
                                           );
        tip.setSuccessEventNumber(taskCompletionEventTracker); 
      }
      //For a failed task update the JT datastructures.For the task state where
      //only the COMMIT is pending, delegate everything to the JT thread. For
      //failed tasks we want the JT to schedule a reexecution ASAP (and not go
      //via the queue for the datastructures' updates).
      else if (state == TaskStatus.State.COMMIT_PENDING) {
        return;
      } else if (state == TaskStatus.State.FAILED ||
                 state == TaskStatus.State.KILLED) {
        // Get the event number for the (possibly) previously successful
        // task. If there exists one, then set that status to OBSOLETE 
        int eventNumber;
        if ((eventNumber = tip.getSuccessEventNumber()) != -1) {
          TaskCompletionEvent t = 
            this.taskCompletionEvents.get(eventNumber);
          if (t.getTaskId().equals(status.getTaskId()))
            t.setTaskStatus(TaskCompletionEvent.Status.OBSOLETE);
        }
        
        // Tell the job to fail the relevant task
        failedTask(tip, status.getTaskId(), status, status.getTaskTracker(),
                   wasRunning, wasComplete, metrics);

        // Did the task failure lead to tip failure?
        TaskCompletionEvent.Status taskCompletionStatus = 
          (state == TaskStatus.State.FAILED ) ?
              TaskCompletionEvent.Status.FAILED :
              TaskCompletionEvent.Status.KILLED;
        if (tip.isFailed()) {
          taskCompletionStatus = TaskCompletionEvent.Status.TIPFAILED;
        }
        taskEvent = new TaskCompletionEvent(taskCompletionEventTracker, 
                                            status.getTaskId(),
                                            tip.idWithinJob(),
                                            status.getIsMap(),
                                            taskCompletionStatus, 
                                            httpTaskLogLocation
                                           );
      }          

      // Add the 'complete' task i.e. successful/failed
      // It _is_ safe to add the TaskCompletionEvent.Status.SUCCEEDED
      // *before* calling TIP.completedTask since:
      // a. One and only one task of a TIP is declared as a SUCCESS, the
      //    other (speculative tasks) are marked KILLED by the TaskCommitThread
      // b. TIP.completedTask *does not* throw _any_ exception at all.
      if (taskEvent != null) {
        this.taskCompletionEvents.add(taskEvent);
        taskCompletionEventTracker++;
        if (state == TaskStatus.State.SUCCEEDED) {
          completedTask(tip, status, metrics);
        }
      }
    }
        
    //
    // Update JobInProgress status
    //
    if(LOG.isDebugEnabled()) {
      LOG.debug("Taking progress for " + tip.getTIPId() + " from " + 
                 oldProgress + " to " + tip.getProgress());
    }
    double progressDelta = tip.getProgress() - oldProgress;
    if (tip.isMapTask()) {
      if (maps.length == 0) {
        this.status.setMapProgress(1.0f);
      } else {
        this.status.setMapProgress((float) (this.status.mapProgress() +
                                            progressDelta / maps.length));
      }
    } else {
      if (reduces.length == 0) {
        this.status.setReduceProgress(1.0f);
      } else {
        this.status.setReduceProgress
          ((float) (this.status.reduceProgress() +
                    (progressDelta / reduces.length)));
      }
    }
  }

  /**
   * Returns the job-level counters.
   * 
   * @return the job-level counters.
   */
  public synchronized Counters getJobCounters() {
    return jobCounters;
  }
  
  /**
   *  Returns map phase counters by summing over all map tasks in progress.
   */
  public synchronized Counters getMapCounters() {
    return sumTaskCounters(maps);
  }
    
  /**
   *  Returns map phase counters by summing over all map tasks in progress.
   */
  public synchronized Counters getReduceCounters() {
    return sumTaskCounters(reduces);
  }
    
  /**
   *  Returns the total job counters, by adding together the job, 
   *  the map and the reduce counters.
   */
  public Counters getCounters() {
    return Counters.sum(getJobCounters(), 
                        Counters.sum(getMapCounters(), getReduceCounters()));
  }
    
  /**
   * Returns a Counters instance representing the sum of all the counters in
   * the array of tasks in progress.
   */
  private Counters sumTaskCounters(TaskInProgress[] tips) {
    Counters counters = new Counters();
    for (TaskInProgress tip : tips) {
      counters.incrAllCounters(tip.getCounters());
    }
    return counters;
  }

  /////////////////////////////////////////////////////
  // Create/manage tasks
  /////////////////////////////////////////////////////
  /**
   * Return a MapTask, if appropriate, to run on the given tasktracker
   */
  public synchronized Task obtainNewMapTask(TaskTrackerStatus tts, 
                                            int clusterSize
                                           ) throws IOException {
    if (!tasksInited) {
      LOG.info("Cannot create task split for " + profile.getJobId());
      return null;
    }
    
    ArrayList mapCache = (ArrayList)hostToMaps.get(tts.getHost());
    int target = findNewTask(tts, clusterSize, status.mapProgress(), 
                             maps, mapCache);
    if (target == -1) {
      return null;
    }
    
    boolean wasRunning = maps[target].isRunning();
    Task result = maps[target].getTaskToRun(tts.getTrackerName());
    if (!wasRunning) {
      runningMapTasks += 1;
      JobHistory.Task.logStarted(profile.getJobId(), 
                                 maps[target].getTIPId(), Values.MAP.name(),
                                 System.currentTimeMillis());
    }

    jobCounters.incrCounter(Counter.TOTAL_LAUNCHED_MAPS, 1);

    return result;
  }    

  /**
   * Return a ReduceTask, if appropriate, to run on the given tasktracker.
   * We don't have cache-sensitivity for reduce tasks, as they
   *  work on temporary MapRed files.  
   */
  public synchronized Task obtainNewReduceTask(TaskTrackerStatus tts,
                                               int clusterSize
                                              ) throws IOException {
    if (!tasksInited) {
      LOG.info("Cannot create task split for " + profile.getJobId());
      return null;
    }

    int target = findNewTask(tts, clusterSize, status.reduceProgress() , 
                             reduces, null);
    if (target == -1) {
      return null;
    }
    
    boolean wasRunning = reduces[target].isRunning();
    Task result = reduces[target].getTaskToRun(tts.getTrackerName());
    if (!wasRunning) {
      runningReduceTasks += 1;
      JobHistory.Task.logStarted(profile.getJobId(), 
                                 reduces[target].getTIPId(), Values.REDUCE.name(),
                                 System.currentTimeMillis());
    }
    
    jobCounters.incrCounter(Counter.TOTAL_LAUNCHED_REDUCES, 1);

    return result;
  }
    
  private String convertTrackerNameToHostName(String trackerName) {
    // Ugly!
    // Convert the trackerName to it's host name
    int indexOfColon = trackerName.indexOf(":");
    String trackerHostName = (indexOfColon == -1) ? 
      trackerName : 
      trackerName.substring(0, indexOfColon);
    return trackerHostName;
  }
    
  /**
   * Note that a task has failed on a given tracker and add the tracker  
   * to the blacklist iff too many trackers in the cluster i.e. 
   * (clusterSize * CLUSTER_BLACKLIST_PERCENT) haven't turned 'flaky' already.
   * 
   * @param trackerName task-tracker on which a task failed
   */
  void addTrackerTaskFailure(String trackerName) {
    if (flakyTaskTrackers < (clusterSize * CLUSTER_BLACKLIST_PERCENT)) { 
      String trackerHostName = convertTrackerNameToHostName(trackerName);

      Integer trackerFailures = trackerToFailuresMap.get(trackerHostName);
      if (trackerFailures == null) {
        trackerFailures = 0;
      }
      trackerToFailuresMap.put(trackerHostName, ++trackerFailures);

      // Check if this tasktracker has turned 'flaky'
      if (trackerFailures.intValue() == conf.getMaxTaskFailuresPerTracker()) {
        ++flakyTaskTrackers;
        LOG.info("TaskTracker at '" + trackerHostName + "' turned 'flaky'");
      }
    }
  }
    
  private int getTrackerTaskFailures(String trackerName) {
    String trackerHostName = convertTrackerNameToHostName(trackerName);
    Integer failedTasks = trackerToFailuresMap.get(trackerHostName);
    return (failedTasks != null) ? failedTasks.intValue() : 0; 
  }
    
  /**
   * Get the no. of 'flaky' tasktrackers for a given job.
   * 
   * @return the no. of 'flaky' tasktrackers for a given job.
   */
  int getNoOfBlackListedTrackers() {
    return flakyTaskTrackers;
  }
    
  /**
   * Get the information on tasktrackers and no. of errors which occurred
   * on them for a given job. 
   * 
   * @return the map of tasktrackers and no. of errors which occurred
   *         on them for a given job. 
   */
  synchronized Map<String, Integer> getTaskTrackerErrors() {
    // Clone the 'trackerToFailuresMap' and return the copy
    Map<String, Integer> trackerErrors = 
      new TreeMap<String, Integer>(trackerToFailuresMap);
    return trackerErrors;
  }
    
  /**
   * Find a new task to run.
   * @param tts The task tracker that is asking for a task
   * @param clusterSize The number of task trackers in the cluster
   * @param avgProgress The average progress of this kind of task in this job
   * @param tasks The list of potential tasks to try
   * @param firstTaskToTry The first index in tasks to check
   * @param cachedTasks A list of tasks that would like to run on this node
   * @return the index in tasks of the selected task (or -1 for no task)
   */
  private int findNewTask(TaskTrackerStatus tts, 
                          int clusterSize,
                          double avgProgress,
                          TaskInProgress[] tasks,
                          List cachedTasks) {
    String taskTracker = tts.getTrackerName();

    //
    // Update the last-known clusterSize
    //
    this.clusterSize = clusterSize;

    //
    // Check if too many tasks of this job have failed on this
    // tasktracker prior to assigning it a new one.
    //
    int taskTrackerFailedTasks = getTrackerTaskFailures(taskTracker);
    if ((flakyTaskTrackers < (clusterSize * CLUSTER_BLACKLIST_PERCENT)) && 
        taskTrackerFailedTasks >= conf.getMaxTaskFailuresPerTracker()) {
      if (LOG.isDebugEnabled()) {
        String flakyTracker = convertTrackerNameToHostName(taskTracker); 
        LOG.debug("Ignoring the black-listed tasktracker: '" + flakyTracker 
                + "' for assigning a new task");
      }
      return -1;
    }
        
    //
    // See if there is a split over a block that is stored on
    // the TaskTracker checking in.  That means the block
    // doesn't have to be transmitted from another node.
    //
    if (cachedTasks != null) {
      Iterator i = cachedTasks.iterator();
      while (i.hasNext()) {
        TaskInProgress tip = (TaskInProgress)i.next();
        i.remove();
        if (tip.isRunnable() && 
            !tip.isRunning() &&
            !tip.hasFailedOnMachine(taskTracker)) {
          LOG.info("Choosing cached task " + tip.getTIPId());
          int cacheTarget = tip.getIdWithinJob();
          jobCounters.incrCounter(Counter.DATA_LOCAL_MAPS, 1);
          return cacheTarget;
        }
      }
    }


    //
    // If there's no cached target, see if there's
    // a std. task to run.
    //
    int failedTarget = -1;
    int specTarget = -1;
    for (int i = 0; i < tasks.length; i++) {
      TaskInProgress task = tasks[i];
      if (task.isRunnable()) {
        // if it failed here and we haven't tried every machine, we
        // don't schedule it here.
        boolean hasFailed = task.hasFailedOnMachine(taskTracker);
        if (hasFailed && (task.getNumberOfFailedMachines() < clusterSize)) {
          continue;
        }
        boolean isRunning = task.isRunning();
        if (hasFailed) {
          // failed tasks that aren't running can be scheduled as a last
          // resort
          if (!isRunning && failedTarget == -1) {
            failedTarget = i;
          }
        } else {
          if (!isRunning) {
            LOG.info("Choosing normal task " + tasks[i].getTIPId());
            return i;
          } else if (specTarget == -1 &&
                     task.hasSpeculativeTask(avgProgress) && 
                     !task.hasRunOnMachine(taskTracker)) {
            specTarget = i;
          }
        }
      }
    }
    if (specTarget != -1) {
      LOG.info("Choosing speculative task " + 
               tasks[specTarget].getTIPId());
    } else if (failedTarget != -1) {
      LOG.info("Choosing failed task " + 
               tasks[failedTarget].getTIPId());          
    }
    
    return specTarget != -1 ? specTarget : failedTarget;
  }

  /**
   * A taskid assigned to this JobInProgress has reported in successfully.
   */
  public synchronized boolean completedTask(TaskInProgress tip, 
                                         TaskStatus status,
                                         JobTrackerMetrics metrics) 
  {
    String taskid = status.getTaskId();
        
    // Sanity check: is the TIP already complete? 
    if (tip.isComplete()) {
      // Mark this task as KILLED
      tip.alreadyCompletedTask(taskid);

      // Let the JobTracker cleanup this taskid if the job isn't running
      if (this.status.getRunState() != JobStatus.RUNNING) {
        jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);
      }
      return false;
    } 

    LOG.info("Task '" + taskid + "' has completed " + tip.getTIPId() + 
             " successfully.");          

    // Mark the TIP as complete
    tip.completed(taskid);

    // Update jobhistory 
    String taskTrackerName = status.getTaskTracker();
    if (status.getIsMap()){
      JobHistory.MapAttempt.logStarted(profile.getJobId(), 
                                       tip.getTIPId(), status.getTaskId(), status.getStartTime(), 
                                       taskTrackerName); 
      JobHistory.MapAttempt.logFinished(profile.getJobId(), 
                                        tip.getTIPId(), status.getTaskId(), status.getFinishTime(), 
                                        taskTrackerName); 
      JobHistory.Task.logFinished(profile.getJobId(), tip.getTIPId(), 
                                  Values.MAP.name(), status.getFinishTime(),
                                  status.getCounters()); 
    }else{
      JobHistory.ReduceAttempt.logStarted(profile.getJobId(), 
                                          tip.getTIPId(), status.getTaskId(), status.getStartTime(), 
                                          taskTrackerName); 
      JobHistory.ReduceAttempt.logFinished(profile.getJobId(), 
                                           tip.getTIPId(), status.getTaskId(), status.getShuffleFinishTime(),
                                           status.getSortFinishTime(), status.getFinishTime(), 
                                           taskTrackerName); 
      JobHistory.Task.logFinished(profile.getJobId(), tip.getTIPId(), 
                                  Values.REDUCE.name(), status.getFinishTime(),
                                  status.getCounters()); 
    }
        
    // Update the running/finished map/reduce counts
    if (tip.isMapTask()){
      runningMapTasks -= 1;
      finishedMapTasks += 1;
      metrics.completeMap();
    } else{
      runningReduceTasks -= 1;
      finishedReduceTasks += 1;
      metrics.completeReduce();
    }
        
    //
    // Figure out whether the Job is done
    //
    isJobComplete(tip, metrics);
    
    if (this.status.getRunState() != JobStatus.RUNNING) {
      // The job has been killed/failed, 
      // JobTracker should cleanup this task
      jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);
      return false;
    }
    
    return true;
  }

  /**
   * Check if the job is done since all it's component tasks are either
   * successful or have failed.
   * 
   * @param tip the current tip which completed either succesfully or failed
   * @param metrics job-tracker metrics
   * @return
   */
  private boolean isJobComplete(TaskInProgress tip, JobTrackerMetrics metrics) {
    boolean allDone = true;
    for (int i = 0; i < maps.length; i++) {
      if (!(maps[i].isComplete() || maps[i].isFailed())) {
        allDone = false;
        break;
      }
    }
    if (allDone) {
      if (tip.isMapTask()) {
        this.status.setMapProgress(1.0f);              
      }
      for (int i = 0; i < reduces.length; i++) {
        if (!(reduces[i].isComplete() || reduces[i].isFailed())) {
          allDone = false;
          break;
        }
      }
    }

    //
    // If all tasks are complete, then the job is done!
    //
    if (this.status.getRunState() == JobStatus.RUNNING && allDone) {
      this.status.setRunState(JobStatus.SUCCEEDED);
      this.status.setReduceProgress(1.0f);
      this.finishTime = System.currentTimeMillis();
      garbageCollect();
      LOG.info("Job " + this.status.getJobId() + 
               " has completed successfully.");
      JobHistory.JobInfo.logFinished(this.status.getJobId(), finishTime, 
                                     this.finishedMapTasks, 
                                     this.finishedReduceTasks, failedMapTasks, 
                                     failedReduceTasks, getCounters());
      metrics.completeJob();
      return true;
    }
    
    return false;
  }
  /**
   * Kill the job and all its component tasks.
   */
  public synchronized void kill() {
    if (status.getRunState() != JobStatus.FAILED) {
      LOG.info("Killing job '" + this.status.getJobId() + "'");
      this.status = new JobStatus(status.getJobId(), 1.0f, 1.0f, JobStatus.FAILED);
      this.finishTime = System.currentTimeMillis();
      this.runningMapTasks = 0;
      this.runningReduceTasks = 0;
      //
      // kill all TIPs.
      //
      for (int i = 0; i < maps.length; i++) {
        maps[i].kill();
      }
      for (int i = 0; i < reduces.length; i++) {
        reduces[i].kill();
      }
      JobHistory.JobInfo.logFailed(this.status.getJobId(), finishTime, 
                                   this.finishedMapTasks, this.finishedReduceTasks);
      garbageCollect();
    }
  }

  /**
   * A task assigned to this JobInProgress has reported in as failed.
   * Most of the time, we'll just reschedule execution.  However, after
   * many repeated failures we may instead decide to allow the entire 
   * job to fail or succeed if the user doesn't care about a few tasks failing.
   *
   * Even if a task has reported as completed in the past, it might later
   * be reported as failed.  That's because the TaskTracker that hosts a map
   * task might die before the entire job can complete.  If that happens,
   * we need to schedule reexecution so that downstream reduce tasks can 
   * obtain the map task's output.
   */
  private void failedTask(TaskInProgress tip, String taskid, 
                          TaskStatus status, String trackerName,
                          boolean wasRunning, boolean wasComplete,
                          JobTrackerMetrics metrics) {
    
    // Mark the taskid as FAILED or KILLED
    tip.incompleteSubTask(taskid, trackerName, this.status);
   
    boolean isRunning = tip.isRunning();
    boolean isComplete = tip.isComplete();
        
    //update running  count on task failure.
    if (wasRunning && !isRunning) {
      if (tip.isMapTask()){
        runningMapTasks -= 1;
      } else {
        runningReduceTasks -= 1;
      }
    }
        
    // the case when the map was complete but the task tracker went down.
    if (wasComplete && !isComplete) {
      if (tip.isMapTask()){
        finishedMapTasks -= 1;
      }
    }
        
    // update job history
    String taskTrackerName = status.getTaskTracker();
    if (status.getIsMap()) {
      JobHistory.MapAttempt.logStarted(profile.getJobId(), 
                tip.getTIPId(), status.getTaskId(), status.getStartTime(), 
                taskTrackerName);
      if (status.getRunState() == TaskStatus.State.FAILED) {
        JobHistory.MapAttempt.logFailed(profile.getJobId(), 
                tip.getTIPId(), status.getTaskId(), System.currentTimeMillis(),
                taskTrackerName, status.getDiagnosticInfo());
      } else {
        JobHistory.MapAttempt.logKilled(profile.getJobId(), 
                tip.getTIPId(), status.getTaskId(), System.currentTimeMillis(),
                taskTrackerName, status.getDiagnosticInfo());
      }
    } else {
      JobHistory.ReduceAttempt.logStarted(profile.getJobId(), 
                tip.getTIPId(), status.getTaskId(), status.getStartTime(), 
                taskTrackerName);
      if (status.getRunState() == TaskStatus.State.FAILED) {
        JobHistory.ReduceAttempt.logFailed(profile.getJobId(), 
                tip.getTIPId(), status.getTaskId(), System.currentTimeMillis(),
                taskTrackerName, status.getDiagnosticInfo());
      } else {
        JobHistory.ReduceAttempt.logKilled(profile.getJobId(), 
                tip.getTIPId(), status.getTaskId(), System.currentTimeMillis(),
                taskTrackerName, status.getDiagnosticInfo());
      }
    }
        
    // After this, try to assign tasks with the one after this, so that
    // the failed task goes to the end of the list.
    if (tip.isMapTask()) {
      failedMapTasks++; 
    } else {
      failedReduceTasks++; 
    }
            
    //
    // Note down that a task has failed on this tasktracker 
    //
    if (status.getRunState() == TaskStatus.State.FAILED) { 
      addTrackerTaskFailure(trackerName);
    }
        
    //
    // Let the JobTracker know that this task has failed
    //
    jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);

    //
    // Check if we need to kill the job because of too many failures or 
    // if the job is complete since all component tasks have completed
    //
    if (tip.isFailed()) {
      //
      // Allow upto 'mapFailuresPercent' of map tasks to fail or
      // 'reduceFailuresPercent' of reduce tasks to fail
      //
      boolean killJob = 
        tip.isMapTask() ? 
            ((++failedMapTIPs*100) > (mapFailuresPercent*numMapTasks)) :
            ((++failedReduceTIPs*100) > (reduceFailuresPercent*numReduceTasks));
      
      if (killJob) {
        LOG.info("Aborting job " + profile.getJobId());
        JobHistory.Task.logFailed(profile.getJobId(), tip.getTIPId(), 
                                  tip.isMapTask() ? 
                                          Values.MAP.name() : 
                                          Values.REDUCE.name(),  
                                  System.currentTimeMillis(), 
                                  status.getDiagnosticInfo());
        JobHistory.JobInfo.logFailed(profile.getJobId(), 
                                     System.currentTimeMillis(), 
                                     this.finishedMapTasks, 
                                     this.finishedReduceTasks
                                    );
        kill();
      } else {
        isJobComplete(tip, metrics);
      }
      
      //
      // Update the counters
      //
      if (tip.isMapTask()) {
        jobCounters.incrCounter(Counter.NUM_FAILED_MAPS, 1);
      } else {
        jobCounters.incrCounter(Counter.NUM_FAILED_REDUCES, 1);
      }
    }
  }

  /**
   * Fail a task with a given reason, but without a status object.
   * @param tip The task's tip
   * @param taskid The task id
   * @param reason The reason that the task failed
   * @param trackerName The task tracker the task failed on
   */
  public void failedTask(TaskInProgress tip, String taskid, String reason, 
                         TaskStatus.Phase phase, TaskStatus.State state, 
                         String trackerName, JobTrackerMetrics metrics) {
    TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), 
                                                    taskid,
                                                    0.0f,
                                                    state,
                                                    reason,
                                                    reason,
                                                    trackerName, phase,
                                                    null);
    updateTaskStatus(tip, status, metrics);
    JobHistory.Task.logFailed(profile.getJobId(), tip.getTIPId(), 
                              tip.isMapTask() ? Values.MAP.name() : Values.REDUCE.name(), 
                              System.currentTimeMillis(), reason); 
  }
       
                           
  /**
   * The job is dead.  We're now GC'ing it, getting rid of the job
   * from all tables.  Be sure to remove all of this job's tasks
   * from the various tables.
   */
  synchronized void garbageCollect() {
    // Let the JobTracker know that a job is complete
    jobtracker.finalizeJob(this);
      
    try {
      // Definitely remove the local-disk copy of the job file
      if (localJobFile != null) {
        localFs.delete(localJobFile);
        localJobFile = null;
      }
      if (localJarFile != null) {
        localFs.delete(localJarFile);
        localJarFile = null;
      }

      // JobClient always creates a new directory with job files
      // so we remove that directory to cleanup
      FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path(profile.getJobFile()).getParent());
        
      // Delete temp dfs dirs created if any, like in case of 
      // speculative exn of reduces.  
      Path tempDir = new Path(conf.getSystemDir(), jobId); 
      fs.delete(tempDir); 

    } catch (IOException e) {
      LOG.warn("Error cleaning up "+profile.getJobId()+": "+e);
    }
    
    cleanUpMetrics();
  }

  /**
   * Return the TaskInProgress that matches the tipid.
   */
  public TaskInProgress getTaskInProgress(String tipid){
    for (int i = 0; i < maps.length; i++) {
      if (tipid.equals(maps[i].getTIPId())){
        return maps[i];
      }               
    }
    for (int i = 0; i < reduces.length; i++) {
      if (tipid.equals(reduces[i].getTIPId())){
        return reduces[i];
      }
    }
    return null;
  }
    
  /**
   * Find the details of someplace where a map has finished
   * @param mapId the id of the map
   * @return the task status of the completed task
   */
  public TaskStatus findFinishedMap(int mapId) {
    TaskInProgress tip = maps[mapId];
    if (tip.isComplete()) {
      TaskStatus[] statuses = tip.getTaskStatuses();
      for(int i=0; i < statuses.length; i++) {
        if (statuses[i].getRunState() == TaskStatus.State.SUCCEEDED) {
          return statuses[i];
        }
      }
    }
    return null;
  }
    
  synchronized public TaskCompletionEvent[] getTaskCompletionEvents(
                                                                    int fromEventId, int maxEvents) {
    TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
    if (taskCompletionEvents.size() > fromEventId) {
      int actualMax = Math.min(maxEvents, 
                               (taskCompletionEvents.size() - fromEventId));
      events = taskCompletionEvents.subList(fromEventId, actualMax + fromEventId).toArray(events);        
    }
    return events; 
  }
  
  synchronized void fetchFailureNotification(TaskInProgress tip, 
                                             String mapTaskId, 
                                             String trackerName, 
                                             JobTrackerMetrics metrics) {
    Integer fetchFailures = mapTaskIdToFetchFailuresMap.get(mapTaskId);
    fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures+1);
    mapTaskIdToFetchFailuresMap.put(mapTaskId, fetchFailures);
    LOG.info("Failed fetch notification #" + fetchFailures + " for task " + 
            mapTaskId);
    
    if (fetchFailures == MAX_FETCH_FAILURES_NOTIFICATIONS) {
      LOG.info("Too many fetch-failures for output of task: " + mapTaskId 
               + " ... killing it");
      
      failedTask(tip, mapTaskId, "Too many fetch-failures",                            
                 (tip.isMapTask() ? TaskStatus.Phase.MAP : 
                                    TaskStatus.Phase.REDUCE), 
                 TaskStatus.State.FAILED, trackerName, metrics);
      
      mapTaskIdToFetchFailuresMap.remove(mapTaskId);
    }
  }
  
  static class JobWithTaskContext {
    private JobInProgress job;
    private TaskInProgress tip;
    private String taskId;
    private JobTrackerMetrics metrics;
    JobWithTaskContext(JobInProgress job, TaskInProgress tip, 
        String taskId, JobTrackerMetrics metrics) {
      this.job = job;
      this.tip = tip;
      this.taskId = taskId;
      this.metrics = metrics;
    }
    JobInProgress getJob() {
      return job;
    }
    TaskInProgress getTIP() {
      return tip;
    }
    String getTaskId() {
      return taskId;
    }
    JobTrackerMetrics getJobTrackerMetrics() {
      return metrics;
    }
  }
}
