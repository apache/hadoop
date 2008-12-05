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
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;

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
  
  // Counters to track currently running/finished/failed Map/Reduce task-attempts
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

  // NetworkTopology Node to the set of TIPs
  Map<Node, List<TaskInProgress>> nonRunningMapCache;
  
  // Map of NetworkTopology Node to set of running TIPs
  Map<Node, Set<TaskInProgress>> runningMapCache;

  // A list of non-local non-running maps
  List<TaskInProgress> nonLocalMaps;

  // A set of non-local running maps
  Set<TaskInProgress> nonLocalRunningMaps;

  // A list of non-running reduce TIPs
  List<TaskInProgress> nonRunningReduces;

  // A set of running reduce TIPs
  Set<TaskInProgress> runningReduces;

  private int maxLevel;
  
  private int taskCompletionEventTracker = 0; 
  List<TaskCompletionEvent> taskCompletionEvents;
    
  // The maximum percentage of trackers in cluster added to the 'blacklist'.
  private static final double CLUSTER_BLACKLIST_PERCENT = 0.25;
  
  // The maximum percentage of fetch failures allowed for a map 
  private static final double MAX_ALLOWED_FETCH_FAILURES_PERCENT = 0.5;
  
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
  private JobID jobId;
  private boolean hasSpeculativeMaps;
  private boolean hasSpeculativeReduces;

  // Per-job counters
  public static enum Counter { 
    NUM_FAILED_MAPS, 
    NUM_FAILED_REDUCES,
    TOTAL_LAUNCHED_MAPS,
    TOTAL_LAUNCHED_REDUCES,
    OTHER_LOCAL_MAPS,
    DATA_LOCAL_MAPS,
    RACK_LOCAL_MAPS
  }
  private Counters jobCounters = new Counters();
  
  private MetricsRecord jobMetrics;
  
  // Maximum no. of fetch-failure notifications after which
  // the map task is killed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;
  
  // Map of mapTaskId -> no. of fetch failures
  private Map<TaskAttemptID, Integer> mapTaskIdToFetchFailuresMap =
    new TreeMap<TaskAttemptID, Integer>();
  
  /**
   * Create a JobInProgress with the given job file, plus a handle
   * to the tracker.
   */
  public JobInProgress(JobID jobid, JobTracker jobtracker, 
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
    Path sysDir = new Path(this.jobtracker.getSystemDir());
    FileSystem fs = sysDir.getFileSystem(default_conf);
    Path jobFile = new Path(sysDir, jobid + "/job.xml");
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
    this.jobMetrics.setTag("jobId", jobid.toString());
    hasSpeculativeMaps = conf.getMapSpeculativeExecution();
    hasSpeculativeReduces = conf.getReduceSpeculativeExecution();
    this.maxLevel = jobtracker.getNumTaskCacheLevels();
    this.nonLocalMaps = new LinkedList<TaskInProgress>();
    this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
    this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
    this.nonRunningReduces = new LinkedList<TaskInProgress>();    
    this.runningReduces = new LinkedHashSet<TaskInProgress>();
  }

  /**
   * Called periodically by JobTrackerMetrics to update the metrics for
   * this job.
   */
  public void updateMetrics() {
    Counters counters = getCounters();
    for (Counters.Group group : counters) {
      jobMetrics.setTag("group", group.getDisplayName());
      for (Counters.Counter counter : group) {
        jobMetrics.setTag("counter", counter.getDisplayName());
        jobMetrics.setMetric("value", (float) counter.getCounter());
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
    
  private void printCache (Map<Node, List<TaskInProgress>> cache) {
    LOG.info("The taskcache info:");
    for (Map.Entry<Node, List<TaskInProgress>> n : cache.entrySet()) {
      List <TaskInProgress> tips = n.getValue();
      LOG.info("Cached TIPs on node: " + n.getKey());
      for (TaskInProgress tip : tips) {
        LOG.info("tip : " + tip.getTIPId());
      }
    }
  }
  
  private Map<Node, List<TaskInProgress>> createCache(
                         JobClient.RawSplit[] splits, int maxLevel) {
    Map<Node, List<TaskInProgress>> cache = 
      new IdentityHashMap<Node, List<TaskInProgress>>(maxLevel);
    
    for (int i = 0; i < splits.length; i++) {
      String[] splitLocations = splits[i].getLocations();
      if (splitLocations.length == 0) {
        nonLocalMaps.add(maps[i]);
        continue;
      }

      for(String host: splitLocations) {
        Node node = jobtracker.resolveAndAddToTopology(host);
        LOG.info("tip:" + maps[i].getTIPId() + " has split on node:" + node);
        for (int j = 0; j < maxLevel; j++) {
          List<TaskInProgress> hostMaps = cache.get(node);
          if (hostMaps == null) {
            hostMaps = new ArrayList<TaskInProgress>();
            cache.put(node, hostMaps);
            hostMaps.add(maps[i]);
          }
          //check whether the hostMaps already contains an entry for a TIP
          //This will be true for nodes that are racks and multiple nodes in
          //the rack contain the input for a tip. Note that if it already
          //exists in the hostMaps, it must be the last element there since
          //we process one TIP at a time sequentially in the split-size order
          if (hostMaps.get(hostMaps.size() - 1) != maps[i]) {
            hostMaps.add(maps[i]);
          }
          node = node.getParent();
        }
      }
    }
    return cache;
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

    Path sysDir = new Path(this.jobtracker.getSystemDir());
    FileSystem fs = sysDir.getFileSystem(conf);
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
                                   splits[i], 
                                   jobtracker, conf, this, i);
    }
    if (numMapTasks > 0) { 
      LOG.info("Split info for job:" + jobId);
      nonRunningMapCache = createCache(splits, maxLevel);
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
      JobHistory.JobInfo.logStarted(profile.getJobID(), 
                                    System.currentTimeMillis(), 0, 0);
      JobHistory.JobInfo.logFinished(profile.getJobID(), 
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
      nonRunningReduces.add(reduces[i]);
    }

    // create job specific temporary directory in output path
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path tmpDir = new Path(outputPath, MRConstants.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(conf);
      if (!fileSys.mkdirs(tmpDir)) {
        LOG.error("Mkdirs failed to create " + tmpDir.toString());
      }
    }

    this.status = new JobStatus(status.getJobID(), 0.0f, 0.0f, JobStatus.RUNNING);
    tasksInited = true;
        
    JobHistory.JobInfo.logStarted(profile.getJobID(), System.currentTimeMillis(), numMapTasks, numReduceTasks);
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
    // If the TIP is already completed and the task reports as SUCCEEDED then 
    // mark the task as KILLED.
    // In case of task with no promotion the task tracker will mark the task 
    // as SUCCEEDED.
    if (wasComplete && (status.getRunState() == TaskStatus.State.SUCCEEDED)) {
      status.setRunState(TaskStatus.State.KILLED);
    }
    boolean change = tip.updateStatus(status);
    if (change) {
      TaskStatus.State state = status.getRunState();
      TaskTrackerStatus ttStatus = 
        this.jobtracker.getTaskTracker(status.getTaskTracker());
      String httpTaskLogLocation = null; 

      if (state == TaskStatus.State.COMMIT_PENDING) {
        JobWithTaskContext j = new JobWithTaskContext(this, tip, 
                                                      status.getTaskID(),
                                                      metrics);
        jobtracker.addToCommitQueue(j);
      }

      if (null != ttStatus){
        String host;
        if (NetUtils.getStaticResolution(ttStatus.getHost()) != null) {
          host = NetUtils.getStaticResolution(ttStatus.getHost());
        } else {
          host = ttStatus.getHost();
        }
        httpTaskLogLocation = "http://" + host + ":" + ttStatus.getHttpPort(); 
           //+ "/tasklog?plaintext=true&taskid=" + status.getTaskID();
      }

      TaskCompletionEvent taskEvent = null;
      if (state == TaskStatus.State.SUCCEEDED) {
        taskEvent = new TaskCompletionEvent(
                                            taskCompletionEventTracker, 
                                            status.getTaskID(),
                                            tip.idWithinJob(),
                                            status.getIsMap(),
                                            TaskCompletionEvent.Status.SUCCEEDED,
                                            httpTaskLogLocation 
                                           );
        taskEvent.setTaskRunTime((int)(status.getFinishTime() 
                                       - status.getStartTime()));
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
        // To remove the temporary output of failed/killed tasks
        JobWithTaskContext j = new JobWithTaskContext(this, tip, 
                                     status.getTaskID(), metrics);
        jobtracker.addToCommitQueue(j);
        // Get the event number for the (possibly) previously successful
        // task. If there exists one, then set that status to OBSOLETE 
        int eventNumber;
        if ((eventNumber = tip.getSuccessEventNumber()) != -1) {
          TaskCompletionEvent t = 
            this.taskCompletionEvents.get(eventNumber);
          if (t.getTaskAttemptId().equals(status.getTaskID()))
            t.setTaskStatus(TaskCompletionEvent.Status.OBSOLETE);
        }
        
        // Tell the job to fail the relevant task
        failedTask(tip, status.getTaskID(), status, ttStatus,
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
                                            status.getTaskID(),
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
    return incrementTaskCounters(new Counters(), maps);
  }
    
  /**
   *  Returns map phase counters by summing over all map tasks in progress.
   */
  public synchronized Counters getReduceCounters() {
    return incrementTaskCounters(new Counters(), reduces);
  }
    
  /**
   *  Returns the total job counters, by adding together the job, 
   *  the map and the reduce counters.
   */
  public Counters getCounters() {
    Counters result = new Counters();
    result.incrAllCounters(getJobCounters());
    incrementTaskCounters(result, maps);
    return incrementTaskCounters(result, reduces);
  }
    
  /**
   * Increments the counters with the counters from each task.
   * @param counters the counters to increment
   * @param tips the tasks to add in to counters
   * @return counters the same object passed in as counters
   */
  private Counters incrementTaskCounters(Counters counters,
                                         TaskInProgress[] tips) {
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
                                            int clusterSize, 
                                            int numUniqueHosts
                                           ) throws IOException {
    if (!tasksInited) {
      LOG.info("Cannot create task split for " + profile.getJobID());
      return null;
    }
        
    int target = findNewMapTask(tts, clusterSize, numUniqueHosts, 
                                status.mapProgress());
    if (target == -1) {
      return null;
    }
    
    Task result = maps[target].getTaskToRun(tts.getTrackerName());
    if (result != null) {
      runningMapTasks += 1;
      if (maps[target].isFirstAttempt(result.getTaskID())) {
        JobHistory.Task.logStarted(maps[target].getTIPId(), Values.MAP.name(),
                                   System.currentTimeMillis(),
                                   maps[target].getSplitNodes());
      }

      jobCounters.incrCounter(Counter.TOTAL_LAUNCHED_MAPS, 1);
    }

    return result;
  }    

  /**
   * Return a ReduceTask, if appropriate, to run on the given tasktracker.
   * We don't have cache-sensitivity for reduce tasks, as they
   *  work on temporary MapRed files.  
   */
  public synchronized Task obtainNewReduceTask(TaskTrackerStatus tts,
                                               int clusterSize,
                                               int numUniqueHosts
                                              ) throws IOException {
    if (!tasksInited) {
      LOG.info("Cannot create task split for " + profile.getJobID());
      return null;
    }

    int  target = findNewReduceTask(tts, clusterSize, numUniqueHosts, 
                                    status.reduceProgress());
    if (target == -1) {
      return null;
    }
    
    Task result = reduces[target].getTaskToRun(tts.getTrackerName());
    if (result != null) {
      runningReduceTasks += 1;
      if (reduces[target].isFirstAttempt(result.getTaskID())) {
        JobHistory.Task.logStarted(reduces[target].getTIPId(), Values.REDUCE.name(),
                                   System.currentTimeMillis(), "");
      }

      jobCounters.incrCounter(Counter.TOTAL_LAUNCHED_REDUCES, 1);
    }

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
   * Remove a map TIP from the lists for running maps.
   * Called when a map fails/completes (note if a map is killed,
   * it won't be present in the list since it was completed earlier)
   * @param tip the tip that needs to be retired
   */
  private synchronized void retireMap(TaskInProgress tip) {
    // Since a list for running maps is maintained if speculation is 'ON'
    if (hasSpeculativeMaps) {
      if (runningMapCache == null) {
        LOG.warn("Running cache for maps missing!! "
                 + "Job details are missing.");
        return;
      }
      String[] splitLocations = tip.getSplitLocations();

      // Remove the TIP from the list for running non-local maps
      if (splitLocations.length == 0) {
        nonLocalRunningMaps.remove(tip);
        return;
      }

      // Remove from the running map caches
      for(String host: splitLocations) {
        Node node = jobtracker.getNode(host);
        
        for (int j = 0; j < maxLevel; ++j) {
          Set<TaskInProgress> hostMaps = runningMapCache.get(node);
          if (hostMaps != null) {
            hostMaps.remove(tip);
            if (hostMaps.size() == 0) {
              runningMapCache.remove(node);
            }
          }
          node = node.getParent();
        }
      }
    }
  }

  /**
   * Remove a reduce TIP from the list for running-reduces
   * Called when a reduce fails/completes
   * @param tip the tip that needs to be retired
   */
  private synchronized void retireReduce(TaskInProgress tip) {
    // Since a list for running reduces is maintained if speculation is 'ON'
    if (hasSpeculativeReduces) {
      if (runningReduces == null) {
        LOG.warn("Running list for reducers missing!! "
                 + "Job details are missing.");
        return;
      }
      runningReduces.remove(tip);
    }
  }

  /**
   * Adds a map tip to the list of running maps.
   * @param tip the tip that needs to be scheduled as running
   */
  private synchronized void scheduleMap(TaskInProgress tip) {
    
    // Since a running list is maintained only if speculation is 'ON'
    if (hasSpeculativeMaps) {
      if (runningMapCache == null) {
        LOG.warn("Running cache for maps is missing!! " 
                 + "Job details are missing.");
        return;
      }
      String[] splitLocations = tip.getSplitLocations();

      // Add the TIP to the list of non-local running TIPs
      if (splitLocations.length == 0) {
        nonLocalRunningMaps.add(tip);
        return;
      }

      for(String host: splitLocations) {
        Node node = jobtracker.getNode(host);
      
        for (int j = 0; j < maxLevel; ++j) {
          Set<TaskInProgress> hostMaps = runningMapCache.get(node);
          if (hostMaps == null) {
            // create a cache if needed
            hostMaps = new LinkedHashSet<TaskInProgress>();
            runningMapCache.put(node, hostMaps);
          }
          hostMaps.add(tip);
          node = node.getParent();
        }
      }
    }
  }
  
  /**
   * Adds a reduce tip to the list of running reduces
   * @param tip the tip that needs to be scheduled as running
   */
  private synchronized void scheduleReduce(TaskInProgress tip) {
    // Since a list for running reduces is maintained if speculation is 'ON'
    if (hasSpeculativeReduces) {
      if (runningReduces == null) {
        LOG.warn("Running cache for reducers missing!! "
                 + "Job details are missing.");
        return;
      }
      runningReduces.add(tip);
    }
  }
  
  /**
   * Adds the failed TIP in the front of the list for non-running maps
   * @param tip the tip that needs to be failed
   */
  private synchronized void failMap(TaskInProgress tip) {
    if (nonRunningMapCache == null) {
      LOG.warn("Non-running cache for maps missing!! "
               + "Job details are missing.");
      return;
    }

    // 1. Its added everywhere since other nodes (having this split local)
    //    might have removed this tip from their local cache
    // 2. Give high priority to failed tip - fail early

    String[] splitLocations = tip.getSplitLocations();

    // Add the TIP in the front of the list for non-local non-running maps
    if (splitLocations.length == 0) {
      nonLocalMaps.add(0, tip);
      return;
    }

    for(String host: splitLocations) {
      Node node = jobtracker.getNode(host);
      
      for (int j = 0; j < maxLevel; ++j) {
        List<TaskInProgress> hostMaps = nonRunningMapCache.get(node);
        if (hostMaps == null) {
          hostMaps = new LinkedList<TaskInProgress>();
          nonRunningMapCache.put(node, hostMaps);
        }
        hostMaps.add(0, tip);
        node = node.getParent();
      }
    }
  }
  
  /**
   * Adds a failed TIP in the front of the list for non-running reduces
   * @param tip the tip that needs to be failed
   */
  private synchronized void failReduce(TaskInProgress tip) {
    if (nonRunningReduces == null) {
      LOG.warn("Failed cache for reducers missing!! "
               + "Job details are missing.");
      return;
    }
    nonRunningReduces.add(0, tip);
  }
  
  /**
   * Find a non-running task in the passed list of TIPs
   * @param tips a collection of TIPs
   * @param ttStatus the status of tracker that has requested a task to run
   * @param numUniqueHosts number of unique hosts that run trask trackers
   * @param removeFailedTip whether to remove the failed tips
   */
  private synchronized TaskInProgress findTaskFromList(
      Collection<TaskInProgress> tips, TaskTrackerStatus ttStatus,
      int numUniqueHosts,
      boolean removeFailedTip) {
    Iterator<TaskInProgress> iter = tips.iterator();
    while (iter.hasNext()) {
      TaskInProgress tip = iter.next();

      // Select a tip if
      //   1. runnable   : still needs to be run and is not completed
      //   2. ~running   : no other node is running it
      //   3. earlier attempt failed : has not failed on this host
      //                               and has failed on all the other hosts
      // A TIP is removed from the list if 
      // (1) this tip is scheduled
      // (2) if the passed list is a level 0 (host) cache
      // (3) when the TIP is non-schedulable (running, killed, complete)
      if (tip.isRunnable() && !tip.isRunning()) {
        // check if the tip has failed on this host
        if (!tip.hasFailedOnMachine(ttStatus.getHost()) || 
             tip.getNumberOfFailedMachines() >= numUniqueHosts) {
          // check if the tip has failed on all the nodes
          iter.remove();
          return tip;
        } else if (removeFailedTip) { 
          // the case where we want to remove a failed tip from the host cache
          // point#3 in the TIP removal logic above
          iter.remove();
        }
      } else {
        // see point#3 in the comment above for TIP removal logic
        iter.remove();
      }
    }
    return null;
  }
  
  /**
   * Find a speculative task
   * @param list a list of tips
   * @param taskTracker the tracker that has requested a tip
   * @param avgProgress the average progress for speculation
   * @param currentTime current time in milliseconds
   * @param shouldRemove whether to remove the tips
   * @return a tip that can be speculated on the tracker
   */
  private synchronized TaskInProgress findSpeculativeTask(
      Collection<TaskInProgress> list, TaskTrackerStatus ttStatus,
      double avgProgress, long currentTime, boolean shouldRemove) {
    
    Iterator<TaskInProgress> iter = list.iterator();

    while (iter.hasNext()) {
      TaskInProgress tip = iter.next();
      // should never be true! (since we delete completed/failed tasks)
      if (!tip.isRunning()) {
        iter.remove();
        continue;
      }

      if (!tip.hasRunOnMachine(ttStatus.getHost(), 
                               ttStatus.getTrackerName())) {
        if (tip.hasSpeculativeTask(currentTime, avgProgress)) {
          // In case of shared list we don't remove it. Since the TIP failed 
          // on this tracker can be scheduled on some other tracker.
          if (shouldRemove) {
            iter.remove(); //this tracker is never going to run it again
          }
          return tip;
        } 
      } else {
        // Check if this tip can be removed from the list.
        // If the list is shared then we should not remove.
        if (shouldRemove) {
          // This tracker will never speculate this tip
          iter.remove();
        }
      }
    }
    return null;
  }
  
  /**
   * Find new map task
   * @param tts The task tracker that is asking for a task
   * @param clusterSize The number of task trackers in the cluster
   * @param numUniqueHosts The number of hosts that run task trackers
   * @param avgProgress The average progress of this kind of task in this job
   * @return the index in tasks of the selected task (or -1 for no task)
   */
  private synchronized int findNewMapTask(TaskTrackerStatus tts, 
                                          int clusterSize,
                                          int numUniqueHosts,
                                          double avgProgress) {
    String taskTracker = tts.getTrackerName();
    TaskInProgress tip = null;
    
    //
    // Update the last-known clusterSize
    //
    this.clusterSize = clusterSize;

    if (!shouldRunOnTaskTracker(taskTracker)) {
      return -1;
    }

    Node node = jobtracker.getNode(tts.getHost());
    Node nodeParentAtMaxLevel = null;
    
    // For scheduling a map task, we have two caches and a list (optional)
    //  I)   one for non-running task
    //  II)  one for running task (this is for handling speculation)
    //  III) a list of TIPs that have empty locations (e.g., dummy splits),
    //       the list is empty if all TIPs have associated locations

    // First a look up is done on the non-running cache and on a miss, a look 
    // up is done on the running cache. The order for lookup within the cache:
    //   1. from local node to root [bottom up]
    //   2. breadth wise for all the parent nodes at max level

    // We fall to linear scan of the list (III above) if we have misses in the 
    // above caches

    //
    // I) Non-running TIP :
    // 

    // 1. check from local node to the root [bottom up cache lookup]
    //    i.e if the cache is available and the host has been resolved
    //    (node!=null)
    
    if (node != null) {
      Node key = node;
      for (int level = 0; level < maxLevel; ++level) {
        List <TaskInProgress> cacheForLevel = nonRunningMapCache.get(key);
        if (cacheForLevel != null) {
          tip = findTaskFromList(cacheForLevel, tts, 
                                 numUniqueHosts,level == 0);
          if (tip != null) {
            // Add to running cache
            scheduleMap(tip);

            // remove the cache if its empty
            if (cacheForLevel.size() == 0) {
              nonRunningMapCache.remove(key);
            }

            if (level == 0) {
              LOG.info("Choosing data-local task " + tip.getTIPId());
              jobCounters.incrCounter(Counter.DATA_LOCAL_MAPS, 1);
            } else if (level == 1){
              LOG.info("Choosing rack-local task " + tip.getTIPId());
              jobCounters.incrCounter(Counter.RACK_LOCAL_MAPS, 1);
            } else {
              LOG.info("Choosing cached task at level " + level 
                       + tip.getTIPId());
              jobCounters.incrCounter(Counter.OTHER_LOCAL_MAPS, 1);
            }

            return tip.getIdWithinJob();
          }
        }
        key = key.getParent();
      }
      // get the node parent at max level
      nodeParentAtMaxLevel = JobTracker.getParentNode(node, maxLevel - 1);
    }

    //2. Search breadth-wise across parents at max level for non-running 
    //   TIP if
    //     - cache exists and there is a cache miss 
    //     - node information for the tracker is missing (tracker's topology
    //       info not obtained yet)

    // collection of node at max level in the cache structure
    Collection<Node> nodesAtMaxLevel = jobtracker.getNodesAtMaxLevel();
    
    for (Node parent : nodesAtMaxLevel) {

      // skip the parent that has already been scanned
      if (parent == nodeParentAtMaxLevel) {
        continue;
      }

      List<TaskInProgress> cache = nonRunningMapCache.get(parent);
      if (cache != null) {
        tip = findTaskFromList(cache, tts, numUniqueHosts, false);
        if (tip != null) {
          // Add to the running cache
          scheduleMap(tip);

          // remove the cache if empty
          if (cache.size() == 0) {
            nonRunningMapCache.remove(parent);
          }
          LOG.info("Choosing a non-local task " + tip.getTIPId());
          return tip.getIdWithinJob();
        }
      }
    }

    // 3. Search non-local tips for a new task
    tip = findTaskFromList(nonLocalMaps, tts, numUniqueHosts, false);
    if (tip != null) {
      // Add to the running list
      scheduleMap(tip);

      LOG.info("Choosing a non-local task " + tip.getTIPId());
      return tip.getIdWithinJob();
    }

    //
    // II) Running TIP :
    // 
 
    if (hasSpeculativeMaps) {
      long currentTime = System.currentTimeMillis();

      // 1. Check bottom up for speculative tasks from the running cache
      if (node != null) {
        Node key = node;
        for (int level = 0; level < maxLevel; ++level) {
          Set<TaskInProgress> cacheForLevel = runningMapCache.get(key);
          if (cacheForLevel != null) {
            tip = findSpeculativeTask(cacheForLevel, tts, 
                                      avgProgress, currentTime, level == 0);
            if (tip != null) {
              if (cacheForLevel.size() == 0) {
                runningMapCache.remove(key);
              }
              if (level == 0) {
                LOG.info("Choosing a data-local task " + tip.getTIPId() 
                         + " for speculation");
              } else if (level == 1){
                LOG.info("Choosing a rack-local task " + tip.getTIPId() 
                         + " for speculation");
              } else {
                LOG.info("Choosing a cached task at level " + level
                         + tip.getTIPId() + " for speculation");
              }
              return tip.getIdWithinJob();
            }
          }
          key = key.getParent();
        }
      }

      // 2. Check breadth-wise for speculative tasks
      
      for (Node parent : nodesAtMaxLevel) {
        // ignore the parent which is already scanned
        if (parent == nodeParentAtMaxLevel) {
          continue;
        }

        Set<TaskInProgress> cache = runningMapCache.get(parent);
        if (cache != null) {
          tip = findSpeculativeTask(cache, tts, avgProgress, 
                                    currentTime, false);
          if (tip != null) {
            // remove empty cache entries
            if (cache.size() == 0) {
              runningMapCache.remove(parent);
            }
            LOG.info("Choosing a non-local task " + tip.getTIPId() 
                     + " for speculation");
            return tip.getIdWithinJob();
          }
        }
      }

      // 3. Check non-local tips for speculation
      tip = findSpeculativeTask(nonLocalRunningMaps, tts, avgProgress, 
                                currentTime, false);
      if (tip != null) {
        LOG.info("Choosing a non-local task " + tip.getTIPId() 
                 + " for speculation");
        return tip.getIdWithinJob();
      }
    }
    return -1;
  }

  /**
   * Find new reduce task
   * @param tts The task tracker that is asking for a task
   * @param clusterSize The number of task trackers in the cluster
   * @param numUniqueHosts The number of hosts that run task trackers
   * @param avgProgress The average progress of this kind of task in this job
   * @return the index in tasks of the selected task (or -1 for no task)
   */
  private synchronized int findNewReduceTask(TaskTrackerStatus tts, 
                                             int clusterSize,
                                             int numUniqueHosts,
                                             double avgProgress) {
    String taskTracker = tts.getTrackerName();
    TaskInProgress tip = null;
    
    // Update the last-known clusterSize
    this.clusterSize = clusterSize;

    if (!shouldRunOnTaskTracker(taskTracker)) {
      return -1;
    }

    // 1. check for a never-executed reduce tip
    // reducers don't have a cache and so pass -1 to explicitly call that out
    tip = findTaskFromList(nonRunningReduces, tts, numUniqueHosts, false);
    if (tip != null) {
      scheduleReduce(tip);
      return tip.getIdWithinJob();
    }

    // 2. check for a reduce tip to be speculated
    if (hasSpeculativeReduces) {
      tip = findSpeculativeTask(runningReduces, tts, avgProgress, 
                                System.currentTimeMillis(), false);
      if (tip != null) {
        scheduleReduce(tip);
        return tip.getIdWithinJob();
      }
    }

    return -1;
  }
  
  private boolean shouldRunOnTaskTracker(String taskTracker) {
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
      return false;
    }
    return true;
  }

  /**
   * A taskid assigned to this JobInProgress has reported in successfully.
   */
  public synchronized boolean completedTask(TaskInProgress tip, 
                                         TaskStatus status,
                                         JobTrackerMetrics metrics) 
  {
    TaskAttemptID taskid = status.getTaskID();
        
    // Sanity check: is the TIP already complete? 
    // It _is_ safe to not decrement running{Map|Reduce}Tasks and
    // finished{Map|Reduce}Tasks variables here because one and only
    // one task-attempt of a TIP gets to completedTask. This is because
    // the TaskCommitThread in the JobTracker marks other, completed, 
    // speculative tasks as _complete_.
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
    String taskTrackerName = jobtracker.getNode(jobtracker.getTaskTracker(
                               status.getTaskTracker()).getHost()).toString();
    if (status.getIsMap()){
      JobHistory.MapAttempt.logStarted(status.getTaskID(), status.getStartTime(), 
                                       taskTrackerName); 
      JobHistory.MapAttempt.logFinished(status.getTaskID(), status.getFinishTime(), 
                                        taskTrackerName); 
      JobHistory.Task.logFinished(tip.getTIPId(), 
                                  Values.MAP.name(), status.getFinishTime(),
                                  status.getCounters()); 
    }else{
      JobHistory.ReduceAttempt.logStarted( status.getTaskID(), status.getStartTime(), 
                                          taskTrackerName); 
      JobHistory.ReduceAttempt.logFinished(status.getTaskID(), status.getShuffleFinishTime(),
                                           status.getSortFinishTime(), status.getFinishTime(), 
                                           taskTrackerName); 
      JobHistory.Task.logFinished(tip.getTIPId(), 
                                  Values.REDUCE.name(), status.getFinishTime(),
                                  status.getCounters()); 
    }
        
    // Update the running/finished map/reduce counts
    if (tip.isMapTask()){
      runningMapTasks -= 1;
      finishedMapTasks += 1;
      metrics.completeMap();
      // remove the completed map from the resp running caches
      retireMap(tip);
    } else{
      runningReduceTasks -= 1;
      finishedReduceTasks += 1;
      metrics.completeReduce();
      // remove the completed reduces from the running reducers set
      retireReduce(tip);
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
    // Job is complete if total-tips = finished-tips + failed-tips
    boolean allDone = 
      ((finishedMapTasks + failedMapTIPs) == numMapTasks);
    if (allDone) {
      if (tip.isMapTask()) {
        this.status.setMapProgress(1.0f);              
      }
      allDone = 
        ((finishedReduceTasks + failedReduceTIPs) == numReduceTasks);
    }

    //
    // If all tasks are complete, then the job is done!
    //
    if (this.status.getRunState() == JobStatus.RUNNING && allDone) {
      this.status.setRunState(JobStatus.SUCCEEDED);
      this.status.setReduceProgress(1.0f);
      this.finishTime = System.currentTimeMillis();
      garbageCollect();
      LOG.info("Job " + this.status.getJobID() + 
               " has completed successfully.");
      JobHistory.JobInfo.logFinished(this.status.getJobID(), finishTime, 
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
    if ((status.getRunState() == JobStatus.RUNNING) ||
         (status.getRunState() == JobStatus.PREP)) {
      LOG.info("Killing job '" + this.status.getJobID() + "'");
      this.status = new JobStatus(status.getJobID(), 1.0f, 1.0f, JobStatus.FAILED);
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
      JobHistory.JobInfo.logFailed(this.status.getJobID(), finishTime, 
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
  private void failedTask(TaskInProgress tip, TaskAttemptID taskid, 
                          TaskStatus status, 
                          TaskTrackerStatus taskTrackerStatus,
                          boolean wasRunning, boolean wasComplete,
                          JobTrackerMetrics metrics) {
    // check if the TIP is already failed
    boolean wasFailed = tip.isFailed();

    // Mark the taskid as FAILED or KILLED
    tip.incompleteSubTask(taskid, taskTrackerStatus, this.status);
   
    boolean isRunning = tip.isRunning();
    boolean isComplete = tip.isComplete();
        
    //update running  count on task failure.
    if (wasRunning && !isRunning) {
      if (tip.isMapTask()){
        runningMapTasks -= 1;
        // remove from the running queue and put it in the non-running cache
        // if the tip is not complete i.e if the tip still needs to be run
        if (!isComplete) {
          retireMap(tip);
          failMap(tip);
        }
      } else {
        runningReduceTasks -= 1;
        // remove from the running queue and put in the failed queue if the tip
        // is not complete
        if (!isComplete) {
          retireReduce(tip);
          failReduce(tip);
        }
      }
    }
        
    // the case when the map was complete but the task tracker went down.
    if (wasComplete && !isComplete) {
      if (tip.isMapTask()) {
        // Put the task back in the cache. This will help locality for cases
        // where we have a different TaskTracker from the same rack/switch
        // asking for a task. 
        // We bother about only those TIPs that were successful
        // earlier (wasComplete and !isComplete) 
        // (since they might have been removed from the cache of other 
        // racks/switches, if the input split blocks were present there too)
        failMap(tip);
        finishedMapTasks -= 1;
      }
    }
        
    // update job history
    String taskTrackerName = jobtracker.getNode(
                               taskTrackerStatus.getHost()).toString();
    if (status.getIsMap()) {
      JobHistory.MapAttempt.logStarted(status.getTaskID(), status.getStartTime(), 
                taskTrackerName);
      if (status.getRunState() == TaskStatus.State.FAILED) {
        JobHistory.MapAttempt.logFailed(status.getTaskID(), System.currentTimeMillis(),
                taskTrackerName, status.getDiagnosticInfo());
      } else {
        JobHistory.MapAttempt.logKilled(status.getTaskID(), System.currentTimeMillis(),
                taskTrackerName, status.getDiagnosticInfo());
      }
    } else {
      JobHistory.ReduceAttempt.logStarted(status.getTaskID(), status.getStartTime(), 
                taskTrackerName);
      if (status.getRunState() == TaskStatus.State.FAILED) {
        JobHistory.ReduceAttempt.logFailed(status.getTaskID(), System.currentTimeMillis(),
                taskTrackerName, status.getDiagnosticInfo());
      } else {
        JobHistory.ReduceAttempt.logKilled(status.getTaskID(), System.currentTimeMillis(),
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
      addTrackerTaskFailure(taskTrackerStatus.getTrackerName());
    }
        
    //
    // Let the JobTracker know that this task has failed
    //
    jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);

    //
    // Check if we need to kill the job because of too many failures or 
    // if the job is complete since all component tasks have completed

    // We do it once per TIP and that too for the task that fails the TIP
    if (!wasFailed && tip.isFailed()) {
      //
      // Allow upto 'mapFailuresPercent' of map tasks to fail or
      // 'reduceFailuresPercent' of reduce tasks to fail
      //
      boolean killJob = 
        tip.isMapTask() ? 
            ((++failedMapTIPs*100) > (mapFailuresPercent*numMapTasks)) :
            ((++failedReduceTIPs*100) > (reduceFailuresPercent*numReduceTasks));
      
      if (killJob) {
        LOG.info("Aborting job " + profile.getJobID());
        JobHistory.Task.logFailed(tip.getTIPId(), 
                                  tip.isMapTask() ? 
                                          Values.MAP.name() : 
                                          Values.REDUCE.name(),  
                                  System.currentTimeMillis(), 
                                  status.getDiagnosticInfo());
        JobHistory.JobInfo.logFailed(profile.getJobID(), 
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
  public void failedTask(TaskInProgress tip, TaskAttemptID taskid, String reason, 
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
    JobHistory.Task.logFailed(tip.getTIPId(), 
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
        localFs.delete(localJobFile, true);
        localJobFile = null;
      }
      if (localJarFile != null) {
        localFs.delete(localJarFile, true);
        localJarFile = null;
      }

      // clean up splits
      for (int i = 0; i < maps.length; i++) {
        maps[i].clearSplit();
      }

      // JobClient always creates a new directory with job files
      // so we remove that directory to cleanup
      // Delete temp dfs dirs created if any, like in case of 
      // speculative exn of reduces.  
      Path tempDir = new Path(jobtracker.getSystemDir(), jobId.toString());
      FileSystem fs = tempDir.getFileSystem(conf);
      fs.delete(tempDir, true); 

      // delete the temporary directory in output directory
      Path outputPath = FileOutputFormat.getOutputPath(conf);
      if (outputPath != null) {
        Path tmpDir = new Path(outputPath, MRConstants.TEMP_DIR_NAME);
        FileSystem fileSys = tmpDir.getFileSystem(conf);
        if (fileSys.exists(tmpDir)) {
          fileSys.delete(tmpDir, true);
        }
      }
    } catch (IOException e) {
      LOG.warn("Error cleaning up "+profile.getJobID()+": "+e);
    }
    
    cleanUpMetrics();
    // free up the memory used by the data structures
    this.nonRunningMapCache = null;
    this.runningMapCache = null;
    this.nonRunningReduces = null;
    this.runningReduces = null;
  }

  /**
   * Return the TaskInProgress that matches the tipid.
   */
  public TaskInProgress getTaskInProgress(TaskID tipid){
    if (tipid.isMap()) {
      for (int i = 0; i < maps.length; i++) {
        if (tipid.equals(maps[i].getTIPId())){
          return maps[i];
        }
      }
    } else {
      for (int i = 0; i < reduces.length; i++) {
        if (tipid.equals(reduces[i].getTIPId())){
          return reduces[i];
        }
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
                                             TaskAttemptID mapTaskId, 
                                             String trackerName, 
                                             JobTrackerMetrics metrics) {
    Integer fetchFailures = mapTaskIdToFetchFailuresMap.get(mapTaskId);
    fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures+1);
    mapTaskIdToFetchFailuresMap.put(mapTaskId, fetchFailures);
    LOG.info("Failed fetch notification #" + fetchFailures + " for task " + 
            mapTaskId);
    
    float failureRate = (float)fetchFailures / runningReduceTasks;
    // declare faulty if fetch-failures >= max-allowed-failures
    boolean isMapFaulty = (failureRate >= MAX_ALLOWED_FETCH_FAILURES_PERCENT) 
                          ? true
                          : false;
    if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS
        && isMapFaulty) {
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
    private TaskAttemptID taskId;
    private JobTrackerMetrics metrics;
    JobWithTaskContext(JobInProgress job, TaskInProgress tip, 
        TaskAttemptID taskId, JobTrackerMetrics metrics) {
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
    TaskAttemptID getTaskID() {
      return taskId;
    }
    JobTrackerMetrics getJobTrackerMetrics() {
      return metrics;
    }
  }
}
