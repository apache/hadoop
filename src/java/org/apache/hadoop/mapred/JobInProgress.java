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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobTracker.JobTrackerMetrics;
import org.apache.hadoop.mapred.JobHistory.Values ; 
import java.io.*;
import java.net.*;
import java.util.*;

///////////////////////////////////////////////////////
// JobInProgress maintains all the info for keeping
// a Job on the straight and narrow.  It keeps its JobProfile
// and its latest JobStatus, plus a set of tables for 
// doing bookkeeping of its Tasks.
///////////////////////////////////////////////////////
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
    int failedMapTasks = 0 ; 
    int failedReduceTasks = 0 ; 
    JobTracker jobtracker = null;
    Map<String,List<TaskInProgress>> hostToMaps = new HashMap();
    private int taskCompletionEventTracker = 0 ; 
    List<TaskCompletionEvent> taskCompletionEvents ;
    
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
    private String uniqueString;
  
    /**
     * Create a JobInProgress with the given job file, plus a handle
     * to the tracker.
     */
    public JobInProgress(String jobFile, JobTracker jobtracker, 
                         Configuration default_conf) throws IOException {
        uniqueString = jobtracker.createUniqueId();
        String jobid = "job_" + uniqueString;
        String url = "http://" + jobtracker.getJobTrackerMachine() + ":" + jobtracker.getInfoPort() + "/jobdetails.jsp?jobid=" + jobid;
        this.jobtracker = jobtracker;
        this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.PREP);
        this.startTime = System.currentTimeMillis();
        this.localFs = (LocalFileSystem)FileSystem.getLocal(default_conf);

        JobConf default_job_conf = new JobConf(default_conf);
        this.localJobFile = default_job_conf.getLocalPath(JobTracker.SUBDIR 
                                                          +"/"+jobid + ".xml");
        this.localJarFile = default_job_conf.getLocalPath(JobTracker.SUBDIR
                                                          +"/"+ jobid + ".jar");
        FileSystem fs = FileSystem.get(default_conf);
        fs.copyToLocalFile(new Path(jobFile), localJobFile);
        conf = new JobConf(localJobFile);
        this.profile = new JobProfile(conf.getUser(), jobid, jobFile, url,
                                      conf.getJobName());
        String jarFile = conf.getJar();
        if (jarFile != null) {
          fs.copyToLocalFile(new Path(jarFile), localJarFile);
          conf.setJar(localJarFile.toString());
        }

        this.numMapTasks = conf.getNumMapTasks();
        this.numReduceTasks = conf.getNumReduceTasks();
        this.taskCompletionEvents = new ArrayList(
            numMapTasks + numReduceTasks + 10);
        
        JobHistory.JobInfo.logSubmitted(jobid, conf.getJobName(), conf.getUser(), 
            System.currentTimeMillis(), jobFile); 
        
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
          maps[i] = new TaskInProgress(uniqueString, jobFile, 
                                       splits[i].getClassName(),
                                       splits[i].getBytes(), 
                                       jobtracker, conf, this, i);
          for(String host: splits[i].getLocations()) {
            List<TaskInProgress> hostMaps = hostToMaps.get(host);
            if (hostMaps == null) {
              hostMaps = new ArrayList();
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
            this.status = new JobStatus(status.getJobId(), 1.0f, 1.0f, JobStatus.SUCCEEDED);
            tasksInited = true;
            return;
        }
        
        //
        // Create reduce tasks
        //
        this.reduces = new TaskInProgress[numReduceTasks];
        for (int i = 0; i < numReduceTasks; i++) {
            reduces[i] = new TaskInProgress(uniqueString, jobFile, 
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
    public int finishedMaps() {
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
    public int finishedReduces() {
        return finishedReduceTasks;
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
     * Return a treeset of completed TaskInProgress objects
     */
    public Vector reportTasksInProgress(boolean shouldBeMap, boolean shouldBeComplete) {
        Vector results = new Vector();
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
          if( null != ttStatus ){
            httpTaskLogLocation = "http://" + ttStatus.getHost() + ":" + 
              ttStatus.getHttpPort() + "/tasklog.jsp?plaintext=true&taskid=" +
              status.getTaskId() + "&all=true";
          }
          
          if (state == TaskStatus.State.SUCCEEDED) {
            this.taskCompletionEvents.add( new TaskCompletionEvent(
                taskCompletionEventTracker, 
                status.getTaskId(),
                tip.idWithinJob(),
                status.getIsMap(),
                TaskCompletionEvent.Status.SUCCEEDED,
                httpTaskLogLocation ));
            tip.setSuccessEventNumber(taskCompletionEventTracker);
            completedTask(tip, status, metrics);
          } else if (state == TaskStatus.State.FAILED ||
                     state == TaskStatus.State.KILLED) {
            this.taskCompletionEvents.add( new TaskCompletionEvent(
                taskCompletionEventTracker, 
                status.getTaskId(),
                tip.idWithinJob(),
                status.getIsMap(),
                TaskCompletionEvent.Status.FAILED, 
                httpTaskLogLocation ));
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
                       wasRunning, wasComplete);
          }          
        }

        taskCompletionEventTracker++;
        
        //
        // Update JobInProgress status
        //
        LOG.debug("Taking progress for " + tip.getTIPId() + " from " + 
                  oldProgress + " to " + tip.getProgress());
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

    /////////////////////////////////////////////////////
    // Create/manage tasks
    /////////////////////////////////////////////////////
    /**
     * Return a MapTask, if appropriate, to run on the given tasktracker
     */
    public Task obtainNewMapTask(TaskTrackerStatus tts, int clusterSize
                                 ) throws IOException {
      if (! tasksInited) {
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
      return result;
    }    

    /**
     * Return a ReduceTask, if appropriate, to run on the given tasktracker.
     * We don't have cache-sensitivity for reduce tasks, as they
     *  work on temporary MapRed files.  
     */
    public Task obtainNewReduceTask(TaskTrackerStatus tts,
                                    int clusterSize) throws IOException {
        if (! tasksInited) {
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
    
    private void addTrackerTaskFailure(String trackerName) {
      String trackerHostName = convertTrackerNameToHostName(trackerName);
      
      Integer trackerFailures = trackerToFailuresMap.get(trackerHostName);
      if (trackerFailures == null) {
        trackerFailures = new Integer(0);
      }
      trackerToFailuresMap.put(trackerHostName, ++trackerFailures);
      
      // Check if this tasktracker has turned 'flaky'
      if (trackerFailures.intValue() == conf.getMaxTaskFailuresPerTracker()) {
        ++flakyTaskTrackers;
        LOG.info("TaskTracker at '" + trackerHostName + "' turned 'flaky'");
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
        // Check if too many tasks of this job have failed on this
        // tasktracker prior to assigning it a new one.
        //
        int taskTrackerFailedTasks = getTrackerTaskFailures(taskTracker);
        if (taskTrackerFailedTasks >= conf.getMaxTaskFailuresPerTracker()) {
          String flakyTracker = convertTrackerNameToHostName(taskTracker); 
          if (flakyTaskTrackers < clusterSize) {
            LOG.debug("Ignoring the black-listed tasktracker: '" + flakyTracker 
                    + "' for assigning a new task");
            return -1;
          } else {
            LOG.warn("Trying to assign a new task for black-listed tracker " + 
                    flakyTracker + " since all task-trackers in the cluster are " +
                    "'flaky' !");
          }
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
                         ! task.hasRunOnMachine(taskTracker)) {
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
    public synchronized void completedTask(TaskInProgress tip, 
                                           TaskStatus status,
                                           JobTrackerMetrics metrics) {
        String taskid = status.getTaskId();
        
        // Sanity check: is the TIP already complete?
        if (tip.isComplete()) {
          LOG.info("Already complete TIP " + tip.getTIPId() + 
               " has completed task " + taskid);
          
          // Just mark this 'task' as complete
          tip.completedTask(taskid);
          
          // Let the JobTracker cleanup this taskid if the job isn't running
          if (this.status.getRunState() != JobStatus.RUNNING) {
            jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);
          }
          return;
        } 

        LOG.info("Task '" + taskid + "' has completed " + tip.getTIPId() + 
          " successfully.");          

        // Update jobhistory 
        String taskTrackerName = status.getTaskTracker();
        if(status.getIsMap()){
          JobHistory.MapAttempt.logStarted(profile.getJobId(), 
               tip.getTIPId(), status.getTaskId(), status.getStartTime(), 
               taskTrackerName); 
          JobHistory.MapAttempt.logFinished(profile.getJobId(), 
               tip.getTIPId(), status.getTaskId(), status.getFinishTime(), 
               taskTrackerName); 
          JobHistory.Task.logFinished(profile.getJobId(), tip.getTIPId(), 
               Values.MAP.name(), status.getFinishTime()); 
        }else{
          JobHistory.ReduceAttempt.logStarted(profile.getJobId(), 
               tip.getTIPId(), status.getTaskId(), status.getStartTime(), 
               taskTrackerName); 
          JobHistory.ReduceAttempt.logFinished(profile.getJobId(), 
               tip.getTIPId(), status.getTaskId(), status.getShuffleFinishTime(),
               status.getSortFinishTime(), status.getFinishTime(), 
               taskTrackerName); 
          JobHistory.Task.logFinished(profile.getJobId(), tip.getTIPId(), 
               Values.REDUCE.name(), status.getFinishTime()); 
        }
        
        // Mark the TIP as complete
        tip.completed(taskid);
        
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
        boolean allDone = true;
        for (int i = 0; i < maps.length; i++) {
            if (! maps[i].isComplete()) {
                allDone = false;
                break;
            }
        }
        if (allDone) {
            if (tip.isMapTask()) {
              this.status.setMapProgress(1.0f);              
            }
            for (int i = 0; i < reduces.length; i++) {
                if (! reduces[i].isComplete()) {
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
                this.finishedMapTasks, this.finishedReduceTasks, failedMapTasks, failedReduceTasks);
            metrics.completeJob();
        } else if (this.status.getRunState() != JobStatus.RUNNING) {
            // The job has been killed/failed, 
            // JobTracker should cleanup this task
            jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);
        }
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
     * job to fail.
     *
     * Even if a task has reported as completed in the past, it might later
     * be reported as failed.  That's because the TaskTracker that hosts a map
     * task might die before the entire job can complete.  If that happens,
     * we need to schedule reexecution so that downstream reduce tasks can 
     * obtain the map task's output.
     */
    private void failedTask(TaskInProgress tip, String taskid, 
                            TaskStatus status, String trackerName,
                            boolean wasRunning, boolean wasComplete) {
        // Mark the taskid as a 'failure'
        tip.failedSubTask(taskid, trackerName);
        
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
          JobHistory.MapAttempt.logFailed(profile.getJobId(), 
              tip.getTIPId(), status.getTaskId(), System.currentTimeMillis(),
              taskTrackerName, status.getDiagnosticInfo()); 
        } else {
          JobHistory.ReduceAttempt.logStarted(profile.getJobId(), 
              tip.getTIPId(), status.getTaskId(), status.getStartTime(), 
              taskTrackerName); 
          JobHistory.ReduceAttempt.logFailed(profile.getJobId(), 
              tip.getTIPId(), status.getTaskId(), System.currentTimeMillis(),
              taskTrackerName, status.getDiagnosticInfo()); 
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
        addTrackerTaskFailure(trackerName);
        
        //
        // Let the JobTracker know that this task has failed
        //
        jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);

        //
        // Check if we need to kill the job because of too many failures
        //
        if (tip.isFailed()) {
            LOG.info("Aborting job " + profile.getJobId());
            JobHistory.Task.logFailed(profile.getJobId(), tip.getTIPId(), 
                tip.isMapTask() ? Values.MAP.name():Values.REDUCE.name(),  
                System.currentTimeMillis(), status.getDiagnosticInfo());
            JobHistory.JobInfo.logFailed(profile.getJobId(), 
                System.currentTimeMillis(), this.finishedMapTasks, this.finishedReduceTasks);
            kill();
        }
    }

    /**
     * Fail a task with a given reason, but without a status object.
     * @author Owen O'Malley
     * @param tip The task's tip
     * @param taskid The task id
     * @param reason The reason that the task failed
     * @param trackerName The task tracker the task failed on
     */
    public void failedTask(TaskInProgress tip, String taskid, 
                           String reason, TaskStatus.Phase phase, 
                           String hostname, String trackerName,
                           JobTrackerMetrics metrics) {
       TaskStatus status = new TaskStatus(taskid,
                                          tip.isMapTask(),
                                          0.0f,
                                          TaskStatus.State.FAILED,
                                          reason,
                                          reason,
                                          trackerName, phase);
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
        String tempDir = conf.get("mapred.system.dir") + "/job_" + uniqueString; 
        fs.delete(new Path(tempDir)); 

      } catch (IOException e) {
        LOG.warn("Error cleaning up "+profile.getJobId()+": "+e);
      }
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
    
    public TaskCompletionEvent[] getTaskCompletionEvents(int fromEventId, 
        int maxEvents) {
      TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
      if( taskCompletionEvents.size() > fromEventId) {
        int actualMax = Math.min(maxEvents, 
            (taskCompletionEvents.size() - fromEventId));
        events = (TaskCompletionEvent[])taskCompletionEvents.subList(
            fromEventId, actualMax + fromEventId).toArray(events);        
      }
      return events; 
    }
}
