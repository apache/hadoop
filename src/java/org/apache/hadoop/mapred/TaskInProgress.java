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


import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.StringUtils;


/*************************************************************
 * TaskInProgress maintains all the info needed for a
 * Task in the lifetime of its owning Job.  A given Task
 * might be speculatively executed or reexecuted, so we
 * need a level of indirection above the running-id itself.
 * <br>
 * A given TaskInProgress contains multiple taskids,
 * 0 or more of which might be executing at any one time.
 * (That's what allows speculative execution.)  A taskid
 * is now *never* recycled.  A TIP allocates enough taskids
 * to account for all the speculation and failures it will
 * ever have to handle.  Once those are up, the TIP is dead.
 * **************************************************************
 */
class TaskInProgress {
  static final int MAX_TASK_EXECS = 1;
  int maxTaskAttempts = 4;    
  static final double SPECULATIVE_GAP = 0.2;
  static final long SPECULATIVE_LAG = 60 * 1000;
  private static NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setMinimumIntegerDigits(6);
    idFormat.setGroupingUsed(false);
  }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.TaskInProgress");

  // Defines the TIP
  private String jobFile = null;
  private String splitClass = null;
  private BytesWritable split = null;
  private int numMaps;
  private int partition;
  private JobTracker jobtracker;
  private String id;
  private JobInProgress job;

  // Status of the TIP
  private int successEventNumber = -1;
  private int numTaskFailures = 0;
  private int numKilledTasks = 0;
  private double progress = 0;
  private String state = "";
  private long startTime = 0;
  private long execStartTime = 0;
  private long execFinishTime = 0;
  private int completes = 0;
  private boolean failed = false;
  private boolean killed = false;

  // The 'unique' prefix for taskids of this tip
  String taskIdPrefix;
    
  // The 'next' usable taskid of this tip
  int nextTaskId = 0;
    
  // Map from task Id -> TaskTracker Id, contains tasks that are
  // currently runnings
  private TreeMap<String, String> activeTasks = new TreeMap<String, String>();
  private JobConf conf;
  private boolean runSpeculative;
  private Map<String,List<String>> taskDiagnosticData =
    new TreeMap<String,List<String>>();
  /**
   * Map from taskId -> TaskStatus
   */
  private TreeMap<String,TaskStatus> taskStatuses = 
    new TreeMap<String,TaskStatus>();

  // Map from taskId -> Task
  private Map<String, Task> tasks = new TreeMap<String, Task>();
  boolean savedTaskOutput = false;

  private TreeSet<String> machinesWhereFailed = new TreeSet<String>();
  private TreeSet<String> tasksReportedClosed = new TreeSet<String>();
  
  //list of tasks to kill, <taskid> -> <shouldFail> 
  private TreeMap<String, Boolean> tasksToKill = new TreeMap<String, Boolean>();
  
  private Counters counters = new Counters();

  /**
   * Constructor for MapTask
   */
  public TaskInProgress(String jobid, String jobFile, 
                        String splitClass, BytesWritable split, 
                        JobTracker jobtracker, JobConf conf, 
                        JobInProgress job, int partition) {
    this.jobFile = jobFile;
    this.splitClass = splitClass;
    this.split = split;
    this.jobtracker = jobtracker;
    this.job = job;
    this.conf = conf;
    this.partition = partition;
    setMaxTaskAttempts();
    init(JobTracker.getJobUniqueString(jobid));
  }
        
  /**
   * Constructor for ReduceTask
   */
  public TaskInProgress(String jobid, String jobFile, 
                        int numMaps, 
                        int partition, JobTracker jobtracker, JobConf conf,
                        JobInProgress job) {
    this.jobFile = jobFile;
    this.numMaps = numMaps;
    this.partition = partition;
    this.jobtracker = jobtracker;
    this.job = job;
    this.conf = conf;
    setMaxTaskAttempts();
    init(JobTracker.getJobUniqueString(jobid));
  }
  /**
   * Set the max number of attempts before we declare a TIP as "failed"
   */
  private void setMaxTaskAttempts() {
    if (isMapTask()) {
      this.maxTaskAttempts = conf.getMaxMapAttempts();
    } else {
      this.maxTaskAttempts = conf.getMaxReduceAttempts();
    }
  }

  /**
   * Make a unique name for this TIP.
   * @param uniqueBase The unique name of the job
   * @return The unique string for this tip
   */
  private String makeUniqueString(String uniqueBase) {
    StringBuilder result = new StringBuilder();
    result.append(uniqueBase);
    if (isMapTask()) {
      result.append("_m_");
    } else {
      result.append("_r_");
    }
    result.append(idFormat.format(partition));
    return result.toString();
  }
    
  /**
   * Return the index of the tip within the job, so 
   * "tip_200707121733_1313_0002_m_012345" would return 12345;
   * @return int the tip index
   */
  public int idWithinJob() {
    return partition;
  }    

  /**
   * Initialization common to Map and Reduce
   */
  void init(String jobUniqueString) {
    this.startTime = System.currentTimeMillis();
    this.runSpeculative = conf.getSpeculativeExecution();
    this.taskIdPrefix = makeUniqueString(jobUniqueString);
    this.id = "tip_" + this.taskIdPrefix;
  }

  ////////////////////////////////////
  // Accessors, info, profiles, etc.
  ////////////////////////////////////

  /**
   * Return the parent job
   */
  public JobInProgress getJob() {
    return job;
  }
  /**
   * Return an ID for this task, not its component taskid-threads
   */
  public String getTIPId() {
    return this.id;
  }
  /**
   * Whether this is a map task
   */
  public boolean isMapTask() {
    return split != null;
  }
    
  /**
   * Is this tip currently running any tasks?
   * @return true if any tasks are running
   */
  public boolean isRunning() {
    return !activeTasks.isEmpty();
  }
    
  /**
   * Is this tip complete?
   * 
   * @return <code>true</code> if the tip is complete, else <code>false</code>
   */
  public boolean isComplete() {
    return (completes > 0);
  }

  /**
   * Is the given taskid in this tip complete?
   * 
   * @param taskid taskid of attempt to check for completion
   * @return <code>true</code> if taskid is complete, else <code>false</code>
   */
  public boolean isComplete(String taskid) {
    TaskStatus status = taskStatuses.get(taskid);
    if (status == null) {
      return false;
    }
    return ((completes > 0) && 
            (status.getRunState() == TaskStatus.State.SUCCEEDED));
  }

  /**
   * Is the tip a failure?
   * 
   * @return <code>true</code> if tip has failed, else <code>false</code>
   */
  public boolean isFailed() {
    return failed;
  }

  /**
   * Number of times the TaskInProgress has failed.
   */
  public int numTaskFailures() {
    return numTaskFailures;
  }

  /**
   * Number of times the TaskInProgress has been killed by the framework.
   */
  public int numKilledTasks() {
    return numKilledTasks;
  }

  /**
   * Get the overall progress (from 0 to 1.0) for this TIP
   */
  public double getProgress() {
    return progress;
  }
    
  /**
   * Get the task's counters
   */
  public Counters getCounters() {
    return counters;
  }
  /**
   * Returns whether a component task-thread should be 
   * closed because the containing JobInProgress has completed
   * or the task is killed by the user
   */
  public boolean shouldClose(String taskid) {
    // If the thing has never been closed,
    // and it belongs to this TIP,
    // and this TIP is somehow FINISHED,
    // then true
    TaskStatus ts = taskStatuses.get(taskid);
    if ((ts != null) &&
        (!tasksReportedClosed.contains(taskid)) &&
        (job.getStatus().getRunState() != JobStatus.RUNNING)) {
      tasksReportedClosed.add(taskid);
      return true;
    } else if (!isMapTask() && isComplete() && 
               !tasksReportedClosed.contains(taskid)){
      tasksReportedClosed.add(taskid);
      return true; 
    } else {
      return tasksToKill.keySet().contains(taskid);
    }
  }

  /**
   * Creates a "status report" for this task.  Includes the
   * task ID and overall status, plus reports for all the
   * component task-threads that have ever been started.
   */
  synchronized TaskReport generateSingleReport() {
    ArrayList<String> diagnostics = new ArrayList<String>();
    for (List<String> l : taskDiagnosticData.values()) {
      diagnostics.addAll(l);
    }
    TaskReport report = new TaskReport
      (getTIPId(), (float)progress, state,
       diagnostics.toArray(new String[diagnostics.size()]),
       execStartTime, execFinishTime, counters);
      
    return report;
  }

  /**
   * Get the diagnostic messages for a given task within this tip.
   * 
   * @param taskId the id of the required task
   * @return the list of diagnostics for that task
   */
  synchronized List<String> getDiagnosticInfo(String taskId) {
    return taskDiagnosticData.get(taskId);
  }
    
  ////////////////////////////////////////////////
  // Update methods, usually invoked by the owning
  // job.
  ////////////////////////////////////////////////
  
  /**
   * Save diagnostic information for a given task.
   * 
   * @param taskId id of the task 
   * @param diagInfo diagnostic information for the task
   */
  private void addDiagnosticInfo(String taskId, String diagInfo) {
    List<String> diagHistory = taskDiagnosticData.get(taskId);
    if (diagHistory == null) {
      diagHistory = new ArrayList<String>();
      taskDiagnosticData.put(taskId, diagHistory);
    }
    diagHistory.add(diagInfo);
  }
  
  /**
   * A status message from a client has arrived.
   * It updates the status of a single component-thread-task,
   * which might result in an overall TaskInProgress status update.
   * @return has the task changed its state noticably?
   */
  synchronized boolean updateStatus(TaskStatus status) {
    String taskid = status.getTaskId();
    String diagInfo = status.getDiagnosticInfo();
    TaskStatus oldStatus = taskStatuses.get(taskid);
    boolean changed = true;
    if (diagInfo != null && diagInfo.length() > 0) {
      LOG.info("Error from "+taskid+": "+diagInfo);
      addDiagnosticInfo(taskid, diagInfo);
    }
    if (oldStatus != null) {
      TaskStatus.State oldState = oldStatus.getRunState();
      TaskStatus.State newState = status.getRunState();
          
      // We should never recieve a duplicate success/failure/killed
      // status update for the same taskid! This is a safety check, 
      // and is addressed better at the TaskTracker to ensure this.
      // @see {@link TaskTracker.transmitHeartbeat()}
      if ((newState != TaskStatus.State.RUNNING) && 
          (oldState == newState)) {
        LOG.warn("Recieved duplicate status update of '" + newState + 
                 "' for '" + taskid + "' of TIP '" + getTIPId() + "'");
        return false;
      }

      // The task is not allowed to move from completed back to running.
      // We have seen out of order status messagesmoving tasks from complete
      // to running. This is a spot fix, but it should be addressed more
      // globally.
      if (newState == TaskStatus.State.RUNNING &&
          (oldState == TaskStatus.State.FAILED || 
           oldState == TaskStatus.State.KILLED || 
           oldState == TaskStatus.State.SUCCEEDED)) {
        return false;
      }
          
      changed = oldState != newState;
    }
        
    taskStatuses.put(taskid, status);

    // Recompute progress
    recomputeProgress();
    return changed;
  }

  /**
   * Indicate that one of the taskids in this TaskInProgress
   * has failed.
   */
  public void incompleteSubTask(String taskid, String trackerName, 
                                JobStatus jobStatus) {
    //
    // Note the failure and its location
    //
    LOG.info("Task '" + taskid + "' has been lost.");
    TaskStatus status = taskStatuses.get(taskid);
    TaskStatus.State taskState = TaskStatus.State.FAILED;
    if (status != null) {
      taskState = status.getRunState();
      if (taskState != TaskStatus.State.FAILED && 
              taskState != TaskStatus.State.KILLED) {
        LOG.info("Task '" + taskid + "' running on '" + trackerName + 
                "' in state: '" + taskState + "' being failed!");
        status.setRunState(TaskStatus.State.FAILED);
        taskState = TaskStatus.State.FAILED;
      }

      // tasktracker went down and failed time was not reported. 
      if (0 == status.getFinishTime()){
        status.setFinishTime(System.currentTimeMillis());
      }
    }

    this.activeTasks.remove(taskid);
    
    // Since we do not fail completed reduces (whose outputs go to hdfs), we 
    // should note this failure only for completed maps; however if the job
    // is done, there is no need to manipulate completed maps
    if (this.completes > 0 && this.isMapTask() && 
        jobStatus.getRunState() != JobStatus.SUCCEEDED) {
      this.completes--;
    }

    // Discard task output
    Task t = tasks.get(taskid);
    try {
      t.discardTaskOutput();
    } catch (IOException ioe) {
      LOG.info("Failed to discard output of task '" + taskid + "' with " + 
              StringUtils.stringifyException(ioe));
    }

    if (taskState == TaskStatus.State.FAILED) {
      numTaskFailures++;
    } else {
      numKilledTasks++;
    }

    if (numTaskFailures >= maxTaskAttempts) {
      LOG.info("TaskInProgress " + getTIPId() + " has failed " + numTaskFailures + " times.");
      kill();
    }
    machinesWhereFailed.add(trackerName);
  }

  /**
   * Finalize the <b>completed</b> task; note that this might not be the first 
   * task-attempt of the {@link TaskInProgress} and hence might be declared 
   * {@link TaskStatus.State.SUCCEEDED} or {@link TaskStatus.State.KILLED}
   * 
   * @param taskId id of the completed task-attempt
   * @param finalTaskState final {@link TaskStatus.State} of the task-attempt
   */
  private void completedTask(String taskId, TaskStatus.State finalTaskState) {
    TaskStatus status = taskStatuses.get(taskId);
    status.setRunState(finalTaskState);
    activeTasks.remove(taskId);
  }
  
  /**
   * Indicate that one of the taskids in this already-completed
   * TaskInProgress has successfully completed; hence we mark this
   * taskid as {@link TaskStatus.State.KILLED}. 
   */
  void alreadyCompletedTask(String taskid) {
    Task t = tasks.get(taskid);
    try {
      t.discardTaskOutput();
    } catch (IOException ioe) {
      LOG.info("Failed to discard output of task '" + taskid + "' with " + 
              StringUtils.stringifyException(ioe));
    }

    // 'KILL' the task 
    completedTask(taskid, TaskStatus.State.KILLED);
    
    // Note the reason for the task being 'KILLED'
    addDiagnosticInfo(taskid, "Already completed TIP");
    
    LOG.info("Already complete TIP " + getTIPId() + 
             " has completed task " + taskid);
  }

  /**
   * Indicate that one of the taskids in this TaskInProgress
   * has successfully completed!
   */
  public void completed(String taskid) throws IOException {
    //
    // Finalize the task's output
    //
    Task t = tasks.get(taskid);
    if (!savedTaskOutput) {
      t.saveTaskOutput();
      savedTaskOutput = true;
    } else {
      try {
        t.discardTaskOutput();
      } catch (IOException ioe) {
        LOG.info("Failed to discard 'already-saved' output of task: " + 
                t.getTaskId() + " with: " + 
                StringUtils.stringifyException(ioe));
      }
    }

    //
    // Record that this taskid is complete
    //
    completedTask(taskid, TaskStatus.State.SUCCEEDED);
    
        
    //
    // Now that the TIP is complete, the other speculative 
    // subtasks will be closed when the owning tasktracker 
    // reports in and calls shouldClose() on this object.
    //

    this.completes++;
    recomputeProgress();
    
    LOG.info("Task '" + taskid + "' has completed succesfully");
  }

  /**
   * Get the Status of the tasks managed by this TIP
   */
  public TaskStatus[] getTaskStatuses() {
    return taskStatuses.values().toArray(new TaskStatus[taskStatuses.size()]);
  }

  /**
   * Get the status of the specified task
   * @param taskid
   * @return
   */
  public TaskStatus getTaskStatus(String taskid) {
    return taskStatuses.get(taskid);
  }
  /**
   * The TIP's been ordered kill()ed.
   */
  public void kill() {
    if (isComplete() || failed) {
      return;
    }
    this.failed = true;
    killed = true;
    recomputeProgress();
  }

  /**
   * Was the task killed?
   * @return true if the task killed
   */
  public boolean wasKilled() {
    return killed;
  }
  
  /**
   * Kill the given task
   */
  boolean killTask(String taskId, boolean shouldFail) {
    TaskStatus st = taskStatuses.get(taskId);
    if(st != null && st.getRunState() == TaskStatus.State.RUNNING
        && tasksToKill.put(taskId, shouldFail) == null ) {
      String logStr = "Request received to " + (shouldFail ? "fail" : "kill") 
                      + " task '" + taskId + "' by user";
      addDiagnosticInfo(taskId, logStr);
      LOG.info(logStr);
      return true;
    }
    return false;
  }

  /** Notification that a task with the given id has been killed */
  void taskKilled(String taskId, String trackerName, JobStatus jobStatus) {
    Boolean shouldFail = tasksToKill.remove(taskId);
    if(shouldFail != null && !shouldFail) {
      LOG.info("Task '" + taskId + "' has been killed");
      this.activeTasks.remove(taskId);
      taskStatuses.get(taskId).setRunState(TaskStatus.State.KILLED );
      addDiagnosticInfo(taskId, "Task has been killed" );
      // Discard task output
      Task t = tasks.get(taskId);
      try {
        t.discardTaskOutput();
      } catch (IOException ioe) {
        LOG.info("Failed to discard output of task '" + taskId + "' with " +
            StringUtils.stringifyException(ioe));
      }
      numKilledTasks++;
      
    }
    else {
      //set the task status as failed. 
      taskStatuses.get(taskId).setRunState(TaskStatus.State.FAILED);
      incompleteSubTask(taskId, trackerName, jobStatus);
    }
  }

  /**
   * This method is called whenever there's a status change
   * for one of the TIP's sub-tasks.  It recomputes the overall 
   * progress for the TIP.  We examine all sub-tasks and find 
   * the one that's most advanced (and non-failed).
   */
  void recomputeProgress() {
    if (isComplete()) {
      this.progress = 1;
      this.execFinishTime = System.currentTimeMillis();
    } else if (failed) {
      this.progress = 0;
      this.execFinishTime = System.currentTimeMillis();
    } else {
      double bestProgress = 0;
      String bestState = "";
      Counters bestCounters = new Counters();
      for (Iterator<String> it = taskStatuses.keySet().iterator(); it.hasNext();) {
        String taskid = it.next();
        TaskStatus status = taskStatuses.get(taskid);
        if (status.getRunState() == TaskStatus.State.SUCCEEDED) {
          bestProgress = 1;
          bestState = status.getStateString();
          bestCounters = status.getCounters();
          break;
        } else if (status.getRunState() == TaskStatus.State.RUNNING) {
          if (status.getProgress() >= bestProgress) {
            bestProgress = status.getProgress();
            bestState = status.getStateString();
            bestCounters = status.getCounters();
          }
        }
      }
      this.progress = bestProgress;
      this.state = bestState;
      this.counters = bestCounters;
    }
  }

  /////////////////////////////////////////////////
  // "Action" methods that actually require the TIP
  // to do something.
  /////////////////////////////////////////////////

  /**
   * Return whether this TIP still needs to run
   */
  boolean isRunnable() {
    return !failed && (completes == 0);
  }
    
  /**
   * Return whether the TIP has a speculative task to run.  We
   * only launch a speculative task if the current TIP is really
   * far behind, and has been behind for a non-trivial amount of 
   * time.
   */
  boolean hasSpeculativeTask(double averageProgress) {
    //
    // REMIND - mjc - these constants should be examined
    // in more depth eventually...
    //
      
    if (activeTasks.size() <= MAX_TASK_EXECS &&
        runSpeculative &&
        (averageProgress - progress >= SPECULATIVE_GAP) &&
        (System.currentTimeMillis() - startTime >= SPECULATIVE_LAG) 
        && completes == 0) {
      return true;
    }
    return false;
  }
    
  /**
   * Return a Task that can be sent to a TaskTracker for execution.
   */
  public Task getTaskToRun(String taskTracker) throws IOException {
    Task t = null;
    if (0 == execStartTime){
      // assume task starts running now
      execStartTime = System.currentTimeMillis();
    }

    // Create the 'taskid'; do not count the 'killed' tasks against the job!
    String taskid = null;
    if (nextTaskId < (MAX_TASK_EXECS + maxTaskAttempts + numKilledTasks)) {
      taskid = "task_" + taskIdPrefix + "_" + nextTaskId;
      ++nextTaskId;
    } else {
      LOG.warn("Exceeded limit of " + (MAX_TASK_EXECS + maxTaskAttempts) +
              " (plus " + numKilledTasks + " killed)"  + 
              " attempts for the tip '" + getTIPId() + "'");
      return null;
    }
        
    String jobId = job.getProfile().getJobId();

    if (isMapTask()) {
      t = new MapTask(jobId, jobFile, this.id, taskid, partition, 
                      splitClass, split);
    } else {
      t = new ReduceTask(jobId, jobFile, this.id, taskid, partition, numMaps);
    }
    t.setConf(conf);
    tasks.put(taskid, t);

    activeTasks.put(taskid, taskTracker);

    // Ask JobTracker to note that the task exists
    jobtracker.createTaskEntry(taskid, taskTracker, this);
    return t;
  }
    
  /**
   * Has this task already failed on this machine?
   * @param tracker The task tracker name
   * @return Has it failed?
   */
  public boolean hasFailedOnMachine(String tracker) {
    return machinesWhereFailed.contains(tracker);
  }
    
  /**
   * Was this task ever scheduled to run on this machine?
   * @param tracker The task tracker name
   * @return Was task scheduled on the tracker?
   */
  public boolean hasRunOnMachine(String tracker){
    return this.activeTasks.values().contains(tracker) || 
      hasFailedOnMachine(tracker);
  }
  /**
   * Get the number of machines where this task has failed.
   * @return the size of the failed machine set
   */
  public int getNumberOfFailedMachines() {
    return machinesWhereFailed.size();
  }
    
  /**
   * Get the id of this map or reduce task.
   * @return The index of this tip in the maps/reduces lists.
   */
  public int getIdWithinJob() {
    return partition;
  }
    
  /**
   * Set the event number that was raised for this tip
   */
  public void setSuccessEventNumber(int eventNumber) {
    successEventNumber = eventNumber;
  }
       
  /**
   * Get the event number that was raised for this tip
   */
  public int getSuccessEventNumber() {
    return successEventNumber;
  }
}
