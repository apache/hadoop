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

import java.text.NumberFormat;
import java.util.*;


////////////////////////////////////////////////////////
// TaskInProgress maintains all the info needed for a
// Task in the lifetime of its owning Job.  A given Task
// might be speculatively executed or reexecuted, so we
// need a level of indirection above the running-id itself.
//
// A given TaskInProgress contains multiple taskids,
// 0 or more of which might be executing at any one time.
// (That's what allows speculative execution.)  A taskid
// is now *never* recycled.  A TIP allocates enough taskids
// to account for all the speculation and failures it will
// ever have to handle.  Once those are up, the TIP is dead.
//
////////////////////////////////////////////////////////
class TaskInProgress {
    static final int MAX_TASK_EXECS = 1;
    static final int MAX_TASK_FAILURES = 4;    
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
    private FileSplit split = null;
    private int numMaps;
    private int partition;
    private JobTracker jobtracker;
    private String id;
    private String totalTaskIds[];
    private JobInProgress job;

    // Status of the TIP
    private int numTaskFailures = 0;
    private double progress = 0;
    private String state = "";
    private long startTime = 0;
    private long execStartTime = 0 ;
    private long execFinishTime = 0 ;
    private int completes = 0;
    private boolean failed = false;
    private boolean killed = false;
    private TreeSet usableTaskIds = new TreeSet();
    // Map from task Id -> TaskTracker Id, contains tasks that are
    // currently runnings
    private TreeMap<String, String> activeTasks = new TreeMap();
    private JobConf conf;
    private boolean runSpeculative;
    private Map<String,List<String>> taskDiagnosticData = new TreeMap();
    /**
     * Map from taskId -> TaskStatus
     */
    private TreeMap taskStatuses = new TreeMap();

    private TreeSet machinesWhereFailed = new TreeSet();
    private TreeSet tasksReportedClosed = new TreeSet();

    /**
     * Constructor for MapTask
     */
    public TaskInProgress(String uniqueString, String jobFile, FileSplit split, 
                          JobTracker jobtracker, JobConf conf, 
                          JobInProgress job, int partition) {
        this.jobFile = jobFile;
        this.split = split;
        this.jobtracker = jobtracker;
        this.job = job;
        this.conf = conf;
        this.partition = partition;
        init(uniqueString);
    }
        
    /**
     * Constructor for ReduceTask
     */
    public TaskInProgress(String uniqueString, String jobFile, 
                          int numMaps, 
                          int partition, JobTracker jobtracker, JobConf conf,
                          JobInProgress job) {
        this.jobFile = jobFile;
        this.numMaps = numMaps;
        this.partition = partition;
        this.jobtracker = jobtracker;
        this.job = job;
        this.conf = conf;
        init(uniqueString);
    }

    /**
     * Make a unique name for this TIP.
     * @param uniqueBase The unique name of the job
     * @return The unique string for this tip
     */
    private String makeUniqueString(String uniqueBase) {
      StringBuffer result = new StringBuffer();
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
     * Initialization common to Map and Reduce
     */
    void init(String jobUniqueString) {
        this.startTime = System.currentTimeMillis();
        this.runSpeculative = conf.getSpeculativeExecution();
        String uniqueString = makeUniqueString(jobUniqueString);
        this.id = "tip_" + uniqueString;
        this.totalTaskIds = new String[MAX_TASK_EXECS + MAX_TASK_FAILURES];
        for (int i = 0; i < totalTaskIds.length; i++) {
          totalTaskIds[i] = "task_" + uniqueString + "_" + i;
          usableTaskIds.add(totalTaskIds[i]);
        }
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
     */
    public boolean isComplete() {
        return (completes > 0);
    }
    /**
     */
    public boolean isComplete(String taskid) {
        TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
        if (status == null) {
            return false;
        }
        return ((completes > 0) && 
                (status.getRunState() == TaskStatus.State.SUCCEEDED));
    }
    /**
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
     * Get the overall progress (from 0 to 1.0) for this TIP
     */
    public double getProgress() {
        return progress;
    }
    /**
     * Returns whether a component task-thread should be 
     * closed because the containing JobInProgress has completed.
     */
    public boolean shouldCloseForClosedJob(String taskid) {
        // If the thing has never been closed,
        // and it belongs to this TIP,
        // and this TIP is somehow FINISHED,
        // then true
        TaskStatus ts = (TaskStatus) taskStatuses.get(taskid);
        if ((ts != null) &&
            (! tasksReportedClosed.contains(taskid)) &&
            (job.getStatus().getRunState() != JobStatus.RUNNING)) {
            tasksReportedClosed.add(taskid);
            return true;
        } else if( !isMapTask() && isComplete() && 
                ! tasksReportedClosed.contains(taskid) ){
            tasksReportedClosed.add(taskid);
            return true; 
        }
        else {
            return false;
        }
    }

    /**
     * Creates a "status report" for this task.  Includes the
     * task ID and overall status, plus reports for all the
     * component task-threads that have ever been started.
     */
    synchronized TaskReport generateSingleReport() {
      ArrayList diagnostics = new ArrayList();
      for (Iterator i = taskDiagnosticData.values().iterator(); i.hasNext();) {
        diagnostics.addAll((List)i.next());
      }
      TaskReport report = new TaskReport
      (getTIPId(), (float)progress, state,
          (String[])diagnostics.toArray(new String[diagnostics.size()]),
          execStartTime, execFinishTime);
      
      return report ;
    }

    /**
     * Get the diagnostic messages for a given task within this tip.
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
     * A status message from a client has arrived.
     * It updates the status of a single component-thread-task,
     * which might result in an overall TaskInProgress status update.
     * @return has the task changed its state noticably?
     */
    synchronized boolean updateStatus(TaskStatus status) {
        String taskid = status.getTaskId();
        String diagInfo = status.getDiagnosticInfo();
        TaskStatus oldStatus = (TaskStatus) taskStatuses.get(taskid);
        boolean changed = true;
        if (diagInfo != null && diagInfo.length() > 0) {
          LOG.info("Error from "+taskid+": "+diagInfo);
          List diagHistory = (List) taskDiagnosticData.get(taskid);
          if (diagHistory == null) {
              diagHistory = new ArrayList();
              taskDiagnosticData.put(taskid, diagHistory);
          }
          diagHistory.add(diagInfo);
        }
        if (oldStatus != null) {
          TaskStatus.State oldState = oldStatus.getRunState();
          TaskStatus.State newState = status.getRunState();
          
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
    public void failedSubTask(String taskid, String trackerName) {
        //
        // Note the failure and its location
        //
        LOG.info("Task '" + taskid + "' has been lost.");
        TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
        if (status != null) {
            status.setRunState(TaskStatus.State.FAILED);
            // tasktracker went down and failed time was not reported. 
            if( 0 == status.getFinishTime() ){
              status.setFinishTime(System.currentTimeMillis());
            }
        }
        this.activeTasks.remove(taskid);
        if (this.completes > 0 && this.isMapTask()) {
            this.completes--;
        }

        numTaskFailures++;
        if (numTaskFailures >= MAX_TASK_FAILURES) {
            LOG.info("TaskInProgress " + getTIPId() + " has failed " + numTaskFailures + " times.");
            kill();
        }
        machinesWhereFailed.add(trackerName);
    }

    /**
     * Indicate that one of the taskids in this TaskInProgress
     * has successfully completed!
     */
    public void completed(String taskid) {
        LOG.info("Task '" + taskid + "' has completed.");
        TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
        status.setRunState(TaskStatus.State.SUCCEEDED);
        activeTasks.remove(taskid);

        //
        // Now that the TIP is complete, the other speculative 
        // subtasks will be closed when the owning tasktracker 
        // reports in and calls shouldClose() on this object.
        //

        this.completes++;
        recomputeProgress();
    }

    /**
     * Get the Status of the tasks managed by this TIP
     */
    public TaskStatus[] getTaskStatuses() {
	return (TaskStatus[])taskStatuses.values().toArray(new TaskStatus[taskStatuses.size()]);
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
            for (Iterator it = taskStatuses.keySet().iterator(); it.hasNext(); ) {
                String taskid = (String) it.next();
                TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
                if (status.getRunState() == TaskStatus.State.SUCCEEDED) {
                    bestProgress = 1;
                    bestState = status.getStateString();
                    break;
                } else if (status.getRunState() == TaskStatus.State.RUNNING) {
                  if (status.getProgress() >= bestProgress) {
                    bestProgress = status.getProgress();
                    bestState = status.getStateString();
                  }
                }
            }
            this.progress = bestProgress;
            this.state = bestState;
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
      
      if( activeTasks.size() <= MAX_TASK_EXECS &&
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
    public Task getTaskToRun(String taskTracker) {
        Task t = null;
        if( 0 == execStartTime ){
          // assume task starts running now
          execStartTime = System.currentTimeMillis();
        }

        String taskid = (String) usableTaskIds.first();
        usableTaskIds.remove(taskid);
        String jobId = job.getProfile().getJobId();

        if (isMapTask()) {
          t = new MapTask(jobId, jobFile, this.id, taskid, partition, split);
        } else {
          t = new ReduceTask(jobId, jobFile, this.id, taskid, partition, numMaps);
        }
        t.setConf(conf);

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
               hasFailedOnMachine(tracker) ;
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
}
