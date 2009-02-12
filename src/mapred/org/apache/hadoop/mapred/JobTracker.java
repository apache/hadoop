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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.Listener;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 *******************************************************/
public class JobTracker implements MRConstants, InterTrackerProtocol,
    JobSubmissionProtocol, TaskTrackerManager {

  static long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  static long RETIRE_JOB_INTERVAL;
  static long RETIRE_JOB_CHECK_INTERVAL;
  public static enum State { INITIALIZING, RUNNING }
  State state = State.INITIALIZING;
  private static final int SYSTEM_DIR_CLEANUP_RETRY_PERIOD = 10000;

  private DNSToSwitchMapping dnsToSwitchMapping;
  private NetworkTopology clusterMap = new NetworkTopology();
  private int numTaskCacheLevels; // the max level to which we cache tasks
  private Set<Node> nodesAtMaxLevel = new HashSet<Node>();
  private final TaskScheduler taskScheduler;
  private final List<JobInProgressListener> jobInProgressListeners =
    new CopyOnWriteArrayList<JobInProgressListener>();

  // system directories are world-wide readable and owner readable
  final static FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0733); // rwx-wx-wx

  /**
   * A client tried to submit a job before the Job Tracker was ready.
   */
  public static class IllegalStateException extends IOException {
    public IllegalStateException(String msg) {
      super(msg);
    }
  }

  /**
   * The maximum no. of 'completed' (successful/failed/killed)
   * jobs kept in memory per-user. 
   */
  final int MAX_COMPLETE_USER_JOBS_IN_MEMORY;

   /**
    * The minimum time (in ms) that a job's information has to remain
    * in the JobTracker's memory before it is retired.
    */
  static final int MIN_TIME_BEFORE_RETIRE = 60000;


  private int nextJobId = 1;

  public static final Log LOG = LogFactory.getLog(JobTracker.class);
    
  /**
   * Start the JobTracker with given configuration.
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the JobTracker is up and running if the user passes the port as
   * <code>zero</code>.
   *   
   * @param conf configuration for the JobTracker.
   * @throws IOException
   */
  public static JobTracker startTracker(JobConf conf
                                        ) throws IOException,
                                                 InterruptedException {
    JobTracker result = null;
    while (true) {
      try {
        result = new JobTracker(conf);
        result.taskScheduler.setTaskTrackerManager(result);
        break;
      } catch (VersionMismatch e) {
        throw e;
      } catch (BindException e) {
        throw e;
      } catch (UnknownHostException e) {
        throw e;
      } catch (IOException e) {
        LOG.warn("Error starting tracker: " + 
                 StringUtils.stringifyException(e));
      }
      Thread.sleep(1000);
    }
    if (result != null) {
      JobEndNotifier.startNotifier();
    }
    return result;
  }

  public void stopTracker() throws IOException {
    JobEndNotifier.stopNotifier();
    close();
  }
    
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())){
      return JobSubmissionProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to job tracker: " + protocol);
    }
  }
  /**
   * A thread to timeout tasks that have been assigned to task trackers,
   * but that haven't reported back yet.
   * Note that I included a stop() method, even though there is no place
   * where JobTrackers are cleaned up.
   */
  private class ExpireLaunchingTasks implements Runnable {
    /**
     * This is a map of the tasks that have been assigned to task trackers,
     * but that have not yet been seen in a status report.
     * map: task-id -> time-assigned 
     */
    private Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();
      
    public void run() {
      while (true) {
        try {
          // Every 3 minutes check for any tasks that are overdue
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
          long now = System.currentTimeMillis();
          LOG.debug("Starting launching task sweep");
          synchronized (JobTracker.this) {
            synchronized (launchingTasks) {
              Iterator<Map.Entry<TaskAttemptID, Long>> itr =
                launchingTasks.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<TaskAttemptID, Long> pair = itr.next();
                TaskAttemptID taskId = pair.getKey();
                long age = now - (pair.getValue()).longValue();
                LOG.info(taskId + " is " + age + " ms debug.");
                if (age > TASKTRACKER_EXPIRY_INTERVAL) {
                  LOG.info("Launching task " + taskId + " timed out.");
                  TaskInProgress tip = null;
                  tip = taskidToTIPMap.get(taskId);
                  if (tip != null) {
                    JobInProgress job = tip.getJob();
                    String trackerName = getAssignedTracker(taskId);
                    TaskTrackerStatus trackerStatus = 
                      getTaskTracker(trackerName);
                    // This might happen when the tasktracker has already
                    // expired and this thread tries to call failedtask
                    // again. expire tasktracker should have called failed
                    // task!
                    if (trackerStatus != null)
                      job.failedTask(tip, taskId, "Error launching task", 
                                     tip.isMapTask()? TaskStatus.Phase.MAP:
                                     TaskStatus.Phase.STARTING,
                                     TaskStatus.State.FAILED,
                                     trackerName, myInstrumentation);
                  }
                  itr.remove();
                } else {
                  // the tasks are sorted by start time, so once we find
                  // one that we want to keep, we are done for this cycle.
                  break;
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          // all done
          break;
        } catch (Exception e) {
          LOG.error("Expire Launching Task Thread got exception: " +
                    StringUtils.stringifyException(e));
        }
      }
    }
      
    public void addNewTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.put(taskName, 
                           System.currentTimeMillis());
      }
    }
      
    public void removeTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.remove(taskName);
      }
    }
  }
    
  ///////////////////////////////////////////////////////
  // Used to expire TaskTrackers that have gone down
  ///////////////////////////////////////////////////////
  class ExpireTrackers implements Runnable {
    public ExpireTrackers() {
    }
    /**
     * The run method lives for the life of the JobTracker, and removes TaskTrackers
     * that have not checked in for some time.
     */
    public void run() {
      while (true) {
        try {
          //
          // Thread runs periodically to check whether trackers should be expired.
          // The sleep interval must be no more than half the maximum expiry time
          // for a task tracker.
          //
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);

          //
          // Loop through all expired items in the queue
          //
          // Need to lock the JobTracker here since we are
          // manipulating it's data-structures via
          // ExpireTrackers.run -> JobTracker.lostTaskTracker ->
          // JobInProgress.failedTask -> JobTracker.markCompleteTaskAttempt
          // Also need to lock JobTracker before locking 'taskTracker' &
          // 'trackerExpiryQueue' to prevent deadlock:
          // @see {@link JobTracker.processHeartbeat(TaskTrackerStatus, boolean)} 
          synchronized (JobTracker.this) {
            synchronized (taskTrackers) {
              synchronized (trackerExpiryQueue) {
                long now = System.currentTimeMillis();
                TaskTrackerStatus leastRecent = null;
                while ((trackerExpiryQueue.size() > 0) &&
                       ((leastRecent = trackerExpiryQueue.first()) != null) &&
                       (now - leastRecent.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL)) {
                        
                  // Remove profile from head of queue
                  trackerExpiryQueue.remove(leastRecent);
                  String trackerName = leastRecent.getTrackerName();
                        
                  // Figure out if last-seen time should be updated, or if tracker is dead
                  TaskTrackerStatus newProfile = taskTrackers.get(leastRecent.getTrackerName());
                  // Items might leave the taskTracker set through other means; the
                  // status stored in 'taskTrackers' might be null, which means the
                  // tracker has already been destroyed.
                  if (newProfile != null) {
                    if (now - newProfile.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL) {
                      // Remove completely after marking the tasks as 'KILLED'
                      lostTaskTracker(leastRecent.getTrackerName());
                      updateTaskTrackerStatus(trackerName, null);
                    } else {
                      // Update time by inserting latest profile
                      trackerExpiryQueue.add(newProfile);
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException iex) {
          break;
        } catch (Exception t) {
          LOG.error("Tracker Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
        
  }

  ///////////////////////////////////////////////////////
  // Used to remove old finished Jobs that have been around for too long
  ///////////////////////////////////////////////////////
  class RetireJobs implements Runnable {
    public RetireJobs() {
    }

    /**
     * The run method lives for the life of the JobTracker,
     * and removes Jobs that are not still running, but which
     * finished a long time ago.
     */
    public void run() {
      while (true) {
        try {
          Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
          List<JobInProgress> retiredJobs = new ArrayList<JobInProgress>();
          long now = System.currentTimeMillis();
          long retireBefore = now - RETIRE_JOB_INTERVAL;

          synchronized (jobs) {
            for(JobInProgress job: jobs.values()) {
              if (job.getStatus().getRunState() != JobStatus.RUNNING &&
                  job.getStatus().getRunState() != JobStatus.PREP &&
                  (job.getFinishTime() + MIN_TIME_BEFORE_RETIRE < now) &&
                  (job.getFinishTime()  < retireBefore)) {
                retiredJobs.add(job);
              }
            }
          }
          if (!retiredJobs.isEmpty()) {
            synchronized (JobTracker.this) {
              synchronized (jobs) {
                synchronized (taskScheduler) {
                  for (JobInProgress job: retiredJobs) {
                    removeJobTasks(job);
                    jobs.remove(job.getProfile().getJobID());
                    for (JobInProgressListener l : jobInProgressListeners) {
                      l.jobRemoved(job);
                    }
                    String jobUser = job.getProfile().getUser();
                    synchronized (userToJobsMap) {
                      ArrayList<JobInProgress> userJobs =
                        userToJobsMap.get(jobUser);
                      synchronized (userJobs) {
                        userJobs.remove(job);
                      }
                      if (userJobs.isEmpty()) {
                        userToJobsMap.remove(jobUser);
                      }
                    }
                    LOG.info("Retired job with id: '" + 
                             job.getProfile().getJobID() + "' of user '" +
                             jobUser + "'");
                  }
                }
              }
            }
          }
        } catch (InterruptedException t) {
          break;
        } catch (Throwable t) {
          LOG.error("Error in retiring job:\n" +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }

 
  ///////////////////////////////////////////////////////
  // Used to recover the jobs upon restart
  ///////////////////////////////////////////////////////
  class RecoveryManager {
    Set<JobID> jobsToRecover; // set of jobs to be recovered
    
    private int totalEventsRecovered = 0;
    
    /** A custom listener that replays the events in the order in which the 
     * events (task attempts) occurred. 
     */
    class JobRecoveryListener implements Listener {
      // The owner job
      private JobInProgress jip;
      
      private JobHistory.JobInfo job; // current job's info object
      
      // Maintain the count of the (attempt) events recovered
      private int numEventsRecovered = 0;
      
      // Maintains open transactions
      private Map<String, String> hangingAttempts = 
        new HashMap<String, String>();
      
      // Whether there are any updates for this job
      private boolean hasUpdates = false;
      
      public JobRecoveryListener(JobInProgress jip) {
        this.jip = jip;
        this.job = new JobHistory.JobInfo(jip.getJobID().toString());
      }

      /**
       * Process a task. Note that a task might commit a previously pending 
       * transaction.
       */
      private void processTask(String taskId, JobHistory.Task task) {
        // Any TASK info commits the previous transaction
        boolean hasHanging = hangingAttempts.remove(taskId) != null;
        if (hasHanging) {
          numEventsRecovered += 2;
        }
        
        TaskID id = TaskID.forName(taskId);
        TaskInProgress tip = getTip(id);
        
        updateTip(tip, task);
      }

      /**
       * Adds a task-attempt in the listener
       */
      private void processTaskAttempt(String taskAttemptId, 
                                      JobHistory.TaskAttempt attempt) {
        TaskAttemptID id = TaskAttemptID.forName(taskAttemptId);
        
        // Check if the transaction for this attempt can be committed
        String taskStatus = attempt.get(Keys.TASK_STATUS);
        
        if (taskStatus.length() > 0) {
          // This means this is an update event
          if (taskStatus.equals(Values.SUCCESS.name())) {
            // Mark this attempt as hanging
            hangingAttempts.put(id.getTaskID().toString(), taskAttemptId);
            addSuccessfulAttempt(jip, id, attempt);
          } else {
            addUnsuccessfulAttempt(jip, id, attempt);
            numEventsRecovered += 2;
          }
        } else {
          createTaskAttempt(jip, id, attempt);
        }
      }

      public void handle(JobHistory.RecordTypes recType, Map<Keys, 
                         String> values) throws IOException {
        if (recType == JobHistory.RecordTypes.Job) {
          // Update the meta-level job information
          job.handle(values);
          
          // Forcefully init the job as we have some updates for it
          checkAndInit();
        } else if (recType.equals(JobHistory.RecordTypes.Task)) {
          String taskId = values.get(Keys.TASKID);
          
          // Create a task
          JobHistory.Task task = new JobHistory.Task();
          task.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(task)) {
            return;
          }
            
          // Process the task i.e update the tip state
          processTask(taskId, task);
        } else if (recType.equals(JobHistory.RecordTypes.MapAttempt)) {
          String attemptId = values.get(Keys.TASK_ATTEMPT_ID);
          
          // Create a task attempt
          JobHistory.MapAttempt attempt = new JobHistory.MapAttempt();
          attempt.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(attempt)) {
            return;
          }
          
          // Process the attempt i.e update the attempt state via job
          processTaskAttempt(attemptId, attempt);
        } else if (recType.equals(JobHistory.RecordTypes.ReduceAttempt)) {
          String attemptId = values.get(Keys.TASK_ATTEMPT_ID);
          
          // Create a task attempt
          JobHistory.ReduceAttempt attempt = new JobHistory.ReduceAttempt();
          attempt.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(attempt)) {
            return;
          }
          
          // Process the attempt i.e update the job state via job
          processTaskAttempt(attemptId, attempt);
        }
      }

      // Check if the task is of type CLEANUP
      private boolean isCleanup(JobHistory.Task task) {
        String taskType = task.get(Keys.TASK_TYPE);
        return Values.CLEANUP.name().equals(taskType);
      }
      
      // Init the job if its ready for init. Also make sure that the scheduler
      // is updated
      private void checkAndInit() throws IOException {
        String jobStatus = this.job.get(Keys.JOB_STATUS);
        if (Values.PREP.name().equals(jobStatus)) {
          hasUpdates = true;
          LOG.info("Calling init from RM for job " + jip.getJobID().toString());
          jip.initTasks();
        }
      }
      
      void close() {
        if (hasUpdates) {
          // Apply the final (job-level) updates
          JobStatusChangeEvent event = updateJob(jip, job);
          
          // Update the job listeners
          updateJobInProgressListeners(event);
        }
      }
      
      public int getNumEventsRecovered() {
        return numEventsRecovered;
      }

    }
    
    public RecoveryManager() {
      jobsToRecover = new TreeSet<JobID>();
    }

    public boolean contains(JobID id) {
      return jobsToRecover.contains(id);
    }

    void addJobForRecovery(JobID id) {
      jobsToRecover.add(id);
    }

    public boolean shouldRecover() {
      return jobsToRecover.size() != 0;
    }

    /** Check if the given string represents a job-id or not 
     */
    private boolean isJobNameValid(String str) {
      if(str == null) {
        return false;
      }
      String[] parts = str.split("_");
      if(parts.length == 3) {
        if(parts[0].equals("job")) {
            // other 2 parts should be parseable
            return JobTracker.validateIdentifier(parts[1])
                   && JobTracker.validateJobNumber(parts[2]);
        }
      }
      return false;
    }
    
    // checks if the job dir has the required files
    public void checkAndAddJob(FileStatus status) throws IOException {
      String jobName = status.getPath().getName();
      if (isJobNameValid(jobName)) {
        if (JobClient.isJobDirValid(status.getPath(), fs)) {
          recoveryManager.addJobForRecovery(JobID.forName(jobName));
        } else {
          LOG.info("Found an incomplete job directory " + jobName + "." 
                   + " Deleting it!!");
          fs.delete(status.getPath(), true);
        }
      } else {
        LOG.info("Deleting " + status.getPath());
        fs.delete(status.getPath(), true);
      }
    }
    
    private JobStatusChangeEvent updateJob(JobInProgress jip, 
                                           JobHistory.JobInfo job) {
      // Change the job priority
      String jobpriority = job.get(Keys.JOB_PRIORITY);
      JobPriority priority = JobPriority.valueOf(jobpriority);
      // It's important to update this via the jobtracker's api as it will 
      // take care of updating the event listeners too
      setJobPriority(jip.getJobID(), priority);

      // Save the previous job status
      JobStatus oldStatus = (JobStatus)jip.getStatus().clone();
      
      // Set the start/launch time only if there are recovered tasks
      // Increment the job's restart count
      jip.updateJobInfo(job.getLong(JobHistory.Keys.SUBMIT_TIME), 
                        job.getLong(JobHistory.Keys.LAUNCH_TIME),
                        job.getInt(Keys.RESTART_COUNT) + 1);

      // Save the new job status
      JobStatus newStatus = (JobStatus)jip.getStatus().clone();
      
      return new JobStatusChangeEvent(jip, EventType.START_TIME_CHANGED, oldStatus, 
                                      newStatus);
    }
    
    private void updateTip(TaskInProgress tip, JobHistory.Task task) {
      long startTime = task.getLong(Keys.START_TIME);
      if (startTime != 0) {
        tip.setExecStartTime(startTime);
      }
      
      long finishTime = task.getLong(Keys.FINISH_TIME);
      // For failed tasks finish-time will be missing
      if (finishTime != 0) {
        tip.setExecFinishTime(finishTime);
      }
      
      String cause = task.get(Keys.TASK_ATTEMPT_ID);
      if (cause.length() > 0) {
        // This means that the this is a FAILED events
        TaskAttemptID id = TaskAttemptID.forName(cause);
        TaskStatus status = tip.getTaskStatus(id);
        // This will add the tip failed event in the new log
        tip.getJob().failedTask(tip, id, status.getDiagnosticInfo(), 
                                status.getPhase(), status.getRunState(), 
                                status.getTaskTracker(), myInstrumentation);
      }
    }
    
    private void createTaskAttempt(JobInProgress job, 
                                   TaskAttemptID attemptId, 
                                   JobHistory.TaskAttempt attempt) {
      TaskID id = attemptId.getTaskID();
      String type = attempt.get(Keys.TASK_TYPE);
      TaskInProgress tip = job.getTaskInProgress(id);
      
      //    I. Get the required info
      TaskStatus taskStatus = null;
      String trackerName = attempt.get(Keys.TRACKER_NAME);
      String trackerHostName = 
        JobInProgress.convertTrackerNameToHostName(trackerName);
      int index = trackerHostName.indexOf("_");
      trackerHostName = 
        trackerHostName.substring(index + 1, trackerHostName.length());
      int port = attempt.getInt(Keys.HTTP_PORT);
      
      long attemptStartTime = attempt.getLong(Keys.START_TIME);

      // II. Create the (appropriate) task status
      if (type.equals(Values.MAP.name())) {
        taskStatus = 
          new MapTaskStatus(attemptId, 0.0f, TaskStatus.State.RUNNING, 
                            "", "", trackerName, TaskStatus.Phase.MAP, 
                            new Counters());
      } else {
        taskStatus = 
          new ReduceTaskStatus(attemptId, 0.0f, TaskStatus.State.RUNNING, 
                               "", "", trackerName, TaskStatus.Phase.REDUCE, 
                               new Counters());
      }

      // Set the start time
      taskStatus.setStartTime(attemptStartTime);

      List<TaskStatus> ttStatusList = new ArrayList<TaskStatus>();
      ttStatusList.add(taskStatus);
      
      // III. Create the dummy tasktracker status
      TaskTrackerStatus ttStatus = 
        new TaskTrackerStatus(trackerName, trackerHostName, port, ttStatusList, 
                              0 , 0, 0);
      ttStatus.setLastSeen(System.currentTimeMillis());

      // IV. Register a new tracker
      boolean isTrackerRegistered = getTaskTracker(trackerName) != null;
      if (!isTrackerRegistered) {
        addNewTracker(ttStatus);
      }
      
      // V. Update the tracker status
      //    This will update the meta info of the jobtracker and also add the
      //    tracker status if missing i.e register it
      updateTaskTrackerStatus(trackerName, ttStatus);
      
      // VI. Register the attempt
      //   a) In the job
      job.addRunningTaskToTIP(tip, attemptId, ttStatus, false);
      //   b) In the tip
      tip.updateStatus(taskStatus);
      
      // VII. Make an entry in the launched tasks
      expireLaunchingTasks.addNewTask(attemptId);
    }
    
    private void addSuccessfulAttempt(JobInProgress job, 
                                      TaskAttemptID attemptId, 
                                      JobHistory.TaskAttempt attempt) {
      // I. Get the required info
      TaskID taskId = attemptId.getTaskID();
      String type = attempt.get(Keys.TASK_TYPE);

      TaskInProgress tip = job.getTaskInProgress(taskId);
      long attemptFinishTime = attempt.getLong(Keys.FINISH_TIME);

      // Get the task status and the tracker name and make a copy of it
      TaskStatus taskStatus = (TaskStatus)tip.getTaskStatus(attemptId).clone();
      taskStatus.setFinishTime(attemptFinishTime);

      String stateString = attempt.get(Keys.STATE_STRING);

      // Update the basic values
      taskStatus.setStateString(stateString);
      taskStatus.setProgress(1.0f);
      taskStatus.setRunState(TaskStatus.State.SUCCEEDED);

      // Set the shuffle/sort finished times
      if (type.equals(Values.REDUCE.name())) {
        long shuffleTime = 
          Long.parseLong(attempt.get(Keys.SHUFFLE_FINISHED));
        long sortTime = 
          Long.parseLong(attempt.get(Keys.SORT_FINISHED));
        taskStatus.setShuffleFinishTime(shuffleTime);
        taskStatus.setSortFinishTime(sortTime);
      }

      // Add the counters
      String counterString = attempt.get(Keys.COUNTERS);
      Counters counter = null;
      //TODO Check if an exception should be thrown
      try {
        counter = Counters.fromEscapedCompactString(counterString);
      } catch (ParseException pe) { 
        counter = new Counters(); // Set it to empty counter
      }
      taskStatus.setCounters(counter);
      
      // II. Replay the status
      job.updateTaskStatus(tip, taskStatus, myInstrumentation);
      
      // III. Prevent the task from expiry
      expireLaunchingTasks.removeTask(attemptId);
    }
    
    private void addUnsuccessfulAttempt(JobInProgress job,
                                        TaskAttemptID attemptId,
                                        JobHistory.TaskAttempt attempt) {
      // I. Get the required info
      TaskID taskId = attemptId.getTaskID();
      TaskInProgress tip = job.getTaskInProgress(taskId);
      long attemptFinishTime = attempt.getLong(Keys.FINISH_TIME);

      TaskStatus taskStatus = (TaskStatus)tip.getTaskStatus(attemptId).clone();
      taskStatus.setFinishTime(attemptFinishTime);

      // Reset the progress
      taskStatus.setProgress(0.0f);
      
      String stateString = attempt.get(Keys.STATE_STRING);
      taskStatus.setStateString(stateString);

      boolean hasFailed = 
        attempt.get(Keys.TASK_STATUS).equals(Values.FAILED.name());
      // Set the state failed/killed
      if (hasFailed) {
        taskStatus.setRunState(TaskStatus.State.FAILED);
      } else {
        taskStatus.setRunState(TaskStatus.State.KILLED);
      }

      // Get/Set the error msg
      String diagInfo = attempt.get(Keys.ERROR);
      taskStatus.setDiagnosticInfo(diagInfo); // diag info

      // II. Update the task status
     job.updateTaskStatus(tip, taskStatus, myInstrumentation);

     // III. Prevent the task from expiry
     expireLaunchingTasks.removeTask(attemptId);
    }
  
    public void recover() throws IOException {
      // I. Init the jobs and cache the recovered job history filenames
      Map<JobID, Path> jobHistoryFilenameMap = new HashMap<JobID, Path>();
      for (JobID id : jobsToRecover) {
        // 1. Create the job object
        JobInProgress job = new JobInProgress(id, JobTracker.this, conf);
        
        // 2. Get the log file and the file path
        String logFileName = 
          JobHistory.JobInfo.getJobHistoryFileName(job.getJobConf(), id);
        Path jobHistoryFilePath = 
          JobHistory.JobInfo.getJobHistoryLogLocation(logFileName);
        
        // 3. Recover the history file. This involved
        //     - deleting file.recover if file exists
        //     - renaming file.recover to file if file doesnt exist
        // This makes sure that the (master) file exists
        JobHistory.JobInfo.recoverJobHistoryFile(job.getJobConf(), 
                                                 jobHistoryFilePath);

        // 4. Cache the history file name as it costs one dfs access
        jobHistoryFilenameMap.put(job.getJobID(), jobHistoryFilePath);

        // 5. Sumbit the job to the jobtracker
        addJob(id, job);
      }

      long recoveryStartTime = System.currentTimeMillis();

      // II. Recover each job
      for (JobID id : jobsToRecover) {
        JobInProgress pJob = getJob(id);

        // 1. Get the required info
        // Get the recovered history file
        Path jobHistoryFilePath = jobHistoryFilenameMap.get(pJob.getJobID());
        String logFileName = jobHistoryFilePath.getName();

        FileSystem fs = jobHistoryFilePath.getFileSystem(conf);

        // 2. Parse the history file
        // Note that this also involves job update
        JobRecoveryListener listener = new JobRecoveryListener(pJob);
        try {
          JobHistory.parseHistoryFromFS(jobHistoryFilePath.toString(), 
                                        listener, fs);
        } catch (IOException e) {
          LOG.info("JobTracker failed to recover job " + pJob + "."
                   + " Ignoring it.", e);
          continue;
        }

        // 3. Close the listener
        listener.close();

        // 4. Update the recovery metric
        totalEventsRecovered += listener.getNumEventsRecovered();

        // 5. Cleanup history
        // Delete the master log file as an indication that the new file
        // should be used in future
        synchronized (pJob) {
          JobHistory.JobInfo.checkpointRecovery(logFileName, 
              pJob.getJobConf());
        }

        // 6. Inform the jobtracker as to how much of the data is recovered.
        // This is done so that TT should rollback to account for lost
        // updates
        lastSeenEventMapOnRestart.put(pJob.getStatus().getJobID(), 
                                      pJob.getNumTaskCompletionEvents());
      }

      recoveryDuration = System.currentTimeMillis() - recoveryStartTime;
      hasRecovered = true;

      // III. Finalize the recovery
      // Make sure that the tracker statuses in the expiry-tracker queue
      // are updated
      long now = System.currentTimeMillis();
      int size = trackerExpiryQueue.size();
      for (int i = 0; i < size ; ++i) {
        // Get the first status
        TaskTrackerStatus status = trackerExpiryQueue.first();

        // Remove it
        trackerExpiryQueue.remove(status);

        // Set the new time
        status.setLastSeen(now);

        // Add back to get the sorted list
        trackerExpiryQueue.add(status);
      }

      // IV. Cleanup
      jobsToRecover.clear();
      LOG.info("Restoration complete");
    }
    
    int totalEventsRecovered() {
      return totalEventsRecovered;
    }
  }

  private JobTrackerInstrumentation myInstrumentation = null;
    
  /////////////////////////////////////////////////////////////////
  // The real JobTracker
  ////////////////////////////////////////////////////////////////
  int port;
  String localMachine;
  private String trackerIdentifier;
  long startTime;
  int totalSubmissions = 0;
  private int totalMapTaskCapacity;
  private int totalReduceTaskCapacity;
  private HostsFileReader hostsReader;
  
  // JobTracker recovery variables
  private volatile boolean hasRestarted = false;
  private volatile boolean hasRecovered = false;
  private volatile long recoveryDuration;

  //
  // Properties to maintain while running Jobs and Tasks:
  //
  // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its 
  //     Tasks are complete.
  //
  // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
  //
  // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
  //
  // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
  //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
  //     reexecution if all downstream Tasks that require its output have already obtained
  //     the necessary files.
  //

  // All the known jobs.  (jobid->JobInProgress)
  Map<JobID, JobInProgress> jobs = new TreeMap<JobID, JobInProgress>();

  // (user -> list of JobInProgress)
  TreeMap<String, ArrayList<JobInProgress>> userToJobsMap =
    new TreeMap<String, ArrayList<JobInProgress>>();
    
  // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
  Map<TaskAttemptID, TaskInProgress> taskidToTIPMap =
    new TreeMap<TaskAttemptID, TaskInProgress>();

  // (taskid --> trackerID) 
  TreeMap<TaskAttemptID, String> taskidToTrackerMap = new TreeMap<TaskAttemptID, String>();

  // (trackerID->TreeSet of taskids running at that tracker)
  TreeMap<String, Set<TaskAttemptID>> trackerToTaskMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // (trackerID -> TreeSet of completed taskids running at that tracker)
  TreeMap<String, Set<TaskAttemptID>> trackerToMarkedTasksMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // (trackerID --> last sent HeartBeatResponse)
  Map<String, HeartbeatResponse> trackerToHeartbeatResponseMap = 
    new TreeMap<String, HeartbeatResponse>();

  // (hostname --> Node (NetworkTopology))
  Map<String, Node> hostnameToNodeMap = 
    Collections.synchronizedMap(new TreeMap<String, Node>());
  
  // A map from JobID to the last known task-completion-event-index on restart
  Map<JobID, Integer> lastSeenEventMapOnRestart = 
    new HashMap<JobID, Integer>();
  
  // Number of resolved entries
  int numResolved;
    
  //
  // Watch and expire TaskTracker objects using these structures.
  // We can map from Name->TaskTrackerStatus, or we can expire by time.
  //
  int totalMaps = 0;
  int totalReduces = 0;
  private HashMap<String, TaskTrackerStatus> taskTrackers =
    new HashMap<String, TaskTrackerStatus>();
  Map<String,Integer>uniqueHostsMap = new ConcurrentHashMap<String, Integer>();
  ExpireTrackers expireTrackers = new ExpireTrackers();
  Thread expireTrackersThread = null;
  RetireJobs retireJobs = new RetireJobs();
  Thread retireJobsThread = null;
  ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
  Thread expireLaunchingTaskThread = new Thread(expireLaunchingTasks,
                                                "expireLaunchingTasks");

  CompletedJobStatusStore completedJobStatusStore = null;
  Thread completedJobsStoreThread = null;
  RecoveryManager recoveryManager;

  /**
   * It might seem like a bug to maintain a TreeSet of status objects,
   * which can be updated at any time.  But that's not what happens!  We
   * only update status objects in the taskTrackers table.  Status objects
   * are never updated once they enter the expiry queue.  Instead, we wait
   * for them to expire and remove them from the expiry queue.  If a status
   * object has been updated in the taskTracker table, the latest status is 
   * reinserted.  Otherwise, we assume the tracker has expired.
   */
  TreeSet<TaskTrackerStatus> trackerExpiryQueue =
    new TreeSet<TaskTrackerStatus>(
                                   new Comparator<TaskTrackerStatus>() {
                                     public int compare(TaskTrackerStatus p1, TaskTrackerStatus p2) {
                                       if (p1.getLastSeen() < p2.getLastSeen()) {
                                         return -1;
                                       } else if (p1.getLastSeen() > p2.getLastSeen()) {
                                         return 1;
                                       } else {
                                         return (p1.getTrackerName().compareTo(p2.getTrackerName()));
                                       }
                                     }
                                   }
                                   );

  // Used to provide an HTML view on Job, Task, and TaskTracker structures
  StatusHttpServer infoServer;
  int infoPort;

  Server interTrackerServer;

  // Some jobs are stored in a local system directory.  We can delete
  // the files when we're done with the job.
  static final String SUBDIR = "jobTracker";
  FileSystem fs = null;
  Path systemDir = null;
  private JobConf conf;

  private QueueManager queueManager;

  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf) throws IOException, InterruptedException {
    //
    // Grab some static constants
    //
    TASKTRACKER_EXPIRY_INTERVAL = 
      conf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);
    RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
    RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
    MAX_COMPLETE_USER_JOBS_IN_MEMORY = conf.getInt("mapred.jobtracker.completeuserjobs.maximum", 100);

    // This is a directory of temporary submission files.  We delete it
    // on startup, and can delete any files that we're done with
    this.conf = conf;
    JobConf jobConf = new JobConf(conf);

    // Read the hosts/exclude files to restrict access to the jobtracker.
    this.hostsReader = new HostsFileReader(conf.get("mapred.hosts", ""),
                                           conf.get("mapred.hosts.exclude", ""));
    
    queueManager = new QueueManager(this.conf);
    
    // Create the scheduler
    Class<? extends TaskScheduler> schedulerClass
      = conf.getClass("mapred.jobtracker.taskScheduler",
          JobQueueTaskScheduler.class, TaskScheduler.class);
    taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(schedulerClass, conf);
                                           
    // Set ports, start RPC servers, etc.
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();
    int handlerCount = conf.getInt("mapred.job.tracker.handler.count", 10);
    this.interTrackerServer = RPC.getServer(this, addr.getHostName(), addr.getPort(), handlerCount, false, conf);
    if (LOG.isDebugEnabled()) {
      Properties p = System.getProperties();
      for (Iterator it = p.keySet().iterator(); it.hasNext();) {
        String key = (String) it.next();
        String val = p.getProperty(key);
        LOG.debug("Property '" + key + "' is " + val);
      }
    }

    String infoAddr = 
      NetUtils.getServerAddress(conf, "mapred.job.tracker.info.bindAddress",
                                "mapred.job.tracker.info.port",
                                "mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.startTime = System.currentTimeMillis();
    infoServer = new StatusHttpServer("job", infoBindAddress, tmpInfoPort, 
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("job.tracker", this);
    // initialize history parameters.
    boolean historyInitialized = JobHistory.init(conf, this.localMachine,
                                                 this.startTime);
    String historyLogDir = null;
    FileSystem historyFS = null;
    if (historyInitialized) {
      historyLogDir = conf.get("hadoop.job.history.location");
      infoServer.setAttribute("historyLogDir", historyLogDir);
      historyFS = new Path(historyLogDir).getFileSystem(conf);
      infoServer.setAttribute("fileSys", historyFS);
    }
    infoServer.start();
    
    trackerIdentifier = getDateFormat().format(new Date());

    Class<? extends JobTrackerInstrumentation> metricsInst = getInstrumentationClass(jobConf);
    try {
      java.lang.reflect.Constructor<? extends JobTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class[] {JobTracker.class, JobConf.class} );
      this.myInstrumentation = c.newInstance(this, jobConf);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by 
      //falling back on the default.
      LOG.error("failed to initialize job tracker metrics", e);
      this.myInstrumentation = new JobTrackerMetricsInst(this, jobConf);
    }
 
    
    // The rpc/web-server ports can be ephemeral ports... 
    // ... ensure we have the correct info
    this.port = interTrackerServer.getListenerAddress().getPort();
    this.conf.set("mapred.job.tracker", (this.localMachine + ":" + this.port));
    LOG.info("JobTracker up at: " + this.port);
    this.infoPort = this.infoServer.getPort();
    this.conf.set("mapred.job.tracker.http.address", 
        infoBindAddress + ":" + this.infoPort); 
    LOG.info("JobTracker webserver: " + this.infoServer.getPort());
    
    // start the recovery manager
    recoveryManager = new RecoveryManager();
    
    while (true) {
      try {
        // if we haven't contacted the namenode go ahead and do it
        if (fs == null) {
          fs = FileSystem.get(conf);
        }
        // clean up the system dir, which will only work if hdfs is out of 
        // safe mode
        if(systemDir == null) {
          systemDir = new Path(getSystemDir());    
        }
        // Make sure that the backup data is preserved
        FileStatus[] systemDirData = fs.listStatus(this.systemDir);
        LOG.info("Cleaning up the system directory");
        // Check if the history is enabled .. as we cant have persistence with 
        // history disabled
        if (conf.getBoolean("mapred.jobtracker.restart.recover", false) 
            && !JobHistory.isDisableHistory()
            && systemDirData != null) {
          for (FileStatus status : systemDirData) {
            recoveryManager.checkAndAddJob(status);
          }
          
          // Check if there are jobs to be recovered
          hasRestarted = recoveryManager.shouldRecover();
          if (hasRestarted) {
            break; // if there is something to recover else clean the sys dir
          }
        }
        fs.delete(systemDir, true);
        if (FileSystem.mkdirs(fs, systemDir, 
            new FsPermission(SYSTEM_DIR_PERMISSION))) {
          break;
        }
        LOG.error("Mkdirs failed to create " + systemDir);
      } catch (IOException ie) {
        if (ie instanceof RemoteException && 
            AccessControlException.class.getName().equals(
                ((RemoteException)ie).getClassName())) {
          throw ie;
        }
        LOG.info("problem cleaning system directory: " + systemDir, ie);
      }
      Thread.sleep(SYSTEM_DIR_CLEANUP_RETRY_PERIOD);
    }
    // Same with 'localDir' except it's always on the local disk.
    jobConf.deleteLocalFiles(SUBDIR);

    // Initialize history again if it is not initialized
    // because history was on dfs and namenode was in safemode.
    if (!historyInitialized) {
      JobHistory.init(conf, this.localMachine, this.startTime); 
      historyLogDir = conf.get("hadoop.job.history.location");
      infoServer.setAttribute("historyLogDir", historyLogDir);
      historyFS = new Path(historyLogDir).getFileSystem(conf);
      infoServer.setAttribute("fileSys", historyFS);
    }

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);
    this.numTaskCacheLevels = conf.getInt("mapred.task.cache.levels", 
        NetworkTopology.DEFAULT_HOST_LEVEL);

    //initializes the job status store
    completedJobStatusStore = new CompletedJobStatusStore(conf,fs);
  }

  private static SimpleDateFormat getDateFormat() {
    return new SimpleDateFormat("yyyyMMddHHmm");
  }

  static boolean validateIdentifier(String id) {
    try {
      // the jobtracker id should be 'date' parseable
      getDateFormat().parse(id);
      return true;
    } catch (ParseException pe) {}
    return false;
  }

  static boolean validateJobNumber(String id) {
    try {
      // the job number should be integer parseable
      Integer.parseInt(id);
      return true;
    } catch (IllegalArgumentException pe) {}
    return false;
  }

  /**
   * Whether the JT has restarted
   */
  public boolean hasRestarted() {
    return hasRestarted;
  }

  /**
   * Whether the JT has recovered upon restart
   */
  public boolean hasRecovered() {
    return hasRecovered;
  }

  /**
   * How long the jobtracker took to recover from restart.
   */
  public long getRecoveryDuration() {
    return hasRestarted() 
           ? recoveryDuration
           : 0;
  }

  public static Class<? extends JobTrackerInstrumentation> getInstrumentationClass(Configuration conf) {
    return conf.getClass("mapred.jobtracker.instrumentation",
        JobTrackerMetricsInst.class, JobTrackerInstrumentation.class);
  }
  
  public static void setInstrumentationClass(Configuration conf, Class<? extends JobTrackerInstrumentation> t) {
    conf.setClass("mapred.jobtracker.instrumentation",
        t, JobTrackerInstrumentation.class);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr =
      conf.get("mapred.job.tracker", "localhost:8012");
    return NetUtils.createSocketAddr(jobTrackerStr);
  }

  /**
   * Run forever
   */
  public void offerService() throws InterruptedException, IOException {
    taskScheduler.start();
    
    //  Start the recovery after starting the scheduler
    recoveryManager.recover();
    
    this.expireTrackersThread = new Thread(this.expireTrackers,
                                          "expireTrackers");
    this.expireTrackersThread.start();
    this.retireJobsThread = new Thread(this.retireJobs, "retireJobs");
    this.retireJobsThread.start();
    expireLaunchingTaskThread.start();

    if (completedJobStatusStore.isActive()) {
      completedJobsStoreThread = new Thread(completedJobStatusStore,
                                            "completedjobsStore-housekeeper");
      completedJobsStoreThread.start();
    }

    // start the inter-tracker server once the jt is ready
    this.interTrackerServer.start();
    
    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");
    
    this.interTrackerServer.join();
    LOG.info("Stopped interTrackerServer");
  }

  void close() throws IOException {
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.interTrackerServer != null) {
      LOG.info("Stopping interTrackerServer");
      this.interTrackerServer.stop();
    }
    if (this.expireTrackersThread != null && this.expireTrackersThread.isAlive()) {
      LOG.info("Stopping expireTrackers");
      this.expireTrackersThread.interrupt();
      try {
        this.expireTrackersThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.retireJobsThread != null && this.retireJobsThread.isAlive()) {
      LOG.info("Stopping retirer");
      this.retireJobsThread.interrupt();
      try {
        this.retireJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (taskScheduler != null) {
      taskScheduler.terminate();
    }
    if (this.expireLaunchingTaskThread != null && this.expireLaunchingTaskThread.isAlive()) {
      LOG.info("Stopping expireLaunchingTasks");
      this.expireLaunchingTaskThread.interrupt();
      try {
        this.expireLaunchingTaskThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.completedJobsStoreThread != null &&
        this.completedJobsStoreThread.isAlive()) {
      LOG.info("Stopping completedJobsStore thread");
      this.completedJobsStoreThread.interrupt();
      try {
        this.completedJobsStoreThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    LOG.info("stopped all jobtracker services");
    return;
  }
    
  ///////////////////////////////////////////////////////
  // Maintain lookup tables; called by JobInProgress
  // and TaskInProgress
  ///////////////////////////////////////////////////////
  void createTaskEntry(TaskAttemptID taskid, String taskTracker, TaskInProgress tip) {
    LOG.info("Adding task " + 
      (tip.isCleanupAttempt(taskid) ? "(cleanup)" : "") + 
      "'"  + taskid + "' to tip " + 
      tip.getTIPId() + ", for tracker '" + taskTracker + "'");

    // taskid --> tracker
    taskidToTrackerMap.put(taskid, taskTracker);

    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToTaskMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToTaskMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);

    // taskid --> TIP
    taskidToTIPMap.put(taskid, tip);
    
    // Note this launch
    if (taskid.isMap()) {
      myInstrumentation.launchMap(taskid);
    } else {
      myInstrumentation.launchReduce(taskid);
    }
  }
    
  void removeTaskEntry(TaskAttemptID taskid) {
    // taskid --> tracker
    String tracker = taskidToTrackerMap.remove(taskid);

    // tracker --> taskid
    if (tracker != null) {
      Set<TaskAttemptID> trackerSet = trackerToTaskMap.get(tracker);
      if (trackerSet != null) {
        trackerSet.remove(taskid);
      }
    }

    // taskid --> TIP
    taskidToTIPMap.remove(taskid);
        
    LOG.debug("Removing task '" + taskid + "'");
  }
    
  /**
   * Mark a 'task' for removal later.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker the tasktracker at which the 'task' was running
   * @param taskid completed (success/failure/killed) task
   */
  void markCompletedTaskAttempt(String taskTracker, TaskAttemptID taskid) {
    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToMarkedTasksMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToMarkedTasksMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);
      
    LOG.debug("Marked '" + taskid + "' from '" + taskTracker + "'");
  }

  /**
   * Mark all 'non-running' jobs of the job for pruning.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param job the completed job
   */
  void markCompletedJob(JobInProgress job) {
    for (TaskInProgress tip : job.getSetupTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getMapTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getReduceTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
  }
    
  /**
   * Remove all 'marked' tasks running on a given {@link TaskTracker}
   * from the {@link JobTracker}'s data-structures.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker tasktracker whose 'non-running' tasks are to be purged
   */
  private void removeMarkedTasks(String taskTracker) {
    // Purge all the 'marked' tasks which were running at taskTracker
    Set<TaskAttemptID> markedTaskSet = 
      trackerToMarkedTasksMap.get(taskTracker);
    if (markedTaskSet != null) {
      for (TaskAttemptID taskid : markedTaskSet) {
        removeTaskEntry(taskid);
        LOG.info("Removed completed task '" + taskid + "' from '" + 
                 taskTracker + "'");
      }
      // Clear
      trackerToMarkedTasksMap.remove(taskTracker);
    }
  }
    
  /**
   * Call {@link #removeTaskEntry(String)} for each of the
   * job's tasks.
   * When the JobTracker is retiring the long-completed
   * job, either because it has outlived {@link #RETIRE_JOB_INTERVAL}
   * or the limit of {@link #MAX_COMPLETE_USER_JOBS_IN_MEMORY} jobs 
   * has been reached, we can afford to nuke all it's tasks; a little
   * unsafe, but practically feasible. 
   * 
   * @param job the job about to be 'retired'
   */
  synchronized private void removeJobTasks(JobInProgress job) { 
    for (TaskInProgress tip : job.getMapTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        removeTaskEntry(taskStatus.getTaskID());
      }
    }
    for (TaskInProgress tip : job.getReduceTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        removeTaskEntry(taskStatus.getTaskID());
      }
    }
  }
    
  /**
   * Safe clean-up all data structures at the end of the 
   * job (success/failure/killed).
   * Here we also ensure that for a given user we maintain 
   * information for only MAX_COMPLETE_USER_JOBS_IN_MEMORY jobs 
   * on the JobTracker.
   *  
   * @param job completed job.
   */
  synchronized void finalizeJob(JobInProgress job) {
    // Mark the 'non-running' tasks for pruning
    markCompletedJob(job);

    //persists the job info in DFS
    completedJobStatusStore.store(job);
    
    JobEndNotifier.registerNotification(job.getJobConf(), job.getStatus());

    // start the merge of log files
    JobID id = job.getStatus().getJobID();
    try {
      JobHistory.JobInfo.finalizeRecovery(id, job.getJobConf());
    } catch (IOException ioe) {
      LOG.info("Failed to finalize the log file recovery for job " + id, ioe);
    }

    long now = System.currentTimeMillis();
    
    // Purge oldest jobs and keep at-most MAX_COMPLETE_USER_JOBS_IN_MEMORY jobs of a given user
    // in memory; information about the purged jobs is available via
    // JobHistory.
    synchronized (jobs) {
      synchronized (taskScheduler) {
        synchronized (userToJobsMap) {
          String jobUser = job.getProfile().getUser();
          if (!userToJobsMap.containsKey(jobUser)) {
            userToJobsMap.put(jobUser, 
                              new ArrayList<JobInProgress>());
          }
          ArrayList<JobInProgress> userJobs = 
            userToJobsMap.get(jobUser);
          synchronized (userJobs) {
            // Add the currently completed 'job'
            userJobs.add(job);

            // Check if we need to retire some jobs of this user
            while (userJobs.size() > 
                   MAX_COMPLETE_USER_JOBS_IN_MEMORY) {
              JobInProgress rjob = userJobs.get(0);
                
              // Do not delete 'current'
              // finished job just yet.
              if (rjob == job) {
                break;
              }

              // do not retire jobs that finished in the very recent past.
              if (rjob.getFinishTime() + MIN_TIME_BEFORE_RETIRE > now) {
                break;
              }
                
              // Cleanup all datastructures
              int rjobRunState = 
                rjob.getStatus().getRunState();
              if (rjobRunState == JobStatus.SUCCEEDED || 
                  rjobRunState == JobStatus.FAILED ||
                  rjobRunState == JobStatus.KILLED) {
                // Ok, this call to removeTaskEntries
                // is dangerous is some very very obscure
                // cases; e.g. when rjob completed, hit
                // MAX_COMPLETE_USER_JOBS_IN_MEMORY job
                // limit and yet some task (taskid)
                // wasn't complete!
                removeJobTasks(rjob);
                  
                userJobs.remove(0);
                jobs.remove(rjob.getProfile().getJobID());
                for (JobInProgressListener listener : jobInProgressListeners) {
                  listener.jobRemoved(rjob);
                }
                  
                LOG.info("Retired job with id: '" + 
                         rjob.getProfile().getJobID() + "' of user: '" +
                         jobUser + "'");
              } else {
                // Do not remove jobs that aren't complete.
                // Stop here, and let the next pass take
                // care of purging jobs.
                break;
              }
            }
          }
          if (userJobs.isEmpty()) {
            userToJobsMap.remove(jobUser);
          }
        }
      }
    }
  }

  ///////////////////////////////////////////////////////
  // Accessors for objects that want info on jobs, tasks,
  // trackers, etc.
  ///////////////////////////////////////////////////////
  public int getTotalSubmissions() {
    return totalSubmissions;
  }
  public String getJobTrackerMachine() {
    return localMachine;
  }
  
  /**
   * Get the unique identifier (ie. timestamp) of this job tracker start.
   * @return a string with a unique identifier
   */
  public String getTrackerIdentifier() {
    return trackerIdentifier;
  }

  public int getTrackerPort() {
    return port;
  }
  public int getInfoPort() {
    return infoPort;
  }
  public long getStartTime() {
    return startTime;
  }
  public Vector<JobInProgress> runningJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.RUNNING) {
        v.add(jip);
      }
    }
    return v;
  }
  /**
   * Version that is called from a timer thread, and therefore needs to be
   * careful to synchronize.
   */
  public synchronized List<JobInProgress> getRunningJobs() {
    synchronized (jobs) {
      return runningJobs();
    }
  }
  public Vector<JobInProgress> failedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if ((status.getRunState() == JobStatus.FAILED)
          || (status.getRunState() == JobStatus.KILLED)) {
        v.add(jip);
      }
    }
    return v;
  }
  public Vector<JobInProgress> completedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.SUCCEEDED) {
        v.add(jip);
      }
    }
    return v;
  }
  public Collection<TaskTrackerStatus> taskTrackers() {
    synchronized (taskTrackers) {
      return taskTrackers.values();
    }
  }
  public TaskTrackerStatus getTaskTracker(String trackerID) {
    synchronized (taskTrackers) {
      return taskTrackers.get(trackerID);
    }
  }

  /**
   * Adds a new node to the jobtracker. It involves adding it to the expiry
   * thread and adding it for resolution
   * @param status Task Tracker's status
   * @param resolveInline Should the resolution happen inline?
   */
  private void addNewTracker(TaskTrackerStatus status) {
    trackerExpiryQueue.add(status);
    //  Register the tracker if its not registered
    if (getNode(status.getTrackerName()) == null) {
      // Making the network location resolution inline .. 
      resolveAndAddToTopology(status.getHost());
    }
  }

  public Node resolveAndAddToTopology(String name) {
    List <String> tmpList = new ArrayList<String>(1);
    tmpList.add(name);
    List <String> rNameList = dnsToSwitchMapping.resolve(tmpList);
    String rName = rNameList.get(0);
    String networkLoc = NodeBase.normalize(rName);
    return addHostToNodeMapping(name, networkLoc);
  }
  
  private Node addHostToNodeMapping(String host, String networkLoc) {
    Node node;
    if ((node = clusterMap.getNode(networkLoc+"/"+host)) == null) {
      node = new NodeBase(host, networkLoc);
      clusterMap.add(node);
      if (node.getLevel() < getNumTaskCacheLevels()) {
        LOG.fatal("Got a host whose level is: " + node.getLevel() + "." 
                  + " Should get at least a level of value: " 
                  + getNumTaskCacheLevels());
        try {
          stopTracker();
        } catch (IOException ie) {
          LOG.warn("Exception encountered during shutdown: " 
                   + StringUtils.stringifyException(ie));
          System.exit(-1);
        }
      }
      hostnameToNodeMap.put(host, node);
      // Make an entry for the node at the max level in the cache
      nodesAtMaxLevel.add(getParentNode(node, getNumTaskCacheLevels() - 1));
    }
    return node;
  }

  /**
   * Returns a collection of nodes at the max level
   */
  public Collection<Node> getNodesAtMaxLevel() {
    return nodesAtMaxLevel;
  }

  public static Node getParentNode(Node node, int level) {
    for (int i = 0; i < level; ++i) {
      node = node.getParent();
    }
    return node;
  }

  /**
   * Return the Node in the network topology that corresponds to the hostname
   */
  public Node getNode(String name) {
    return hostnameToNodeMap.get(name);
  }
  public int getNumTaskCacheLevels() {
    return numTaskCacheLevels;
  }
  public int getNumResolvedTaskTrackers() {
    return numResolved;
  }
  
  public int getNumberOfUniqueHosts() {
    return uniqueHostsMap.size();
  }
  
  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }
  
  // Update the listeners about the job
  private void updateJobInProgressListeners(JobChangeEvent event) {
    for (JobInProgressListener listener : jobInProgressListeners) {
      listener.jobUpdated(event);
    }
  }
  
  /**
   * Return the {@link QueueManager} associated with the JobTracker.
   */
  public QueueManager getQueueManager() {
    return queueManager;
  }
  
  ////////////////////////////////////////////////////
  // InterTrackerProtocol
  ////////////////////////////////////////////////////
  
  public String getBuildVersion() throws IOException{
    return VersionInfo.getBuildVersion();
  }

  /**
   * The periodic heartbeat mechanism between the {@link TaskTracker} and
   * the {@link JobTracker}.
   * 
   * The {@link JobTracker} processes the status information sent by the 
   * {@link TaskTracker} and responds with instructions to start/stop 
   * tasks or jobs, and also 'reset' instructions during contingencies. 
   */
  public synchronized HeartbeatResponse heartbeat(TaskTrackerStatus status, 
                                                  boolean initialContact, boolean acceptNewTasks, short responseId) 
    throws IOException {
    LOG.debug("Got heartbeat from: " + status.getTrackerName() + 
              " (initialContact: " + initialContact + 
              " acceptNewTasks: " + acceptNewTasks + ")" +
              " with responseId: " + responseId);

    // Make sure heartbeat is from a tasktracker allowed by the jobtracker.
    if (!acceptTaskTracker(status)) {
      throw new DisallowedTaskTrackerException(status);
    }

    // First check if the last heartbeat response got through
    String trackerName = status.getTrackerName();
    
    HeartbeatResponse prevHeartbeatResponse =
      trackerToHeartbeatResponseMap.get(trackerName);
    boolean addRestartInfo = false;

    if (initialContact != true) {
      // If this isn't the 'initial contact' from the tasktracker,
      // there is something seriously wrong if the JobTracker has
      // no record of the 'previous heartbeat'; if so, ask the 
      // tasktracker to re-initialize itself.
      if (prevHeartbeatResponse == null) {
        // This is the first heartbeat from the old tracker to the newly 
        // started JobTracker
        if (hasRestarted()) {
          addRestartInfo = true;
        } else {
          // Jobtracker might have restarted but no recovery is needed
          LOG.warn("Serious problem, cannot find record of 'previous' " +
                   "heartbeat for '" + trackerName + 
                   "'; reinitializing the tasktracker");
          return new HeartbeatResponse(responseId, 
              new TaskTrackerAction[] {new ReinitTrackerAction()});
        }

      } else {
                
        // It is completely safe to not process a 'duplicate' heartbeat from a 
        // {@link TaskTracker} since it resends the heartbeat when rpcs are 
        // lost see {@link TaskTracker.transmitHeartbeat()};
        // acknowledge it by re-sending the previous response to let the 
        // {@link TaskTracker} go forward. 
        if (prevHeartbeatResponse.getResponseId() != responseId) {
          LOG.info("Ignoring 'duplicate' heartbeat from '" + 
              trackerName + "'; resending the previous 'lost' response");
          return prevHeartbeatResponse;
        }
      }
    }
      
    // Process this heartbeat 
    short newResponseId = (short)(responseId + 1);
    if (!processHeartbeat(status, initialContact)) {
      if (prevHeartbeatResponse != null) {
        trackerToHeartbeatResponseMap.remove(trackerName);
      }

      return new HeartbeatResponse(newResponseId, 
                                   new TaskTrackerAction[] {new ReinitTrackerAction()});
    }
      
    // Initialize the response to be sent for the heartbeat
    HeartbeatResponse response = new HeartbeatResponse(newResponseId, null);
    List<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();
      
    // Check for new tasks to be executed on the tasktracker
    if (acceptNewTasks) {
      TaskTrackerStatus taskTrackerStatus = getTaskTracker(trackerName);
      if (taskTrackerStatus == null) {
        LOG.warn("Unknown task tracker polling; ignoring: " + trackerName);
      } else {
        List<Task> tasks = getSetupAndCleanupTasks(taskTrackerStatus);
        if (tasks == null ) {
          tasks = taskScheduler.assignTasks(taskTrackerStatus);
        }
        if (tasks != null) {
          for (Task task : tasks) {
            expireLaunchingTasks.addNewTask(task.getTaskID());
            LOG.debug(trackerName + " -> LaunchTask: " + task.getTaskID());
            actions.add(new LaunchTaskAction(task));
          }
        }
      }
    }
      
    // Check for tasks to be killed
    List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
    if (killTasksList != null) {
      actions.addAll(killTasksList);
    }
     
    // Check for tasks whose outputs can be saved
    List<TaskTrackerAction> commitTasksList = getTasksToSave(status);
    if (commitTasksList != null) {
      actions.addAll(commitTasksList);
    }

    // calculate next heartbeat interval and put in heartbeat response
    int nextInterval = getNextHeartbeatInterval();
    response.setHeartbeatInterval(nextInterval);
    response.setActions(
                        actions.toArray(new TaskTrackerAction[actions.size()]));
    
    // check if the restart info is req
    if (addRestartInfo) {
      response.setLastKnownIndices(lastSeenEventMapOnRestart);
    }
        
    // Update the trackerToHeartbeatResponseMap
    trackerToHeartbeatResponseMap.put(trackerName, response);

    // Done processing the hearbeat, now remove 'marked' tasks
    removeMarkedTasks(trackerName);
        
    return response;
  }
  
  /**
   * Calculates next heartbeat interval using cluster size.
   * Heartbeat interval is incremented 1second for every 50 nodes. 
   * @return next heartbeat interval.
   */
  public int getNextHeartbeatInterval() {
    // get the no of task trackers
    int clusterSize = getClusterStatus().getTaskTrackers();
    int heartbeatInterval =  Math.max(
                                1000 * (clusterSize / CLUSTER_INCREMENT + 1),
                                HEARTBEAT_INTERVAL_MIN) ;
    return heartbeatInterval;
  }

  /**
   * Return if the specified tasktracker is in the hosts list, 
   * if one was configured.  If none was configured, then this 
   * returns true.
   */
  private boolean inHostsList(TaskTrackerStatus status) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || hostsList.contains(status.getHost()));
  }

  /**
   * Return if the specified tasktracker is in the exclude list.
   */
  private boolean inExcludedHostsList(TaskTrackerStatus status) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return excludeList.contains(status.getHost());
  }

  /**
   * Returns true if the tasktracker is in the hosts list and 
   * not in the exclude list. 
   */
  private boolean acceptTaskTracker(TaskTrackerStatus status) {
    return (inHostsList(status) && !inExcludedHostsList(status));
  }
    
  /**
   * Update the last recorded status for the given task tracker.
   * It assumes that the taskTrackers are locked on entry.
   * @param trackerName The name of the tracker
   * @param status The new status for the task tracker
   * @return Was an old status found?
   */
  private boolean updateTaskTrackerStatus(String trackerName,
                                          TaskTrackerStatus status) {
    TaskTrackerStatus oldStatus = taskTrackers.get(trackerName);
    if (oldStatus != null) {
      totalMaps -= oldStatus.countMapTasks();
      totalReduces -= oldStatus.countReduceTasks();
      totalMapTaskCapacity -= oldStatus.getMaxMapTasks();
      totalReduceTaskCapacity -= oldStatus.getMaxReduceTasks();
      if (status == null) {
        taskTrackers.remove(trackerName);
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(oldStatus.getHost());
        numTaskTrackersInHost --;
        if (numTaskTrackersInHost > 0)  {
          uniqueHostsMap.put(oldStatus.getHost(), numTaskTrackersInHost);
        }
        else {
          uniqueHostsMap.remove(oldStatus.getHost());
        }
      }
    }
    if (status != null) {
      totalMaps += status.countMapTasks();
      totalReduces += status.countReduceTasks();
      totalMapTaskCapacity += status.getMaxMapTasks();
      totalReduceTaskCapacity += status.getMaxReduceTasks();
      boolean alreadyPresent = false;
      if (taskTrackers.containsKey(trackerName)) {
        alreadyPresent = true;
      }
      taskTrackers.put(trackerName, status);

      if (!alreadyPresent)  {
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(status.getHost());
        if (numTaskTrackersInHost == null) {
          numTaskTrackersInHost = 0;
        }
        numTaskTrackersInHost ++;
        uniqueHostsMap.put(status.getHost(), numTaskTrackersInHost);
      }
    }
    return oldStatus != null;
  }
    
  /**
   * Process incoming heartbeat messages from the task trackers.
   */
  private synchronized boolean processHeartbeat(
                                                TaskTrackerStatus trackerStatus, boolean initialContact) {
    String trackerName = trackerStatus.getTrackerName();
    trackerStatus.setLastSeen(System.currentTimeMillis());

    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                     trackerStatus);
        if (initialContact) {
          // If it's first contact, then clear out 
          // any state hanging around
          if (seenBefore) {
            lostTaskTracker(trackerName);
          }
        } else {
          // If not first contact, there should be some record of the tracker
          if (!seenBefore) {
            LOG.warn("Status from unknown Tracker : " + trackerName);
            updateTaskTrackerStatus(trackerName, null);
            return false;
          }
        }

        if (initialContact) {
          addNewTracker(trackerStatus);
        }
      }
    }

    updateTaskStatuses(trackerStatus);
    
    return true;
  }

  /**
   * A tracker wants to know if any of its Tasks have been
   * closed (because the job completed, whether successfully or not)
   */
  private synchronized List<TaskTrackerAction> getTasksToKill(
                                                              String taskTracker) {
    
    Set<TaskAttemptID> taskIds = trackerToTaskMap.get(taskTracker);
    if (taskIds != null) {
      List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
      Set<JobID> killJobIds = new TreeSet<JobID>(); 
      for (TaskAttemptID killTaskId : taskIds) {
        TaskInProgress tip = taskidToTIPMap.get(killTaskId);
        if (tip.shouldClose(killTaskId)) {
          // 
          // This is how the JobTracker ends a task at the TaskTracker.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (tip.getJob().getStatus().getRunState() == JobStatus.RUNNING ||
              tip.getJob().getStatus().getRunState() == JobStatus.PREP) {
            killList.add(new KillTaskAction(killTaskId));
            LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
          } else {
            JobID killJobId = tip.getJob().getStatus().getJobID(); 
            killJobIds.add(killJobId);
          }
        }
      }
            
      for (JobID killJobId : killJobIds) {
        killList.add(new KillJobAction(killJobId));
        LOG.debug(taskTracker + " -> KillJobAction: " + killJobId);
      }

      return killList;
    }
    return null;
  }

  /**
   * A tracker wants to know if any of its Tasks can be committed 
   */
  private synchronized List<TaskTrackerAction> getTasksToSave(
                                                 TaskTrackerStatus tts) {
    List<TaskStatus> taskStatuses = tts.getTaskReports();
    if (taskStatuses != null) {
      List<TaskTrackerAction> saveList = new ArrayList<TaskTrackerAction>();
      for (TaskStatus taskStatus : taskStatuses) {
        if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING) {
          TaskAttemptID taskId = taskStatus.getTaskID();
          TaskInProgress tip = taskidToTIPMap.get(taskId);
          if (tip.shouldCommit(taskId)) {
            saveList.add(new CommitTaskAction(taskId));
            LOG.debug(tts.getTrackerName() + 
                      " -> CommitTaskAction: " + taskId);
          }
        }
      }
      return saveList;
    }
    return null;
  }
  
  // returns cleanup tasks first, then setup tasks.
  private synchronized List<Task> getSetupAndCleanupTasks(
    TaskTrackerStatus taskTracker) throws IOException {
    int maxMapTasks = taskTracker.getMaxMapTasks();
    int maxReduceTasks = taskTracker.getMaxReduceTasks();
    int numMaps = taskTracker.countMapTasks();
    int numReduces = taskTracker.countReduceTasks();
    int numTaskTrackers = getClusterStatus().getTaskTrackers();
    int numUniqueHosts = getNumberOfUniqueHosts();

    Task t = null;
    synchronized (jobs) {
      if (numMaps < maxMapTasks) {
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                  numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
      if (numReduces < maxReduceTasks) {
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
    }
    return null;
  }

  /**
   * Grab the local fs name
   */
  public synchronized String getFilesystemName() throws IOException {
    if (fs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return fs.getUri().toString();
  }


  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) throws IOException {
    LOG.warn("Report from " + taskTracker + ": " + errorMessage);        
  }

  /**
   * Remove the job_ from jobids to get the unique string.
   */
  static String getJobUniqueString(String jobid) {
    return jobid.substring(4);
  }

  ////////////////////////////////////////////////////
  // JobSubmissionProtocol
  ////////////////////////////////////////////////////

  /**
   * Allocates a new JobId string.
   */
  public synchronized JobID getNewJobId() throws IOException {
    return new JobID(getTrackerIdentifier(), nextJobId++);
  }

  /**
   * JobTracker.submitJob() kicks off a new job.  
   *
   * Create a 'JobInProgress' object, which contains both JobProfile
   * and JobStatus.  Those two sub-objects are sometimes shipped outside
   * of the JobTracker.  But JobInProgress adds info that's useful for
   * the JobTracker alone.
   */
  public synchronized JobStatus submitJob(JobID jobId) throws IOException {
    if(jobs.containsKey(jobId)) {
      //job already running, don't start twice
      return jobs.get(jobId).getStatus();
    }
    
    JobInProgress job = new JobInProgress(jobId, this, this.conf);

    // check for access
    checkAccess(job, QueueManager.QueueOperation.SUBMIT_JOB);

   return addJob(jobId, job); 
  }

  /**
   * Adds a job to the jobtracker. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * @param jobId The id for the job submitted which needs to be added
   */
  private synchronized JobStatus addJob(JobID jobId, JobInProgress job) 
  throws IOException {
    totalSubmissions++;

    synchronized (jobs) {
      synchronized (taskScheduler) {
        jobs.put(job.getProfile().getJobID(), job);
        for (JobInProgressListener listener : jobInProgressListeners) {
          listener.jobAdded(job);
        }
      }
    }
    myInstrumentation.submitJob(this.conf, jobId);
    return job.getStatus();
  }

  // Check whether the specified operation can be performed
  // related to the job. If ownerAllowed is true, then an owner
  // of the job can perform the operation irrespective of
  // access control.
  private void checkAccess(JobInProgress job, 
                                QueueManager.QueueOperation oper) 
                                  throws IOException {
    // get the user group info
    UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();

    // get the queue
    String queue = job.getProfile().getQueueName();
    if (!queueManager.hasAccess(queue, job, oper, ugi)) {
      throw new AccessControlException("User " 
                            + ugi.getUserName() 
                            + " cannot perform "
                            + "operation " + oper + " on queue " + queue);
    }
  }

  public synchronized ClusterStatus getClusterStatus() {
    synchronized (taskTrackers) {
      return new ClusterStatus(taskTrackers.size(),
                               totalMaps,
                               totalReduces,
                               totalMapTaskCapacity,
                               totalReduceTaskCapacity, 
                               state);          
    }
  }
    
  public synchronized void killJob(JobID jobid) throws IOException {
    if (null == jobid) {
      LOG.info("Null jobid object sent to JobTracker.killJob()");
      return;
    }
    
    JobInProgress job = jobs.get(jobid);
    
    if (null == job) {
      LOG.info("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }
        
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    checkAccess(job, QueueManager.QueueOperation.ADMINISTER_JOBS);
    job.kill();
    
    // Inform the listeners if the job is killed
    // Note : 
    //   If the job is killed in the PREP state then the listeners will be 
    //   invoked
    //   If the job is killed in the RUNNING state then cleanup tasks will be 
    //   launched and the updateTaskStatuses() will take care of it
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()
        && newStatus.getRunState() == JobStatus.KILLED) {
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
            newStatus);
      updateJobInProgressListeners(event);
    }
  }

  /**
   * Set the priority of a job
   * @param jobid id of the job
   * @param priority new priority of the job
   */
  public synchronized void setJobPriority(JobID jobid, 
                                              String priority)
                                                throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (null == job) {
        LOG.info("setJobPriority(): JobId " + jobid.toString()
            + " is not a valid job");
        return;
    }
    checkAccess(job, QueueManager.QueueOperation.ADMINISTER_JOBS);
    JobPriority newPriority = JobPriority.valueOf(priority);
    setJobPriority(jobid, newPriority);
  }
                           
  public synchronized JobProfile getJobProfile(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getProfile();
    } else {
      return completedJobStatusStore.readJobProfile(jobid);
    }
  }
  public synchronized JobStatus getJobStatus(JobID jobid) {
    if (null == jobid) {
      LOG.warn("JobTracker.getJobStatus() cannot get status for null jobid");
      return null;
    }
    
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getStatus();
    } else {
      return completedJobStatusStore.readJobStatus(jobid);
    }
  }
  public synchronized Counters getJobCounters(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getCounters();
    } else {
      return completedJobStatusStore.readCounters(jobid);
    }
  }
  public synchronized TaskReport[] getMapTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null) {
      return new TaskReport[0];
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeMapTasks =
        job.reportTasksInProgress(true, true);
      for (Iterator it = completeMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteMapTasks =
        job.reportTasksInProgress(true, false);
      for (Iterator it = incompleteMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  public synchronized TaskReport[] getReduceTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null) {
      return new TaskReport[0];
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector completeReduceTasks = job.reportTasksInProgress(false, true);
      for (Iterator it = completeReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector incompleteReduceTasks = job.reportTasksInProgress(false, false);
      for (Iterator it = incompleteReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  public synchronized TaskReport[] getCleanupTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null) {
      return new TaskReport[0];
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportCleanupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportCleanupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  
  }
  
  public synchronized TaskReport[] getSetupTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null) {
      return new TaskReport[0];
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportSetupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportSetupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }
  
  TaskCompletionEvent[] EMPTY_EVENTS = new TaskCompletionEvent[0];
  
  /* 
   * Returns a list of TaskCompletionEvent for the given job, 
   * starting from fromEventId.
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getTaskCompletionEvents(java.lang.String, int, int)
   */
  public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
      JobID jobid, int fromEventId, int maxEvents) throws IOException{
    TaskCompletionEvent[] events = EMPTY_EVENTS;

    JobInProgress job = this.jobs.get(jobid);
    if (null != job) {
      if (job.inited()) {
        events = job.getTaskCompletionEvents(fromEventId, maxEvents);
      }
    }
    else {
      events = completedJobStatusStore.readJobTaskCompletionEvents(jobid, fromEventId, maxEvents);
    }
    return events;
  }

  /**
   * Get the diagnostics for a given task
   * @param taskId the id of the task
   * @return an array of the diagnostic messages
   */
  public synchronized String[] getTaskDiagnostics(TaskAttemptID taskId)  
    throws IOException {
    
    JobID jobId = taskId.getJobID();
    TaskID tipId = taskId.getTaskID();
    JobInProgress job = jobs.get(jobId);
    if (job == null) {
      throw new IllegalArgumentException("Job " + jobId + " not found.");
    }
    TaskInProgress tip = job.getTaskInProgress(tipId);
    if (tip == null) {
      throw new IllegalArgumentException("TIP " + tipId + " not found.");
    }
    List<String> taskDiagnosticInfo = tip.getDiagnosticInfo(taskId);
    return ((taskDiagnosticInfo == null) ? null 
            : taskDiagnosticInfo.toArray(new String[0]));
  }
    
  /** Get all the TaskStatuses from the tipid. */
  TaskStatus[] getTaskStatuses(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? new TaskStatus[0] 
            : tip.getTaskStatuses());
  }

  /** Returns the TaskStatus for a particular taskid. */
  TaskStatus getTaskStatus(TaskAttemptID taskid) {
    TaskInProgress tip = getTip(taskid.getTaskID());
    return (tip == null ? null 
            : tip.getTaskStatus(taskid));
  }
    
  /**
   * Returns the counters for the specified task in progress.
   */
  Counters getTipCounters(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? null : tip.getCounters());
  }

  /**
   * Returns the configured task scheduler for this job tracker.
   * @return the configured task scheduler
   */
  TaskScheduler getTaskScheduler() {
    return taskScheduler;
  }
  
  /**
   * Returns specified TaskInProgress, or null.
   */
  public TaskInProgress getTip(TaskID tipid) {
    JobInProgress job = jobs.get(tipid.getJobID());
    return (job == null ? null : job.getTaskInProgress(tipid));
  }
    
  /** Mark a Task to be killed */
  public synchronized boolean killTask(TaskAttemptID taskid, boolean shouldFail) throws IOException{
    TaskInProgress tip = taskidToTIPMap.get(taskid);
    if(tip != null) {
      checkAccess(tip.getJob(), QueueManager.QueueOperation.ADMINISTER_JOBS);
      return tip.killTask(taskid, shouldFail);
    }
    else {
      LOG.info("Kill task attempt failed since task " + taskid + " was not found");
      return false;
    }
  }
  
  /**
   * Get tracker name for a given task id.
   * @param taskId the name of the task
   * @return The name of the task tracker
   */
  public synchronized String getAssignedTracker(TaskAttemptID taskId) {
    return taskidToTrackerMap.get(taskId);
  }
    
  public JobStatus[] jobsToComplete() {
    return getJobStatus(jobs.values(), true);
  } 
  
  public JobStatus[] getAllJobs() {
    return getJobStatus(jobs.values(),false);
  }
    
  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }
  
  ///////////////////////////////////////////////////////////////
  // JobTracker methods
  ///////////////////////////////////////////////////////////////
  public JobInProgress getJob(JobID jobid) {
    return jobs.get(jobid);
  }

  /**
   * Change the run-time priority of the given job.
   * @param jobId job id
   * @param priority new {@link JobPriority} for the job
   */
  synchronized void setJobPriority(JobID jobId, JobPriority priority) {
    JobInProgress job = jobs.get(jobId);
    if (job != null) {
      synchronized (taskScheduler) {
        JobStatus oldStatus = (JobStatus)job.getStatus().clone();
        job.setPriority(priority);
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.PRIORITY_CHANGED, oldStatus, 
                                   newStatus);
        updateJobInProgressListeners(event);
      }
    } else {
      LOG.warn("Trying to change the priority of an unknown job: " + jobId);
    }
  }
  
  ////////////////////////////////////////////////////
  // Methods to track all the TaskTrackers
  ////////////////////////////////////////////////////
  /**
   * Accept and process a new TaskTracker profile.  We might
   * have known about the TaskTracker previously, or it might
   * be brand-new.  All task-tracker structures have already
   * been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void updateTaskStatuses(TaskTrackerStatus status) {
    String trackerName = status.getTrackerName();
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(trackerName);
      TaskAttemptID taskId = report.getTaskID();
      TaskInProgress tip = taskidToTIPMap.get(taskId);
      // Check if the tip is known to the jobtracker. In case of a restarted
      // jt, some tasks might join in later
      if (tip != null || hasRestarted()) {
        JobInProgress job = getJob(taskId.getJobID());
        if (tip == null) {
          tip = job.getTaskInProgress(taskId.getTaskID());
          job.addRunningTaskToTIP(tip, taskId, status, false);
        }
        expireLaunchingTasks.removeTask(taskId);
        
        // Update the job and inform the listeners if necessary
        JobStatus prevStatus = (JobStatus)job.getStatus().clone();
        job.updateTaskStatus(tip, report, myInstrumentation);
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        
        // Update the listeners if an incomplete job completes
        if (prevStatus.getRunState() != newStatus.getRunState()) {
          JobStatusChangeEvent event = 
            new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, 
                                     prevStatus, newStatus);
          updateJobInProgressListeners(event);
        }
      } else {
        LOG.info("Serious problem.  While updating status, cannot find taskid " 
                 + report.getTaskID());
      }
      
      // Process 'failed fetch' notifications 
      List<TaskAttemptID> failedFetchMaps = report.getFetchFailedMaps();
      if (failedFetchMaps != null) {
        for (TaskAttemptID mapTaskId : failedFetchMaps) {
          TaskInProgress failedFetchMap = taskidToTIPMap.get(mapTaskId);
          
          if (failedFetchMap != null) {
            // Gather information about the map which has to be failed, if need be
            String failedFetchTrackerName = getAssignedTracker(mapTaskId);
            if (failedFetchTrackerName == null) {
              failedFetchTrackerName = "Lost task tracker";
            }
            failedFetchMap.getJob().fetchFailureNotification(failedFetchMap, 
                                                             mapTaskId, 
                                                             failedFetchTrackerName, 
                                                             myInstrumentation);
          }
        }
      }
    }
  }

  /**
   * We lost the task tracker!  All task-tracker structures have 
   * already been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void lostTaskTracker(String trackerName) {
    LOG.info("Lost tracker '" + trackerName + "'");
    Set<TaskAttemptID> lostTasks = trackerToTaskMap.get(trackerName);
    trackerToTaskMap.remove(trackerName);

    if (lostTasks != null) {
      // List of jobs which had any of their tasks fail on this tracker
      Set<JobInProgress> jobsWithFailures = new HashSet<JobInProgress>(); 
      for (TaskAttemptID taskId : lostTasks) {
        TaskInProgress tip = taskidToTIPMap.get(taskId);
        JobInProgress job = tip.getJob();

        // Completed reduce tasks never need to be failed, because 
        // their outputs go to dfs
        // And completed maps with zero reducers of the job 
        // never need to be failed. 
        if (!tip.isComplete() || 
            (tip.isMapTask() && !tip.isJobSetupTask() && 
             job.desiredReduces() != 0)) {
          // if the job is done, we don't want to change anything
          if (job.getStatus().getRunState() == JobStatus.RUNNING ||
              job.getStatus().getRunState() == JobStatus.PREP) {
            job.failedTask(tip, taskId, ("Lost task tracker: " + trackerName), 
                           (tip.isMapTask() ? 
                               TaskStatus.Phase.MAP : 
                               TaskStatus.Phase.REDUCE), 
                            tip.isRunningTask(taskId) ? 
                              TaskStatus.State.KILLED_UNCLEAN : 
                              TaskStatus.State.KILLED,
                            trackerName, myInstrumentation);
            jobsWithFailures.add(job);
          }
        } else {
          // Completed 'reduce' task and completed 'maps' with zero 
          // reducers of the job, not failed;
          // only removed from data-structures.
          markCompletedTaskAttempt(trackerName, taskId);
        }
      }
      
      // Penalize this tracker for each of the jobs which   
      // had any tasks running on it when it was 'lost' 
      for (JobInProgress job : jobsWithFailures) {
        job.addTrackerTaskFailure(trackerName);
      }
      
      // Purge 'marked' tasks, needs to be done  
      // here to prevent hanging references!
      removeMarkedTasks(trackerName);
    }
  }
  

  /**
   * Get the localized job file path on the job trackers local file system
   * @param jobId id of the job
   * @return the path of the job conf file on the local file system
   */
  public static String getLocalJobFilePath(JobID jobId){
    return JobHistory.JobInfo.getLocalJobFilePath(jobId);
  }
  ////////////////////////////////////////////////////////////
  // main()
  ////////////////////////////////////////////////////////////

  /**
   * Start the JobTracker process.  This is used only for debugging.  As a rule,
   * JobTracker should be run as part of the DFS Namenode process.
   */
  public static void main(String argv[]
                          ) throws IOException, InterruptedException {
    StringUtils.startupShutdownMessage(JobTracker.class, argv, LOG);
    if (argv.length != 0) {
      System.out.println("usage: JobTracker");
      System.exit(-1);
    }
      
    try {
      JobTracker tracker = startTracker(new JobConf());
      tracker.offerService();
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  @Override
  public JobQueueInfo[] getQueues() throws IOException {
    return queueManager.getJobQueueInfos();
  }


  @Override
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return queueManager.getJobQueueInfo(queue);
  }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) throws IOException {
    Collection<JobInProgress> jips = taskScheduler.getJobs(queue);
    return getJobStatus(jips,false);
  }
  
  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if(jips == null || jips.isEmpty()) {
      return new JobStatus[]{};
    }
    ArrayList<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for(JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();
      status.setStartTime(jip.getStartTime());
      status.setUsername(jip.getProfile().getUser());
      if(toComplete) {
        if(status.getRunState() == JobStatus.RUNNING || 
            status.getRunState() == JobStatus.PREP) {
          jobStatusList.add(status);
        }
      }else {
        jobStatusList.add(status);
      }
    }
    return (JobStatus[]) jobStatusList.toArray(
        new JobStatus[jobStatusList.size()]);
  }

  /**
   * Returns the confgiured maximum number of tasks for a single job
   */
  int getMaxTasksPerJob() {
    return conf.getInt("mapred.jobtracker.maxtasks.per.job", -1);
  }
  
}
