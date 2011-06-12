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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.mapred.SimulatorJobInProgress;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.TokenStorage;

/**
 * {@link SimulatorJobTracker} extends {@link JobTracker}. It implements the
 * {@link InterTrackerProtocol} protocols.
 */
@SuppressWarnings("deprecation")
public class SimulatorJobTracker extends JobTracker {
  // A queue for cleaning up jobs from the memory. The length of this queue
  // is always less than the constant specified by JOBS_IN_MUMAK_MEMORY.
  private LinkedList<JobID> cleanupQueue;

  // The simulatorClock maintains the current simulation time 
  // and should always be synchronized with the time maintained by the engine.
  private static SimulatorClock clock = null;

  public static final Log LOG = LogFactory.getLog(SimulatorJobTracker.class);

  // This constant is used to specify how many jobs should be maintained in the
  // memory of the mumak simulator. 
  private static final int JOBS_IN_MUMAK_MEMORY = 50;

  // The SimulatorEngine data structure is engine that drives the simulator.
  private static SimulatorEngine engine = null;

  private static synchronized void resetEngineClock(SimulatorEngine engine, SimulatorClock clock) {
    SimulatorJobTracker.engine = engine;
    SimulatorJobTracker.clock = clock;
  }
  
  /**
   * In addition to the standard JobConf object, the constructor for SimulatorJobTracker requires a 
   * start time for simulation and a reference to the SimulatorEngine object. The clock of the
   * JobTracker is set with this start time.
   * @param conf the starting configuration of the SimulatorJobTracker.
   * @param clock the SimulatorClock object that we use to maintain simulator time.
   * @param simulatorEngine the simulatorEngine that is running the simulation.
   */
  SimulatorJobTracker(JobConf conf, SimulatorClock clock, 
		              SimulatorEngine simulatorEngine) 
      throws IOException {
    // Invoke the super constructor with a flag that 
    // indicates simulation
    super(conf, clock, true);
    resetEngineClock(simulatorEngine, clock);
    cleanupQueue = new LinkedList<JobID>();
  }

  /**
   * Starts the JobTracker with given configuration and a given time. It also
   * starts the JobNotifier thread. 
   * @param conf the starting configuration of the SimulatorJobTracker.
   * @param startTime the starting time of simulation -- this is used to
   * initialize the clock.
   * @param engine the SimulatorEngine that we talk to. 
   * @throws IOException
   */
  public static SimulatorJobTracker startTracker(JobConf conf, long startTime, SimulatorEngine engine)
  throws IOException {
    SimulatorJobTracker result = null;
    try {
      SimulatorClock simClock = new SimulatorClock(startTime);
      result = new SimulatorJobTracker(conf, simClock, engine);
      result.taskScheduler.setTaskTrackerManager(result);
    } catch (IOException e) {
      LOG.warn("Error starting tracker: "
          + StringUtils.stringifyException(e));
    }
    if (result != null) {
      JobEndNotifier.startNotifier();
    }

    return result;
  }

  /**
   * Start the SimulatorJobTracker with given configuration after
   * creating its own SimulatorEngine. Pretty much
   * used for debugging only. 
   * @param conf :The starting configuration of the SimulatorJobTracker
   * @param startTime :The starting time of simulation
   * @return void
   * @throws IOException
   * @throws InterruptedException
   */
  public static SimulatorJobTracker startTracker(JobConf conf, long startTime)
  throws IOException, InterruptedException {
    return startTracker(conf, startTime, new SimulatorEngine());
  }

  @Override
  public void offerService() throws InterruptedException, IOException {
    taskScheduler.start();
    LOG.info("started taskScheduler...");
    synchronized (this) {
      state = State.RUNNING;
    }
  }

  /**
   * Returns the simulatorClock in that is a static object in SimulatorJobTracker. 
   * 
   * @return SimulatorClock object.
   */
  static Clock getClock() {
    assert(engine.getCurrentTime() == clock.getTime()): 
    	   " Engine time = " + engine.getCurrentTime() + 
    	   " JobTracker time = " + clock.getTime();
    return clock;
  }

  /**
   * Overriding the getCleanTaskReports function of the
   * original JobTracker since we do not have setup and cleanup 
   * tasks.
   * @param jobid JobID for which we desire cleanup reports.
   */
  @Override
  public synchronized TaskReport[] getCleanupTaskReports(JobID jobid) {
    return null;
  }
  
  /**
   * Overriding since we do not support queue acls.
   */
  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException {
    return null;
  }

  /**
   * Overriding since we do not simulate setup/cleanup tasks.
   */
  @Override
  public synchronized TaskReport[] getSetupTaskReports(JobID jobid) {
    return null;
  }

  @Override
  public synchronized JobStatus submitJob(
      JobID jobId, String jobSubmitDir, TokenStorage ts) 
  throws IOException {
    boolean loggingEnabled = LOG.isDebugEnabled();
    if (loggingEnabled) {
      LOG.debug("submitJob for jobname = " + jobId);
    }
    if (jobs.containsKey(jobId)) {
      // job already running, don't start twice
      if (loggingEnabled) {
        LOG.debug("Job '" + jobId.getId() + "' already present ");
      }
      return jobs.get(jobId).getStatus();
    }
    JobStory jobStory = SimulatorJobCache.get(jobId);
    if (jobStory == null) {
      throw new IllegalArgumentException("Job not found in SimulatorJobCache: "+jobId);
    }
    validateAndSetClock(jobStory.getSubmissionTime());
    
    SimulatorJobInProgress job = new SimulatorJobInProgress(jobId, jobSubmitDir, this,
                                                            this.conf, 
                                                            jobStory);
    return addJob(jobId, job);
  }
  
  /**
   * Return the SimulatorJob object given a jobID.
   * @param jobid
   * @return
   */
  private SimulatorJobInProgress getSimulatorJob(JobID jobid) {
    return (SimulatorJobInProgress)jobs.get(jobid);
  }
  
  /**
   * Safely clean-up all data structures at the end of the 
   * job (success/failure/killed). In addition to performing the tasks that the
   * original finalizeJob does, we also inform the SimulatorEngine about the 
   * completion of this job. 
   *  
   * @param job completed job.
   */
  @Override
  synchronized void finalizeJob(JobInProgress job) {

    // Let the SimulatorEngine know that the job is done
    JobStatus cloneStatus = (JobStatus)job.getStatus().clone();
    engine.markCompletedJob(cloneStatus, 
                            SimulatorJobTracker.getClock().getTime());

    JobID jobId = job.getStatus().getJobID();
    LOG.info("Finished job " + jobId + " endtime = " +
              getClock().getTime() + " with status: " +
              JobStatus.getJobRunState(job.getStatus().getRunState()));
    
    // for updating the metrics and JobHistory, invoke the original 
    // finalizeJob.
    super.finalizeJob(job);
    
    // now placing this job in queue for future nuking
    cleanupJob(job);
  }

  /**
   * The cleanupJob method maintains the queue cleanQueue. When a job is finalized, 
   * it is added to the cleanupQueue. Jobs are removed from the cleanupQueue
   * so that its size is maintained to be less than that specified by
   * JOBS_IN_MUMAK_MEMORY.
   * @param job : The JobInProgress object that was just finalized and is 
   * going to be added to the cleanupQueue. 
   */
  private void cleanupJob(JobInProgress job) {
   
    cleanupQueue.add(job.getJobID());
    
    while(cleanupQueue.size()> JOBS_IN_MUMAK_MEMORY) {
      JobID removedJob = cleanupQueue.poll();
      retireJob(removedJob, "");
    } 
  }
  // //////////////////////////////////////////////////
  // InterTrackerProtocol
  // //////////////////////////////////////////////////

  @Override
  synchronized boolean processHeartbeat(TaskTrackerStatus trackerStatus,
      boolean initialContact) {
    boolean loggingEnabled = LOG.isDebugEnabled();
    String trackerName = trackerStatus.getTrackerName();
    boolean seenBefore = updateTaskTrackerStatus(trackerName, trackerStatus);
    TaskTracker taskTracker = getTaskTracker(trackerName);
    // update the status of the task tracker. Also updates all aggregate
    // statistics
    if (loggingEnabled) {
      LOG.debug("processing heartbeat for " + trackerName);
      LOG.debug("updating TaskTracker status for " + trackerName);
    }
    if (initialContact) {
      // If it's first contact, then clear out 
      // any state hanging around
      if (seenBefore) {
        lostTaskTracker(taskTracker);
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
      if (loggingEnabled) {
        LOG.debug("adding new tracker name = " + trackerName);
      }
      addNewTracker(taskTracker);
    }

    if (loggingEnabled) {
      LOG.debug("updating TaskStatus for " + trackerName);
    }
    // update status of all tasks from heartbeat
    updateTaskStatuses(trackerStatus);

    return true;
  }
  
  /**
   * Utility to validate the current simulation time
   * @param newSimulationTime
   */
  
  private void validateAndSetClock(long newSimulationTime) {
     
    // We do not use the getClock routine here as 
    // the Engine and JobTracker clocks are different at
    // this point.
    long currentSimulationTime = clock.getTime();   
    if (newSimulationTime < currentSimulationTime) {
      // time has gone backwards
      throw new IllegalArgumentException("Time has gone backwards! " +
                                 "newSimulationTime: " + newSimulationTime +
                                 " while currentTime: " + 
                                 currentSimulationTime);
    }
    // the simulation time should also match that in the engine
    assert(newSimulationTime == engine.getCurrentTime()) : 
    	     " newTime =" + newSimulationTime + 
    	     " engineTime = " + engine.getCurrentTime();

    // set the current simulation time
    clock.setTime(newSimulationTime);
  }

  @Override
  public synchronized HeartbeatResponse heartbeat(TaskTrackerStatus status,
      boolean restarted, boolean initialContact, boolean acceptNewTasks,
      short responseId) throws IOException {
    boolean loggingEnabled = LOG.isDebugEnabled();
    if (loggingEnabled) {
      LOG.debug("Got heartbeat from: " + status.getTrackerName()
          + " (restarted: " + restarted + " initialContact: " + initialContact
          + " acceptNewTasks: " + acceptNewTasks + ")" + " with responseId: "
          + responseId);
    }
    if (!(status instanceof SimulatorTaskTrackerStatus)) {
      throw new IllegalArgumentException(
          "Expecting SimulatorTaskTrackerStatus, but got " + status.getClass());
    }
    SimulatorTaskTrackerStatus taskTrackerStatus = 
      (SimulatorTaskTrackerStatus) status;
    
    String trackerName = taskTrackerStatus.getTrackerName();

    // validate and set the simulation time
    // according to the time sent by the tracker
    validateAndSetClock(taskTrackerStatus.getCurrentSimulationTime());

    HeartbeatResponse prevHeartbeatResponse = 
      trackerToHeartbeatResponseMap.get(trackerName);

    if (initialContact != true) {
      // If this isn't the 'initial contact' from the tasktracker,
      // there is something seriously wrong if the JobTracker has
      // no record of the 'previous heartbeat'; if so, ask the
      // tasktracker to re-initialize itself.
      if (prevHeartbeatResponse == null) {
        // This is the first heartbeat from the old tracker to the newly
        // started JobTracker
        // Jobtracker might have restarted but no recovery is needed
        // otherwise this code should not be reached
        LOG.warn("Serious problem, cannot find record of 'previous' " +
                 " heartbeat for '" + trackerName +
                 "'; reinitializing the tasktracker");
        return new HeartbeatResponse(responseId,
            new TaskTrackerAction[] { new ReinitTrackerAction() });
      } else {

        // It is completely safe to not process a 'duplicate' heartbeat
        // from a
        // {@link TaskTracker} since it resends the heartbeat when rpcs
        // are
        // lost see {@link TaskTracker.transmitHeartbeat()};
        // acknowledge it by re-sending the previous response to let the
        // {@link TaskTracker} go forward.
        if (prevHeartbeatResponse.getResponseId() != responseId) {
          if (loggingEnabled) {
            LOG.debug("Ignoring 'duplicate' heartbeat from '" + trackerName
                + "'; resending the previous 'lost' response");
          }
          return prevHeartbeatResponse;
        }
      }
    }

    if (loggingEnabled) {
      LOG.debug("processed heartbeat for responseId = " + responseId);
    }
    short newResponseId = (short) (responseId + 1);
    status.setLastSeen(getClock().getTime());
    
    // process the incoming heartbeat 
    if (!processHeartbeat(taskTrackerStatus, initialContact)) {
      if (prevHeartbeatResponse != null) {
        trackerToHeartbeatResponseMap.remove(trackerName);
      }
      return new HeartbeatResponse(newResponseId,
          new TaskTrackerAction[] { new ReinitTrackerAction() });
    }

    
    // Initialize the response to be sent for the heartbeat
    HeartbeatResponse response = new HeartbeatResponse(newResponseId, null);
    List<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();
    if (acceptNewTasks) {
      TaskTracker taskTracker = getTaskTracker(trackerName);
      // get the list of tasks to be executed on this tasktracker
      List<Task> tasks = taskScheduler.assignTasks(taskTracker);
      if (tasks != null) {
        if (loggingEnabled && tasks.size()>0) {
          LOG.debug("Tasks found from TaskScheduler: number = " + tasks.size());
        }

        for (Task task : tasks) {
          TaskAttemptID taskAttemptID = task.getTaskID();
          // get the JobID and the JIP object for this taskAttempt
          JobID jobID = taskAttemptID.getJobID();
          SimulatorJobInProgress job = getSimulatorJob(jobID);

          if (job == null) {
        	  LOG.error("Getting taskAttemptId=" + taskAttemptID + 
                      " for job " + jobID + 
                      " not present in SimulatorJobTracker");
            
            throw new IOException("Getting taskAttemptId=" + taskAttemptID + 
                                  " for job " + jobID + 
                                  " not present in SimulatorJobTracker");
          }
          // add the launch task action to the list
          if (loggingEnabled) {
            LOG.debug("Getting taskAttemptInfo for '" + taskAttemptID
                + "' for tracker '" + trackerName + "'");
          }
          TaskAttemptInfo taskAttemptInfo = 
                          job.getTaskAttemptInfo(taskTracker, taskAttemptID);

          if (taskAttemptInfo == null) { 
            throw new RuntimeException("Empty taskAttemptInfo: " +
            		                       "task information missing");
          }

          // create the SLTA object using the task attempt information
          if (loggingEnabled) {
            LOG
                .debug("Adding LaunchTaskAction for '" + taskAttemptID
                    + "' for tracker " + trackerName + " taskType="
                    + taskAttemptID.getTaskType() + " time="
                    + getClock().getTime());
          }
          SimulatorLaunchTaskAction newlaunchTask = 
        	  new SimulatorLaunchTaskAction(task, taskAttemptInfo);
          
          actions.add(newlaunchTask);
        }
      }
    }

    // Check for tasks to be killed
    // also get the attemptIDs in a separate set for quick lookup
    // during the MapCompletion generation
    List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
     
    if (killTasksList != null) {
      if (loggingEnabled) {
        for (TaskTrackerAction ttAction : killTasksList) {
          LOG.debug("Time =" + getClock().getTime() + " tracker=" + trackerName
              + " KillTaskAction for:"
              + ((KillTaskAction) ttAction).getTaskID());
        }
      }
      actions.addAll(killTasksList);
    }

    // Check for tasks whose outputs can be saved
    // this is currently a no-op
    List<TaskTrackerAction> commitTasksList = getTasksToSave(status);
    if (commitTasksList != null) {
      actions.addAll(commitTasksList);
    }

    // check the reduce tasks in this task-tracker, and add in the
    // AllMapTasksCompletedTaskAction for each of the reduce tasks
    // this enables the reduce tasks to move from shuffle to reduce phase
    List<TaskTrackerAction> mapCompletionTasks = 
      getMapCompletionTasks(taskTrackerStatus, killTasksList);

    if (mapCompletionTasks != null) {
      actions.addAll(mapCompletionTasks);
    }

    if (loggingEnabled) {
      LOG.debug("Done with collection actions for tracker " + trackerName
          + " for responseId " + responseId);
    }
    // calculate next heartbeat interval and put in heartbeat response
    int nextInterval = getNextHeartbeatInterval();
    response.setHeartbeatInterval(nextInterval);
    response.setActions(actions.toArray(new TaskTrackerAction[actions
                                                              .size()]));
    if (loggingEnabled) {
      LOG.debug("Nextinterval for tracker " + trackerName + " is "
          + nextInterval);
    }
    // Update the trackerToHeartbeatResponseMap
    trackerToHeartbeatResponseMap.put(trackerName, response);

    // Done processing the hearbeat, now remove 'marked' tasks
    removeMarkedTasks(trackerName);

    return response;
  }

  /**
   * The getMapCompletion method is intended to inform taskTrackes when to change the status
   * of reduce tasks from "shuffle" to "reduce".
   * For all reduce tasks in this TaskTracker that are
   * in the shuffle phase, getMapCompletionTasks finds the number of finished maps for 
   * this job from the jobInProgress object. If this
   * number equals the number of desired maps for this job, then it adds an 
   * AllMapsCompletedTaskAction for this reduce task-attempt.
   * 
   * @param status
   *            The status of the task tracker
   * @return List of TaskTrackerActions
   */
  private List<TaskTrackerAction> getMapCompletionTasks(
      TaskTrackerStatus status, 
      List<TaskTrackerAction> tasksToKill) {
    boolean loggingEnabled = LOG.isDebugEnabled();
    
    // Build up the list of tasks about to be killed
    Set<TaskAttemptID> killedTasks = new HashSet<TaskAttemptID>();
    if (tasksToKill != null) {
      for (TaskTrackerAction taskToKill : tasksToKill) {
        killedTasks.add(((KillTaskAction)taskToKill).getTaskID());
      }
    }

    String trackerName = status.getTrackerName();

    List<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();

    // loop through the list of task statuses
    for (TaskStatus report : status.getTaskReports()) {

      TaskAttemptID taskAttemptId = report.getTaskID();
      SimulatorJobInProgress job = getSimulatorJob(taskAttemptId.getJobID());
      
      if(job ==null) {
        // This job has completed before.
        // and this is a zombie reduce-task
        Set<JobID> jobsToCleanup = trackerToJobsToCleanup.get(trackerName);
        if (jobsToCleanup == null) {
          jobsToCleanup = new HashSet<JobID>();
          trackerToJobsToCleanup.put(trackerName, jobsToCleanup);
        }
        jobsToCleanup.add(taskAttemptId.getJobID());
        continue;
      }   
      JobStatus jobStatus = job.getStatus();
      TaskInProgress tip = taskidToTIPMap.get(taskAttemptId);

      // if the  job is running, attempt is running
      // no KillTask is being sent for this attempt
      // task is a reduce and attempt is in shuffle phase
      // this precludes sending both KillTask and AllMapsCompletion 
      // for same reduce-attempt 

      if (jobStatus.getRunState()== JobStatus.RUNNING && 
          tip.isRunningTask(taskAttemptId) &&
          !killedTasks.contains(taskAttemptId) && 
          !report.getIsMap() &&
          report.getPhase() == TaskStatus.Phase.SHUFFLE) {

        if (loggingEnabled) {
          LOG.debug("Need map-completion information for REDUCEattempt "
              + taskAttemptId + " in tracker " + trackerName);

          LOG.debug("getMapCompletion: job=" + job.getJobID() + " pendingMaps="
              + job.pendingMaps());
        }
        // Check whether the number of finishedMaps equals the
        // number of maps
        boolean canSendMapCompletion = false;
       
        canSendMapCompletion = (job.finishedMaps()==job.desiredMaps());	

        if (canSendMapCompletion) {
          if (loggingEnabled) {
            LOG.debug("Adding MapCompletion for taskAttempt " + taskAttemptId
                + " in tracker " + trackerName);

            LOG.debug("FinishedMaps for job:" + job.getJobID() + " is = "
                + job.finishedMaps() + "/" + job.desiredMaps());

            LOG.debug("AllMapsCompleted for task " + taskAttemptId + " time="
                + getClock().getTime());
          }
          actions.add(new AllMapsCompletedTaskAction(taskAttemptId));
        }
      }
    }
    return actions;
  }

 
  @Override
  void updateTaskStatuses(TaskTrackerStatus status) {
    boolean loggingEnabled = LOG.isDebugEnabled();
    String trackerName = status.getTrackerName();
    // loop through the list of task statuses
    if (loggingEnabled) {
      LOG.debug("Updating task statuses for tracker " + trackerName);
    }
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(trackerName);
      TaskAttemptID taskAttemptId = report.getTaskID();
      JobID jobid = taskAttemptId.getJobID();
      if (loggingEnabled) {
        LOG.debug("Updating status for job " + jobid + " for task = "
            + taskAttemptId + " status=" + report.getProgress()
            + " for tracker: " + trackerName);
      }
      SimulatorJobInProgress job = 
        getSimulatorJob(taskAttemptId.getJobID());

      if(job ==null) {
        // This job bas completed before.
        Set<JobID> jobsToCleanup = trackerToJobsToCleanup.get(trackerName);
        if (jobsToCleanup == null) {
          jobsToCleanup = new HashSet<JobID>();
          trackerToJobsToCleanup.put(trackerName, jobsToCleanup);
        }
        jobsToCleanup.add(taskAttemptId.getJobID());
        continue;
      }
      TaskInProgress tip = taskidToTIPMap.get(taskAttemptId);

      JobStatus prevStatus = (JobStatus) job.getStatus().clone();
      job.updateTaskStatus(tip, (TaskStatus) report.clone());
      JobStatus newStatus = (JobStatus) job.getStatus().clone();
      if (tip.isComplete()) {
        if (loggingEnabled) {
          LOG.debug("Completed task attempt " + taskAttemptId + " tracker:"
              + trackerName + " time=" + getClock().getTime());
        }
      }

      if (prevStatus.getRunState() != newStatus.getRunState()) {
        if (loggingEnabled) {
          LOG.debug("Informing Listeners of job " + jobid + " of newStatus "
              + JobStatus.getJobRunState(newStatus.getRunState()));
        }
        JobStatusChangeEvent event = new JobStatusChangeEvent(job,
            EventType.RUN_STATE_CHANGED, prevStatus, newStatus);

        updateJobInProgressListeners(event);
      }

    }
  }
}
