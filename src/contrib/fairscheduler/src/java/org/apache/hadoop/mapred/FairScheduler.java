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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that implements fair sharing.
 */
public class FairScheduler extends TaskScheduler {
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.FairScheduler");

  // How often fair shares are re-calculated
  protected long updateInterval = 500;

  // How often to dump scheduler state to the event log
  protected long dumpInterval = 10000;
  
  // How often tasks are preempted (must be longer than a couple
  // of heartbeats to give task-kill commands a chance to act).
  protected long preemptionInterval = 15000;
  
  // Used to iterate through map and reduce task types
  private static final TaskType[] MAP_AND_REDUCE = 
    new TaskType[] {TaskType.MAP, TaskType.REDUCE};
  
  protected PoolManager poolMgr;
  protected LoadManager loadMgr;
  protected TaskSelector taskSelector;
  protected WeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected Map<JobInProgress, JobInfo> infos = // per-job scheduling variables
    new HashMap<JobInProgress, JobInfo>();
  protected long lastUpdateTime;           // Time when we last updated infos
  protected boolean initialized;  // Are we initialized?
  protected volatile boolean running; // Are we running?
  protected boolean useFifo;      // Set if we want to revert to FIFO behavior
  protected boolean assignMultiple; // Simultaneously assign map and reduce?
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected boolean waitForMapsBeforeLaunchingReduces = true;
  protected boolean preemptionEnabled;
  protected boolean onlyLogPreemption; // Only log when tasks should be killed
  private Clock clock;
  private EagerTaskInitializationListener eagerInitListener;
  private JobListener jobListener;
  private boolean mockMode; // Used for unit tests; disables background updates
                            // and scheduler event log
  private FairSchedulerEventLog eventLog;
  protected long lastDumpTime;       // Time when we last dumped state to log
  protected long lastHeartbeatTime;  // Time we last ran assignTasks 
  private long lastPreemptCheckTime; // Time we last ran preemptTasksIfNecessary
  
  /**
   * A class for holding per-job scheduler variables. These always contain the
   * values of the variables at the last update(), and are used along with a
   * time delta to update the map and reduce deficits before a new update().
   */
  static class JobInfo {
    boolean runnable = false;   // Can the job run given user/pool limits?
    double mapWeight = 0;       // Weight of job in calculation of map share
    double reduceWeight = 0;    // Weight of job in calculation of reduce share
    long mapDeficit = 0;        // Time deficit for maps
    long reduceDeficit = 0;     // Time deficit for reduces
    int runningMaps = 0;        // Maps running at last update
    int runningReduces = 0;     // Reduces running at last update
    int neededMaps;             // Maps needed at last update
    int neededReduces;          // Reduces needed at last update
    int minMaps = 0;            // Minimum maps as guaranteed by pool
    int minReduces = 0;         // Minimum reduces as guaranteed by pool
    double mapFairShare = 0;    // Fair share of map slots at last update
    double reduceFairShare = 0; // Fair share of reduce slots at last update
    // Variables used for preemption
    long lastTimeAtMapMinShare;      // When was the job last at its min maps?
    long lastTimeAtReduceMinShare;   // Similar for reduces.
    long lastTimeAtMapHalfFairShare; // When was the job last at half fair maps?
    long lastTimeAtReduceHalfFairShare;  // Similar for reduces.
    
    public JobInfo(long currentTime) {
      lastTimeAtMapMinShare = currentTime;
      lastTimeAtReduceMinShare = currentTime;
      lastTimeAtMapHalfFairShare = currentTime;
      lastTimeAtReduceHalfFairShare = currentTime;
    }
  }
  
  public FairScheduler() {
    this(new Clock(), false);
  }
  
  /**
   * Constructor used for tests, which can change the clock and disable updates.
   */
  protected FairScheduler(Clock clock, boolean mockMode) {
    this.clock = clock;
    this.mockMode = mockMode;
    this.jobListener = new JobListener();
  }

  @Override
  public void start() {
    try {
      Configuration conf = getConf();
      // Create scheduling log and initialize it if it is enabled
      eventLog = new FairSchedulerEventLog();
      boolean logEnabled = conf.getBoolean(
          "mapred.fairscheduler.eventlog.enabled", false);
      if (!mockMode && logEnabled) {
        String hostname = "localhost";
        if (taskTrackerManager instanceof JobTracker) {
          hostname = ((JobTracker) taskTrackerManager).getJobTrackerMachine();
        }
        eventLog.init(conf, hostname);
      }
      // Initialize other pieces of the scheduler
      taskTrackerManager.addJobInProgressListener(jobListener);
      if (!mockMode) {
        eagerInitListener = new EagerTaskInitializationListener(conf);
        eagerInitListener.setTaskTrackerManager(taskTrackerManager);
        eagerInitListener.start();
        taskTrackerManager.addJobInProgressListener(eagerInitListener);
      }
      poolMgr = new PoolManager(conf);
      loadMgr = (LoadManager) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.loadmanager", 
              CapBasedLoadManager.class, LoadManager.class), conf);
      loadMgr.setTaskTrackerManager(taskTrackerManager);
      loadMgr.setEventLog(eventLog);
      loadMgr.start();
      taskSelector = (TaskSelector) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.taskselector", 
              DefaultTaskSelector.class, TaskSelector.class), conf);
      taskSelector.setTaskTrackerManager(taskTrackerManager);
      taskSelector.start();
      Class<?> weightAdjClass = conf.getClass(
          "mapred.fairscheduler.weightadjuster", null);
      if (weightAdjClass != null) {
        weightAdjuster = (WeightAdjuster) ReflectionUtils.newInstance(
            weightAdjClass, conf);
      }
      updateInterval = conf.getLong(
          "mapred.fairscheduler.update.interval", 500);
      dumpInterval = conf.getLong(
          "mapred.fairscheduler.dump.interval", 10000);
      preemptionInterval = conf.getLong(
          "mapred.fairscheduler.preemption.interval", 15000);
      assignMultiple = conf.getBoolean(
          "mapred.fairscheduler.assignmultiple", true);
      sizeBasedWeight = conf.getBoolean(
          "mapred.fairscheduler.sizebasedweight", false);
      preemptionEnabled = conf.getBoolean(
          "mapred.fairscheduler.preemption", false);
      onlyLogPreemption = conf.getBoolean(
          "mapred.fairscheduler.preemption.only.log", false);
      initialized = true;
      running = true;
      lastUpdateTime = clock.getTime();
      // Start a thread to update deficits every UPDATE_INTERVAL
      if (!mockMode) {
        new UpdateThread().start();
      }
      // Register servlet with JobTracker's Jetty server
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        HttpServer infoServer = jobTracker.infoServer;
        infoServer.setAttribute("scheduler", this);
        infoServer.addServlet("scheduler", "/scheduler",
            FairSchedulerServlet.class);
      }
      eventLog.log("INITIALIZED");
    } catch (Exception e) {
      // Can't load one of the managers - crash the JobTracker now while it is
      // starting up so that the user notices.
      throw new RuntimeException("Failed to start FairScheduler", e);
    }
    LOG.info("Successfully configured FairScheduler");
  }

  @Override
  public void terminate() throws IOException {
    if (eventLog != null)
      eventLog.log("SHUTDOWN");
    running = false;
    if (jobListener != null)
      taskTrackerManager.removeJobInProgressListener(jobListener);
    if (eagerInitListener != null)
      taskTrackerManager.removeJobInProgressListener(eagerInitListener);
    if (eventLog != null)
      eventLog.shutdown();
  }
  
  /**
   * Used to listen for jobs added/removed by our {@link TaskTrackerManager}.
   */
  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) {
      synchronized (FairScheduler.this) {
        eventLog.log("JOB_ADDED", job.getJobID());
        poolMgr.addJob(job);
        JobInfo info = new JobInfo(clock.getTime());
        infos.put(job, info);
        update();
      }
    }
    
    @Override
    public void jobRemoved(JobInProgress job) {
      synchronized (FairScheduler.this) {
        eventLog.log("JOB_REMOVED", job.getJobID());
        poolMgr.removeJob(job);
        infos.remove(job);
      }
    }
  
    @Override
    public void jobUpdated(JobChangeEvent event) {
      eventLog.log("JOB_UPDATED", event.getJobInProgress().getJobID());
    }
  }

  /**
   * A thread which calls {@link FairScheduler#update()} ever
   * <code>UPDATE_INTERVAL</code> milliseconds.
   */
  private class UpdateThread extends Thread {
    private UpdateThread() {
      super("FairScheduler update thread");
    }

    public void run() {
      while (running) {
        try {
          Thread.sleep(updateInterval);
          update();
          dumpIfNecessary();
          preemptTasksIfNecessary();
        } catch (Exception e) {
          LOG.error("Exception in fair scheduler UpdateThread", e);
        }
      }
    }
  }
  
  @Override
  public synchronized List<Task> assignTasks(TaskTracker tracker)
      throws IOException {
    if (!initialized) // Don't try to assign tasks if we haven't yet started up
      return null;
    String trackerName = tracker.getTrackerName();
    eventLog.log("HEARTBEAT", trackerName);
    
    // Reload allocations file if it hasn't been loaded in a while
    poolMgr.reloadAllocsIfNecessary();
    
    // Compute total runnable maps and reduces, and currently running ones
    int runnableMaps = 0;
    int runningMaps = 0;
    int runnableReduces = 0;
    int runningReduces = 0;
    for (JobInProgress job: infos.keySet()) {
      runnableMaps += runnableTasks(job, TaskType.MAP);
      runningMaps += runningTasks(job, TaskType.MAP);
      runnableReduces += runnableTasks(job, TaskType.REDUCE);
      runningReduces += runningTasks(job, TaskType.REDUCE);
    }

    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    // Compute total map/reduce slots
    // In the future we can precompute this if the Scheduler becomes a 
    // listener of tracker join/leave events.
    int totalMapSlots = getTotalSlots(TaskType.MAP, clusterStatus);
    int totalReduceSlots = getTotalSlots(TaskType.REDUCE, clusterStatus);
    
    eventLog.log("RUNNABLE_TASKS", 
        runnableMaps, runningMaps, runnableReduces, runningReduces);

    TaskTrackerStatus trackerStatus = tracker.getStatus();

    // Scan to see whether any job needs to run a map, then a reduce
    ArrayList<Task> tasks = new ArrayList<Task>();
    for (TaskType taskType: MAP_AND_REDUCE) {
      // Continue if all runnable tasks of this type are already running
      if (taskType == TaskType.MAP && runningMaps == runnableMaps ||
          taskType == TaskType.REDUCE && runningReduces == runnableReduces)
        continue;
      // Continue if the node can't support another task of the given type
      boolean canAssign = (taskType == TaskType.MAP) ? 
          loadMgr.canAssignMap(trackerStatus, runnableMaps, totalMapSlots) :
          loadMgr.canAssignReduce(trackerStatus, runnableReduces, totalReduceSlots);
      if (canAssign) {
        // Figure out the jobs that need this type of task
        List<JobInProgress> candidates = new ArrayList<JobInProgress>();
        for (JobInProgress job: infos.keySet()) {
          if (job.getStatus().getRunState() == JobStatus.RUNNING && 
              neededTasks(job, taskType) > 0) {
            candidates.add(job);
          }
        }
        // Sort jobs by deficit (for Fair Sharing) or submit time (for FIFO)
        Comparator<JobInProgress> comparator = useFifo ?
            new FifoJobComparator() : new DeficitComparator(taskType);
        Collections.sort(candidates, comparator);
        for (JobInProgress job: candidates) {
          eventLog.log("INFO", 
              "Checking for " + taskType + " task in " + job.getJobID());
          Task task;
          if (taskType == TaskType.MAP) {
            task = taskSelector.obtainNewMapTask(trackerStatus, job);
          } else {
            task = taskSelector.obtainNewReduceTask(trackerStatus, job);
          }
          if (task != null) {
            eventLog.log("ASSIGN", trackerName, taskType,
                job.getJobID(), task.getTaskID());
            // Update the JobInfo for this job so we account for the launched
            // tasks during this update interval and don't try to launch more
            // tasks than the job needed on future heartbeats
            JobInfo info = infos.get(job);
            if (taskType == TaskType.MAP) {
              info.runningMaps++;
              info.neededMaps--;
            } else {
              info.runningReduces++;
              info.neededReduces--;
            }
            // Add task to the list of assignments
            tasks.add(task);
            // If not allowed to assign multiple tasks per heartbeat, return
            if (!assignMultiple)
              return tasks;
            break;
          }
        }
      } else {
        eventLog.log("INFO", 
            "Can't assign another " + taskType + " to " + trackerName);
      }
    }
    
    // If no tasks were found, return null
    return tasks.isEmpty() ? null : tasks;
  }

  /**
   * Compare jobs by deficit for a given task type, putting jobs whose current
   * allocation is less than their minimum share always ahead of others. This is
   * the default job comparator used for Fair Sharing.
   */
  private class DeficitComparator implements Comparator<JobInProgress> {
    private final TaskType taskType;

    private DeficitComparator(TaskType taskType) {
      this.taskType = taskType;
    }

    public int compare(JobInProgress j1, JobInProgress j2) {
      // Put needy jobs ahead of non-needy jobs (where needy means must receive
      // new tasks to meet slot minimum), comparing among jobs of the same type
      // by deficit so as to put jobs with higher deficit ahead.
      JobInfo j1Info = infos.get(j1);
      JobInfo j2Info = infos.get(j2);
      double deficitDif;
      boolean j1Needy, j2Needy;
      if (taskType == TaskType.MAP) {
        j1Needy = j1.runningMaps() < Math.floor(j1Info.minMaps);
        j2Needy = j2.runningMaps() < Math.floor(j2Info.minMaps);
        deficitDif = j2Info.mapDeficit - j1Info.mapDeficit;
      } else {
        j1Needy = j1.runningReduces() < Math.floor(j1Info.minReduces);
        j2Needy = j2.runningReduces() < Math.floor(j2Info.minReduces);
        deficitDif = j2Info.reduceDeficit - j1Info.reduceDeficit;
      }
      if (j1Needy && !j2Needy)
        return -1;
      else if (j2Needy && !j1Needy)
        return 1;
      else // Both needy or both non-needy; compare by deficit
        return (int) Math.signum(deficitDif);
    }
  }
  
  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and numbers of running
   * and needed tasks of each type. 
   */
  protected void update() {
    //Making more granual locking so that clusterStatus can be fetched from Jobtracker.
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    // Got clusterStatus hence acquiring scheduler lock now
    // Remove non-running jobs
    synchronized(this){
      List<JobInProgress> toRemove = new ArrayList<JobInProgress>();
      for (JobInProgress job: infos.keySet()) { 
        int runState = job.getStatus().getRunState();
        if (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED
          || runState == JobStatus.KILLED) {
            toRemove.add(job);
        }
      }
      for (JobInProgress job: toRemove) {
        infos.remove(job);
        poolMgr.removeJob(job);
      }
      // Update running jobs with deficits since last update, and compute new
      // slot allocations, weight, shares and task counts
      long now = clock.getTime();
      long timeDelta = now - lastUpdateTime;
      updateDeficits(timeDelta);
      updateRunnability();
      updateTaskCounts();
      updateWeights();
      updateMinSlots();
      updateFairShares(clusterStatus);
      if (preemptionEnabled)
        updatePreemptionVariables();
      lastUpdateTime = now;
    }
  }
  
  private void updateDeficits(long timeDelta) {
    for (JobInfo info: infos.values()) {
      info.mapDeficit +=
        (info.mapFairShare - info.runningMaps) * timeDelta;
      info.reduceDeficit +=
        (info.reduceFairShare - info.runningReduces) * timeDelta;
    }
  }
  
  private void updateRunnability() {
    // Start by marking everything as not runnable
    for (JobInfo info: infos.values()) {
      info.runnable = false;
    }
    // Create a list of sorted jobs in order of start time and priority
    List<JobInProgress> jobs = new ArrayList<JobInProgress>(infos.keySet());
    Collections.sort(jobs, new FifoJobComparator());
    // Mark jobs as runnable in order of start time and priority, until
    // user or pool limits have been reached.
    Map<String, Integer> userJobs = new HashMap<String, Integer>();
    Map<String, Integer> poolJobs = new HashMap<String, Integer>();
    for (JobInProgress job: jobs) {
      if (job.getStatus().getRunState() == JobStatus.RUNNING) {
        String user = job.getJobConf().getUser();
        String pool = poolMgr.getPoolName(job);
        int userCount = userJobs.containsKey(user) ? userJobs.get(user) : 0;
        int poolCount = poolJobs.containsKey(pool) ? poolJobs.get(pool) : 0;
        if (userCount < poolMgr.getUserMaxJobs(user) && 
            poolCount < poolMgr.getPoolMaxJobs(pool)) {
          infos.get(job).runnable = true;
          userJobs.put(user, userCount + 1);
          poolJobs.put(pool, poolCount + 1);
        }
      }
    }
  }

  private void updateTaskCounts() {
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      if (job.getStatus().getRunState() != JobStatus.RUNNING) {
        // Job is still in PREP state and tasks aren't initialized; skip it.
        continue;
      }
      // Count maps
      int totalMaps = job.numMapTasks;
      int finishedMaps = 0;
      int runningMaps = 0;
      for (TaskInProgress tip: job.getMapTasks()) {
        if (tip.isComplete()) {
          finishedMaps += 1;
        } else if (tip.isRunning()) {
          runningMaps += tip.getActiveTasks().size();
        }
      }
      info.runningMaps = runningMaps;
      info.neededMaps = (totalMaps - runningMaps - finishedMaps
          + taskSelector.neededSpeculativeMaps(job));
      // Count reduces
      int totalReduces = job.numReduceTasks;
      int finishedReduces = 0;
      int runningReduces = 0;
      for (TaskInProgress tip: job.getReduceTasks()) {
        if (tip.isComplete()) {
          finishedReduces += 1;
        } else if (tip.isRunning()) {
          runningReduces += tip.getActiveTasks().size();
        }
      }
      info.runningReduces = runningReduces;
      if (enoughMapsFinishedToRunReduces(finishedMaps, totalMaps)) {
        info.neededReduces = (totalReduces - runningReduces - finishedReduces 
            + taskSelector.neededSpeculativeReduces(job));
      } else {
        info.neededReduces = 0;
      }
      // If the job was marked as not runnable due to its user or pool having
      // too many active jobs, set the neededMaps/neededReduces to 0. We still
      // count runningMaps/runningReduces however so we can give it a deficit.
      if (!info.runnable) {
        info.neededMaps = 0;
        info.neededReduces = 0;
      }
    }
  }

  /**
   * Has a job finished enough maps to allow launching its reduces?
   */
  protected boolean enoughMapsFinishedToRunReduces(
      int finishedMaps, int totalMaps) {
    if (waitForMapsBeforeLaunchingReduces) {
      return finishedMaps >= Math.max(1, totalMaps * 0.05);
    } else {
      return true;
    }
  }

  private void updateWeights() {
    // First, calculate raw weights for each job
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      info.mapWeight = calculateRawWeight(job, TaskType.MAP);
      info.reduceWeight = calculateRawWeight(job, TaskType.REDUCE);
    }
    // Now calculate job weight sums for each pool
    Map<String, Double> mapWeightSums = new HashMap<String, Double>();
    Map<String, Double> reduceWeightSums = new HashMap<String, Double>();
    for (Pool pool: poolMgr.getPools()) {
      double mapWeightSum = 0;
      double reduceWeightSum = 0;
      for (JobInProgress job: pool.getJobs()) {
        if (isRunnable(job)) {
          if (runnableTasks(job, TaskType.MAP) > 0) {
            mapWeightSum += infos.get(job).mapWeight;
          }
          if (runnableTasks(job, TaskType.REDUCE) > 0) {
            reduceWeightSum += infos.get(job).reduceWeight;
          }
        }
      }
      mapWeightSums.put(pool.getName(), mapWeightSum);
      reduceWeightSums.put(pool.getName(), reduceWeightSum);
    }
    // And normalize the weights based on pool sums and pool weights
    // to share fairly across pools (proportional to their weights)
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      String pool = poolMgr.getPoolName(job);
      double poolWeight = poolMgr.getPoolWeight(pool);
      double mapWeightSum = mapWeightSums.get(pool);
      double reduceWeightSum = reduceWeightSums.get(pool);
      if (mapWeightSum == 0)
        info.mapWeight = 0;
      else
        info.mapWeight *= (poolWeight / mapWeightSum); 
      if (reduceWeightSum == 0)
        info.reduceWeight = 0;
      else
        info.reduceWeight *= (poolWeight / reduceWeightSum); 
    }
  }
  
  private void updateMinSlots() {
    // Clear old minSlots
    for (JobInfo info: infos.values()) {
      info.minMaps = 0;
      info.minReduces = 0;
    }
    // For each pool, distribute its task allocation among jobs in it that need
    // slots. This is a little tricky since some jobs in the pool might not be
    // able to use all the slots, e.g. they might have only a few tasks left.
    // To deal with this, we repeatedly split up the available task slots
    // between the jobs left, give each job min(its alloc, # of slots it needs),
    // and redistribute any slots that are left over between jobs that still
    // need slots on the next pass. If, in total, the jobs in our pool don't
    // need all its allocation, we leave the leftover slots for general use.
    PoolManager poolMgr = getPoolManager();
    for (Pool pool: poolMgr.getPools()) {
      for (final TaskType type: MAP_AND_REDUCE) {
        Set<JobInProgress> jobs = new HashSet<JobInProgress>(pool.getJobs());
        int slotsLeft = poolMgr.getAllocation(pool.getName(), type);
        // Keep assigning slots until none are left
        while (slotsLeft > 0) {
          // Figure out total weight of jobs that still need slots
          double totalWeight = 0;
          for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
            JobInProgress job = it.next();
            if (isRunnable(job) &&
                runnableTasks(job, type) > minTasks(job, type)) {
              totalWeight += weight(job, type);
            } else {
              it.remove();
            }
          }
          if (totalWeight == 0) // No jobs that can use more slots are left 
            break;
          // Assign slots to jobs, using the floor of their weight divided by
          // total weight. This ensures that all jobs get some chance to take
          // a slot. Then, if no slots were assigned this way, we do another
          // pass where we use ceil, in case some slots were still left over.
          int oldSlots = slotsLeft; // Copy slotsLeft so we can modify it
          for (JobInProgress job: jobs) {
            double weight = weight(job, type);
            int share = (int) Math.floor(oldSlots * weight / totalWeight);
            slotsLeft = giveMinSlots(job, type, slotsLeft, share);
          }
          if (slotsLeft == oldSlots) {
            // No tasks were assigned; do another pass using ceil, giving the
            // extra slots to jobs in order of weight then deficit
            List<JobInProgress> sortedJobs = new ArrayList<JobInProgress>(jobs);
            Collections.sort(sortedJobs, new Comparator<JobInProgress>() {
              public int compare(JobInProgress j1, JobInProgress j2) {
                double dif = weight(j2, type) - weight(j1, type);
                if (dif == 0) // Weights are equal, compare by deficit 
                  dif = deficit(j2, type) - deficit(j1, type);
                return (int) Math.signum(dif);
              }
            });
            for (JobInProgress job: sortedJobs) {
              double weight = weight(job, type);
              int share = (int) Math.ceil(oldSlots * weight / totalWeight);
              slotsLeft = giveMinSlots(job, type, slotsLeft, share);
            }
            if (slotsLeft > 0) {
              LOG.warn("Had slotsLeft = " + slotsLeft + " after the final "
                  + "loop in updateMinSlots. This probably means some fair "
                  + "scheduler weights are being set to NaN or Infinity.");
            }
            break;
          }
        }
      }
    }
  }

  /**
   * Give up to <code>tasksToGive</code> min slots to a job (potentially fewer
   * if either the job needs fewer slots or there aren't enough slots left).
   * Returns the number of slots left over.
   */
  private int giveMinSlots(JobInProgress job, TaskType type,
      int slotsLeft, int slotsToGive) {
    int runnable = runnableTasks(job, type);
    int curMin = minTasks(job, type);
    slotsToGive = Math.min(Math.min(slotsLeft, runnable - curMin), slotsToGive);
    slotsLeft -= slotsToGive;
    JobInfo info = infos.get(job);
    if (type == TaskType.MAP)
      info.minMaps += slotsToGive;
    else
      info.minReduces += slotsToGive;
    return slotsLeft;
  }

  private void updateFairShares(ClusterStatus clusterStatus) {
    // Clear old fairShares
    for (JobInfo info: infos.values()) {
      info.mapFairShare = 0;
      info.reduceFairShare = 0;
    }
    // Assign new shares, based on weight and minimum share. This is done
    // as follows. First, we split up the available slots between all
    // jobs according to weight. Then if there are any jobs whose minSlots is
    // larger than their fair allocation, we give them their minSlots and
    // remove them from the list, and start again with the amount of slots
    // left over. This continues until all jobs' minSlots are less than their
    // fair allocation, and at this point we know that we've met everyone's
    // guarantee and we've split the excess capacity fairly among jobs left.
    for (TaskType type: MAP_AND_REDUCE) {
      // Select only jobs that still need this type of task
      HashSet<JobInfo> jobsLeft = new HashSet<JobInfo>();
      for (Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
        JobInProgress job = entry.getKey();
        JobInfo info = entry.getValue();
        if (isRunnable(job) && runnableTasks(job, type) > 0) {
          jobsLeft.add(info);
        }
      }
      double slotsLeft = getTotalSlots(type, clusterStatus);
      while (!jobsLeft.isEmpty()) {
        double totalWeight = 0;
        for (JobInfo info: jobsLeft) {
          double weight = (type == TaskType.MAP ?
              info.mapWeight : info.reduceWeight);
          totalWeight += weight;
        }
        boolean recomputeSlots = false;
        double oldSlots = slotsLeft; // Copy slotsLeft so we can modify it
        for (Iterator<JobInfo> iter = jobsLeft.iterator(); iter.hasNext();) {
          JobInfo info = iter.next();
          double minSlots = (type == TaskType.MAP ?
              info.minMaps : info.minReduces);
          double weight = (type == TaskType.MAP ?
              info.mapWeight : info.reduceWeight);
          double fairShare = weight / totalWeight * oldSlots;
          if (minSlots > fairShare) {
            // Job needs more slots than its fair share; give it its minSlots,
            // remove it from the list, and set recomputeSlots = true to 
            // remember that we must loop again to redistribute unassigned slots
            if (type == TaskType.MAP)
              info.mapFairShare = minSlots;
            else
              info.reduceFairShare = minSlots;
            slotsLeft -= minSlots;
            iter.remove();
            recomputeSlots = true;
          }
        }
        if (!recomputeSlots) {
          // All minimums are met. Give each job its fair share of excess slots.
          for (JobInfo info: jobsLeft) {
            double weight = (type == TaskType.MAP ?
                info.mapWeight : info.reduceWeight);
            double fairShare = weight / totalWeight * oldSlots;
            if (type == TaskType.MAP)
              info.mapFairShare = fairShare;
            else
              info.reduceFairShare = fairShare;
          }
          break;
        }
      }
    }
  }

  private double calculateRawWeight(JobInProgress job, TaskType taskType) {
    if (!isRunnable(job)) {
      return 0;
    } else {
      double weight = 1.0;
      if (sizeBasedWeight) {
        // Set weight based on runnable tasks
        weight = Math.log1p(runnableTasks(job, taskType)) / Math.log(2);
      }
      weight *= getPriorityFactor(job.getPriority());
      if (weightAdjuster != null) {
        // Run weight through the user-supplied weightAdjuster
        weight = weightAdjuster.adjustWeight(job, taskType, weight);
      }
      return weight;
    }
  }

  private double getPriorityFactor(JobPriority priority) {
    switch (priority) {
    case VERY_HIGH: return 4.0;
    case HIGH:      return 2.0;
    case NORMAL:    return 1.0;
    case LOW:       return 0.5;
    default:        return 0.25; // priority = VERY_LOW
    }
  }
  
  public PoolManager getPoolManager() {
    return poolMgr;
  }

  private int getTotalSlots(TaskType type, ClusterStatus clusterStatus) {
    return (type == TaskType.MAP ?
      clusterStatus.getMaxMapTasks() : clusterStatus.getMaxReduceTasks());
  }

  /**
   * Update the preemption JobInfo fields for all jobs, i.e. the times since
   * each job last was at its guaranteed share and at > 1/2 of its fair share
   * for each type of task.
   */
  private void updatePreemptionVariables() {
    long now = clock.getTime();
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      if (job.getStatus().getRunState() != JobStatus.RUNNING) {
        // Job is still in PREP state and tasks aren't initialized. Count it as
        // both at min and fair share since we shouldn't start any timeouts now.
        info.lastTimeAtMapMinShare = now;
        info.lastTimeAtReduceMinShare = now;
        info.lastTimeAtMapHalfFairShare = now;
        info.lastTimeAtReduceHalfFairShare = now;
      } else {
        if (!isStarvedForMinShare(job, TaskType.MAP))
          info.lastTimeAtMapMinShare = now;
        if (!isStarvedForMinShare(job, TaskType.REDUCE))
          info.lastTimeAtReduceMinShare = now;
        if (!isStarvedForFairShare(job, TaskType.MAP))
          info.lastTimeAtMapHalfFairShare = now;
        if (!isStarvedForFairShare(job, TaskType.REDUCE))
          info.lastTimeAtReduceHalfFairShare = now;
      }
      eventLog.log("PREEMPT_VARS", job.getJobID(),
          now - info.lastTimeAtMapMinShare,
          now - info.lastTimeAtMapHalfFairShare);
    }
  }

  /**
   * Is a job below its min share for the given task type?
   */
  boolean isStarvedForMinShare(JobInProgress job, TaskType taskType) {
    return runningTasks(job, taskType) < minTasks(job, taskType);
  }
  
  /**
   * Is a job being starved for fair share for the given task type?
   * This is defined as being below half its fair share *and* having a
   * positive deficit.
   */
  boolean isStarvedForFairShare(JobInProgress job, TaskType type) {
    int desiredFairShare = (int) Math.floor(Math.min(
        fairTasks(job, type) / 2, neededTasks(job, type)));
    return (runningTasks(job, type) < desiredFairShare &&
            deficit(job, type) > 0);
  }
  
  /**
   * Check for jobs that need tasks preempted, either because they have been
   * below their guaranteed share for their pool's preemptionTimeout or they
   * have been below half their fair share for the fairSharePreemptionTimeout.
   * If such jobs exist, compute how many tasks of each type need to be
   * preempted and then select the right ones using selectTasksToPreempt.
   * 
   * This method computes and logs the number of tasks we want to preempt even
   * if preemption is disabled, for debugging purposes.
   */
  protected void preemptTasksIfNecessary() {
    if (!preemptionEnabled || useFifo)
      return;
    
    long curTime = clock.getTime();
    if (curTime - lastPreemptCheckTime < preemptionInterval)
      return;
    lastPreemptCheckTime = curTime;
    
    // Acquire locks on both the JobTracker (task tracker manager) and this
    // because we might need to call some JobTracker methods (killTask).
    synchronized (taskTrackerManager) {
      synchronized (this) {
        List<JobInProgress> jobs = new ArrayList<JobInProgress>(infos.keySet());
        for (TaskType type: MAP_AND_REDUCE) {
          int tasksToPreempt = 0;
          for (JobInProgress job: jobs) {
            tasksToPreempt += tasksToPreempt(job, type, curTime);
          }
          if (tasksToPreempt > 0) {
            eventLog.log("SHOULD_PREEMPT", type, tasksToPreempt);
            if (!onlyLogPreemption) {
              // Actually preempt the tasks. The policy for this is to pick
              // tasks from jobs that are above their min share and have very 
              // negative deficits (meaning they've been over-scheduled). 
              // However, we also want to minimize the amount of computation
              // wasted by preemption, so prefer tasks that started recently.
              // We go through all jobs in order of deficit (highest first), 
              // and for each job, we preempt tasks in order of start time 
              // until we hit either minTasks or fairTasks tasks left (so as
              // not to create a new starved job).
              Collections.sort(jobs, new DeficitComparator(type));
              for (int i = jobs.size() - 1; i >= 0; i--) {
                JobInProgress job = jobs.get(i);
                int tasksPreempted = preemptTasks(job, type, tasksToPreempt);
                tasksToPreempt -= tasksPreempted;
                if (tasksToPreempt == 0) break;
              }
            }
          }
        }
      }
    }
  }

  /**
   * Count how many tasks of a given type the job needs to preempt, if any.
   * If the job has been below its min share for at least its pool's preemption
   * timeout, it should preempt the difference between its current share and
   * this min share. If it has been below half its fair share for at least the
   * fairSharePreemptionTimeout, it should preempt enough tasks to get up to
   * its full fair share. If both situations hold, we preempt the max of the
   * two amounts (this shouldn't happen unless someone sets the timeouts to
   * be identical for some reason).
   */
  protected int tasksToPreempt(JobInProgress job, TaskType type, long curTime) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    String pool = poolMgr.getPoolName(job);
    long minShareTimeout = poolMgr.getMinSharePreemptionTimeout(pool);
    long fairShareTimeout = poolMgr.getFairSharePreemptionTimeout();
    int tasksDueToMinShare = 0;
    int tasksDueToFairShare = 0;
    if (type == TaskType.MAP) {
      if (curTime - info.lastTimeAtMapMinShare > minShareTimeout) {
        tasksDueToMinShare = info.minMaps - info.runningMaps;
      }
      if (curTime - info.lastTimeAtMapHalfFairShare > fairShareTimeout) {
        double fairShare = Math.min(info.mapFairShare, info.neededMaps);
        tasksDueToFairShare = (int) (fairShare - info.runningMaps);
      }
    } else { // type == TaskType.REDUCE
      if (curTime - info.lastTimeAtReduceMinShare > minShareTimeout) {
        tasksDueToMinShare = info.minReduces - info.runningReduces;
      }
      if (curTime - info.lastTimeAtReduceHalfFairShare > fairShareTimeout) {
        double fairShare = Math.min(info.reduceFairShare, info.neededReduces);
        tasksDueToFairShare = (int) (fairShare - info.runningReduces);
      }
    }
    int tasksToPreempt = Math.max(tasksDueToMinShare, tasksDueToFairShare);
    if (tasksToPreempt > 0) {
      String message = "Should preempt " + tasksToPreempt + " " 
          + type + " tasks for " + job.getJobID() 
          + ": tasksDueToMinShare = " + tasksDueToMinShare
          + ", tasksDueToFairShare = " + tasksDueToFairShare;
      eventLog.log("INFO", message);
      LOG.info(message);
    }
    return tasksToPreempt;
  }

  /**
   * Preempt up to maxToPreempt tasks of the given type from the given job,
   * without having it go below its min share or below half its fair share.
   * Selects the tasks so as to preempt the least recently launched one first,
   * thus minimizing wasted compute time. Returns the number of tasks preempted.
   */
  private int preemptTasks(JobInProgress job, TaskType type, int maxToPreempt) {
    // Figure out how many tasks to preempt. NOTE: We use the runningTasks, etc
    // values in JobInfo rather than re-counting them, but this should be safe
    // because we are being called only inside update(), which has a lock on
    // the JobTracker, so all the values are fresh.
    int desiredFairShare = (int) Math.floor(Math.min(
        fairTasks(job, type) / 2, neededTasks(job, type)));
    int tasksToLeave = Math.max(minTasks(job, type), desiredFairShare);
    int tasksToPreempt = Math.min(
        maxToPreempt, runningTasks(job, type) - tasksToLeave);
    if (tasksToPreempt == 0)
      return 0;
    // Create a list of all running TaskInProgress'es in the job
    List<TaskInProgress> tips = new ArrayList<TaskInProgress>();
    if (type == TaskType.MAP) {
      // Jobs may have both "non-local maps" which have a split with no locality
      // info (e.g. the input file is not in HDFS), and maps with locality info,
      // which are stored in the runningMapCache map from location to task list
      tips.addAll(job.nonLocalRunningMaps);
      for (Set<TaskInProgress> set: job.runningMapCache.values()) {
        tips.addAll(set);
      }
    }
    else {
      tips.addAll(job.runningReduces);
    }
    // Get the active TaskStatus'es for each TaskInProgress (there may be
    // more than one if the task has multiple copies active due to speculation)
    List<TaskStatus> statuses = new ArrayList<TaskStatus>();
    for (TaskInProgress tip: tips) {
      for (TaskAttemptID id: tip.getActiveTasks().keySet()) {
        statuses.add(tip.getTaskStatus(id));
      }
    }
    // Sort the statuses in order of start time, with the latest launched first
    Collections.sort(statuses, new Comparator<TaskStatus>() {
      public int compare(TaskStatus t1, TaskStatus t2) {
        return (int) Math.signum(t2.getStartTime() - t1.getStartTime());
      }
    });
    // Preempt the tasks in order of start time until we've done enough
    int numKilled = 0;
    for (int i = 0; i < tasksToPreempt; i++) {
      if (i > statuses.size() - tasksToLeave) {
        // Sanity check in case we computed maxToPreempt incorrectly due to
        // stale data in JobInfos. Shouldn't happen if we are called from update.
        LOG.error("Stale task counts in the JobInfos in preemptTasks - "
            + "probaly due to calling preemptTasks() from outside update(). ");
        break;
      }
      TaskStatus status = statuses.get(i);
      eventLog.log("PREEMPT", status.getTaskID(), status.getTaskTracker());
      try {
        taskTrackerManager.killTask(status.getTaskID(), false);
        numKilled++;
      } catch (IOException e) {
        LOG.error("Failed to kill task " + status.getTaskID(), e);
      }
    }
    return numKilled;
  }

  public synchronized boolean getUseFifo() {
    return useFifo;
  }
  
  public synchronized void setUseFifo(boolean useFifo) {
    this.useFifo = useFifo;
  }
  
  // Getter methods for reading JobInfo values based on TaskType, safely
  // returning 0's for jobs with no JobInfo present.

  protected int neededTasks(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.neededMaps : info.neededReduces;
  }
  
  protected int runningTasks(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.runningMaps : info.runningReduces;
  }

  protected int runnableTasks(JobInProgress job, TaskType type) {
    return neededTasks(job, type) + runningTasks(job, type);
  }

  protected int minTasks(JobInProgress job, TaskType type) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (type == TaskType.MAP) ? info.minMaps : info.minReduces;
  }

  protected double fairTasks(JobInProgress job, TaskType type) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (type == TaskType.MAP) ? info.mapFairShare : info.reduceFairShare;
  }

  protected double weight(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (taskType == TaskType.MAP ? info.mapWeight : info.reduceWeight);
  }

  protected double deficit(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.mapDeficit : info.reduceDeficit;
  }

  protected boolean isRunnable(JobInProgress job) {
    JobInfo info = infos.get(job);
    if (info == null) return false;
    return info.runnable;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    Pool myJobPool = poolMgr.getPool(queueName);
    return myJobPool.getJobs();
  }

  protected void dumpIfNecessary() {
    long now = clock.getTime();
    long timeDelta = now - lastDumpTime;
    if (timeDelta > dumpInterval && eventLog.isEnabled()) {
      dump();
      lastDumpTime = now;
    }
  }

  /**
   * Dump scheduler state to the fairscheduler log.
   */
  private synchronized void dump() {
    synchronized (eventLog) {
      eventLog.log("BEGIN_DUMP");
      // List jobs in order of submit time
      ArrayList<JobInProgress> jobs = 
        new ArrayList<JobInProgress>(infos.keySet());
      Collections.sort(jobs, new Comparator<JobInProgress>() {
        public int compare(JobInProgress j1, JobInProgress j2) {
          return (int) Math.signum(j1.getStartTime() - j2.getStartTime());
        }
      });
      // Dump info for each job
      for (JobInProgress job: jobs) {
        JobProfile profile = job.getProfile();
        JobInfo info = infos.get(job);
        eventLog.log("JOB",
            profile.getJobID(), profile.name, profile.user,
            job.getPriority(), poolMgr.getPoolName(job),
            job.numMapTasks, info.runningMaps, info.neededMaps, 
            info.mapFairShare, info.mapWeight, info.mapDeficit,
            job.numReduceTasks, info.runningReduces, info.neededReduces, 
            info.reduceFairShare, info.reduceWeight, info.reduceDeficit);
      }
      // List pools in alphabetical order
      List<Pool> pools = new ArrayList<Pool>(poolMgr.getPools());
      Collections.sort(pools, new Comparator<Pool>() {
        public int compare(Pool p1, Pool p2) {
          if (p1.isDefaultPool())
            return 1;
          else if (p2.isDefaultPool())
            return -1;
          else return p1.getName().compareTo(p2.getName());
        }});
      for (Pool pool: pools) {
        int runningMaps = 0;
        int runningReduces = 0;
        for (JobInProgress job: pool.getJobs()) {
          JobInfo info = infos.get(job);
          if (info != null) {
            runningMaps += info.runningMaps;
            runningReduces += info.runningReduces;
          }
        }
        String name = pool.getName();
        eventLog.log("POOL",
            name, poolMgr.getPoolWeight(name), pool.getJobs().size(),
            poolMgr.getAllocation(name, TaskType.MAP), runningMaps,
            poolMgr.getAllocation(name, TaskType.REDUCE), runningReduces);
      }
      // Dump info for each pool
      eventLog.log("END_DUMP");
    }
  }
}
