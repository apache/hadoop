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
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link TaskScheduler} that implements fair sharing.
 */
public class FairScheduler extends TaskScheduler {
  /** How often fair shares are re-calculated */
  public static final long UPDATE_INTERVAL = 500;
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.FairScheduler");
  
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
  private Clock clock;
  private boolean runBackgroundUpdates; // Can be set to false for testing
  private EagerTaskInitializationListener eagerInitListener;
  private JobListener jobListener;
  
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
  }
  
  /**
   * A clock class - can be mocked out for testing.
   */
  static class Clock {
    long getTime() {
      return System.currentTimeMillis();
    }
  }
  
  public FairScheduler() {
    this(new Clock(), true);
  }
  
  /**
   * Constructor used for tests, which can change the clock and disable updates.
   */
  protected FairScheduler(Clock clock, boolean runBackgroundUpdates) {
    this.clock = clock;
    this.runBackgroundUpdates = runBackgroundUpdates;
    this.jobListener = new JobListener();
  }

  @Override
  public void start() {
    try {
      Configuration conf = getConf();
      this.eagerInitListener = new EagerTaskInitializationListener(conf);
      eagerInitListener.setTaskTrackerManager(taskTrackerManager);
      eagerInitListener.start();
      taskTrackerManager.addJobInProgressListener(eagerInitListener);
      taskTrackerManager.addJobInProgressListener(jobListener);
      poolMgr = new PoolManager(conf);
      loadMgr = (LoadManager) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.loadmanager", 
              CapBasedLoadManager.class, LoadManager.class), conf);
      loadMgr.setTaskTrackerManager(taskTrackerManager);
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
      assignMultiple = conf.getBoolean("mapred.fairscheduler.assignmultiple",
          false);
      sizeBasedWeight = conf.getBoolean("mapred.fairscheduler.sizebasedweight",
          false);
      initialized = true;
      running = true;
      lastUpdateTime = clock.getTime();
      // Start a thread to update deficits every UPDATE_INTERVAL
      if (runBackgroundUpdates)
        new UpdateThread().start();
      // Register servlet with JobTracker's Jetty server
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        HttpServer infoServer = jobTracker.infoServer;
        infoServer.setAttribute("scheduler", this);
        infoServer.addServlet("scheduler", "/scheduler",
            FairSchedulerServlet.class);
      }
    } catch (Exception e) {
      // Can't load one of the managers - crash the JobTracker now while it is
      // starting up so that the user notices.
      throw new RuntimeException("Failed to start FairScheduler", e);
    }
    LOG.info("Successfully configured FairScheduler");
  }

  @Override
  public void terminate() throws IOException {
    running = false;
    if (jobListener != null)
      taskTrackerManager.removeJobInProgressListener(jobListener);
    if (eagerInitListener != null)
      taskTrackerManager.removeJobInProgressListener(eagerInitListener);
  }
  
  /**
   * Used to listen for jobs added/removed by our {@link TaskTrackerManager}.
   */
  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) {
      synchronized (FairScheduler.this) {
        poolMgr.addJob(job);
        JobInfo info = new JobInfo();
        infos.put(job, info);
        update();
      }
    }
    
    @Override
    public void jobRemoved(JobInProgress job) {
      synchronized (FairScheduler.this) {
        poolMgr.removeJob(job);
        infos.remove(job);
      }
    }
  
    @Override
    public void jobUpdated(JobChangeEvent event) {
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
          Thread.sleep(UPDATE_INTERVAL);
          update();
        } catch (Exception e) {
          LOG.error("Failed to update fair share calculations", e);
        }
      }
    }
  }
  
  @Override
  public synchronized List<Task> assignTasks(TaskTrackerStatus tracker)
      throws IOException {
    if (!initialized) // Don't try to assign tasks if we haven't yet started up
      return null;
    
    // Reload allocations file if it hasn't been loaded in a while
    poolMgr.reloadAllocsIfNecessary();
    
    // Compute total runnable maps and reduces
    int runnableMaps = 0;
    int runnableReduces = 0;
    for (JobInProgress job: infos.keySet()) {
      runnableMaps += runnableTasks(job, TaskType.MAP);
      runnableReduces += runnableTasks(job, TaskType.REDUCE);
    }

    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    // Compute total map/reduce slots
    // In the future we can precompute this if the Scheduler becomes a 
    // listener of tracker join/leave events.
    int totalMapSlots = getTotalSlots(TaskType.MAP, clusterStatus);
    int totalReduceSlots = getTotalSlots(TaskType.REDUCE, clusterStatus);
    
    // Scan to see whether any job needs to run a map, then a reduce
    ArrayList<Task> tasks = new ArrayList<Task>();
    TaskType[] types = new TaskType[] {TaskType.MAP, TaskType.REDUCE};
    for (TaskType taskType: types) {
      boolean canAssign = (taskType == TaskType.MAP) ? 
          loadMgr.canAssignMap(tracker, runnableMaps, totalMapSlots) :
          loadMgr.canAssignReduce(tracker, runnableReduces, totalReduceSlots);
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
          Task task = (taskType == TaskType.MAP ? 
              taskSelector.obtainNewMapTask(tracker, job) :
              taskSelector.obtainNewReduceTask(tracker, job));
          if (task != null) {
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
            tasks.add(task);
            if (!assignMultiple)
              return tasks;
            break;
          }
        }
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
      long deficitDif;
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
      if (job.getStatus().getRunState() != JobStatus.RUNNING)
        continue; // Job is still in PREP state and tasks aren't initialized
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
      for (final TaskType type: TaskType.values()) {
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
    for (TaskType type: TaskType.values()) {
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
}
