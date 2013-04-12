/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.gridmix.Statistics.ClusterStats;
import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class StressJobFactory extends JobFactory<Statistics.ClusterStats> {
  public static final Log LOG = LogFactory.getLog(StressJobFactory.class);

  private final LoadStatus loadStatus = new LoadStatus();
  /**
   * The minimum ratio between pending+running map tasks (aka. incomplete map
   * tasks) and cluster map slot capacity for us to consider the cluster is
   * overloaded. For running maps, we only count them partially. Namely, a 40%
   * completed map is counted as 0.6 map tasks in our calculation.
   */
  private static final float OVERLOAD_MAPTASK_MAPSLOT_RATIO = 2.0f;
  public static final String CONF_OVERLOAD_MAPTASK_MAPSLOT_RATIO=
      "gridmix.throttle.maps.task-to-slot-ratio";
  final float overloadMapTaskMapSlotRatio;

  /**
   * The minimum ratio between pending+running reduce tasks (aka. incomplete
   * reduce tasks) and cluster reduce slot capacity for us to consider the
   * cluster is overloaded. For running reduces, we only count them partially.
   * Namely, a 40% completed reduce is counted as 0.6 reduce tasks in our
   * calculation.
   */
  private static final float OVERLOAD_REDUCETASK_REDUCESLOT_RATIO = 2.5f;
  public static final String CONF_OVERLOAD_REDUCETASK_REDUCESLOT_RATIO=
    "gridmix.throttle.reduces.task-to-slot-ratio";
  final float overloadReduceTaskReduceSlotRatio;

  /**
   * The maximum share of the cluster's mapslot capacity that can be counted
   * toward a job's incomplete map tasks in overload calculation.
   */
  private static final float MAX_MAPSLOT_SHARE_PER_JOB=0.1f;
  public static final String CONF_MAX_MAPSLOT_SHARE_PER_JOB=
    "gridmix.throttle.maps.max-slot-share-per-job";  
  final float maxMapSlotSharePerJob;
  
  /**
   * The maximum share of the cluster's reduceslot capacity that can be counted
   * toward a job's incomplete reduce tasks in overload calculation.
   */
  private static final float MAX_REDUCESLOT_SHARE_PER_JOB=0.1f;
  public static final String CONF_MAX_REDUCESLOT_SHARE_PER_JOB=
    "gridmix.throttle.reducess.max-slot-share-per-job";  
  final float maxReduceSlotSharePerJob;

  /**
   * The ratio of the maximum number of pending+running jobs over the number of
   * task trackers.
   */
  private static final float MAX_JOB_TRACKER_RATIO=1.0f;
  public static final String CONF_MAX_JOB_TRACKER_RATIO=
    "gridmix.throttle.jobs-to-tracker-ratio";  
  final float maxJobTrackerRatio;

  /**
   * Creating a new instance does not start the thread.
   *
   * @param submitter   Component to which deserialized jobs are passed
   * @param jobProducer Stream of job traces with which to construct a
   *                    {@link org.apache.hadoop.tools.rumen.ZombieJobProducer}
   * @param scratch     Directory into which to write output from simulated jobs
   * @param conf        Config passed to all jobs to be submitted
   * @param startFlag   Latch released from main to start pipeline
   * @throws java.io.IOException
   */
  public StressJobFactory(
    JobSubmitter submitter, JobStoryProducer jobProducer, Path scratch,
    Configuration conf, CountDownLatch startFlag, UserResolver resolver)
    throws IOException {
    super(
      submitter, jobProducer, scratch, conf, startFlag, resolver);
    overloadMapTaskMapSlotRatio = conf.getFloat(
        CONF_OVERLOAD_MAPTASK_MAPSLOT_RATIO, OVERLOAD_MAPTASK_MAPSLOT_RATIO);
    overloadReduceTaskReduceSlotRatio = conf.getFloat(
        CONF_OVERLOAD_REDUCETASK_REDUCESLOT_RATIO, 
        OVERLOAD_REDUCETASK_REDUCESLOT_RATIO);
    maxMapSlotSharePerJob = conf.getFloat(
        CONF_MAX_MAPSLOT_SHARE_PER_JOB, MAX_MAPSLOT_SHARE_PER_JOB);
    maxReduceSlotSharePerJob = conf.getFloat(
        CONF_MAX_REDUCESLOT_SHARE_PER_JOB, MAX_REDUCESLOT_SHARE_PER_JOB);
    maxJobTrackerRatio = conf.getFloat(
        CONF_MAX_JOB_TRACKER_RATIO, MAX_JOB_TRACKER_RATIO);
  }

  public Thread createReaderThread() {
    return new StressReaderThread("StressJobFactory");
  }

  /*
  * Worker thread responsible for reading descriptions, assigning sequence
  * numbers, and normalizing time.
  */
  private class StressReaderThread extends Thread {

    public StressReaderThread(String name) {
      super(name);
    }

    /**
     * STRESS: Submits the job in STRESS mode.
     * while(JT is overloaded) {
     * wait();
     * }
     * If not overloaded , get number of slots available.
     * Keep submitting the jobs till ,total jobs  is sufficient to
     * load the JT.
     * That is submit  (Sigma(no of maps/Job)) > (2 * no of slots available)
     */
    public void run() {
      try {
        startFlag.await();
        if (Thread.currentThread().isInterrupted()) {
          return;
        }
        LOG.info("START STRESS @ " + System.currentTimeMillis());
        while (!Thread.currentThread().isInterrupted()) {
          try {
            while (loadStatus.overloaded()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Cluster overloaded in run! Sleeping...");
              }
              // sleep 
              try {
                Thread.sleep(1000);
              } catch (InterruptedException ie) {
                return;
              }
            }

            while (!loadStatus.overloaded()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Cluster underloaded in run! Stressing...");
              }
              try {
                //TODO This in-line read can block submission for large jobs.
                final JobStory job = getNextJobFiltered();
                if (null == job) {
                  return;
                }
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Job Selected: " + job.getJobID());
                }
                submitter.add(
                  jobCreator.createGridmixJob(
                    conf, 0L, job, scratch, 
                    userResolver.getTargetUgi(
                      UserGroupInformation.createRemoteUser(job.getUser())), 
                    sequence.getAndIncrement()));
                // TODO: We need to take care of scenario when one map/reduce
                // takes more than 1 slot.
                
                // Lock the loadjob as we are making updates
                int incompleteMapTasks = (int) calcEffectiveIncompleteMapTasks(
                                                 loadStatus.getMapCapacity(), 
                                                 job.getNumberMaps(), 0.0f);
                loadStatus.decrementMapLoad(incompleteMapTasks);
                
                int incompleteReduceTasks = 
                  (int) calcEffectiveIncompleteReduceTasks(
                          loadStatus.getReduceCapacity(), 
                          job.getNumberReduces(), 0.0f);
                loadStatus.decrementReduceLoad(incompleteReduceTasks);
                  
                loadStatus.decrementJobLoad(1);
              } catch (IOException e) {
                LOG.error("Error while submitting the job ", e);
                error = e;
                return;
              }

            }
          } finally {
            // do nothing
          }
        }
      } catch (InterruptedException e) {
        return;
      } finally {
        IOUtils.cleanup(null, jobProducer);
      }
    }
  }

  /**
   * STRESS Once you get the notification from StatsCollector.Collect the
   * clustermetrics. Update current loadStatus with new load status of JT.
   *
   * @param item
   */
  @Override
  public void update(Statistics.ClusterStats item) {
    ClusterStatus clusterMetrics = item.getStatus();
    try {
      checkLoadAndGetSlotsToBackfill(item, clusterMetrics);
    } catch (Exception e) {
      LOG.error("Couldn't get the new Status",e);
    }
  }

  float calcEffectiveIncompleteMapTasks(int mapSlotCapacity,
      int numMaps, float mapProgress) {
    float maxEffIncompleteMapTasks = Math.max(1.0f, mapSlotCapacity
        * maxMapSlotSharePerJob);
    float mapProgressAdjusted = Math.max(Math.min(mapProgress, 1.0f), 0.0f);
    return Math.min(maxEffIncompleteMapTasks, 
                    numMaps * (1.0f - mapProgressAdjusted));
  }

  float calcEffectiveIncompleteReduceTasks(int reduceSlotCapacity,
      int numReduces, float reduceProgress) {
    float maxEffIncompleteReduceTasks = Math.max(1.0f, reduceSlotCapacity
        * maxReduceSlotSharePerJob);
    float reduceProgressAdjusted = 
      Math.max(Math.min(reduceProgress, 1.0f), 0.0f);
    return Math.min(maxEffIncompleteReduceTasks, 
                    numReduces * (1.0f - reduceProgressAdjusted));
  }

  /**
   * We try to use some light-weight mechanism to determine cluster load.
   *
   * @throws java.io.IOException
   */
  private void checkLoadAndGetSlotsToBackfill(
    ClusterStats stats, ClusterStatus clusterStatus) throws IOException, InterruptedException {
    
    // update the max cluster capacity incase its updated
    int mapCapacity = clusterStatus.getMaxMapTasks();
    loadStatus.updateMapCapacity(mapCapacity);
    
    int reduceCapacity = clusterStatus.getMaxReduceTasks();
    
    loadStatus.updateReduceCapacity(reduceCapacity);
    
    int numTrackers = clusterStatus.getTaskTrackers();
    
    int jobLoad = 
      (int) (maxJobTrackerRatio * numTrackers) - stats.getNumRunningJob();
    loadStatus.updateJobLoad(jobLoad);
    if (loadStatus.getJobLoad() <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(System.currentTimeMillis() + " [JobLoad] Overloaded is "
                  + Boolean.TRUE.toString() + " NumJobsBackfill is "
                  + loadStatus.getJobLoad());
      }
      return; // stop calculation because we know it is overloaded.
    }

    float incompleteMapTasks = 0; // include pending & running map tasks.
    for (JobStats job : ClusterStats.getRunningJobStats()) {
      float mapProgress = job.getJob().mapProgress();
      int noOfMaps = job.getNoOfMaps();
      incompleteMapTasks += 
        calcEffectiveIncompleteMapTasks(mapCapacity, noOfMaps, mapProgress);
    }
    
    int mapSlotsBackFill = 
      (int) ((overloadMapTaskMapSlotRatio * mapCapacity) - incompleteMapTasks);
    loadStatus.updateMapLoad(mapSlotsBackFill);
    
    if (loadStatus.getMapLoad() <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(System.currentTimeMillis() + " [MAP-LOAD] Overloaded is "
                  + Boolean.TRUE.toString() + " MapSlotsBackfill is "
                  + loadStatus.getMapLoad());
      }
      return; // stop calculation because we know it is overloaded.
    }

    float incompleteReduceTasks = 0; // include pending & running reduce tasks.
    for (JobStats job : ClusterStats.getRunningJobStats()) {
      // Cached the num-reds value in JobStats
      int noOfReduces = job.getNoOfReds();
      if (noOfReduces > 0) {
        float reduceProgress = job.getJob().reduceProgress();
        incompleteReduceTasks += 
          calcEffectiveIncompleteReduceTasks(reduceCapacity, noOfReduces, 
                                             reduceProgress);
      }
    }
    
    int reduceSlotsBackFill = 
      (int)((overloadReduceTaskReduceSlotRatio * reduceCapacity) 
             - incompleteReduceTasks);
    loadStatus.updateReduceLoad(reduceSlotsBackFill);
    if (loadStatus.getReduceLoad() <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(System.currentTimeMillis() + " [REDUCE-LOAD] Overloaded is "
                  + Boolean.TRUE.toString() + " ReduceSlotsBackfill is "
                  + loadStatus.getReduceLoad());
      }
      return; // stop calculation because we know it is overloaded.
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(System.currentTimeMillis() + " [OVERALL] Overloaded is "
                + Boolean.FALSE.toString() + "Current load Status is " 
                + loadStatus);
    }
  }

  static class LoadStatus {
    /**
     * Additional number of map slots that can be requested before
     * declaring (by Gridmix STRESS mode) the cluster as overloaded. 
     */
    private volatile int mapSlotsBackfill;
    
    /**
     * Determines the total map slot capacity of the cluster.
     */
    private volatile int mapSlotCapacity;
    
    /**
     * Additional number of reduce slots that can be requested before
     * declaring (by Gridmix STRESS mode) the cluster as overloaded.
     */
    private volatile int reduceSlotsBackfill;
    
    /**
     * Determines the total reduce slot capacity of the cluster.
     */
    private volatile int reduceSlotCapacity;

    /**
     * Determines the max count of running jobs in the cluster.
     */
    private volatile int numJobsBackfill;
    
    // set the default to true
    private AtomicBoolean overloaded = new AtomicBoolean(true);

    /**
     * Construct the LoadStatus in an unknown state - assuming the cluster is
     * overloaded by setting numSlotsBackfill=0.
     */
    LoadStatus() {
      mapSlotsBackfill = 0;
      reduceSlotsBackfill = 0;
      numJobsBackfill = 0;
      
      mapSlotCapacity = -1;
      reduceSlotCapacity = -1;
    }
    
    public synchronized int getMapLoad() {
      return mapSlotsBackfill;
    }
    
    public synchronized int getMapCapacity() {
      return mapSlotCapacity;
    }
    
    public synchronized int getReduceLoad() {
      return reduceSlotsBackfill;
    }
    
    public synchronized int getReduceCapacity() {
      return reduceSlotCapacity;
    }
    
    public synchronized int getJobLoad() {
      return numJobsBackfill;
    }
    
    public synchronized void decrementMapLoad(int mapSlotsConsumed) {
      this.mapSlotsBackfill -= mapSlotsConsumed;
      updateOverloadStatus();
    }
    
    public synchronized void decrementReduceLoad(int reduceSlotsConsumed) {
      this.reduceSlotsBackfill -= reduceSlotsConsumed;
      updateOverloadStatus();
    }

    public synchronized void decrementJobLoad(int numJobsConsumed) {
      this.numJobsBackfill -= numJobsConsumed;
      updateOverloadStatus();
    }
    
    public synchronized void updateMapCapacity(int mapSlotsCapacity) {
      this.mapSlotCapacity = mapSlotsCapacity;
      updateOverloadStatus();
    }
    
    public synchronized void updateReduceCapacity(int reduceSlotsCapacity) {
      this.reduceSlotCapacity = reduceSlotsCapacity;
      updateOverloadStatus();
    }
    
    public synchronized void updateMapLoad(int mapSlotsBackfill) {
      this.mapSlotsBackfill = mapSlotsBackfill;
      updateOverloadStatus();
    }
    
    public synchronized void updateReduceLoad(int reduceSlotsBackfill) {
      this.reduceSlotsBackfill = reduceSlotsBackfill;
      updateOverloadStatus();
    }
    
    public synchronized void updateJobLoad(int numJobsBackfill) {
      this.numJobsBackfill = numJobsBackfill;
      updateOverloadStatus();
    }
    
    private synchronized void updateOverloadStatus() {
      overloaded.set((mapSlotsBackfill <= 0) || (reduceSlotsBackfill <= 0)
                     || (numJobsBackfill <= 0));
    }
    
    public synchronized boolean overloaded() {
      return overloaded.get();
    }
    
    public synchronized String toString() {
    // TODO Use StringBuilder instead
      return " Overloaded = " + overloaded()
             + ", MapSlotBackfill = " + mapSlotsBackfill 
             + ", MapSlotCapacity = " + mapSlotCapacity
             + ", ReduceSlotBackfill = " + reduceSlotsBackfill 
             + ", ReduceSlotCapacity = " + reduceSlotCapacity
             + ", NumJobsBackfill = " + numJobsBackfill;
    }
  }

  /**
   * Start the reader thread, wait for latch if necessary.
   */
  @Override
  public void start() {
    LOG.info(" Starting Stress submission ");
    this.rThread.start();
  }

}
