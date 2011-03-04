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
import java.util.concurrent.locks.Condition;

public class StressJobFactory extends JobFactory<Statistics.ClusterStats> {
  public static final Log LOG = LogFactory.getLog(StressJobFactory.class);

  private LoadStatus loadStatus = new LoadStatus();
  private final Condition overloaded = this.lock.newCondition();
  /**
   * The minimum ratio between pending+running map tasks (aka. incomplete map
   * tasks) and cluster map slot capacity for us to consider the cluster is
   * overloaded. For running maps, we only count them partially. Namely, a 40%
   * completed map is counted as 0.6 map tasks in our calculation.
   */
  static final float OVERLOAD_MAPTASK_MAPSLOT_RATIO = 2.0f;

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

    //Setting isOverloaded as true , now JF would wait for atleast first
    //set of ClusterStats based on which it can decide how many job it has
    //to submit.
    this.loadStatus.isOverloaded = true;
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
          lock.lock();
          try {
            while (loadStatus.isOverloaded) {
              //Wait while JT is overloaded.
              try {
                overloaded.await();
              } catch (InterruptedException ie) {
                return;
              }
            }

            int noOfSlotsAvailable = loadStatus.numSlotsBackfill;
            LOG.info("No of slots to be backfilled are " + noOfSlotsAvailable);

            for (int i = 0; i < noOfSlotsAvailable; i++) {
              try {
                final JobStory job = getNextJobFiltered();
                if (null == job) {
                  return;
                }
                //TODO: We need to take care of scenario when one map takes more
                //than 1 slot.
                i += job.getNumberMaps();

                submitter.add(
                  jobCreator.createGridmixJob(
                    conf, 0L, job, scratch, userResolver.getTargetUgi(
                      UserGroupInformation.createRemoteUser(
                        job.getUser())), sequence.getAndIncrement()));
              } catch (IOException e) {
                LOG.error("Error while submitting the job ", e);
                error = e;
                return;
              }

            }
          } finally {
            lock.unlock();
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
    lock.lock();
    try {
      ClusterStatus clusterMetrics = item.getStatus();
      try {
        checkLoadAndGetSlotsToBackfill(item,clusterMetrics);
      } catch (IOException e) {
        LOG.error("Couldn't get the new Status",e);
      }
      overloaded.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * We try to use some light-weight mechanism to determine cluster load.
   *
   * @param stats
   * @param clusterStatus Cluster status
   * @throws java.io.IOException
   */
  private void checkLoadAndGetSlotsToBackfill(
    ClusterStats stats, ClusterStatus clusterStatus) throws IOException {
    // If there are more jobs than number of task trackers, we assume the
    // cluster is overloaded. 
    if (stats.getNumRunningJob() >= clusterStatus.getTaskTrackers()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          System.currentTimeMillis() + " Overloaded is " +
            Boolean.TRUE.toString() + " #runningJobs >= taskTrackerCount (" +
            stats.getNumRunningJob() + " >= " +
            clusterStatus.getTaskTrackers() + " )\n");
      }
      loadStatus.isOverloaded = true;
      loadStatus.numSlotsBackfill = 0;
      return;
    }

    float incompleteMapTasks = 0; // include pending & running map tasks.
    for (JobStats job : ClusterStats.getRunningJobStats()) {
      float mapProgress = job.getJob().mapProgress();
      int noOfMaps = job.getNoOfMaps();
      incompleteMapTasks += (1 - Math.min(mapProgress,1.0))* noOfMaps;
    }

    float overloadedThreshold =
      OVERLOAD_MAPTASK_MAPSLOT_RATIO * clusterStatus.getMaxMapTasks();
    boolean overloaded = incompleteMapTasks > overloadedThreshold;
    String relOp = (overloaded) ? ">" : "<=";
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        System.currentTimeMillis() + " Overloaded is " + Boolean.toString(
          overloaded) + " incompleteMapTasks " + relOp + " " +
          OVERLOAD_MAPTASK_MAPSLOT_RATIO + "*mapSlotCapacity" + "(" +
          incompleteMapTasks + " " + relOp + " " +
          OVERLOAD_MAPTASK_MAPSLOT_RATIO + "*" +
          clusterStatus.getMaxMapTasks() + ")");
    }
    if (overloaded) {
      loadStatus.isOverloaded = true;
      loadStatus.numSlotsBackfill = 0;
    } else {
      loadStatus.isOverloaded = false;
      loadStatus.numSlotsBackfill =
        (int) (overloadedThreshold - incompleteMapTasks);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Current load Status is " + loadStatus);
    }
  }

  static class LoadStatus {
    boolean isOverloaded = false;
    int numSlotsBackfill = -1;

    public String toString() {
      return " is Overloaded " + isOverloaded + " no of slots available " +
        numSlotsBackfill;
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
