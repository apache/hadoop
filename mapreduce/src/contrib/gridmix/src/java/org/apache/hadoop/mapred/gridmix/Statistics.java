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
import org.apache.hadoop.mapred.gridmix.Gridmix.Component;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Component collecting the stats required by other components
 * to make decisions.
 * Single thread Collector tries to collec the stats.
 * Each of thread poll updates certain datastructure(Currently ClusterStats).
 * Components interested in these datastructure, need to register.
 * StatsCollector notifies each of the listeners.
 */
public class Statistics implements Component<Job> {
  public static final Log LOG = LogFactory.getLog(Statistics.class);

  private final StatCollector statistics = new StatCollector();
  private final Cluster cluster;

  //List of cluster status listeners.
  private final List<StatListener<ClusterStats>> clusterStatlisteners =
    new ArrayList<StatListener<ClusterStats>>();

  //List of job status listeners.
  private final List<StatListener<JobStats>> jobStatListeners =
    new ArrayList<StatListener<JobStats>>();

  private int completedJobsInCurrentInterval = 0;
  private final int jtPollingInterval;
  private volatile boolean shutdown = false;
  private final int maxJobCompletedInInterval;
  private static final String MAX_JOBS_COMPLETED_IN_POLL_INTERVAL_KEY =
    "gridmix.max-jobs-completed-in-poll-interval";
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition jobCompleted = lock.newCondition();
  private final CountDownLatch startFlag;

  public Statistics(
    Configuration conf, int pollingInterval, CountDownLatch startFlag)
    throws IOException {
    this.cluster = new Cluster(conf);
    this.jtPollingInterval = pollingInterval;
    maxJobCompletedInInterval = conf.getInt(
      MAX_JOBS_COMPLETED_IN_POLL_INTERVAL_KEY, 1);
    this.startFlag = startFlag;
  }

  /**
   * Used by JobMonitor to add the completed job.
   */
  @Override
  public void add(Job job) {
    //This thread will be notified initially by jobmonitor incase of
    //data generation. Ignore that as we are getting once the input is
    //generated.
    if (!statistics.isAlive()) {
      return;
    }
    completedJobsInCurrentInterval++;
    //check if we have reached the maximum level of job completions.
    if (completedJobsInCurrentInterval >= maxJobCompletedInInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          " Reached maximum limit of jobs in a polling interval " +
            completedJobsInCurrentInterval);
      }
      completedJobsInCurrentInterval = 0;
      lock.lock();
      try {
        //Job is completed notify all the listeners.
        if (jobStatListeners.size() > 0) {
          for (StatListener<JobStats> l : jobStatListeners) {
            JobStats stats = new JobStats();
            stats.setCompleteJob(job);
            l.update(stats);
          }
        }
        this.jobCompleted.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  //TODO: We have just 2 types of listeners as of now . If no of listeners
  //increase then we should move to map kind of model.

  public void addClusterStatsObservers(StatListener<ClusterStats> listener) {
    clusterStatlisteners.add(listener);
  }

  public void addJobStatsListeners(StatListener<JobStats> listener) {
    this.jobStatListeners.add(listener);
  }

  /**
   * Attempt to start the service.
   */
  @Override
  public void start() {
    statistics.start();
  }

  private class StatCollector extends Thread {

    StatCollector() {
      super("StatsCollectorThread");
    }

    public void run() {
      try {
        startFlag.await();
        if (Thread.currentThread().isInterrupted()) {
          return;
        }
      } catch (InterruptedException ie) {
        LOG.error(
          "Statistics Error while waiting for other threads to get ready ", ie);
        return;
      }
      while (!shutdown) {
        lock.lock();
        try {
          jobCompleted.await(jtPollingInterval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
          LOG.error(
            "Statistics interrupt while waiting for polling " + ie.getCause(),
            ie);
          return;
        } finally {
          lock.unlock();
        }

        //Fetch cluster data only if required.i.e .
        // only if there are clusterStats listener.
        if (clusterStatlisteners.size() > 0) {
          try {
            ClusterMetrics clusterStatus = cluster.getClusterStatus();
            Job[] allJobs = cluster.getAllJobs();
            List<Job> runningWaitingJobs = getRunningWaitingJobs(allJobs);
            updateAndNotifyClusterStatsListeners(
              clusterStatus, runningWaitingJobs);
          } catch (IOException e) {
            LOG.error(
              "Statistics io exception while polling JT ", e);
            return;
          } catch (InterruptedException e) {
            LOG.error(
              "Statistics interrupt exception while polling JT ", e);
            return;
          }
        }
      }
    }

    private void updateAndNotifyClusterStatsListeners(
      ClusterMetrics clusterMetrics, List<Job> runningWaitingJobs) {
      ClusterStats stats = ClusterStats.getClusterStats();
      stats.setClusterMetric(clusterMetrics);
      stats.setRunningWaitingJobs(runningWaitingJobs);
      for (StatListener<ClusterStats> listener : clusterStatlisteners) {
        listener.update(stats);
      }
    }


    /**
     * From the list of Jobs , give the list of jobs whoes state is eigther
     * PREP or RUNNING.
     *
     * @param allJobs
     * @return
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    private List<Job> getRunningWaitingJobs(Job[] allJobs)
      throws IOException, InterruptedException {
      List<Job> result = new ArrayList<Job>();
      for (Job job : allJobs) {
        //TODO Check if job.getStatus() makes a rpc call
        org.apache.hadoop.mapreduce.JobStatus.State state =
          job.getStatus().getState();
        if (org.apache.hadoop.mapreduce.JobStatus.State.PREP.equals(state) ||
          org.apache.hadoop.mapreduce.JobStatus.State.RUNNING.equals(state)) {
          result.add(job);
        }
      }
      return result;
    }
  }

  /**
   * Wait until the service completes. It is assumed that either a
   * {@link #shutdown} or {@link #abort} has been requested.
   */
  @Override
  public void join(long millis) throws InterruptedException {
    statistics.join(millis);
  }

  @Override
  public void shutdown() {
    shutdown = true;
    clusterStatlisteners.clear();
    jobStatListeners.clear();
    statistics.interrupt();
  }

  @Override
  public void abort() {
    shutdown = true;
    clusterStatlisteners.clear();
    jobStatListeners.clear();
    statistics.interrupt();
  }

  /**
   * Class to encapsulate the JobStats information.
   * Current we just need information about completedJob.
   * TODO: In future we need to extend this to send more information.
   */
  static class JobStats {
    private Job completedJob;

    public Job getCompleteJob() {
      return completedJob;
    }

    public void setCompleteJob(Job job) {
      this.completedJob = job;
    }
  }

  static class ClusterStats {
    private ClusterMetrics status = null;
    private static ClusterStats stats = new ClusterStats();
    private List<Job> runningWaitingJobs;

    private ClusterStats() {

    }

    /**
     * @return stats
     */
    static ClusterStats getClusterStats() {
      return stats;
    }

    /**
     * @param metrics
     */
    void setClusterMetric(ClusterMetrics metrics) {
      this.status = metrics;
    }

    /**
     * @return metrics
     */
    public ClusterMetrics getStatus() {
      return status;
    }

    /**
     * @return runningWatitingJobs
     */
    public List<Job> getRunningWaitingJobs() {
      return runningWaitingJobs;
    }

    public void setRunningWaitingJobs(List<Job> runningWaitingJobs) {
      this.runningWaitingJobs = runningWaitingJobs;
    }

  }
}
