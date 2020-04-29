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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.gridmix.Gridmix.Component;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Component collecting the stats required by other components
 * to make decisions.
 * Single thread collector tries to collect the stats (currently cluster stats)
 * and caches it internally.
 * Components interested in these stats need to register themselves and will get
 * notified either on every job completion event or some fixed time interval.
 */
public class Statistics implements Component<Statistics.JobStats> {
  public static final Logger LOG = LoggerFactory.getLogger(Statistics.class);

  private final StatCollector statistics = new StatCollector();
  private JobClient cluster;

  //List of cluster status listeners.
  private final List<StatListener<ClusterStats>> clusterStatlisteners =
    new CopyOnWriteArrayList<StatListener<ClusterStats>>();

  //List of job status listeners.
  private final List<StatListener<JobStats>> jobStatListeners =
    new CopyOnWriteArrayList<StatListener<JobStats>>();

  // A map of job-sequence-id to job-stats of submitted jobs
  private static final Map<Integer, JobStats> submittedJobsMap =
    new ConcurrentHashMap<Integer, JobStats>();
  
  // total number of map tasks submitted
  private static volatile int numMapsSubmitted = 0;

  // total number of reduce tasks submitted
  private static volatile int numReducesSubmitted = 0;
  
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
    final Configuration conf, int pollingInterval, CountDownLatch startFlag)
    throws IOException, InterruptedException {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      this.cluster = ugi.doAs(new PrivilegedExceptionAction<JobClient>() {
        public JobClient run() throws IOException {
          return new JobClient(new JobConf(conf));
        }
      });

    this.jtPollingInterval = pollingInterval;
    maxJobCompletedInInterval = conf.getInt(
      MAX_JOBS_COMPLETED_IN_POLL_INTERVAL_KEY, 1);
    this.startFlag = startFlag;
  }

  /**
   * Generates a job stats.
   */
  public static JobStats generateJobStats(Job job, JobStory jobdesc) {
    int seq = GridmixJob.getJobSeqId(job);
    // bail out if job description is missing for a job to be simulated
    if (seq >= 0 && jobdesc == null) {
      throw new IllegalArgumentException("JobStory not available for job " 
                                         + job.getJobID());
    }
    
    int maps = -1;
    int reds = -1;
    if (jobdesc != null) {
      // Note that the ZombieJob will return a >= 0 value
      maps = jobdesc.getNumberMaps();
      reds = jobdesc.getNumberReduces();
    }
    return new JobStats(maps, reds, job);
  }

  private static void addToNumMapsSubmitted(int numMaps) {
    numMapsSubmitted += numMaps;
  }

  private static void addToNumReducesSubmitted(int numReduces) {
    numReducesSubmitted += numReduces;
  }

  private static void subtractFromNumMapsSubmitted(int numMaps) {
    numMapsSubmitted -= numMaps;
  }

  private static void subtractFromNumReducesSubmitted(int numReduces) {
    numReducesSubmitted -= numReduces;
  }

  /**
   * Add a submitted job for monitoring.
   */
  public void addJobStats(JobStats stats) {
    int seq = GridmixJob.getJobSeqId(stats.getJob());
    if (seq < 0) {
      LOG.info("Not tracking job " + stats.getJob().getJobName()
               + " as seq id is less than zero: " + seq);
      return;
    }
    submittedJobsMap.put(seq, stats);
    addToNumMapsSubmitted(stats.getNoOfMaps());
    addToNumReducesSubmitted(stats.getNoOfReds());
  }

  /**
   * Used by JobMonitor to add the completed job.
   */
  @Override
  public void add(Statistics.JobStats job) {
    //This thread will be notified initially by job-monitor incase of
    //data generation. Ignore that as we are getting once the input is
    //generated.
    if (!statistics.isAlive()) {
      return;
    }
    JobStats stat = submittedJobsMap.remove(GridmixJob.getJobSeqId(job.getJob()));
    
    // stat cannot be null
    if (stat == null) {
      LOG.error("[Statistics] Missing entry for job " 
                + job.getJob().getJobID());
      return;
    }
    
    // update the total number of submitted map/reduce task count
    subtractFromNumMapsSubmitted(stat.getNoOfMaps());
    subtractFromNumReducesSubmitted(stat.getNoOfReds());
    
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
        for (StatListener<JobStats> l : jobStatListeners) {
          l.update(stat);
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
          if (!shutdown) {
            LOG.error("Statistics interrupt while waiting for completion of "
                + "a job.", ie);
          }
          return;
        } finally {
          lock.unlock();
        }

        //Fetch cluster data only if required.i.e .
        // only if there are clusterStats listener.
        if (clusterStatlisteners.size() > 0) {
          try {
            ClusterStatus clusterStatus = cluster.getClusterStatus();
            updateAndNotifyClusterStatsListeners(clusterStatus);
          } catch (IOException e) {
            LOG.error(
              "Statistics io exception while polling JT ", e);
            return;
          }
        }
      }
    }

    private void updateAndNotifyClusterStatsListeners(
      ClusterStatus clusterStatus) {
      ClusterStats stats = ClusterStats.getClusterStats();
      stats.setClusterMetric(clusterStatus);
      for (StatListener<ClusterStats> listener : clusterStatlisteners) {
        listener.update(stats);
      }
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
    submittedJobsMap.clear();
    clusterStatlisteners.clear();
    jobStatListeners.clear();
    statistics.interrupt();
  }

  @Override
  public void abort() {
    shutdown = true;
    submittedJobsMap.clear();
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
    private final int noOfMaps;
    private final int noOfReds;
    private JobStatus currentStatus;
    private final Job job;

    public JobStats(int noOfMaps,int numOfReds, Job job){
      this.job = job;
      this.noOfMaps = noOfMaps;
      this.noOfReds = numOfReds;
    }
    public int getNoOfMaps() {
      return noOfMaps;
    }
    public int getNoOfReds() {
      return noOfReds;
    }

    /**
     * Returns the job ,
     * We should not use job.getJobID it returns null in 20.1xx.
     * Use (GridmixJob.getJobSeqId(job)) instead
     * @return job
     */
    public Job getJob() {
      return job;
    }
    
    /**
     * Update the job statistics.
     */
    public synchronized void updateJobStatus(JobStatus status) {
      this.currentStatus = status;
    }
    
    /**
     * Get the current job status.
     */
    public synchronized JobStatus getJobStatus() {
      return currentStatus;
    }
  }

  static class ClusterStats {
    private ClusterStatus status = null;
    private static ClusterStats stats = new ClusterStats();

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
    void setClusterMetric(ClusterStatus metrics) {
      this.status = metrics;
    }

    /**
     * @return metrics
     */
    public ClusterStatus getStatus() {
      return status;
    }

    int getNumRunningJob() {
      return submittedJobsMap.size();
    }

    /**
     * @return runningWatitingJobs
     */
    static Collection<JobStats> getRunningJobStats() {
      return submittedJobsMap.values();
    }

    /**
     * Returns the total number of submitted map tasks
     */
    static int getSubmittedMapTasks() {
      return numMapsSubmitted;
    }
    
    /**
     * Returns the total number of submitted reduce tasks
     */
    static int getSubmittedReduceTasks() {
      return numReducesSubmitted;
    }
  }
}
