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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * Periodically monitors the status of jobs registered with it.
 *
 * Jobs that are submitted for the same policy name are kept in the same list,
 * and the list itself is kept in a map that has the policy name as the key and
 * the list as value.
 */
class JobMonitor implements Runnable {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.JobMonitor");

  volatile boolean running = true;

  private Map<String, List<DistRaid>> jobs;
  private long jobMonitorInterval;
  private volatile long jobsMonitored = 0;
  private volatile long jobsSucceeded = 0;

  public JobMonitor(Configuration conf) {
    jobMonitorInterval = conf.getLong("raid.jobmonitor.interval", 60000);
    jobs = new java.util.HashMap<String, List<DistRaid>>();
  }

  public void run() {
    while (running) {
      try {
        LOG.info("JobMonitor thread continuing to run...");
        doMonitor();
      } catch (Throwable e) {
        LOG.error("JobMonitor encountered exception " +
          StringUtils.stringifyException(e));
        // All expected exceptions are caught by doMonitor(). It is better
        // to exit now, this will prevent RaidNode from submitting more jobs
        // since the number of running jobs will never decrease.
        return;
      }
    }
  }

  /**
   * Periodically checks status of running map-reduce jobs.
   */
  public void doMonitor() {
    while (running) {
      String[] keys = null;
      // Make a copy of the names of the current jobs.
      synchronized(jobs) {
        keys = jobs.keySet().toArray(new String[0]);
      }

      // Check all the jobs. We do not want to block access to `jobs`
      // because that will prevent new jobs from being added.
      // This is safe because JobMonitor.run is the only code that can
      // remove a job from `jobs`. Thus all elements in `keys` will have
      // valid values.
      Map<String, List<DistRaid>> finishedJobs =
        new HashMap<String, List<DistRaid>>();

      for (String key: keys) {
        // For each policy being monitored, get the list of jobs running.
        DistRaid[] jobListCopy = null;
        synchronized(jobs) {
          List<DistRaid> jobList = jobs.get(key);
          synchronized(jobList) {
            jobListCopy = jobList.toArray(new DistRaid[jobList.size()]);
          }
        }
        // The code that actually contacts the JobTracker is not synchronized,
        // it uses copies of the list of jobs.
        for (DistRaid job: jobListCopy) {
          // Check each running job.
          try {
            boolean complete = job.checkComplete();
            if (complete) {
              addJob(finishedJobs, key, job);
              if (job.successful()) {
                jobsSucceeded++;
              }
            }
          } catch (IOException ioe) {
            // If there was an error, consider the job finished.
            addJob(finishedJobs, key, job);
          }
        }
      }

      if (finishedJobs.size() > 0) {
        for (String key: finishedJobs.keySet()) {
          List<DistRaid> finishedJobList = finishedJobs.get(key);
          // Iterate through finished jobs and remove from jobs.
          // removeJob takes care of locking.
          for (DistRaid job: finishedJobList) {
            removeJob(jobs, key, job);
          }
        }
      }

      try {
        Thread.sleep(jobMonitorInterval);
      } catch (InterruptedException ie) {
      }
    }
  }

  public int runningJobsCount(String key) {
    int count = 0;
    synchronized(jobs) {
      if (jobs.containsKey(key)) {
        List<DistRaid> jobList = jobs.get(key);
        synchronized(jobList) {
          count = jobList.size();
        }
      }
    }
    return count;
  }

  public void monitorJob(String key, DistRaid job) {
    addJob(jobs, key, job);
    jobsMonitored++;
  }

  public long jobsMonitored() {
    return this.jobsMonitored;
  }

  public long jobsSucceeded() {
    return this.jobsSucceeded;
  }

  private static void addJob(Map<String, List<DistRaid>> jobsMap,
                              String jobName, DistRaid job) {
    synchronized(jobsMap) {
      List<DistRaid> list = null;
      if (jobsMap.containsKey(jobName)) {
        list = jobsMap.get(jobName);
      } else {
        list = new LinkedList<DistRaid>();
        jobsMap.put(jobName, list);
      }
      synchronized(list) {
        list.add(job);
      }
    }
  }

  private static void removeJob(Map<String, List<DistRaid>> jobsMap,
                                  String jobName, DistRaid job) {
    synchronized(jobsMap) {
      if (jobsMap.containsKey(jobName)) {
        List<DistRaid> list = jobsMap.get(jobName);
        synchronized(list) {
          for (Iterator<DistRaid> it = list.iterator(); it.hasNext(); ) {
            DistRaid val = it.next();
            if (val == job) {
              it.remove();
            }
          }
          if (list.size() == 0) {
            jobsMap.remove(jobName);
          }
        }
      }
    }
  }
}
