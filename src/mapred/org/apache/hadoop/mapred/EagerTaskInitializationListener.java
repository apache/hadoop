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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.util.StringUtils;

/**
 * A {@link JobInProgressListener} which initializes the tasks for a job as soon
 * as the job is added (using the {@link #jobAdded(JobInProgress)} method).
 */
class EagerTaskInitializationListener extends JobInProgressListener {
  
  private static final Log LOG = LogFactory.getLog(
      EagerTaskInitializationListener.class.getName());
  
  /////////////////////////////////////////////////////////////////
  //  Used to init new jobs that have just been created
  /////////////////////////////////////////////////////////////////
  class JobInitThread implements Runnable {
    public void run() {
      JobInProgress job;
      while (true) {
        job = null;
        try {
          synchronized (jobInitQueue) {
            while (jobInitQueue.isEmpty()) {
              jobInitQueue.wait();
            }
            job = jobInitQueue.remove(0);
          }
          job.initTasks();
        } catch (InterruptedException t) {
          break;
        } catch (Throwable t) {
          LOG.error("Job initialization failed:\n" +
                    StringUtils.stringifyException(t));
          if (job != null) {
            job.fail();
          }
        }
      }
    }
  }
  
  private JobInitThread initJobs = new JobInitThread();
  private Thread initJobsThread;
  private List<JobInProgress> jobInitQueue = new ArrayList<JobInProgress>();
  
  public void start() throws IOException {
    this.initJobsThread = new Thread(initJobs, "initJobs");
    this.initJobsThread.start();
  }
  
  public void terminate() throws IOException {
    if (this.initJobsThread != null && this.initJobsThread.isAlive()) {
      LOG.info("Stopping initer");
      this.initJobsThread.interrupt();
      try {
        this.initJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }

  /**
   * We add the JIP to the jobInitQueue, which is processed 
   * asynchronously to handle split-computation and build up
   * the right TaskTracker/Block mapping.
   */
  @Override
  public void jobAdded(JobInProgress job) {
    synchronized (jobInitQueue) {
      jobInitQueue.add(job);
      resortInitQueue();
      jobInitQueue.notifyAll();
    }

  }
  
  /**
   * Sort jobs by priority and then by start time.
   */
  private synchronized void resortInitQueue() {
    Comparator<JobInProgress> comp = new Comparator<JobInProgress>() {
      public int compare(JobInProgress o1, JobInProgress o2) {
        int res = o1.getPriority().compareTo(o2.getPriority());
        if(res == 0) {
          if(o1.getStartTime() < o2.getStartTime())
            res = -1;
          else
            res = (o1.getStartTime()==o2.getStartTime() ? 0 : 1);
        }
          
        return res;
      }
    };
    
    synchronized (jobInitQueue) {
      Collections.sort(jobInitQueue, comp);
    }
  }

  @Override
  public void jobRemoved(JobInProgress job) {
    synchronized (jobInitQueue) {
      jobInitQueue.remove(job);
    }
  }

  @Override
  public void jobUpdated(JobChangeEvent event) {
    if (event instanceof JobStatusChangeEvent) {
      jobStateChanged((JobStatusChangeEvent)event);
    }
  }
  
  // called when the job's status is changed
  private void jobStateChanged(JobStatusChangeEvent event) {
    // Resort the job queue if the job-start-time or job-priority changes
    if (event.getEventType() == EventType.START_TIME_CHANGED
        || event.getEventType() == EventType.PRIORITY_CHANGED) {
      synchronized (jobInitQueue) {
        resortInitQueue();
      }
    }
  }

}
