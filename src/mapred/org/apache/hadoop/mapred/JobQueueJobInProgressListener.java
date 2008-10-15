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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;

/**
 * A {@link JobInProgressListener} that maintains the jobs being managed in
 * a queue. By default the queue is FIFO, but it is possible to use custom
 * queue ordering by using the
 * {@link #JobQueueJobInProgressListener(Collection)} constructor.
 */
class JobQueueJobInProgressListener extends JobInProgressListener {

  private static final Comparator<JobInProgress> FIFO_JOB_QUEUE_COMPARATOR
    = new Comparator<JobInProgress>() {
    public int compare(JobInProgress o1, JobInProgress o2) {
      int res = o1.getPriority().compareTo(o2.getPriority());
      if (res == 0) {
        if (o1.getStartTime() < o2.getStartTime()) {
          res = -1;
        } else {
          res = (o1.getStartTime() == o2.getStartTime() ? 0 : 1);
        }
      }
      if (res == 0) {
        res = o1.getJobID().compareTo(o2.getJobID());
      }
      return res;
    }
  };
  
  private Collection<JobInProgress> jobQueue;
  
  public JobQueueJobInProgressListener() {
    this(new TreeSet<JobInProgress>(FIFO_JOB_QUEUE_COMPARATOR));
  }

  /**
   * For clients that want to provide their own job priorities.
   * @param jobQueue A collection whose iterator returns jobs in priority order.
   */
  protected JobQueueJobInProgressListener(Collection<JobInProgress> jobQueue) {
    this.jobQueue = Collections.synchronizedCollection(jobQueue);
  }

  /**
   * Returns a synchronized view of the the job queue.
   */
  public Collection<JobInProgress> getJobQueue() {
    return jobQueue;
  }
  
  @Override
  public void jobAdded(JobInProgress job) {
    jobQueue.add(job);
  }

  // Job will be removed once the job completes
  @Override
  public void jobRemoved(JobInProgress job) {}
  
  private void jobCompleted(JobInProgress job) {
    jobQueue.remove(job);
  }
  
  @Override
  public synchronized void jobUpdated(JobChangeEvent event) {
    JobInProgress job = event.getJobInProgress();
    if (event instanceof JobStatusChangeEvent) {
      // Check if the ordering of the job has changed
      // For now priority and start-time can change the job ordering
      JobStatusChangeEvent statusEvent = (JobStatusChangeEvent)event;
      if (statusEvent.getEventType() == EventType.PRIORITY_CHANGED 
          || statusEvent.getEventType() == EventType.START_TIME_CHANGED) {
        // Make a priority change
        reorderJobs(job);
      } else if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
        // Check if the job is complete
        int runState = statusEvent.getNewStatus().getRunState();
        if (runState == JobStatus.SUCCEEDED
            || runState == JobStatus.FAILED
            || runState == JobStatus.KILLED) {
          jobCompleted(job);
        }
      }
    }
  }
  
  private void reorderJobs(JobInProgress job) {
    synchronized (jobQueue) {
      jobQueue.remove(job);
      jobQueue.add(job);
    }
  }

}
