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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * A {@link TaskScheduler} that keeps jobs in a queue in priority order (FIFO
 * by default).
 */
class JobQueueTaskScheduler extends TaskScheduler {
  
  private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
  
  protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
  private EagerTaskInitializationListener eagerTaskInitializationListener;
  private float padFraction;
  
  public JobQueueTaskScheduler() {
    this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
    this.eagerTaskInitializationListener =
      new EagerTaskInitializationListener();
  }
  
  @Override
  public synchronized void start() throws IOException {
    super.start();
    taskTrackerManager.addJobInProgressListener(jobQueueJobInProgressListener);
    
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(
        eagerTaskInitializationListener);
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    if (jobQueueJobInProgressListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          jobQueueJobInProgressListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);
  }

  @Override
  public synchronized List<Task> assignTasks(TaskTrackerStatus taskTracker)
      throws IOException {

    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    int numTaskTrackers = clusterStatus.getTaskTrackers();

    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    //
    // Get map + reduce counts for the current tracker.
    //
    int maxCurrentMapTasks = taskTracker.getMaxMapTasks();
    int maxCurrentReduceTasks = taskTracker.getMaxReduceTasks();
    int numMaps = taskTracker.countMapTasks();
    int numReduces = taskTracker.countReduceTasks();

    //
    // Compute average map and reduce task numbers across pool
    //
    int remainingReduceLoad = 0;
    int remainingMapLoad = 0;
    synchronized (jobQueue) {
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          int totalMapTasks = job.desiredMaps();
          int totalReduceTasks = job.desiredReduces();
          remainingMapLoad += (totalMapTasks - job.finishedMaps());
          remainingReduceLoad += (totalReduceTasks - job.finishedReduces());
        }
      }
    }

    // find out the maximum number of maps or reduces that we are willing
    // to run on any node.
    int maxMapLoad = 0;
    int maxReduceLoad = 0;
    if (numTaskTrackers > 0) {
      maxMapLoad = Math.min(maxCurrentMapTasks,
                            (int) Math.ceil((double) remainingMapLoad / 
                                            numTaskTrackers));
      maxReduceLoad = Math.min(maxCurrentReduceTasks,
                               (int) Math.ceil((double) remainingReduceLoad
                                               / numTaskTrackers));
    }
        
    int totalMaps = clusterStatus.getMapTasks();
    int totalMapTaskCapacity = clusterStatus.getMaxMapTasks();
    int totalReduces = clusterStatus.getReduceTasks();
    int totalReduceTaskCapacity = clusterStatus.getMaxReduceTasks();

    //
    // In the below steps, we allocate first a map task (if appropriate),
    // and then a reduce task if appropriate.  We go through all jobs
    // in order of job arrival; jobs only get serviced if their 
    // predecessors are serviced, too.
    //

    //
    // We hand a task to the current taskTracker if the given machine 
    // has a workload that's less than the maximum load of that kind of
    // task.
    //
       
    if (numMaps < maxMapLoad) {

      int totalNeededMaps = 0;
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }

          Task t = job.obtainNewMapTask(taskTracker, numTaskTrackers,
              taskTrackerManager.getNumberOfUniqueHosts());
          if (t != null) {
            return Collections.singletonList(t);
          }

          //
          // Beyond the highest-priority task, reserve a little 
          // room for failures and speculative executions; don't 
          // schedule tasks to the hilt.
          //
          totalNeededMaps += job.desiredMaps();
          int padding = 0;
          if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
            padding = Math.min(maxCurrentMapTasks,
                               (int)(totalNeededMaps * padFraction));
          }
          if (totalMaps + padding >= totalMapTaskCapacity) {
            break;
          }
        }
      }
    }

    //
    // Same thing, but for reduce tasks
    //
    if (numReduces < maxReduceLoad) {

      int totalNeededReduces = 0;
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
              job.numReduceTasks == 0) {
            continue;
          }

          Task t = job.obtainNewReduceTask(taskTracker, numTaskTrackers, 
              taskTrackerManager.getNumberOfUniqueHosts());
          if (t != null) {
            return Collections.singletonList(t);
          }

          //
          // Beyond the highest-priority task, reserve a little 
          // room for failures and speculative executions; don't 
          // schedule tasks to the hilt.
          //
          totalNeededReduces += job.desiredReduces();
          int padding = 0;
          if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
            padding = 
              Math.min(maxCurrentReduceTasks,
                       (int) (totalNeededReduces * padFraction));
          }
          if (totalReduces + padding >= totalReduceTaskCapacity) {
            break;
          }
        }
      }
    }
    return null;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return jobQueueJobInProgressListener.getJobQueue();
  }  
}
