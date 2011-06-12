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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapred.JobQueueJobInProgressListener.JobSchedulingInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
class JobQueue extends AbstractQueue {

  static final Log LOG = LogFactory.getLog(JobQueue.class);

  public JobQueue(AbstractQueue parent, QueueSchedulingContext qsc) {
    super(parent, qsc);
    if (qsc.supportsPriorities()) {
      // use the default priority-aware comparator
      comparator = JobQueueJobInProgressListener.FIFO_JOB_QUEUE_COMPARATOR;
    } else {
      comparator = STARTTIME_JOB_COMPARATOR;
    }
    waitingJobs =
      new
        TreeMap<JobSchedulingInfo, JobInProgress>(
        comparator);
    runningJobs =               
      new
        TreeMap<JobSchedulingInfo, JobInProgress>(
        comparator);
  }


  /*
  * If a queue supports priorities, jobs must be
  * sorted on priorities, and then on their start times (technically,
  * their insertion time.
  * If a queue doesn't support priorities, jobs are
  * sorted based on their start time.
  */
  static final Comparator<JobSchedulingInfo>
    STARTTIME_JOB_COMPARATOR;

  static {
    STARTTIME_JOB_COMPARATOR =
      new Comparator<JobSchedulingInfo>() {
        // comparator for jobs in queues that don't support priorities
        public int compare(
          JobSchedulingInfo o1,
          JobSchedulingInfo o2) {
          // the job that started earlier wins
          if (o1.getStartTime() < o2.getStartTime()) {
            return -1;
          } else {
            return (o1.getStartTime() == o2.getStartTime()
              ? o1.getJobID().compareTo(o2.getJobID())
              : 1);
          }
        }
      };
  }


  /**
   * This involves updating each qC structure.
   *
   * @param mapClusterCapacity
   * @param reduceClusterCapacity
   */
  @Override
  public void update(int mapClusterCapacity, int reduceClusterCapacity) {
    super.update(mapClusterCapacity, reduceClusterCapacity);
    for (JobInProgress j :
      this.getRunningJobs()) {
      updateStatsOnRunningJob(qsc, j);
    }
  }

  private void updateStatsOnRunningJob(
    QueueSchedulingContext qC, JobInProgress j) {
    if (j.getStatus().getRunState() != JobStatus.RUNNING) {
      return;
    }

    TaskSchedulingContext mapTSI = qC.getMapTSC();
    TaskSchedulingContext reduceTSI = qC.getReduceTSC();

    int numMapsRunningForThisJob = j.runningMaps();
    int numReducesRunningForThisJob = j.runningReduces();
    TaskDataView mapScheduler = TaskDataView.getTaskDataView(TaskType.MAP);
    TaskDataView reduceScheduler = 
        TaskDataView.getTaskDataView(TaskType.REDUCE);
    int numRunningMapSlots =
      numMapsRunningForThisJob * mapScheduler.getSlotsPerTask(j);
    int numRunningReduceSlots =
      numReducesRunningForThisJob * reduceScheduler.getSlotsPerTask(j);
    int numMapSlotsForThisJob = mapScheduler.getSlotsOccupied(j);
    int numReduceSlotsForThisJob = reduceScheduler.getSlotsOccupied(j);
    int numReservedMapSlotsForThisJob =
      (mapScheduler.getNumReservedTaskTrackers(j) *
        mapScheduler.getSlotsPerTask(j));
    int numReservedReduceSlotsForThisJob =
      (reduceScheduler.getNumReservedTaskTrackers(j) *
        reduceScheduler.getSlotsPerTask(j));

    j.setSchedulingInfo(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
        numMapsRunningForThisJob,
        numRunningMapSlots,
        numReservedMapSlotsForThisJob,
        numReducesRunningForThisJob,
        numRunningReduceSlots,
        numReservedReduceSlotsForThisJob));


    mapTSI.setNumRunningTasks(
      mapTSI.getNumRunningTasks() + numMapsRunningForThisJob);
    reduceTSI.setNumRunningTasks(
      reduceTSI.getNumRunningTasks() + numReducesRunningForThisJob);
    mapTSI.setNumSlotsOccupied(
      mapTSI.getNumSlotsOccupied() + numMapSlotsForThisJob);
    reduceTSI.setNumSlotsOccupied(
      reduceTSI.getNumSlotsOccupied() + numReduceSlotsForThisJob);
    Integer i =
      mapTSI.getNumSlotsOccupiedByUser().get(
        j.getProfile().getUser());
    mapTSI.getNumSlotsOccupiedByUser().put(
      j.getProfile().getUser(),
      i.intValue() + numMapSlotsForThisJob);
    i = reduceTSI.getNumSlotsOccupiedByUser().get(
      j.getProfile().getUser());
    reduceTSI.getNumSlotsOccupiedByUser().put(
      j.getProfile().getUser(),
      i.intValue() + numReduceSlotsForThisJob);
    if (LOG.isDebugEnabled()) {
      synchronized (j) {
        LOG.debug(String.format("updateQSI: job %s: run(m)=%d, "
            + "occupied(m)=%d, run(r)=%d, occupied(r)=%d, finished(m)=%d,"
            + " finished(r)=%d, failed(m)=%d, failed(r)=%d, "
            + "spec(m)=%d, spec(r)=%d, total(m)=%d, total(r)=%d", j.getJobID()
            .toString(), numMapsRunningForThisJob, numMapSlotsForThisJob,
            numReducesRunningForThisJob, numReduceSlotsForThisJob, j
                .finishedMaps(), j.finishedReduces(), j.failedMapTasks,
            j.failedReduceTasks, j.speculativeMapTasks,
            j.speculativeReduceTasks, j.numMapTasks, j.numReduceTasks));
      }
    }

    /*
    * it's fine walking down the entire list of running jobs - there
  * probably will not be many, plus, we may need to go through the
  * list to compute numSlotsOccupiedByUser. If this is expensive, we
  * can keep a list of running jobs per user. Then we only need to
  * consider the first few jobs per user.
  */
  }


  Map<JobSchedulingInfo, JobInProgress>
    waitingJobs; // for waiting jobs
  Map<JobSchedulingInfo, JobInProgress>
    runningJobs; // for running jobs

  public Comparator<JobSchedulingInfo>
    comparator;

  Collection<JobInProgress> getWaitingJobs() {
    synchronized (waitingJobs) {
      return Collections.unmodifiableCollection(
        new LinkedList<JobInProgress>(waitingJobs.values()));
    }
  }

  Collection<JobInProgress> getRunningJobs() {
    synchronized (runningJobs) {
      return Collections.unmodifiableCollection(
        new LinkedList<JobInProgress>(runningJobs.values()));
    }
  }

  private void addRunningJob(JobInProgress job) {
    synchronized (runningJobs) {
      runningJobs.put(
        new JobSchedulingInfo(
          job), job);
    }
  }

  private JobInProgress removeRunningJob(
    JobSchedulingInfo jobInfo) {
    synchronized (runningJobs) {
      return runningJobs.remove(jobInfo);
    }
  }

  JobInProgress removeWaitingJob(
    JobSchedulingInfo schedInfo) {
    synchronized (waitingJobs) {
      JobInProgress jip = waitingJobs.remove(schedInfo);
      this.qsc.setNumOfWaitingJobs(waitingJobs.size());
      return jip;
    }
  }

  private void addWaitingJob(JobInProgress job) {
    synchronized (waitingJobs) {
      waitingJobs.put(
        new JobSchedulingInfo(
          job), job);
      this.qsc.setNumOfWaitingJobs(waitingJobs.size());
    }
  }

  int getWaitingJobCount() {
    synchronized (waitingJobs) {
      return waitingJobs.size();
    }
  }

  // called when a job is added
  synchronized void jobAdded(JobInProgress job) throws IOException {
    // add job to waiting queue. It will end up in the right place,
    // based on priority. 
    addWaitingJob(job);
    // update user-specific info
    Integer i = qsc.getNumJobsByUser().get(job.getProfile().getUser());
    if (null == i) {
      i = 1;
      // set the count for running tasks to 0
      qsc.getMapTSC().getNumSlotsOccupiedByUser().put(
        job.getProfile().getUser(),
        0);
      qsc.getReduceTSC().getNumSlotsOccupiedByUser().
        put(
          job.getProfile().getUser(),
          0);
    } else {
      i++;
    }
    qsc.getNumJobsByUser().put(job.getProfile().getUser(), i);

    // setup scheduler specific job information
    preInitializeJob(job);

    LOG.debug(
      "Job " + job.getJobID().toString() + " is added under user "
        + job.getProfile().getUser() + ", user now has " + i + " jobs");
  }


  /**
   * Setup {@link CapacityTaskScheduler} specific information prior to
   * job initialization.
   * <p/>
   * TO DO: Currently this method uses , CapacityTaskScheduler based variables
   * need to shift those.
   */
  void preInitializeJob(JobInProgress job) {
    JobConf jobConf = job.getJobConf();

    // Compute number of slots required to run a single map/reduce task
    int slotsPerMap = 1;
    int slotsPerReduce = 1;
    if (MemoryMatcher.isSchedulingBasedOnMemEnabled()) {
      slotsPerMap = jobConf.computeNumSlotsPerMap(
        MemoryMatcher.getMemSizeForMapSlot());
      slotsPerReduce =
        jobConf.computeNumSlotsPerReduce(
          MemoryMatcher.getMemSizeForReduceSlot());
    }
    job.setNumSlotsPerMap(slotsPerMap);
    job.setNumSlotsPerReduce(slotsPerReduce);
  }

  // called when a job completes
  synchronized void jobCompleted(JobInProgress job) {

    LOG.debug("Job to be removed for user " + job.getProfile().getUser());
    Integer i = qsc.getNumJobsByUser().get(job.getProfile().getUser());
    i--;
    if (0 == i.intValue()) {
      qsc.getNumJobsByUser().remove(job.getProfile().getUser());
      // remove job footprint from our TSIs
      qsc.getMapTSC().getNumSlotsOccupiedByUser().remove(
        job.getProfile().getUser());
      qsc.getReduceTSC().getNumSlotsOccupiedByUser().remove(
        job.getProfile().getUser());
      LOG.debug(
        "No more jobs for user, number of users = " + qsc
          .getNumJobsByUser().size());
    } else {
      qsc.getNumJobsByUser().put(job.getProfile().getUser(), i);
      LOG.debug(
        "User still has " + i + " jobs, number of users = "
          + qsc.getNumJobsByUser().size());
    }
  }

  // This is used to reposition a job in the queue. A job can get repositioned
  // because of the change in the job priority or job start-time.
  private void reorderJobs(
    JobInProgress job, JobSchedulingInfo oldInfo
  ) {

    if (removeWaitingJob(oldInfo) != null) {
      addWaitingJob(job);
    }
    if (removeRunningJob(oldInfo) != null) {
      addRunningJob(job);
    }
  }

  /**
   * @return
   */
  @Override
  List<AbstractQueue> getDescendentJobQueues() {
    List<AbstractQueue> l = new ArrayList<AbstractQueue>();
    l.add(this);
    return l;
  }

  @Override
  List<AbstractQueue> getDescendantContainerQueues() {
    return new ArrayList<AbstractQueue>();
  }

  public void jobUpdated(JobChangeEvent event) {
    // Check if this is the status change
    if (event instanceof JobStatusChangeEvent) {
      jobStateChanged((JobStatusChangeEvent) event);
    }
  }

  /**
   * @return
   */
  @Override
  List<AbstractQueue> getChildren() {
    return null;
  }

  /**
   * Dont do anything in sort , this is leaf level queue.
   *
   * @param queueComparator
   */
  @Override
  public void sort(Comparator queueComparator) {
    return;
  }

  // Update the scheduler as job's state has changed
  private void jobStateChanged(JobStatusChangeEvent event) {
    JobInProgress job = event.getJobInProgress();
    JobSchedulingInfo oldJobStateInfo =
      new JobSchedulingInfo(event.getOldStatus());
    // Check if the ordering of the job has changed
    // For now priority and start-time can change the job ordering
    if (event.getEventType() == JobStatusChangeEvent.EventType.PRIORITY_CHANGED
      || event.getEventType() ==
      JobStatusChangeEvent.EventType.START_TIME_CHANGED) {
      // Make a priority change
      reorderJobs(job, oldJobStateInfo);
    } else if (event.getEventType() ==
      JobStatusChangeEvent.EventType.RUN_STATE_CHANGED) {
      // Check if the job is complete
      int runState = job.getStatus().getRunState();
      if (runState == JobStatus.SUCCEEDED
        || runState == JobStatus.FAILED
        || runState == JobStatus.KILLED) {
        jobCompleted(job, oldJobStateInfo);
      } else if (runState == JobStatus.RUNNING) {
        // Removing of the job from job list is responsibility of the
        //initialization poller.
        // Add the job to the running queue
        addRunningJob(job);
      }
    }
  }

  /*
  * Method removes the jobs from both running and waiting job queue in
  * job queue manager.
  */
  private void jobCompleted(
    JobInProgress job, JobSchedulingInfo oldInfo
  ) {
    LOG.info(
      "Job " + job.getJobID().toString() + " submitted to queue "
        + job.getProfile().getQueueName() + " has completed");
    //remove jobs from both queue's a job can be in
    //running and waiting queue at the same time.
    removeRunningJob(oldInfo);
    removeWaitingJob(oldInfo);
    // let scheduler know
    jobCompleted(job);
  }

  @Override
  public void addChild(AbstractQueue queue) {
    throw new UnsupportedOperationException(
      "addChildren is not allowed for " +
        "" + getName());
  }

  @Override
  void distributeUnConfiguredCapacity() {
    return;
  }
}
