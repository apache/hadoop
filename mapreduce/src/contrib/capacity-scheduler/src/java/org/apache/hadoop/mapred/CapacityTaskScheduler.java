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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.AbstractQueue.AbstractQueueComparator;
import org.apache.hadoop.mapred.JobTracker.IllegalStateException;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.util.StringUtils;

/**
 * A {@link TaskScheduler} that implements the requirements in HADOOP-3421
 * and provides a HOD-less way to share large clusters. This scheduler 
 * provides the following features: 
 *  * support for queues, where a job is submitted to a queue. 
 *  * Queues are assigned a fraction of the capacity of the grid (their
 *  'capacity') in the sense that a certain capacity of resources 
 *  will be at their disposal. All jobs submitted to the queues of an Org 
 *  will have access to the capacity to the Org.
 *  * Free resources can be allocated to any queue beyond its 
 *  capacity.
 *  * Queues optionally support job priorities (disabled by default). 
 *  * Within a queue, jobs with higher priority will have access to the 
 *  queue's resources before jobs with lower priority. However, once a job 
 *  is running, it will not be preempted for a higher priority job.
 *  * In order to prevent one or more users from monopolizing its resources, 
 *  each queue enforces a limit on the percentage of resources allocated to a 
 *  user at any given time, if there is competition for them.
 *  
 */
class CapacityTaskScheduler extends TaskScheduler {

  /** quick way to get qsc object given a queue name */
  private Map<String, QueueSchedulingContext> queueInfoMap =
    new HashMap<String, QueueSchedulingContext>();

  //Root level queue . It has all the
  //cluster capacity at its disposal.
  //Queues declared by users would
  //be children of this queue.
  //CS would have handle to root.
  private AbstractQueue root = null;

  /**
   * This class captures scheduling information we want to display or log.
   */
  private static class SchedulingDisplayInfo {
    private String queueName;
    CapacityTaskScheduler scheduler;
    
    SchedulingDisplayInfo(String queueName, CapacityTaskScheduler scheduler) { 
      this.queueName = queueName;
      this.scheduler = scheduler;
    }
    
    @Override
    public String toString(){
      // note that we do not call updateContextObjects() here for performance
      // reasons. This means that the data we print out may be slightly
      // stale. This data is updated whenever assignTasks() is called
      // If this doesn't happen, the data gets stale. If we see
      // this often, we may need to detect this situation and call 
      // updateContextObjects(), or just call it each time.
      return scheduler.getDisplayInfo(queueName);
    }
  }


  // this class encapsulates the result of a task lookup
  private static class TaskLookupResult {

    static enum LookUpStatus {
      TASK_FOUND,
      NO_TASK_FOUND,
      TASK_FAILING_MEMORY_REQUIREMENT,
    }
    // constant TaskLookupResult objects. Should not be accessed directly.
    private static final TaskLookupResult NoTaskLookupResult = 
      new TaskLookupResult(null, TaskLookupResult.LookUpStatus.NO_TASK_FOUND);
    private static final TaskLookupResult MemFailedLookupResult = 
      new TaskLookupResult(null, 
          TaskLookupResult.LookUpStatus.TASK_FAILING_MEMORY_REQUIREMENT);

    private LookUpStatus lookUpStatus;
    private Task task;

    // should not call this constructor directly. use static factory methods.
    private TaskLookupResult(Task t, LookUpStatus lUStatus) {
      this.task = t;
      this.lookUpStatus = lUStatus;
    }
    
    static TaskLookupResult getTaskFoundResult(Task t) {
      LOG.debug("Returning task " + t);
      return new TaskLookupResult(t, LookUpStatus.TASK_FOUND);
    }
    static TaskLookupResult getNoTaskFoundResult() {
      return NoTaskLookupResult;
    }
    static TaskLookupResult getMemFailedResult() {
      return MemFailedLookupResult;
    }
    

    Task getTask() {
      return task;
    }

    LookUpStatus getLookUpStatus() {
      return lookUpStatus;
    }
  }

  /**
   * This class handles the scheduling algorithms.
   * The algos are the same for both Map and Reduce tasks.
   * There may be slight variations later, in which case we can make this
   * an abstract base class and have derived classes for Map and Reduce.
   */
  private static abstract class TaskSchedulingMgr {

    /** our TaskScheduler object */
    protected CapacityTaskScheduler scheduler;
    protected TaskType type = null;

    abstract Task obtainNewTask(TaskTrackerStatus taskTracker,
        JobInProgress job) throws IOException;

    abstract int getClusterCapacity();
    abstract TaskSchedulingContext getTSC(
      QueueSchedulingContext qsc);
    /**
     * To check if job has a speculative task on the particular tracker.
     *
     * @param job job to check for speculative tasks.
     * @param tts task tracker on which speculative task would run.
     * @return true if there is a speculative task to run on the tracker.
     */
    abstract boolean hasSpeculativeTask(JobInProgress job,
        TaskTrackerStatus tts);

    /**
     * Comparator to sort queues.
     * For maps, we need to sort on QueueSchedulingContext.mapTSC. For
     * reducers, we use reduceTSC. So we'll need separate comparators.
     */
    private static abstract class QueueComparator
      implements Comparator<AbstractQueue> {
      abstract TaskSchedulingContext getTSC(
        QueueSchedulingContext qsi);
      public int compare(AbstractQueue q1, AbstractQueue q2) {
        TaskSchedulingContext t1 = getTSC(q1.getQueueSchedulingContext());
        TaskSchedulingContext t2 = getTSC(q2.getQueueSchedulingContext());
        // look at how much capacity they've filled. Treat a queue with
        // capacity=0 equivalent to a queue running at capacity
        double r1 = (0 == t1.getCapacity())? 1.0f:
          (double) t1.getNumSlotsOccupied() /(double) t1.getCapacity();
        double r2 = (0 == t2.getCapacity())? 1.0f:
          (double) t2.getNumSlotsOccupied() /(double) t2.getCapacity();
        if (r1<r2) return -1;
        else if (r1>r2) return 1;
        else return 0;
      }
    }
    // subclass for map and reduce comparators
    private static final class MapQueueComparator extends QueueComparator {
      TaskSchedulingContext getTSC(QueueSchedulingContext qsi) {
        return qsi.getMapTSC();
      }
    }
    private static final class ReduceQueueComparator extends QueueComparator {
      TaskSchedulingContext getTSC(QueueSchedulingContext qsi) {
        return qsi.getReduceTSC();
      }
    }

    // these are our comparator instances
    protected final static MapQueueComparator mapComparator =
      new MapQueueComparator();
    protected final static ReduceQueueComparator reduceComparator =
      new ReduceQueueComparator();
    // and this is the comparator to use
    protected QueueComparator queueComparator;

    // Returns queues sorted according to the QueueComparator.
    // Mainly for testing purposes.
    String[] getOrderedQueues() {
      List<AbstractQueue> queueList = getOrderedJobQueues();
      List<String> queues = new ArrayList<String>(queueList.size());
      for (AbstractQueue q : queueList) {
        queues.add(q.getName());
      }
      return queues.toArray(new String[queues.size()]);
    }

    /**
     * Return an ordered list of {@link JobQueue}s wrapped as
     * {@link AbstractQueue}s. Ordering is according to {@link QueueComparator}.
     * To reflect the true ordering of the JobQueues, the complete hierarchy is
     * sorted such that {@link AbstractQueue}s are ordered according to their
     * needs at each level in the hierarchy, after which only the leaf level
     * {@link JobQueue}s are returned.
     * 
     * @return a list of {@link JobQueue}s wrapped as {@link AbstractQueue}s
     *         sorted by their needs.
     */
    List<AbstractQueue> getOrderedJobQueues() {
      scheduler.root.sort(queueComparator);
      return scheduler.root.getDescendentJobQueues();
    }

    TaskSchedulingMgr(CapacityTaskScheduler sched) {
      scheduler = sched;
    }

    private boolean isUserOverLimit(JobInProgress j,
                                    QueueSchedulingContext qsc) {
      // what is our current capacity? It is equal to the queue-capacity if
      // we're running below capacity. If we're running over capacity, then its
      // #running plus slotPerTask of the job (which is the number of extra
      // slots we're getting).
      int currentCapacity;
      TaskSchedulingContext tsi = getTSC(qsc);
      if (tsi.getNumSlotsOccupied() < tsi.getCapacity()) {
        currentCapacity = tsi.getCapacity();
      }
      else {
        currentCapacity =
          tsi.getNumSlotsOccupied() +
            TaskDataView.getTaskDataView(type).getSlotsPerTask(j);
      }
      int limit = Math.max((int)(Math.ceil((double)currentCapacity/
          (double) qsc.getNumJobsByUser().size())),
          (int)(Math.ceil((double)(qsc.getUlMin() *currentCapacity)/100.0)));
      String user = j.getProfile().getUser();
      if (tsi.getNumSlotsOccupiedByUser().get(user) >= limit) {
        LOG.debug("User " + user + " is over limit, num slots occupied = " +
            tsi.getNumSlotsOccupiedByUser().get(user) + ", limit = " + limit);
        return true;
      }
      else {
        return false;
      }
    }

    /*
     * This is the central scheduling method.
     * It tries to get a task from jobs in a single queue.
     * Always return a TaskLookupResult object. Don't return null.
     */
    private TaskLookupResult getTaskFromQueue(TaskTracker taskTracker,
                                              QueueSchedulingContext qsi)
    throws IOException {
      TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus();
      // we only look at jobs in the running queues, as these are the ones
      // who have been potentially initialized

      for (JobInProgress j :
        scheduler.jobQueuesManager.getJobQueue(qsi.getQueueName())
          .getRunningJobs()) {
        // only look at jobs that can be run. We ignore jobs that haven't
        // initialized, or have completed but haven't been removed from the
        // running queue.

        //Check queue for maximum capacity .
        if(areTasksInQueueOverMaxCapacity(qsi,j.getNumSlotsPerTask(type))) {
          continue;
        }
        
        if (j.getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }
        // check if the job's user is over limit
        if (isUserOverLimit(j, qsi)) {
          continue;
        }
        //If this job meets memory requirements. Ask the JobInProgress for
        //a task to be scheduled on the task tracker.
        //if we find a job then we pass it on.
        if (scheduler.memoryMatcher.matchesMemoryRequirements(j, type,
                                                              taskTrackerStatus)) {
          // We found a suitable job. Get task from it.
          Task t = obtainNewTask(taskTrackerStatus, j);
          //if there is a task return it immediately.
          if (t != null) {
            // we're successful in getting a task
            return TaskLookupResult.getTaskFoundResult(t);
          } else {
            //skip to the next job in the queue.
            LOG.debug("Job " + j.getJobID().toString()
                + " returned no tasks of type " + type);
          }
        } else {
          // if memory requirements don't match then we check if the job has
          // pending tasks and has insufficient number of 'reserved'
          // tasktrackers to cover all pending tasks. If so we reserve the
          // current tasktracker for this job so that high memory jobs are not
          // starved
          TaskDataView view = TaskDataView.getTaskDataView(type);
          if ((view.getPendingTasks(j) != 0 && 
                !view.hasSufficientReservedTaskTrackers(j))) {
            // Reserve all available slots on this tasktracker
            LOG.info(j.getJobID() + ": Reserving "
                + taskTracker.getTrackerName()
                + " since memory-requirements don't match");
            taskTracker.reserveSlots(type, j, taskTracker
                .getAvailableSlots(type));

            // Block
            return TaskLookupResult.getMemFailedResult();
          }
        }//end of memory check block
        // if we're here, this job has no task to run. Look at the next job.
      }//end of for loop

      // if we're here, we haven't found any task to run among all jobs in
      // the queue. This could be because there is nothing to run, or that
      // the user limit for some user is too strict, i.e., there's at least
      // one user who doesn't have enough tasks to satisfy his limit. If
      // it's the latter case, re-look at jobs without considering user
      // limits, and get a task from the first eligible job; however
      // we do not 'reserve' slots on tasktrackers anymore since the user is
      // already over the limit
      // Note: some of the code from above is repeated here. This is on
      // purpose as it improves overall readability.
      // Note: we walk through jobs again. Some of these jobs, which weren't
      // considered in the first pass, shouldn't be considered here again,
      // but we still check for their viability to keep the code simple. In
      // some cases, for high mem jobs that have nothing to run, we call
      // obtainNewTask() unnecessarily. Should this be a problem, we can
      // create a list of jobs to look at (those whose users were over
      // limit) in the first pass and walk through that list only.
      for (JobInProgress j :
        scheduler.jobQueuesManager.getJobQueue(qsi.getQueueName())
          .getRunningJobs()) {
        if (j.getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }

        //Check for the maximum-capacity.
        if(areTasksInQueueOverMaxCapacity(qsi,j.getNumSlotsPerTask(type))) {
          continue;
        }


        if (scheduler.memoryMatcher.matchesMemoryRequirements(j, type,
            taskTrackerStatus)) {
          // We found a suitable job. Get task from it.
          Task t = obtainNewTask(taskTrackerStatus, j);
          //if there is a task return it immediately.
          if (t != null) {
            // we're successful in getting a task
            return TaskLookupResult.getTaskFoundResult(t);
          } else {
          }
        } else {
          //if memory requirements don't match then we check if the
          //job has either pending or speculative task. If the job
          //has pending or speculative task we block till this job
          //tasks get scheduled, so that high memory jobs are not
          //starved
          if (TaskDataView.getTaskDataView(type).getPendingTasks(j) != 0 ||
            hasSpeculativeTask(j, taskTrackerStatus)) {
            return TaskLookupResult.getMemFailedResult();
          }
        }//end of memory check block
      }//end of for loop

      // found nothing for this queue, look at the next one.
      String msg = "Found no task from the queue " + qsi.getQueueName();
      LOG.debug(msg);
      return TaskLookupResult.getNoTaskFoundResult();
    }

    // Always return a TaskLookupResult object. Don't return null.
    // The caller is responsible for ensuring that the QSC objects and the
    // collections are up-to-date.
    private TaskLookupResult assignTasks(TaskTracker taskTracker)
    throws IOException {
      TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus();

      printQSCs();

      // Check if this tasktracker has been reserved for a job...
      JobInProgress job = taskTracker.getJobForFallowSlot(type);
      if (job != null) {
        int availableSlots = taskTracker.getAvailableSlots(type);
        if (LOG.isDebugEnabled()) {
          LOG.debug(job.getJobID() + ": Checking 'reserved' tasktracker " +
                    taskTracker.getTrackerName() + " with " + availableSlots +
                    " '" + type + "' slots");
        }

        if (availableSlots >= job.getNumSlotsPerTask(type)) {
          // Unreserve
          taskTracker.unreserveSlots(type, job);

          // We found a suitable job. Get task from it.
          Task t = obtainNewTask(taskTrackerStatus, job);
          //if there is a task return it immediately.
          if (t != null) {
            if (LOG.isDebugEnabled()) {
              LOG.info(job.getJobID() + ": Got " + t.getTaskID() +
                       " for reserved tasktracker " +
                       taskTracker.getTrackerName());
            }
            // we're successful in getting a task
            return TaskLookupResult.getTaskFoundResult(t);
          }
        } else {
          // Re-reserve the current tasktracker
          taskTracker.reserveSlots(type, job, availableSlots);

          if (LOG.isDebugEnabled()) {
            LOG.debug(job.getJobID() + ": Re-reserving " +
                      taskTracker.getTrackerName());
          }

          return TaskLookupResult.getMemFailedResult();
        }
      }

      for (AbstractQueue q : getOrderedJobQueues()) {
        QueueSchedulingContext qsc = q.getQueueSchedulingContext();
        // we may have queues with capacity=0. We shouldn't look at jobs from
        // these queues
        if (0 == getTSC(qsc).getCapacity()) {
          continue;
        }

        //This call is important for optimization purposes , if we
        //have reached the limit already no need for traversing the queue.
        if(this.areTasksInQueueOverMaxCapacity(qsc,1)) {
          continue;
        }
        
        TaskLookupResult tlr = getTaskFromQueue(taskTracker, qsc);
        TaskLookupResult.LookUpStatus lookUpStatus = tlr.getLookUpStatus();

        if (lookUpStatus == TaskLookupResult.LookUpStatus.NO_TASK_FOUND) {
          continue; // Look in other queues.
        }

        // if we find a task, return
        if (lookUpStatus == TaskLookupResult.LookUpStatus.TASK_FOUND) {
          return tlr;
        }
        // if there was a memory mismatch, return
        else if (lookUpStatus ==
          TaskLookupResult.LookUpStatus.TASK_FAILING_MEMORY_REQUIREMENT) {
            return tlr;
        }
      }

      // nothing to give
      return TaskLookupResult.getNoTaskFoundResult();
    }


    /**
     * Check if maximum-capacity is set  for this queue.
     * If set and greater than 0 ,
     * check if numofslotsoccupied+numSlotsPerTask is greater than
     * maximum-Capacity ,if yes , implies this queue is over limit.
     *
     * Incase noOfSlotsOccupied is less than maximum-capacity ,but ,
     * numOfSlotsOccupied+noSlotsPerTask is more than maximum-capacity we still
     * dont assign the task . This may lead to under utilization of very small
     * set of slots. But this is ok ,as we strictly respect the maximum-capacity
     * @param qsc
     * @param noOfSlotsPerTask
     * @return true if queue is over maximum-capacity
     */
    private boolean areTasksInQueueOverMaxCapacity(
      QueueSchedulingContext qsc,int noOfSlotsPerTask) {
      TaskSchedulingContext tsi = getTSC(qsc);
      //check for maximum-capacity
      if(tsi.getMaxCapacity() >= 0) {
        if ((tsi.getNumSlotsOccupied() + noOfSlotsPerTask) >
          tsi.getMaxCapacity()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "Queue " + qsc.getQueueName() + " " + "has reached its  max " +
                type + "Capacity");
            LOG.debug("Current running tasks " + tsi.getCapacity());

          }
          return true;
        }
      }
      return false;
    }


    // for debugging.
    private void printQSCs() {
      if (LOG.isDebugEnabled()) {
        StringBuffer s = new StringBuffer();
        for (AbstractQueue aq: getOrderedJobQueues()) {
          QueueSchedulingContext qsi = aq.getQueueSchedulingContext();
          TaskSchedulingContext tsi = getTSC(qsi);
          Collection<JobInProgress> runJobs =
            scheduler.jobQueuesManager.getJobQueue(qsi.getQueueName())
              .getRunningJobs();
          s.append(
            String.format(
              " Queue '%s'(%s): runningTasks=%d, "
                + "occupiedSlots=%d, capacity=%d, runJobs=%d  maximumCapacity=%d ",
              qsi.getQueueName(),
              this.type, tsi.getNumRunningTasks(),
              tsi.getNumSlotsOccupied(), tsi.getCapacity(), (runJobs.size()),
              tsi.getMaxCapacity()));
        }
        LOG.debug(s);
      }
    }

    /**
     * Check if one of the tasks have a speculative task to execute on the
     * particular task tracker.
     *
     * @param tips tasks of a job
     * @param tts task tracker status for which we are asking speculative tip
     * @return true if job has a speculative task to run on particular TT.
     */
    boolean hasSpeculativeTask(
      TaskInProgress[] tips,
      TaskTrackerStatus tts) {
      long currentTime = System.currentTimeMillis();
      for(TaskInProgress tip : tips)  {
        if(tip.isRunning()
            && !(tip.hasRunOnMachine(tts.getHost(), tts.getTrackerName()))
            && tip.canBeSpeculated(currentTime)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * The scheduling algorithms for map tasks.
   */
  private static class MapSchedulingMgr extends TaskSchedulingMgr {

    MapSchedulingMgr(CapacityTaskScheduler schedulr) {
      super(schedulr);
      type = TaskType.MAP;
      queueComparator = mapComparator;
    }

    @Override
    Task obtainNewTask(TaskTrackerStatus taskTracker, JobInProgress job)
    throws IOException {
      synchronized (scheduler) {
        ClusterStatus clusterStatus = scheduler.taskTrackerManager
            .getClusterStatus();
        int numTaskTrackers = clusterStatus.getTaskTrackers();
        return job.obtainNewMapTask(taskTracker, numTaskTrackers,
            scheduler.taskTrackerManager.getNumberOfUniqueHosts());
      }
    }

    @Override
    int getClusterCapacity() {
      synchronized (scheduler) {
        return scheduler.taskTrackerManager.getClusterStatus().getMaxMapTasks();
      }
    }

    @Override
    TaskSchedulingContext getTSC(QueueSchedulingContext qsi) {
      return qsi.getMapTSC();
    }


    @Override
    boolean hasSpeculativeTask(JobInProgress job, TaskTrackerStatus tts) {
      //Check if job supports speculative map execution first then
      //check if job has speculative maps.
      return (job.getJobConf().getMapSpeculativeExecution())&& (
          hasSpeculativeTask(job.getTasks(TaskType.MAP),
                             tts));
    }

  }

  /**
   * The scheduling algorithms for reduce tasks.
   */
  private static class ReduceSchedulingMgr extends TaskSchedulingMgr {

    ReduceSchedulingMgr(CapacityTaskScheduler schedulr) {
      super(schedulr);
      type = TaskType.REDUCE;
      queueComparator = reduceComparator;
    }

    @Override
    Task obtainNewTask(TaskTrackerStatus taskTracker, JobInProgress job)
    throws IOException {
      synchronized (scheduler) {
        ClusterStatus clusterStatus = scheduler.taskTrackerManager
            .getClusterStatus();
        int numTaskTrackers = clusterStatus.getTaskTrackers();
        return job.obtainNewReduceTask(taskTracker, numTaskTrackers,
            scheduler.taskTrackerManager.getNumberOfUniqueHosts());
      }
    }

    @Override
    int getClusterCapacity() {
      synchronized (scheduler) {
        return scheduler.taskTrackerManager.getClusterStatus()
            .getMaxReduceTasks();
      }
    }

    @Override
    TaskSchedulingContext getTSC(QueueSchedulingContext qsi) {
      return qsi.getReduceTSC();
    }

    @Override
    boolean hasSpeculativeTask(JobInProgress job, TaskTrackerStatus tts) {
      //check if the job supports reduce speculative execution first then
      //check if the job has speculative tasks.
      return (job.getJobConf().getReduceSpeculativeExecution()) && (
          hasSpeculativeTask(job.getTasks(TaskType.REDUCE),
                             tts));
    }

  }

  /** the scheduling mgrs for Map and Reduce tasks */
  protected TaskSchedulingMgr mapScheduler = new MapSchedulingMgr(this);
  protected TaskSchedulingMgr reduceScheduler = new ReduceSchedulingMgr(this);

  MemoryMatcher memoryMatcher = new MemoryMatcher();

  static final Log LOG = LogFactory.getLog(CapacityTaskScheduler.class);
  protected JobQueuesManager jobQueuesManager;

  /** whether scheduler has started or not */
  private boolean started = false;

  /**
   * A clock class - can be mocked out for testing.
   */
  static class Clock {
    long getTime() {
      return System.currentTimeMillis();
    }
  }

  private Clock clock;
  private JobInitializationPoller initializationPoller;

  class CapacitySchedulerQueueRefresher extends QueueRefresher {
    @Override
    void refreshQueues(List<JobQueueInfo> newRootQueues)
        throws Throwable {
      if (!started) {
        String msg =
            "Capacity Scheduler is not in the 'started' state."
                + " Cannot refresh queues.";
        LOG.error(msg);
        throw new IOException(msg);
      }
      CapacitySchedulerConf schedConf = new CapacitySchedulerConf();
      initializeQueues(newRootQueues, schedConf, true);
      initializationPoller.refreshQueueInfo(schedConf);
    }
  }

  public CapacityTaskScheduler() {
    this(new Clock());
  }
  
  // for testing
  public CapacityTaskScheduler(Clock clock) {
    this.jobQueuesManager = new JobQueuesManager();
    this.clock = clock;
  }

  @Override
  QueueRefresher getQueueRefresher() {
    return new CapacitySchedulerQueueRefresher();
  }

  /**
   * Only for testing.
   * @param type
   * @return
   */
  String[] getOrderedQueues(TaskType type) {
    if (type == TaskType.MAP) {
      return mapScheduler.getOrderedQueues();
    } else if (type == TaskType.REDUCE) {
      return reduceScheduler.getOrderedQueues();
    }
    return null;
  }

  @Override
  public synchronized void start() throws IOException {
    if (started) return;
    super.start();

    // Initialize MemoryMatcher
    MemoryMatcher.initializeMemoryRelatedConf(conf);

    // read queue info from config file
    QueueManager queueManager = taskTrackerManager.getQueueManager();

    // initialize our queues from the config settings
    CapacitySchedulerConf schedConf = new CapacitySchedulerConf();
    try {
      initializeQueues(queueManager.getRoot().getJobQueueInfo().getChildren(),
          schedConf, false);
    } catch (Throwable e) {
      LOG.error("Couldn't initialize queues because of the excecption : "
          + StringUtils.stringifyException(e));
      throw new IOException(e);
    }

    // Queues are ready. Now register jobQueuesManager with the JobTracker so as
    // to listen to job changes
    taskTrackerManager.addJobInProgressListener(jobQueuesManager);

    //Start thread for initialization
    if (initializationPoller == null) {
      this.initializationPoller = new JobInitializationPoller(
          jobQueuesManager, taskTrackerManager);
    }
    initializationPoller.init(jobQueuesManager.getJobQueueNames(), schedConf);
    initializationPoller.setDaemon(true);
    initializationPoller.start();

    started = true;

    LOG.info("Capacity scheduler started successfully");  
  }

  /**
   * Read the configuration and initialize the queues. This operation should be
   * done only when either the scheduler is starting or a request is received
   * from {@link QueueManager} to refresh the queue configuration.
   * 
   * <p>
   * 
   * Even in case of refresh, we do not explicitly destroy AbstractQueue items,
   * or the info maps, they will be automatically garbage-collected.
   * 
   * <p>
   * 
   * We don't explicitly lock the scheduler completely. This method is called at
   * two times. 1) When the scheduler is starting. During this time, the lock
   * sequence is JT->scheduler and so we don't need any more locking here. 2)
   * When refresh is issued to {@link QueueManager}. When this happens, parallel
   * refreshes are guarded by {@link QueueManager} itself by taking its lock.
   * 
   * @param newRootQueues
   * @param schedConf
   * @param refreshingQueues
   * @throws Throwable
   */
  private void initializeQueues(List<JobQueueInfo> newRootQueues,
      CapacitySchedulerConf schedConf, boolean refreshingQueues)
      throws Throwable {

    if (newRootQueues == null) {
      throw new IOException(
          "Cannot initialize the queues with null root-queues!");
    }

    // Sanity check: there should be at least one queue.
    if (0 == newRootQueues.size()) {
      throw new IllegalStateException("System has no queue configured!");
    }

    // Create a new queue-hierarchy builder and try loading the complete
    // hierarchy of queues.
    AbstractQueue newRootAbstractQueue;
    try {
      newRootAbstractQueue =
          new QueueHierarchyBuilder().createHierarchy(newRootQueues, schedConf);

    } catch (Throwable e) {
      LOG.error("Exception while tryign to (re)initializing queues : "
          + StringUtils.stringifyException(e));
      LOG.info("(Re)initializing the queues with the new configuration "
          + "failed, so keeping the old configuration.");
      throw e;
    }

    // New configuration is successfully validated and applied, set the new
    // configuration to the current queue-hierarchy.

    if (refreshingQueues) {
      // Scheduler is being refreshed.

      // Going to commit the changes to the hierarchy. Lock the scheduler.
      synchronized (this) {
        AbstractQueueComparator comparator = new AbstractQueueComparator();
        this.root.sort(comparator);
        newRootAbstractQueue.sort(comparator);
        root.validateAndCopyQueueContexts(newRootAbstractQueue);
      }
    } else {
      // Scheduler is just starting.

      this.root = newRootAbstractQueue;

      // JobQueue objects are created. Inform the JobQueuesManager so that it
      // can track the running/waiting jobs. JobQueuesManager is still not added
      // as a listener to JobTracker, so no locking needed.
      addJobQueuesToJobQueuesManager();
    }

    List<AbstractQueue> allQueues = new ArrayList<AbstractQueue>();
    allQueues.addAll(getRoot().getDescendantContainerQueues());
    allQueues.addAll(getRoot().getDescendentJobQueues());
    for (AbstractQueue queue : allQueues) {
      if (!refreshingQueues) {
        // Scheduler is just starting, create the display info also
        createDisplayInfo(taskTrackerManager.getQueueManager(), queue.getName());
      }

      // QueueSchedulingContext objects are created/have changed. Put them
      // (back) in the queue-info so as to be consumed by the UI.
      addToQueueInfoMap(queue.getQueueSchedulingContext());
    }
  }

  /**
   * Inform the {@link JobQueuesManager} about the newly constructed
   * {@link JobQueue}s.
   */
  private void addJobQueuesToJobQueuesManager() {
    List<AbstractQueue> allJobQueues = getRoot().getDescendentJobQueues();
    for (AbstractQueue jobQ : allJobQueues) {
      jobQueuesManager.addQueue((JobQueue)jobQ);
    }
  }

  /** mostly for testing purposes */
  synchronized void setInitializationPoller(JobInitializationPoller p) {
    this.initializationPoller = p;
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    if (!started) return;
    if (jobQueuesManager != null) {
      taskTrackerManager.removeJobInProgressListener(
          jobQueuesManager);
    }
    started = false;
    initializationPoller.terminate();
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
  }

  /**
   * provided for the test classes
   * lets you update the QSI objects and sorted collections
   */ 
  synchronized void updateContextInfoForTests() {
    ClusterStatus c = taskTrackerManager.getClusterStatus();
    int mapClusterCapacity = c.getMaxMapTasks();
    int reduceClusterCapacity = c.getMaxReduceTasks();
    // update the QSI objects
    updateContextObjects(mapClusterCapacity, reduceClusterCapacity);
    mapScheduler.scheduler.root.sort(mapScheduler.queueComparator);
    reduceScheduler.scheduler.root.sort(reduceScheduler.queueComparator);
  }

  /**
   * Update individual QSC objects.
   * We don't need exact information for all variables, just enough for us
   * to make scheduling decisions. For example, we don't need an exact count
   * of numRunningTasks. Once we count upto the grid capacity, any
   * number beyond that will make no difference.
   *
   **/
  private synchronized void updateContextObjects(int mapClusterCapacity,
      int reduceClusterCapacity) {
    root.update(mapClusterCapacity,reduceClusterCapacity);

  }

  /*
   * The grand plan for assigning a task. 
   * Always assigns 1 reduce and 1 map , if sufficient slots are
   * available for each of types.
   * If not , then which ever type of slots are available , that type of task is
   * assigned.
   * Next, pick a queue. We only look at queues that need a slot. Among these,
   * we first look at queues whose (# of running tasks)/capacity is the least.
   * Next, pick a job in a queue. we pick the job at the front of the queue
   * unless its user is over the user limit. 
   * Finally, given a job, pick a task from the job. 
   *  
   */
  @Override
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
  throws IOException {
    
    TaskLookupResult tlr;
    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus();
    List<Task> result = new ArrayList<Task>();
    
    /* 
     * If TT has Map and Reduce slot free, we assign 1 map and 1 reduce
     * We  base decision on how much is needed
     * versus how much is used
     */
    ClusterStatus c = taskTrackerManager.getClusterStatus();
    int mapClusterCapacity = c.getMaxMapTasks();
    int reduceClusterCapacity = c.getMaxReduceTasks();
    int maxMapSlots = taskTrackerStatus.getMaxMapSlots();
    int currentMapSlots = taskTrackerStatus.countOccupiedMapSlots();
    int maxReduceSlots = taskTrackerStatus.getMaxReduceSlots();
    int currentReduceSlots = taskTrackerStatus.countOccupiedReduceSlots();
    LOG.debug("TT asking for task, max maps="
      + taskTrackerStatus.getMaxMapSlots() + 
        ", run maps=" + taskTrackerStatus.countMapTasks() + ", max reds=" + 
        taskTrackerStatus.getMaxReduceSlots() + ", run reds=" + 
        taskTrackerStatus.countReduceTasks() + ", map cap=" + 
        mapClusterCapacity + ", red cap = " + 
        reduceClusterCapacity);

    /* 
     * update all our QSC objects.
     * This involves updating each qsC structure. This operation depends
     * on the number of running jobs in a queue, and some waiting jobs. If it
     * becomes expensive, do it once every few heartbeats only.
     */ 
    updateContextObjects(mapClusterCapacity, reduceClusterCapacity);
    // make sure we get our map or reduce scheduling object to update its 
    // collection of QSC objects too.

    if (maxReduceSlots > currentReduceSlots) {
      //reduce slot available , try to get a
      //reduce task
      tlr = reduceScheduler.assignTasks(taskTracker);
      if (TaskLookupResult.LookUpStatus.TASK_FOUND == 
        tlr.getLookUpStatus()) {
        result.add(tlr.getTask());
      }
    }

    if(maxMapSlots > currentMapSlots) {
      //map slot available , try to get a map task
      tlr = mapScheduler.assignTasks(taskTracker);
      if (TaskLookupResult.LookUpStatus.TASK_FOUND == 
        tlr.getLookUpStatus()) {
        result.add(tlr.getTask());
      }
    }
    
    return (result.isEmpty()) ? null : result;
  }

  
  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    Collection<JobInProgress> jobCollection = new ArrayList<JobInProgress>();
    JobQueue jobQueue = jobQueuesManager.getJobQueue(queueName);
    if (jobQueue == null) {
      return jobCollection;
    }
    Collection<JobInProgress> runningJobs =
      jobQueue.getRunningJobs();
    if (runningJobs != null) {
      jobCollection.addAll(runningJobs);
    }
    Collection<JobInProgress> waitingJobs = 
      jobQueue.getWaitingJobs();
    Collection<JobInProgress> tempCollection = new ArrayList<JobInProgress>();
    if(waitingJobs != null) {
      tempCollection.addAll(waitingJobs);
    }
    tempCollection.removeAll(runningJobs);
    if(!tempCollection.isEmpty()) {
      jobCollection.addAll(tempCollection);
    }
    return jobCollection;
  }
  
  synchronized JobInitializationPoller getInitializationPoller() {
    return initializationPoller;
  }

  private synchronized String getDisplayInfo(String queueName) {
    QueueSchedulingContext qsi = queueInfoMap.get(queueName);
    if (null == qsi) {
      return null;
    }
    return qsi.toString();
  }

  private synchronized void addToQueueInfoMap(QueueSchedulingContext qsc) {
    queueInfoMap.put(qsc.getQueueName(), qsc);
  }

  /**
   * Create the scheduler information and set it in the {@link QueueManager}.
   * this should be only called when the scheduler is starting.
   * 
   * @param queueManager
   * @param queueName
   */
  private void createDisplayInfo(QueueManager queueManager, String queueName) {
    if (queueManager != null) {
      SchedulingDisplayInfo schedulingInfo =
        new SchedulingDisplayInfo(queueName, this);
      queueManager.setSchedulerInfo(queueName, schedulingInfo);
    }
  }


  /**
   * Use for testing purposes.
   * returns the root
   * @return
   */
  AbstractQueue getRoot() {
    return this.root;
  }


  /**
   * This is used for testing purpose only
   * Dont use this method.
   * @param rt
   */
  void setRoot(AbstractQueue rt) {
    this.root = rt;
  }

}
