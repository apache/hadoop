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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobTracker.IllegalStateException;
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
  
  /***********************************************************************
   * Keeping track of scheduling information for queues
   * 
   * We need to maintain scheduling information relevant to a queue (its 
   * name, capacity, etc), along with information specific to 
   * each kind of task, Map or Reduce (num of running tasks, pending 
   * tasks etc). 
   * 
   * This scheduling information is used to decide how to allocate
   * tasks, redistribute capacity, etc.
   *  
   * A QueueSchedulingInfo(QSI) object represents scheduling information for
   * a queue. A TaskSchedulingInfo (TSI) object represents scheduling 
   * information for a particular kind of task (Map or Reduce).
   *   
   **********************************************************************/

  private static class TaskSchedulingInfo {
    /** 
     * the actual gc, which depends on how many slots are available
     * in the cluster at any given time. 
     */
    int capacity = 0;
    // number of running tasks
    int numRunningTasks = 0;
    /** for each user, we need to keep track of number of running tasks */
    Map<String, Integer> numRunningTasksByUser = 
      new HashMap<String, Integer>();
    
    /**
     * reset the variables associated with tasks
     */
    void resetTaskVars() {
      numRunningTasks = 0;
      for (String s: numRunningTasksByUser.keySet()) {
        numRunningTasksByUser.put(s, 0);
      }
    }

    /**
     * return information about the tasks
     */
    public String toString(){
      float runningTasksAsPercent = capacity!= 0 ?
          ((float)numRunningTasks * 100/capacity):0;
      StringBuffer sb = new StringBuffer();
      sb.append("Capacity: " + capacity + "\n");
      sb.append(String.format("Running tasks: %.1f%% of Capacity\n",
          runningTasksAsPercent));
      // include info on active users
      if (numRunningTasks != 0) {
        sb.append("Active users:\n");
        for (Map.Entry<String, Integer> entry: numRunningTasksByUser.entrySet()) {
          if ((entry.getValue() == null) || (entry.getValue().intValue() <= 0)) {
            // user has no tasks running
            continue;
          }
          sb.append("User '" + entry.getKey()+ "': ");
          float p = (float)entry.getValue().intValue()*100/numRunningTasks;
          sb.append(String.format("%.1f%% of running tasks\n", p));
        }
      }
      return sb.toString();
    }
  }
  
  private static class QueueSchedulingInfo {
    String queueName;

    /** capacity(%) is set in the config */ 
    float capacityPercent = 0;
    
    /** 
     * to handle user limits, we need to know how many users have jobs in 
     * the queue.
     */  
    Map<String, Integer> numJobsByUser = new HashMap<String, Integer>();
      
    /** min value of user limit (same for all users) */
    int ulMin;
    
    /**
     * We keep track of the JobQueuesManager only for reporting purposes 
     * (in toString()). 
     */
    private JobQueuesManager jobQueuesManager;
    
    /**
     * We keep a TaskSchedulingInfo object for each kind of task we support
     */
    TaskSchedulingInfo mapTSI;
    TaskSchedulingInfo reduceTSI;
    
    public QueueSchedulingInfo(String queueName, float gcPercent, 
        int ulMin, JobQueuesManager jobQueuesManager) {
      this.queueName = new String(queueName);
      this.capacityPercent = gcPercent;
      this.ulMin = ulMin;
      this.jobQueuesManager = jobQueuesManager;
      this.mapTSI = new TaskSchedulingInfo();
      this.reduceTSI = new TaskSchedulingInfo();
    }
    
    /**
     * return information about the queue
     */
    public String toString(){
      // We print out the queue information first, followed by info
      // on map and reduce tasks and job info
      StringBuffer sb = new StringBuffer();
      sb.append("Queue configuration\n");
      //sb.append("Name: " + queueName + "\n");
      sb.append("Capacity Percentage: ");
      sb.append(capacityPercent);
      sb.append("%\n");
      sb.append(String.format("User Limit: %d%s\n",ulMin, "%"));
      sb.append(String.format("Priority Supported: %s\n",
          (jobQueuesManager.doesQueueSupportPriorities(queueName))?
              "YES":"NO"));
      sb.append("-------------\n");
      
      sb.append("Map tasks\n");
      sb.append(mapTSI.toString());
      sb.append("-------------\n");
      sb.append("Reduce tasks\n");
      sb.append(reduceTSI.toString());
      sb.append("-------------\n");
      
      sb.append("Job info\n");
      sb.append(String.format("Number of Waiting Jobs: %d\n", 
          jobQueuesManager.getWaitingJobCount(queueName)));
      sb.append(String.format("Number of users who have submitted jobs: %d\n", 
          numJobsByUser.size()));
      return sb.toString();
    }
  }

  /** quick way to get qsi object given a queue name */
  private Map<String, QueueSchedulingInfo> queueInfoMap = 
    new HashMap<String, QueueSchedulingInfo>();
  
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
      // note that we do not call updateQSIObjects() here for performance
      // reasons. This means that the data we print out may be slightly
      // stale. This data is updated whenever assignTasks() is called
      // If this doesn't happen, the data gets stale. If we see
      // this often, we may need to detect this situation and call 
      // updateQSIObjects(), or just call it each time. 
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
    // can be replaced with a global type, if we have one
    protected static enum TYPE {
      MAP, REDUCE
    }
    protected TYPE type = null;

    abstract Task obtainNewTask(TaskTrackerStatus taskTracker, 
        JobInProgress job) throws IOException; 
    abstract int getPendingTasks(JobInProgress job);
    abstract TaskSchedulingInfo getTSI(QueueSchedulingInfo qsi);

    /**
     * List of QSIs for assigning tasks.
     * Queues are ordered by a ratio of (# of running tasks)/capacity, which
     * indicates how much 'free space' the queue has, or how much it is over
     * capacity. This ordered list is iterated over, when assigning tasks.
     */  
    private List<QueueSchedulingInfo> qsiForAssigningTasks = 
      new ArrayList<QueueSchedulingInfo>();  
    /** 
     * Comparator to sort queues.
     * For maps, we need to sort on QueueSchedulingInfo.mapTSI. For 
     * reducers, we use reduceTSI. So we'll need separate comparators.  
     */ 
    private static abstract class QueueComparator 
      implements Comparator<QueueSchedulingInfo> {
      abstract TaskSchedulingInfo getTSI(QueueSchedulingInfo qsi);
      public int compare(QueueSchedulingInfo q1, QueueSchedulingInfo q2) {
        TaskSchedulingInfo t1 = getTSI(q1);
        TaskSchedulingInfo t2 = getTSI(q2);
        // look at how much capacity they've filled. Treat a queue with gc=0 
        // equivalent to a queue running at capacity
        double r1 = (0 == t1.capacity)? 1.0f:
          (double)t1.numRunningTasks/(double)t1.capacity;
        double r2 = (0 == t2.capacity)? 1.0f:
          (double)t2.numRunningTasks/(double)t2.capacity;
        if (r1<r2) return -1;
        else if (r1>r2) return 1;
        else return 0;
      }
    }
    // subclass for map and reduce comparators
    private static final class MapQueueComparator extends QueueComparator {
      TaskSchedulingInfo getTSI(QueueSchedulingInfo qsi) {
        return qsi.mapTSI;
      }
    }
    private static final class ReduceQueueComparator extends QueueComparator {
      TaskSchedulingInfo getTSI(QueueSchedulingInfo qsi) {
        return qsi.reduceTSI;
      }
    }
    // these are our comparator instances
    protected final static MapQueueComparator mapComparator = new MapQueueComparator();
    protected final static ReduceQueueComparator reduceComparator = new ReduceQueueComparator();
    // and this is the comparator to use
    protected QueueComparator queueComparator;
   
    TaskSchedulingMgr(CapacityTaskScheduler sched) {
      scheduler = sched;
    }
    
    // let the scheduling mgr know which queues are in the system
    void initialize(Map<String, QueueSchedulingInfo> qsiMap) { 
      // add all the qsi objects to our list and sort
      qsiForAssigningTasks.addAll(qsiMap.values());
      Collections.sort(qsiForAssigningTasks, queueComparator);
    }
    
    private synchronized void updateCollectionOfQSIs() {
      Collections.sort(qsiForAssigningTasks, queueComparator);
    }


    private boolean isUserOverLimit(String user, QueueSchedulingInfo qsi) {
      // what is our current capacity? It's capacity if we're running below capacity.
      // If we're running over capacity, then its #running plus 1 (which is the
      // extra slot we're getting). 
      int currentCapacity;
      TaskSchedulingInfo tsi = getTSI(qsi);
      if (tsi.numRunningTasks < tsi.capacity) {
        currentCapacity = tsi.capacity;
      }
      else {
        currentCapacity = tsi.numRunningTasks+1;
      }
      int limit = Math.max((int)(Math.ceil((double)currentCapacity/
          (double)qsi.numJobsByUser.size())), 
          (int)(Math.ceil((double)(qsi.ulMin*currentCapacity)/100.0)));
      if (tsi.numRunningTasksByUser.get(user) >= limit) {
        LOG.debug("User " + user + " is over limit, num running tasks = " + 
            tsi.numRunningTasksByUser.get(user) + ", limit = " + limit);
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
    private TaskLookupResult getTaskFromQueue(TaskTrackerStatus taskTracker,
        QueueSchedulingInfo qsi)
        throws IOException {

      // we only look at jobs in the running queues, as these are the ones
      // who have been potentially initialized

      for (JobInProgress j : 
        scheduler.jobQueuesManager.getRunningJobQueue(qsi.queueName)) {
        // only look at jobs that can be run. We ignore jobs that haven't 
        // initialized, or have completed but haven't been removed from the 
        // running queue. 
        if (j.getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }
        // check if the job's user is over limit
        if (isUserOverLimit(j.getProfile().getUser(), qsi)) {
          continue;
        }
        if (getPendingTasks(j) != 0) {
          // Not accurate TODO:
          // check if the job's memory requirements are met
          if (scheduler.memoryMatcher.matchesMemoryRequirements(j, taskTracker)) {
            // We found a suitable job. Get task from it.
            Task t = obtainNewTask(taskTracker, j);
            if (t != null) {
              // we're successful in getting a task
              return TaskLookupResult.getTaskFoundResult(t);
            }
          }
          else {
            // mem requirements not met. Rather than look at the next job, 
            // we return nothing to the TT, with the hope that we improve 
            // chances of finding a suitable TT for this job. This lets us
            // avoid starving jobs with high mem requirements.         
            return TaskLookupResult.getMemFailedResult();
          }
        }
        // if we're here, this job has no task to run. Look at the next job.
      }

      // if we're here, we haven't found any task to run among all jobs in 
      // the queue. This could be because there is nothing to run, or that 
      // the user limit for some user is too strict, i.e., there's at least 
      // one user who doesn't have enough tasks to satisfy his limit. If 
      // it's the latter case, re-look at jobs without considering user 
      // limits, and get a task from the first eligible job
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
        scheduler.jobQueuesManager.getRunningJobQueue(qsi.queueName)) {
        if (j.getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }
        if (getPendingTasks(j) != 0) {
          // Not accurate TODO:
          // check if the job's memory requirements are met
          if (scheduler.memoryMatcher.matchesMemoryRequirements(j, taskTracker)) {
            // We found a suitable job. Get task from it.
            Task t = obtainNewTask(taskTracker, j);
            if (t != null) {
              // we're successful in getting a task
              return TaskLookupResult.getTaskFoundResult(t);
            }
          }
          else {
            // mem requirements not met. 
            return TaskLookupResult.getMemFailedResult();
          }
        }
        // if we're here, this job has no task to run. Look at the next job.
      }

      // found nothing for this queue, look at the next one.
      String msg = "Found no task from the queue " + qsi.queueName;
      LOG.debug(msg);
      return TaskLookupResult.getNoTaskFoundResult();
    }

    // Always return a TaskLookupResult object. Don't return null. 
    // The caller is responsible for ensuring that the QSI objects and the 
    // collections are up-to-date.
    private TaskLookupResult assignTasks(TaskTrackerStatus taskTracker) throws IOException {
      for (QueueSchedulingInfo qsi : qsiForAssigningTasks) {
        // we may have queues with gc=0. We shouldn't look at jobs from 
        // these queues
        if (0 == getTSI(qsi).capacity) {
          continue;
        }
        TaskLookupResult tlr = getTaskFromQueue(taskTracker, qsi);
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
    
    // for debugging.
    private void printQSIs() {
      StringBuffer s = new StringBuffer();
      for (QueueSchedulingInfo qsi: qsiForAssigningTasks) {
        TaskSchedulingInfo tsi = getTSI(qsi);
        Collection<JobInProgress> runJobs = 
          scheduler.jobQueuesManager.getRunningJobQueue(qsi.queueName);
        s.append(" Queue '" + qsi.queueName + "'(" + this.type + "): run=" + 
            tsi.numRunningTasks + ", gc=" + tsi.capacity
            + ", run jobs="+ runJobs.size() +
            "*** ");
      }
      LOG.debug(s);
    }
    
  }

  /**
   * The scheduling algorithms for map tasks. 
   */
  private static class MapSchedulingMgr extends TaskSchedulingMgr {
    MapSchedulingMgr(CapacityTaskScheduler dad) {
      super(dad);
      type = TaskSchedulingMgr.TYPE.MAP;
      queueComparator = mapComparator;
    }
    Task obtainNewTask(TaskTrackerStatus taskTracker, JobInProgress job) 
    throws IOException {
      ClusterStatus clusterStatus = 
        scheduler.taskTrackerManager.getClusterStatus();
      int numTaskTrackers = clusterStatus.getTaskTrackers();
      return job.obtainNewMapTask(taskTracker, numTaskTrackers, 
          scheduler.taskTrackerManager.getNumberOfUniqueHosts());
    }
    int getClusterCapacity() {
      return scheduler.taskTrackerManager.getClusterStatus().getMaxMapTasks();
    }
    int getRunningTasks(JobInProgress job) {
      return job.runningMaps();
    }
    int getPendingTasks(JobInProgress job) {
      return job.pendingMaps();
    }

    TaskSchedulingInfo getTSI(QueueSchedulingInfo qsi) {
      return qsi.mapTSI;
    }

  }

  /**
   * The scheduling algorithms for reduce tasks. 
   */
  private static class ReduceSchedulingMgr extends TaskSchedulingMgr {
    ReduceSchedulingMgr(CapacityTaskScheduler dad) {
      super(dad);
      type = TaskSchedulingMgr.TYPE.REDUCE;
      queueComparator = reduceComparator;
    }
    Task obtainNewTask(TaskTrackerStatus taskTracker, JobInProgress job) 
    throws IOException {
      ClusterStatus clusterStatus = 
        scheduler.taskTrackerManager.getClusterStatus();
      int numTaskTrackers = clusterStatus.getTaskTrackers();
      return job.obtainNewReduceTask(taskTracker, numTaskTrackers, 
          scheduler.taskTrackerManager.getNumberOfUniqueHosts());
    }
    int getClusterCapacity() {
      return scheduler.taskTrackerManager.getClusterStatus().getMaxReduceTasks();
    }
    int getRunningTasks(JobInProgress job) {
      return job.runningReduces();
    }
    int getPendingTasks(JobInProgress job) {
      return job.pendingReduces();
    }

    TaskSchedulingInfo getTSI(QueueSchedulingInfo qsi) {
      return qsi.reduceTSI;
    }
  }
  
  /** the scheduling mgrs for Map and Reduce tasks */ 
  protected TaskSchedulingMgr mapScheduler = new MapSchedulingMgr(this);
  protected TaskSchedulingMgr reduceScheduler = new ReduceSchedulingMgr(this);

  MemoryMatcher memoryMatcher = new MemoryMatcher(this);

  /** we keep track of the number of map/reduce slots we saw last */
  private int prevMapClusterCapacity = 0;
  private int prevReduceClusterCapacity = 0;
  
    
  static final Log LOG = LogFactory.getLog(CapacityTaskScheduler.class);
  protected JobQueuesManager jobQueuesManager;
  protected CapacitySchedulerConf schedConf;
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

  long limitMaxVmemForTasks;
  long limitMaxPmemForTasks;
  long defaultMaxVmPerTask;
  float defaultPercentOfPmemInVmem;

  public CapacityTaskScheduler() {
    this(new Clock());
  }
  
  // for testing
  public CapacityTaskScheduler(Clock clock) {
    this.jobQueuesManager = new JobQueuesManager(this);
    this.clock = clock;
  }
  
  /** mostly for testing purposes */
  public void setResourceManagerConf(CapacitySchedulerConf conf) {
    this.schedConf = conf;
  }

  /**
   * Normalize the negative values in configuration
   * 
   * @param val
   * @return normalized value
   */
  private long normalizeMemoryConfigValue(long val) {
    if (val < 0) {
      val = JobConf.DISABLED_MEMORY_LIMIT;
    }
    return val;
  }

  private void initializeMemoryRelatedConf() {
    limitMaxVmemForTasks =
        normalizeMemoryConfigValue(conf.getLong(
            JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));

    limitMaxPmemForTasks =
        normalizeMemoryConfigValue(schedConf.getLimitMaxPmemForTasks());

    defaultMaxVmPerTask =
        normalizeMemoryConfigValue(conf.getLong(
            JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));

    defaultPercentOfPmemInVmem = schedConf.getDefaultPercentOfPmemInVmem();
    if (defaultPercentOfPmemInVmem < 0) {
      defaultPercentOfPmemInVmem = JobConf.DISABLED_MEMORY_LIMIT;
    }
  }

  @Override
  public synchronized void start() throws IOException {
    if (started) return;
    super.start();
    // initialize our queues from the config settings
    if (null == schedConf) {
      schedConf = new CapacitySchedulerConf();
    }

    initializeMemoryRelatedConf();
    
    // read queue info from config file
    QueueManager queueManager = taskTrackerManager.getQueueManager();
    Set<String> queues = queueManager.getQueues();
    // Sanity check: there should be at least one queue. 
    if (0 == queues.size()) {
      throw new IllegalStateException("System has no queue configured");
    }

    Set<String> queuesWithoutConfiguredCapacity = new HashSet<String>();
    float totalCapacity = 0.0f;
    for (String queueName: queues) {
      float gc = schedConf.getCapacity(queueName);
      if(gc == -1.0) {
        queuesWithoutConfiguredCapacity.add(queueName);
      }else {
        totalCapacity += gc;
      }
      int ulMin = schedConf.getMinimumUserLimitPercent(queueName); 
      // create our QSI and add to our hashmap
      QueueSchedulingInfo qsi = new QueueSchedulingInfo(queueName, gc, 
                                                    ulMin, jobQueuesManager);
      queueInfoMap.put(queueName, qsi);

      // create the queues of job objects
      boolean supportsPrio = schedConf.isPrioritySupported(queueName);
      jobQueuesManager.createQueue(queueName, supportsPrio);
      
      SchedulingDisplayInfo schedulingInfo = 
        new SchedulingDisplayInfo(queueName, this);
      queueManager.setSchedulerInfo(queueName, schedulingInfo);
      
    }
    float remainingQuantityToAllocate = 100 - totalCapacity;
    float quantityToAllocate = 
      remainingQuantityToAllocate/queuesWithoutConfiguredCapacity.size();
    for(String queue: queuesWithoutConfiguredCapacity) {
      QueueSchedulingInfo qsi = queueInfoMap.get(queue); 
      qsi.capacityPercent = quantityToAllocate;
      schedConf.setCapacity(queue, quantityToAllocate);
    }    
    
    if (totalCapacity > 100.0) {
      throw new IllegalArgumentException("Sum of queue capacities over 100% at "
                                         + totalCapacity);
    }    
    
    // let our mgr objects know about the queues
    mapScheduler.initialize(queueInfoMap);
    reduceScheduler.initialize(queueInfoMap);
    
    // listen to job changes
    taskTrackerManager.addJobInProgressListener(jobQueuesManager);

    //Start thread for initialization
    if (initializationPoller == null) {
      this.initializationPoller = new JobInitializationPoller(
          jobQueuesManager,schedConf,queues);
    }
    initializationPoller.init(queueManager.getQueues(), schedConf);
    initializationPoller.setDaemon(true);
    initializationPoller.start();

    started = true;
    LOG.info("Capacity scheduler initialized " + queues.size() + " queues");  
  }
  
  /** mostly for testing purposes */
  void setInitializationPoller(JobInitializationPoller p) {
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
  void updateQSIInfoForTests() {
    ClusterStatus c = taskTrackerManager.getClusterStatus();
    int mapClusterCapacity = c.getMaxMapTasks();
    int reduceClusterCapacity = c.getMaxReduceTasks();
    // update the QSI objects
    updateQSIObjects(mapClusterCapacity, reduceClusterCapacity);
    mapScheduler.updateCollectionOfQSIs();
    reduceScheduler.updateCollectionOfQSIs();
  }

  /**
   * Update individual QSI objects.
   * We don't need exact information for all variables, just enough for us
   * to make scheduling decisions. For example, we don't need an exact count
   * of numRunningTasks. Once we count upto the grid capacity, any
   * number beyond that will make no difference.
   *
   **/
  private synchronized void updateQSIObjects(int mapClusterCapacity,
      int reduceClusterCapacity) {
    // if # of slots have changed since last time, update.
    // First, compute whether the total number of TT slots have changed
    for (QueueSchedulingInfo qsi: queueInfoMap.values()) {
      // compute new capacities, if TT slots have changed
      if (mapClusterCapacity != prevMapClusterCapacity) {
        qsi.mapTSI.capacity =
          (int)(qsi.capacityPercent*mapClusterCapacity/100);
      }
      if (reduceClusterCapacity != prevReduceClusterCapacity) {
        qsi.reduceTSI.capacity =
          (int)(qsi.capacityPercent*reduceClusterCapacity/100);
      }
      // reset running/pending tasks, tasks per user
      qsi.mapTSI.resetTaskVars();
      qsi.reduceTSI.resetTaskVars();
      // update stats on running jobs
      for (JobInProgress j:
        jobQueuesManager.getRunningJobQueue(qsi.queueName)) {
        if (j.getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }
        int runningMaps = j.runningMaps();
        int runningReduces = j.runningReduces();
        qsi.mapTSI.numRunningTasks += runningMaps;
        qsi.reduceTSI.numRunningTasks += runningReduces;
        Integer i =
          qsi.mapTSI.numRunningTasksByUser.get(j.getProfile().getUser());
        qsi.mapTSI.numRunningTasksByUser.put(j.getProfile().getUser(),
            i+runningMaps);
        i = qsi.reduceTSI.numRunningTasksByUser.get(j.getProfile().getUser());
        qsi.reduceTSI.numRunningTasksByUser.put(j.getProfile().getUser(),
            i+runningReduces);
        LOG.debug("updateQSI: job " + j.getJobID().toString() + ": run(m) = " +
            j.runningMaps() + ", run(r) = " + j.runningReduces() +
            ", finished(m) = " + j.finishedMaps() + ", finished(r)= " +
            j.finishedReduces() + ", failed(m) = " + j.failedMapTasks +
            ", failed(r) = " + j.failedReduceTasks + ", spec(m) = " +
            j.speculativeMapTasks + ", spec(r) = " + j.speculativeReduceTasks
            + ", total(m) = " + j.numMapTasks + ", total(r) = " +
            j.numReduceTasks);
        /*
         * it's fine walking down the entire list of running jobs - there
         * probably will not be many, plus, we may need to go through the
         * list to compute numRunningTasksByUser. If this is expensive, we
         * can keep a list of running jobs per user. Then we only need to
         * consider the first few jobs per user.
         */
      }
    }

    prevMapClusterCapacity = mapClusterCapacity;
    prevReduceClusterCapacity = reduceClusterCapacity;
  }

  /*
   * The grand plan for assigning a task. 
   * First, decide whether a Map or Reduce task should be given to a TT 
   * (if the TT can accept either). 
   * Next, pick a queue. We only look at queues that need a slot. Among
   * these, we first look at queues whose ac is less than gc (queues that 
   * gave up capacity in the past). Next, we look at any other queue that
   * needs a slot. 
   * Next, pick a job in a queue. we pick the job at the front of the queue
   * unless its user is over the user limit. 
   * Finally, given a job, pick a task from the job. 
   *  
   */
  @Override
  public synchronized List<Task> assignTasks(TaskTrackerStatus taskTracker)
      throws IOException {
    
    TaskLookupResult tlr;
    /* 
     * If TT has Map and Reduce slot free, we need to figure out whether to
     * give it a Map or Reduce task.
     * Number of ways to do this. For now, base decision on how much is needed
     * versus how much is used (default to Map, if equal).
     */
    ClusterStatus c = taskTrackerManager.getClusterStatus();
    int mapClusterCapacity = c.getMaxMapTasks();
    int reduceClusterCapacity = c.getMaxReduceTasks();
    int maxMapTasks = taskTracker.getMaxMapTasks();
    int currentMapTasks = taskTracker.countMapTasks();
    int maxReduceTasks = taskTracker.getMaxReduceTasks();
    int currentReduceTasks = taskTracker.countReduceTasks();
    LOG.debug("TT asking for task, max maps=" + taskTracker.getMaxMapTasks() + 
        ", run maps=" + taskTracker.countMapTasks() + ", max reds=" + 
        taskTracker.getMaxReduceTasks() + ", run reds=" + 
        taskTracker.countReduceTasks() + ", map cap=" + 
        mapClusterCapacity + ", red cap = " + 
        reduceClusterCapacity);

    /* 
     * update all our QSI objects.
     * This involves updating each qsi structure. This operation depends
     * on the number of running jobs in a queue, and some waiting jobs. If it
     * becomes expensive, do it once every few heartbeats only.
     */ 
    updateQSIObjects(mapClusterCapacity, reduceClusterCapacity);
    // make sure we get our map or reduce scheduling object to update its 
    // collection of QSI objects too. 

    if ((maxReduceTasks - currentReduceTasks) > 
    (maxMapTasks - currentMapTasks)) {
      // get a reduce task first
      reduceScheduler.updateCollectionOfQSIs();
      tlr = reduceScheduler.assignTasks(taskTracker);
      if (TaskLookupResult.LookUpStatus.TASK_FOUND == 
        tlr.getLookUpStatus()) {
        // found a task; return
        return Collections.singletonList(tlr.getTask());
      }
      else if (TaskLookupResult.LookUpStatus.TASK_FAILING_MEMORY_REQUIREMENT == 
        tlr.getLookUpStatus()) {
        // return no task
        return null;
      }
      // if we didn't get any, look at map tasks, if TT has space
      else if ((TaskLookupResult.LookUpStatus.NO_TASK_FOUND == 
        tlr.getLookUpStatus()) && (maxMapTasks > currentMapTasks)) {
        mapScheduler.updateCollectionOfQSIs();
        tlr = mapScheduler.assignTasks(taskTracker);
        if (TaskLookupResult.LookUpStatus.TASK_FOUND == 
          tlr.getLookUpStatus()) {
          return Collections.singletonList(tlr.getTask());
        }
      }
    }
    else {
      // get a map task first
      mapScheduler.updateCollectionOfQSIs();
      tlr = mapScheduler.assignTasks(taskTracker);
      if (TaskLookupResult.LookUpStatus.TASK_FOUND == 
        tlr.getLookUpStatus()) {
        // found a task; return
        return Collections.singletonList(tlr.getTask());
      }
      else if (TaskLookupResult.LookUpStatus.TASK_FAILING_MEMORY_REQUIREMENT == 
        tlr.getLookUpStatus()) {
        return null;
      }
      // if we didn't get any, look at reduce tasks, if TT has space
      else if ((TaskLookupResult.LookUpStatus.NO_TASK_FOUND == 
        tlr.getLookUpStatus()) && (maxReduceTasks > currentReduceTasks)) {
        reduceScheduler.updateCollectionOfQSIs();
        tlr = reduceScheduler.assignTasks(taskTracker);
        if (TaskLookupResult.LookUpStatus.TASK_FOUND == 
          tlr.getLookUpStatus()) {
          return Collections.singletonList(tlr.getTask());
        }
      }
    }

    return null;
  }

  /**
   * Kill the job if it has invalid requirements and return why it is killed
   * 
   * @param job
   * @return string mentioning why the job is killed. Null if the job has valid
   *         requirements.
   */
  private String killJobIfInvalidRequirements(JobInProgress job) {
    if (!memoryMatcher.isSchedulingBasedOnVmemEnabled()) {
      return null;
    }
    if ((job.getMaxVirtualMemoryForTask() > limitMaxVmemForTasks)
        || (memoryMatcher.isSchedulingBasedOnPmemEnabled() && (job
            .getMaxPhysicalMemoryForTask() > limitMaxPmemForTasks))) {
      String msg =
          job.getJobID() + " (" + job.getMaxVirtualMemoryForTask() + "vmem, "
              + job.getMaxPhysicalMemoryForTask()
              + "pmem) exceeds the cluster's max-memory-limits ("
              + limitMaxVmemForTasks + "vmem, " + limitMaxPmemForTasks
              + "pmem). Cannot run in this cluster, so killing it.";
      LOG.warn(msg);
      try {
        taskTrackerManager.killJob(job.getJobID());
        return msg;
      } catch (IOException ioe) {
        LOG.warn("Failed to kill the job " + job.getJobID() + ". Reason : "
            + StringUtils.stringifyException(ioe));
      }
    }
    return null;
  }

  // called when a job is added
  synchronized void jobAdded(JobInProgress job) throws IOException {
    QueueSchedulingInfo qsi = 
      queueInfoMap.get(job.getProfile().getQueueName());
    // qsi shouldn't be null
    // update user-specific info
    Integer i = qsi.numJobsByUser.get(job.getProfile().getUser());
    if (null == i) {
      i = 1;
      // set the count for running tasks to 0
      qsi.mapTSI.numRunningTasksByUser.put(job.getProfile().getUser(), 0);
      qsi.reduceTSI.numRunningTasksByUser.put(job.getProfile().getUser(), 0);
    }
    else {
      i++;
    }
    qsi.numJobsByUser.put(job.getProfile().getUser(), i);
    LOG.debug("Job " + job.getJobID().toString() + " is added under user " 
              + job.getProfile().getUser() + ", user now has " + i + " jobs");

    // Kill the job if it cannot run in the cluster because of invalid
    // resource requirements.
    String statusMsg = killJobIfInvalidRequirements(job);
    if (statusMsg != null) {
      throw new IOException(statusMsg);
    }
  }

  // called when a job completes
  synchronized void jobCompleted(JobInProgress job) {
    QueueSchedulingInfo qsi = 
      queueInfoMap.get(job.getProfile().getQueueName());
    // qsi shouldn't be null
    // update numJobsByUser
    LOG.debug("JOb to be removed for user " + job.getProfile().getUser());
    Integer i = qsi.numJobsByUser.get(job.getProfile().getUser());
    i--;
    if (0 == i.intValue()) {
      qsi.numJobsByUser.remove(job.getProfile().getUser());
      // remove job footprint from our TSIs
      qsi.mapTSI.numRunningTasksByUser.remove(job.getProfile().getUser());
      qsi.reduceTSI.numRunningTasksByUser.remove(job.getProfile().getUser());
      LOG.debug("No more jobs for user, number of users = " + qsi.numJobsByUser.size());
    }
    else {
      qsi.numJobsByUser.put(job.getProfile().getUser(), i);
      LOG.debug("User still has " + i + " jobs, number of users = "
                + qsi.numJobsByUser.size());
    }
  }
  
  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    Collection<JobInProgress> jobCollection = new ArrayList<JobInProgress>();
    Collection<JobInProgress> runningJobs = 
        jobQueuesManager.getRunningJobQueue(queueName);
    if (runningJobs != null) {
      jobCollection.addAll(runningJobs);
    }
    Collection<JobInProgress> waitingJobs = 
      jobQueuesManager.getWaitingJobs(queueName);
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
  
  JobInitializationPoller getInitializationPoller() {
    return initializationPoller;
  }

  synchronized String getDisplayInfo(String queueName) {
    QueueSchedulingInfo qsi = queueInfoMap.get(queueName);
    if (null == qsi) { 
      return null;
    }
    return qsi.toString();
  }

}

