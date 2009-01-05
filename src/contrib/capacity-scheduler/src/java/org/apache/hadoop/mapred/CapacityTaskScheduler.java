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
 *  * Queues are guaranteed a fraction of the capacity of the grid (their 
 *  'guaranteed capacity') in the sense that a certain capacity of resources 
 *  will be at their disposal. All jobs submitted to the queues of an Org 
 *  will have access to the capacity guaranteed to the Org.
 *  * Free resources can be allocated to any queue beyond its guaranteed 
 *  capacity. These excess allocated resources can be reclaimed and made 
 *  available to another queue in order to meet its capacity guarantee.
 *  * The scheduler guarantees that excess resources taken from a queue will 
 *  be restored to it within N minutes of its need for them.
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
  
  /** 
   * For keeping track of reclaimed capacity. 
   * Whenever slots need to be reclaimed, we create one of these objects. 
   * As the queue gets slots, the amount to reclaim gets decremented. if 
   * we haven't reclaimed enough within a certain time, we need to kill 
   * tasks. This object 'expires' either if all resources are reclaimed
   * before the deadline, or the deadline passes . 
   */
  private static class ReclaimedResource {
    // how much resource to reclaim
    public int originalAmount;
    // how much is to be reclaimed currently
    public int currentAmount;
    // the time, in millisecs, when this object expires. 
    // This time is equal to the time when the object was created, plus
    // the reclaim-time SLA for the queue.  
    public long whenToExpire;
    // we also keep track of when to kill tasks, in millisecs. This is a 
    // fraction of 'whenToExpire', but we store it here so we don't 
    // recompute it every time. 
    public long whenToKill;
    // whether tasks have been killed for this resource
    boolean tasksKilled;
    
    public ReclaimedResource(int amount, long expiryTime, 
        long whenToKill) {
      this.originalAmount = amount;
      this.currentAmount = amount;
      this.whenToExpire = expiryTime;
      this.whenToKill = whenToKill;
      this.tasksKilled = false;
    }
  }

  /** 
   * This class keeps track of scheduling info for each queue for either 
   * Map or Reduce tasks. . 
   * This scheduling information is used by the JT to decide how to allocate
   * tasks, redistribute capacity, etc. 
   */
  private static class QueueSchedulingInfo {
    String queueName;

    /** guaranteed capacity(%) is set at config time */ 
    float guaranteedCapacityPercent = 0;
    /** 
     * the actual gc, which depends on how many slots are available
     * in the cluster at any given time. 
     */
    int guaranteedCapacity = 0;
    
    /** 
     * we also keep track of how many tasks are running for all jobs in 
     * the queue, and how many overall tasks there are. This info is 
     * available for each job, but keeping a sum makes our algos faster.
     */  
    // number of running tasks
    int numRunningTasks = 0;
    // number of pending tasks
    int numPendingTasks = 0;
    
    /** 
     * to handle user limits, we need to know how many users have jobs in 
     * the queue.
     */  
    Map<String, Integer> numJobsByUser = new HashMap<String, Integer>();
    /** for each user, we need to keep track of number of running tasks */
    Map<String, Integer> numRunningTasksByUser = 
      new HashMap<String, Integer>();
      
    /** min value of user limit (same for all users) */
    int ulMin;
    
    /**
     * We need to keep track of resources to reclaim. 
     * Whenever a queue is under capacity and has tasks pending, we offer it 
     * an SLA that gives it free slots equal to or greater than the gap in 
     * its capacity, within a period of time (reclaimTime). 
     * To do this, we periodically check if queues need to reclaim capacity. 
     * If they do, we create a ResourceReclaim object. We also periodically
     * check if a queue has received enough free slots within, say, 80% of 
     * its reclaimTime. If not, we kill enough tasks to make up the 
     * difference. 
     * We keep two queues of ResourceReclaim objects. when an object is 
     * created, it is placed in one queue. Once we kill tasks to recover 
     * resources for that object, it is placed in an expiry queue. we need
     * to do this to prevent creating spurious ResourceReclaim objects. We 
     * keep a count of total resources that are being reclaimed. This count 
     * is decremented when an object expires. 
     */
    
    /**
     * reclaim time limit (in msec). This time represents the SLA we offer 
     * a queue - a queue gets back any lost capacity withing this period 
     * of time.  
     */ 
    long reclaimTime;  
    /**
     * the list of resources to reclaim. This list is always sorted so that
     * resources that need to be reclaimed sooner occur earlier in the list.
     */
    LinkedList<ReclaimedResource> reclaimList = 
      new LinkedList<ReclaimedResource>();
    /**
     * the list of resources to expire. This list is always sorted so that
     * resources that need to be expired sooner occur earlier in the list.
     */
    LinkedList<ReclaimedResource> reclaimExpireList = 
      new LinkedList<ReclaimedResource>();
    /** 
     * sum of all resources that are being reclaimed. 
     * We keep this to prevent unnecessary ReclaimResource objects from being
     * created.  
     */
    int numReclaimedResources = 0;
    
    public QueueSchedulingInfo(String queueName, float guaranteedCapacity, 
        int ulMin, long reclaimTime) {
      this.queueName = new String(queueName);
      this.guaranteedCapacityPercent = guaranteedCapacity;
      this.ulMin = ulMin;
      this.reclaimTime = reclaimTime;
    }
  }

  /**
   * Top level scheduling information to be set to the queueManager
   */
  
  
  private static class SchedulingInfo {
    private QueueSchedulingInfo mqsi;
    private QueueSchedulingInfo rqsi;
    private boolean supportsPriority;
    private JobQueuesManager mgr;
    private long pollingInterval;
    
    
    SchedulingInfo(QueueSchedulingInfo mqsi, 
        QueueSchedulingInfo rqsi, boolean supportsPriority,
        JobQueuesManager mgr, long pollingInterval) {
      this.mqsi = mqsi;
      this.rqsi = rqsi;
      this.supportsPriority=supportsPriority;
      this.mgr = mgr;
      this.pollingInterval = pollingInterval;
    }
    
    @Override
    public String toString(){
      float runningMaps = 0;
      float runningReduces = 0;
      
      Collection<JobInProgress> runningJobs = 
        mgr.getRunningJobQueue(mqsi.queueName);
      
      for(JobInProgress job : runningJobs) {
        runningMaps += job.runningMaps();
        runningReduces += job.runningReduces();
      }
      float usedMaps = mqsi.guaranteedCapacity!= 0 ? 
          (runningMaps * 100/mqsi.guaranteedCapacity):0;
      float usedReduces = rqsi.guaranteedCapacity != 0 ? 
          (runningReduces * 100/rqsi.guaranteedCapacity) :0;
      StringBuffer sb = new StringBuffer();
      sb.append("Guaranteed Capacity : ");
      sb.append(mqsi.guaranteedCapacityPercent);
      sb.append(" %\n");
      sb.append(String.format("Guaranteed Capacity Maps : %d \n",
          mqsi.guaranteedCapacity));
      sb.append(String.format("Guaranteed Capacity Reduces : %d \n",
          rqsi.guaranteedCapacity));
      sb.append(String.format("User Limit : %d %s\n",mqsi.ulMin, "%"));
      sb.append(String.format("Reclaim Time limit : %s \n", 
          StringUtils.formatTime(mqsi.reclaimTime)));
      sb.append(String.format("Priority Supported : %s \n",
          supportsPriority?"YES":"NO"));
      sb.append("-------------\n");
      sb.append(String.format("Running Maps : %s %s\n",
          Float.valueOf(usedMaps).toString(),
          "% of Guaranteed Capacity"));
      sb.append(String.format("Running Reduces : %s %s\n",
          Float.valueOf(usedReduces).toString(),
          "% of Guaranteed Capacity" ));
      sb.append(String.format("Number of Waiting Jobs : %d \n", mgr
          .getWaitingJobCount(mqsi.queueName)));
      return sb.toString();
    }
  }

  private static enum TaskLookUpStatus {
    TASK_FOUND,
    NO_TASK_IN_JOB,
    NO_TASK_IN_QUEUE,
    NO_TASK_MATCHING_MEMORY_REQUIREMENTS,
  }

  private static class TaskLookupResult {

    private Task task;
    private String lookupStatusInfo;

    private TaskLookUpStatus lookUpStatus;

    TaskLookupResult(Task t, TaskLookUpStatus lUStatus, String statusInfo) {
      this.task = t;
      this.lookUpStatus = lUStatus;
      this.lookupStatusInfo = statusInfo;
    }

    Task getTask() {
      return task;
    }

    TaskLookUpStatus getLookUpStatus() {
      return lookUpStatus;
    }

    String getLookupStatusInfo() {
      return lookupStatusInfo;
    }
  }

  /** 
   * This class handles the scheduling algorithms. 
   * The algos are the same for both Map and Reduce tasks. 
   * There may be slight variations later, in which case we can make this
   * an abstract base class and have derived classes for Map and Reduce.  
   */
  private static abstract class TaskSchedulingMgr {

    /** quick way to get qsi object given a queue name */
    private Map<String, QueueSchedulingInfo> queueInfoMap = 
      new HashMap<String, QueueSchedulingInfo>();
    /** we keep track of the number of map or reduce slots we saw last */
    private int numSlots = 0;
    /** our enclosing TaskScheduler object */
    protected CapacityTaskScheduler scheduler;
    // for debugging
    protected static enum TYPE {
      MAP, REDUCE
    }

    protected TYPE type = null;

    abstract Task obtainNewTask(TaskTrackerStatus taskTracker, 
        JobInProgress job) throws IOException; 
    abstract int getClusterCapacity();
    abstract int getRunningTasks(JobInProgress job);
    abstract int getPendingTasks(JobInProgress job);
    abstract int killTasksFromJob(JobInProgress job, int tasksToKill);

    /**
     * List of QSIs for assigning tasks.
     * This list is ordered such that queues that need to reclaim capacity
     * sooner, come before queues that don't. For queues that don't, they're
     * ordered by a ratio of (# of running tasks)/Guaranteed capacity, which
     * indicates how much 'free space' the queue has, or how much it is over
     * capacity. This ordered list is iterated over, when assigning tasks.
     */  
    private List<QueueSchedulingInfo> qsiForAssigningTasks = 
      new ArrayList<QueueSchedulingInfo>();  
    /** comparator to sort queues */ 
    private static final class QueueComparator 
      implements Comparator<QueueSchedulingInfo> {
      public int compare(QueueSchedulingInfo q1, QueueSchedulingInfo q2) {
        // if one queue needs to reclaim something and the other one doesn't, 
        // the former is first
        if ((0 == q1.reclaimList.size()) && (0 != q2.reclaimList.size())) {
          return 1;
        }
        else if ((0 != q1.reclaimList.size()) && (0 == q2.reclaimList.size())){
          return -1;
        }
        else if ((0 == q1.reclaimList.size()) && (0 == q2.reclaimList.size())){
          // neither needs to reclaim. If either doesn't have a capacity yet,
          // it comes at the end of the queue.
          if ((q1.guaranteedCapacity == 0) &&
                (q2.guaranteedCapacity != 0)) {
            return 1;
          } else if ((q1.guaranteedCapacity != 0) &&
                      (q2.guaranteedCapacity == 0)) {
            return -1;
          } else if ((q1.guaranteedCapacity == 0) &&
                      (q2.guaranteedCapacity == 0)) {
            // both don't have capacities, treat them as equal.
            return 0;
          } else {
            // look at how much capacity they've filled
            double r1 = (double)q1.numRunningTasks/(double)q1.guaranteedCapacity;
            double r2 = (double)q2.numRunningTasks/(double)q2.guaranteedCapacity;
            if (r1<r2) return -1;
            else if (r1>r2) return 1;
            else return 0;
          }
        }
        else {
          // both have to reclaim. Look at which one needs to reclaim earlier
          long t1 = q1.reclaimList.get(0).whenToKill;
          long t2 = q2.reclaimList.get(0).whenToKill;
          if (t1<t2) return -1;
          else if (t1>t2) return 1;
          else return 0;
        }
      }
    }
    private final static QueueComparator queueComparator = new QueueComparator();

   
    TaskSchedulingMgr(CapacityTaskScheduler sched) {
      scheduler = sched;
    }
    
    private void add(QueueSchedulingInfo qsi) {
      queueInfoMap.put(qsi.queueName, qsi);
      qsiForAssigningTasks.add(qsi);
    }
    private int getNumQueues() {
      return queueInfoMap.size();
    }
    private boolean isQueuePresent(String queueName) {
      return queueInfoMap.containsKey(queueName);
    }

    /** 
     * Periodically, we walk through our queues to do the following: 
     * a. Check if a queue needs to reclaim any resources within a period
     * of time (because it's running below capacity and more tasks are
     * waiting)
     * b. Check if a queue hasn't received enough of the resources it needed
     * to be reclaimed and thus tasks need to be killed.
     */
    private synchronized void reclaimCapacity() {
      int tasksToKill = 0;
      // with only one queue, there's nothing to do
      if (queueInfoMap.size() < 2) {
        return;
      }
      
      // make sure we always get the latest values
      updateQSIObjects();
      updateCollectionOfQSIs();
      
      QueueSchedulingInfo lastQsi = 
        qsiForAssigningTasks.get(qsiForAssigningTasks.size()-1);
      long currentTime = scheduler.clock.getTime();
      for (QueueSchedulingInfo qsi: queueInfoMap.values()) {
        if (qsi.guaranteedCapacity <= 0) {
          // no capacity, hence nothing can be reclaimed.
          continue;
        }
        // is there any resource that needs to be reclaimed? 
        if ((!qsi.reclaimList.isEmpty()) &&  
            (qsi.reclaimList.getFirst().whenToKill < 
              currentTime + CapacityTaskScheduler.RECLAIM_CAPACITY_INTERVAL)) {
          // make a note of how many tasks to kill to claim resources
          tasksToKill += qsi.reclaimList.getFirst().currentAmount;
          // move this to expiry list
          ReclaimedResource r = qsi.reclaimList.remove();
          qsi.reclaimExpireList.add(r);
        }
        // is there any resource that needs to be expired?
        if ((!qsi.reclaimExpireList.isEmpty()) && 
            (qsi.reclaimExpireList.getFirst().whenToExpire <= currentTime)) {
          ReclaimedResource r = qsi.reclaimExpireList.remove();
          qsi.numReclaimedResources -= r.originalAmount;
        }
        // do we need to reclaim a resource later? 
        // if no queue is over capacity, there's nothing to reclaim
        if (lastQsi.numRunningTasks <= lastQsi.guaranteedCapacity) {
          continue;
        }
        if (qsi.numRunningTasks < qsi.guaranteedCapacity) {
          // usedCap is how much capacity is currently accounted for
          int usedCap = qsi.numRunningTasks + qsi.numReclaimedResources;
          // see if we have remaining capacity and if we have enough pending 
          // tasks to use up remaining capacity
          if ((usedCap < qsi.guaranteedCapacity) && 
              ((qsi.numPendingTasks - qsi.numReclaimedResources)>0)) {
            // create a request for resources to be reclaimed
            int amt = Math.min((qsi.guaranteedCapacity-usedCap), 
                (qsi.numPendingTasks - qsi.numReclaimedResources));
            // create a rsource object that needs to be reclaimed some time
            // in the future
            long whenToKill = qsi.reclaimTime - 
              (CapacityTaskScheduler.HEARTBEATS_LEFT_BEFORE_KILLING * 
                  scheduler.taskTrackerManager.getNextHeartbeatInterval());
            if (whenToKill < 0) whenToKill = 0;
            qsi.reclaimList.add(new ReclaimedResource(amt, 
                currentTime + qsi.reclaimTime, 
                currentTime + whenToKill));
            qsi.numReclaimedResources += amt;
            LOG.debug("Queue " + qsi.queueName + " needs to reclaim " + 
                amt + " resources");
          }
        }
      }
      // kill tasks to reclaim capacity
      if (0 != tasksToKill) {
        killTasks(tasksToKill);
      }
    }

    // kill 'tasksToKill' tasks 
    private void killTasks(int tasksToKill)
    {
      /* 
       * There are a number of fair ways in which one can figure out how
       * many tasks to kill from which queue, so that the total number of
       * tasks killed is equal to 'tasksToKill'.
       * Maybe the best way is to keep a global ordering of running tasks
       * and kill the ones that ran last, irrespective of what queue or 
       * job they belong to. 
       * What we do here is look at how many tasks is each queue running
       * over capacity, and use that as a weight to decide how many tasks
       * to kill from that queue.
       */ 
      
      // first, find out all queues over capacity
      int loc;
      for (loc=0; loc<qsiForAssigningTasks.size(); loc++) {
        QueueSchedulingInfo qsi = qsiForAssigningTasks.get(loc);
        if (qsi.numRunningTasks > qsi.guaranteedCapacity) {
          // all queues from here onwards are running over cap
          break;
        }
      }
      // if some queue needs to reclaim cap, there must be at least one queue
      // over cap. But check, just in case. 
      if (loc == qsiForAssigningTasks.size()) {
        LOG.warn("In Capacity scheduler, we need to kill " + tasksToKill + 
            " tasks but there is no queue over capacity.");
        return;
      }
      // calculate how many total tasks are over cap
      int tasksOverCap = 0;
      for (int i=loc; i<qsiForAssigningTasks.size(); i++) {
        QueueSchedulingInfo qsi = qsiForAssigningTasks.get(i);
        tasksOverCap += (qsi.numRunningTasks - qsi.guaranteedCapacity);
      }
      // now kill tasks from each queue
      for (int i=loc; i<qsiForAssigningTasks.size(); i++) {
        QueueSchedulingInfo qsi = qsiForAssigningTasks.get(i);
        killTasksFromQueue(qsi, (int)Math.round(
            ((double)(qsi.numRunningTasks - qsi.guaranteedCapacity))*
            tasksToKill/(double)tasksOverCap));
      }
    }

    // kill 'tasksToKill' tasks from queue represented by qsi
    private void killTasksFromQueue(QueueSchedulingInfo qsi, int tasksToKill) {
      // we start killing as many tasks as possible from the jobs that started
      // last. This way, we let long-running jobs complete faster.
      int tasksKilled = 0;
      JobInProgress jobs[] = scheduler.jobQueuesManager.
        getRunningJobQueue(qsi.queueName).toArray(new JobInProgress[0]);
      for (int i=jobs.length-1; i>=0; i--) {
        if (jobs[i].getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }
        tasksKilled += killTasksFromJob(jobs[i], tasksToKill-tasksKilled);
        if (tasksKilled >= tasksToKill) break;
      }
    }
   
    // return the TaskAttemptID of the running task, if any, that has made 
    // the least progress.
    TaskAttemptID getRunningTaskWithLeastProgress(TaskInProgress tip) {
      double leastProgress = 1;
      TaskAttemptID tID = null;
      for (Iterator<TaskAttemptID> it = 
        tip.getActiveTasks().keySet().iterator(); it.hasNext();) {
        TaskAttemptID taskid = it.next();
        TaskStatus status = tip.getTaskStatus(taskid);
        if (status.getRunState() == TaskStatus.State.RUNNING) {
          if (status.getProgress() < leastProgress) {
            leastProgress = status.getProgress();
            tID = taskid;
          }
        }
      }
      return tID;
    }
    
    
    /**
     * Update individual QSI objects.
     * We don't need exact information for all variables, just enough for us
     * to make scheduling decisions. For example, we don't need an exact count
     * of numRunningTasks. Once we count upto the grid capacity (gcSum), any
     * number beyond that will make no difference.
     * 
     * The pending task count is only required in reclaim capacity. So 
     * if the computation becomes expensive, we can add a boolean to 
     * denote if pending task computation is required or not.
     * */
    private synchronized void updateQSIObjects() {
      // if # of slots have changed since last time, update. 
      // First, compute whether the total number of TT slots have changed
      int slotsDiff = getClusterCapacity()- numSlots;
      numSlots += slotsDiff;
      for (QueueSchedulingInfo qsi: queueInfoMap.values()) {
        // compute new GCs and ACs, if TT slots have changed
        if (slotsDiff != 0) {
          qsi.guaranteedCapacity =
            (int)(qsi.guaranteedCapacityPercent*numSlots/100);
        }
        qsi.numRunningTasks = 0;
        qsi.numPendingTasks = 0;
        for (String s: qsi.numRunningTasksByUser.keySet()) {
          qsi.numRunningTasksByUser.put(s, 0);
        }
        // update stats on running jobs
        for (JobInProgress j: 
          scheduler.jobQueuesManager.getRunningJobQueue(qsi.queueName)) {
          if (j.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }
          qsi.numRunningTasks += getRunningTasks(j);
          Integer i = qsi.numRunningTasksByUser.get(j.getProfile().getUser());
          qsi.numRunningTasksByUser.put(j.getProfile().getUser(), 
              i+getRunningTasks(j));
          qsi.numPendingTasks += getPendingTasks(j);
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
        
        //update stats on waiting jobs
        for(JobInProgress j : 
          scheduler.jobQueuesManager.getJobs(qsi.queueName)) {
          // pending tasks
          if(qsi.numPendingTasks > getClusterCapacity()) {
            // that's plenty. no need for more computation
            break;
          }
          /*
           * Consider only the waiting jobs in the job queue. Job queue can
           * contain:
           * 1. Jobs which are in running state but not scheduled
           * (these would also be present in running queue), the pending 
           * task count of these jobs is computed when scheduler walks
           * through running job queue.
           * 2. Jobs which are killed by user, but waiting job initialization
           * poller to walk through the job queue to clean up killed jobs.
           */
          if (j.getStatus().getRunState() == JobStatus.PREP) {
            qsi.numPendingTasks += getPendingTasks(j);
          }
        }
      }
    }

    void jobAdded(JobInProgress job) {
      // update qsi 
      QueueSchedulingInfo qsi = 
        queueInfoMap.get(job.getProfile().getQueueName());
      // qsi shouldn't be null
      
      // update user-specific info
      Integer i = qsi.numJobsByUser.get(job.getProfile().getUser());
      if (null == i) {
        i = 1;
        qsi.numRunningTasksByUser.put(job.getProfile().getUser(), 0);
      }
      else {
        i++;
      }
      qsi.numJobsByUser.put(job.getProfile().getUser(), i);
      LOG.debug("Job " + job.getJobID().toString() + " is added under user " 
                + job.getProfile().getUser() + ", user now has " + i + " jobs");
    }

    void jobRemoved(JobInProgress job) {
      // update qsi 
      QueueSchedulingInfo qsi = 
        queueInfoMap.get(job.getProfile().getQueueName());
      // qsi shouldn't be null
      
      // update numJobsByUser
      LOG.debug("JOb to be removed for user " + job.getProfile().getUser());
      Integer i = qsi.numJobsByUser.get(job.getProfile().getUser());
      i--;
      if (0 == i.intValue()) {
        qsi.numJobsByUser.remove(job.getProfile().getUser());
        qsi.numRunningTasksByUser.remove(job.getProfile().getUser());
        LOG.debug("No more jobs for user, number of users = " + qsi.numJobsByUser.size());
      }
      else {
        qsi.numJobsByUser.put(job.getProfile().getUser(), i);
        LOG.debug("User still has " + i + " jobs, number of users = "
                  + qsi.numJobsByUser.size());
      }
    }

    // called when a task is allocated to queue represented by qsi. 
    // update our info about reclaimed resources
    private synchronized void updateReclaimedResources(QueueSchedulingInfo qsi) {
      // if we needed to reclaim resources, we have reclaimed one
      if (qsi.reclaimList.isEmpty()) {
        return;
      }
      ReclaimedResource res = qsi.reclaimList.getFirst();
      res.currentAmount--;
      if (0 == res.currentAmount) {
        // move this resource to the expiry list
        ReclaimedResource r = qsi.reclaimList.remove();
        qsi.reclaimExpireList.add(r);
      }
    }

    private synchronized void updateCollectionOfQSIs() {
      Collections.sort(qsiForAssigningTasks, queueComparator);
    }


    private boolean isUserOverLimit(JobInProgress j, QueueSchedulingInfo qsi) {
      // what is our current capacity? It's GC if we're running below GC. 
      // If we're running over GC, then its #running plus 1 (which is the 
      // extra slot we're getting). 
      int currentCapacity;
      if (qsi.numRunningTasks < qsi.guaranteedCapacity) {
        currentCapacity = qsi.guaranteedCapacity;
      }
      else {
        currentCapacity = qsi.numRunningTasks+1;
      }
      int limit = Math.max((int)(Math.ceil((double)currentCapacity/
          (double)qsi.numJobsByUser.size())), 
          (int)(Math.ceil((double)(qsi.ulMin*currentCapacity)/100.0)));
      if (qsi.numRunningTasksByUser.get(
          j.getProfile().getUser()) >= limit) {
        LOG.debug("User " + j.getProfile().getUser() + 
            " is over limit, num running tasks = " + 
            qsi.numRunningTasksByUser.get(j.getProfile().getUser()) + 
            ", limit = " + limit);
        return true;
      }
      else {
        return false;
      }
    }

    private TaskLookupResult getTaskFromQueue(TaskTrackerStatus taskTracker,
        QueueSchedulingInfo qsi)
        throws IOException {

      // keep track of users over limit
      Set<String> usersOverLimit = new HashSet<String>();

      // Look at running jobs first, skipping jobs of those users who are over
      // their limits
      TaskLookupResult result =
          getTaskFromRunningJobQueue(taskTracker, qsi, usersOverLimit, true);
      TaskLookUpStatus lookUpStatus = result.getLookUpStatus();
      if (lookUpStatus == TaskLookUpStatus.TASK_FOUND
          || lookUpStatus == TaskLookUpStatus.NO_TASK_MATCHING_MEMORY_REQUIREMENTS) {
        // No need for looking elsewhere
        return result;
      }

      // if we're here, we haven't found anything. This could be because
      // there is nothing to run, or that the user limit for some user is
      // too strict, i.e., there's at least one user who doesn't have
      // enough tasks to satisfy his limit. If it's the later case, look at
      // jobs without considering user limits, and get task from first
      // eligible job
      if (usersOverLimit.size() > 0) {
        // look at running jobs, considering users over limit
        result =
            getTaskFromRunningJobQueue(taskTracker, qsi, usersOverLimit, false);
        lookUpStatus = result.getLookUpStatus();
        if (lookUpStatus == TaskLookUpStatus.TASK_FOUND
            || lookUpStatus == TaskLookUpStatus.NO_TASK_MATCHING_MEMORY_REQUIREMENTS) {
          // No need for looking elsewhere
          return result;
        }
      }

      // found nothing for this queue, look at the next one.
      String msg = "Found no task from the queue" + qsi.queueName;
      LOG.info(msg);
      return new TaskLookupResult(null, TaskLookUpStatus.NO_TASK_IN_QUEUE,
          msg);
    }

    // get a task from the running queue
    private TaskLookupResult getTaskFromRunningJobQueue(
        TaskTrackerStatus taskTracker, QueueSchedulingInfo qsi,
        Set<String> usersOverLimit, boolean skipUsersOverLimit)
        throws IOException {

      for (JobInProgress j : scheduler.jobQueuesManager
          .getRunningJobQueue(qsi.queueName)) {
        // some jobs may be in the running queue but may have completed
        // and not yet have been removed from the running queue
        if (j.getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }

        if (skipUsersOverLimit) {
          // consider jobs of only those users who are under limits
          if (isUserOverLimit(j, qsi)) {
            usersOverLimit.add(j.getProfile().getUser());
            continue;
          }
        } else {
          // consider jobs of only those users who are over limit
          if (!usersOverLimit.contains(j.getProfile().getUser())) {
            continue;
          }
        }

        // We found a suitable job. Try getting a task from it.
        TaskLookupResult tlr = getTaskFromJob(j, taskTracker, qsi);
        TaskLookUpStatus lookUpStatus = tlr.getLookUpStatus();
        if (lookUpStatus == TaskLookUpStatus.NO_TASK_IN_JOB) {
          // Go to the next job in the same queue.
          continue;
        } else if (lookUpStatus == TaskLookUpStatus.NO_TASK_MATCHING_MEMORY_REQUIREMENTS
            || lookUpStatus == TaskLookUpStatus.TASK_FOUND) {
          // No need for considering the next jobs in this queue.
          return tlr;
        }
      }

      String msg =
          qsi.queueName + " queue's running jobs queue don't have "
              + "any more tasks to run.";
      LOG.info(msg);
      return new TaskLookupResult(null,
          TaskLookUpStatus.NO_TASK_IN_QUEUE, msg);
    }

    private TaskLookupResult getTaskFromJob(JobInProgress j,
        TaskTrackerStatus taskTracker, QueueSchedulingInfo qsi)
        throws IOException {
      String msg;

      if (getPendingTasks(j) != 0) {
        // Not accurate TODO:
        if (scheduler.memoryMatcher.matchesMemoryRequirements(j, taskTracker)) {
          // We found a suitable job. Get task from it.
          Task t = obtainNewTask(taskTracker, j);
          if (t != null) {
            msg =
                "Got task from job " + j.getJobID() + " in queue "
                    + qsi.queueName;
            LOG.debug(msg);
            return new TaskLookupResult(t, TaskLookUpStatus.TASK_FOUND, msg);
          }
        } else {
          // block the cluster, till this job's tasks can be scheduled.
          msg =
              j.getJobID() + "'s tasks don't fit on the TaskTracker "
                  + taskTracker.trackerName
                  + ". Returning no task to the taskTracker";
          LOG.info(msg);
          return new TaskLookupResult(null,
              TaskLookUpStatus.NO_TASK_MATCHING_MEMORY_REQUIREMENTS, msg);
        }
      }

      msg = j.getJobID() + " doesn't have any more tasks to run.";
      LOG.debug(msg);
      return new TaskLookupResult(null,
          TaskLookUpStatus.NO_TASK_IN_JOB, msg);
    }

    // don't return null
    private TaskLookupResult assignTasks(TaskTrackerStatus taskTracker) throws IOException {
      Task t = null;

      /* 
       * update all our QSI objects.
       * This involves updating each qsi structure. This operation depends
       * on the number of running jobs in a queue, and some waiting jobs. If it
       * becomes expensive, do it once every few heartbeats only.
       */ 
      updateQSIObjects();
      LOG.debug("After updating QSI objects in " + this.type + " scheduler :");
      printQSIs();
      /*
       * sort list of queues first, as we want queues that need the most to
       * get first access. If this is expensive, sort every few heartbeats.
       * We're only sorting a collection of queues - there shouldn't be many.
       */
      updateCollectionOfQSIs();
      for (QueueSchedulingInfo qsi : qsiForAssigningTasks) {
        if (qsi.guaranteedCapacity <= 0.0f) {
          // No capacity is guaranteed yet for this queue.
          // Queues are sorted so that ones without capacities
          // come towards the end. Hence, we can simply return
          // from here without considering any further queues.
          return new TaskLookupResult(null, TaskLookUpStatus.NO_TASK_IN_QUEUE,
              null);
        }

        TaskLookupResult tlr = getTaskFromQueue(taskTracker, qsi);
        TaskLookUpStatus lookUpStatus = tlr.getLookUpStatus();

        if (lookUpStatus == TaskLookUpStatus.NO_TASK_IN_QUEUE) {
          continue; // Look in other queues.
        }

        if (lookUpStatus == TaskLookUpStatus.TASK_FOUND) {
          // we have a task. Update reclaimed resource info
          updateReclaimedResources(qsi);
          return tlr;
        }
        
        if (lookUpStatus == TaskLookUpStatus.NO_TASK_MATCHING_MEMORY_REQUIREMENTS) {
          // blocking the cluster.
          String msg = tlr.getLookupStatusInfo();
          if (msg != null) {
            LOG.warn(msg);
            LOG.warn("Returning nothing to the Tasktracker "
                + taskTracker.trackerName);
            return tlr;
          }
        }
      }

      // nothing to give
      return new TaskLookupResult(null, TaskLookUpStatus.NO_TASK_IN_QUEUE,
          null);
    }
    
    private void printQSIs() {
      StringBuffer s = new StringBuffer();
      for (QueueSchedulingInfo qsi: qsiForAssigningTasks) {
        Collection<JobInProgress> runJobs = 
          scheduler.jobQueuesManager.getRunningJobQueue(qsi.queueName);
        s.append(" Queue '" + qsi.queueName + "'(" + this.type + "): run=" + 
            qsi.numRunningTasks + ", gc=" + qsi.guaranteedCapacity + 
            ", wait=" + qsi.numPendingTasks + ", run jobs="+ runJobs.size() + 
            "*** ");
      }
      LOG.debug(s);
    }
    
    public QueueSchedulingInfo getQueueSchedulingInfo(String queueName) {
      return this.queueInfoMap.get(queueName) ;
    }

  }

  /**
   * The scheduling algorithms for map tasks. 
   */
  private static class MapSchedulingMgr extends TaskSchedulingMgr {
    MapSchedulingMgr(CapacityTaskScheduler dad) {
      super(dad);
      type = TaskSchedulingMgr.TYPE.MAP;
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
    int killTasksFromJob(JobInProgress job, int tasksToKill) {
      /*
       * We'd like to kill tasks that ran the last, or that have made the
       * least progress.
       * Ideally, each job would have a list of tasks, sorted by start 
       * time or progress. That's a lot of state to keep, however. 
       * For now, we do something a little different. We first try and kill
       * non-local tasks, as these can be run anywhere. For each TIP, we 
       * kill the task that has made the least progress, if the TIP has
       * more than one active task. 
       * We then look at tasks in runningMapCache.
       */
      int tasksKilled = 0;
      
      /* 
       * For non-local running maps, we 'cheat' a bit. We know that the set
       * of non-local running maps has an insertion order such that tasks 
       * that ran last are at the end. So we iterate through the set in 
       * reverse. This is OK because even if the implementation changes, 
       * we're still using generic set iteration and are no worse of.
       */ 
      TaskInProgress[] tips = 
        job.getNonLocalRunningMaps().toArray(new TaskInProgress[0]);
      for (int i=tips.length-1; i>=0; i--) {
        // pick the tast attempt that has progressed least
        TaskAttemptID tid = getRunningTaskWithLeastProgress(tips[i]);
        if (null != tid) {
          if (tips[i].killTask(tid, false)) {
            if (++tasksKilled >= tasksToKill) {
              return tasksKilled;
            }
          }
        }
      }
      // now look at other running tasks
      for (Set<TaskInProgress> s: job.getRunningMapCache().values()) {
        for (TaskInProgress tip: s) {
          TaskAttemptID tid = getRunningTaskWithLeastProgress(tip);
          if (null != tid) {
            if (tip.killTask(tid, false)) {
              if (++tasksKilled >= tasksToKill) {
                return tasksKilled;
              }
            }
          }
        }
      }
      return tasksKilled;
    }

  }

  /**
   * The scheduling algorithms for reduce tasks. 
   */
  private static class ReduceSchedulingMgr extends TaskSchedulingMgr {
    ReduceSchedulingMgr(CapacityTaskScheduler dad) {
      super(dad);
      type = TaskSchedulingMgr.TYPE.REDUCE;
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
    int killTasksFromJob(JobInProgress job, int tasksToKill) {
      /* 
       * For reduces, we 'cheat' a bit. We know that the set
       * of running reduces has an insertion order such that tasks 
       * that ran last are at the end. So we iterate through the set in 
       * reverse. This is OK because even if the implementation changes, 
       * we're still using generic set iteration and are no worse of.
       */ 
      int tasksKilled = 0;
      TaskInProgress[] tips = 
        job.getRunningReduces().toArray(new TaskInProgress[0]);
      for (int i=tips.length-1; i>=0; i--) {
        // pick the tast attempt that has progressed least
        TaskAttemptID tid = getRunningTaskWithLeastProgress(tips[i]);
        if (null != tid) {
          if (tips[i].killTask(tid, false)) {
            if (++tasksKilled >= tasksToKill) {
              return tasksKilled;
            }
          }
        }
      }
      return tasksKilled;
    }
  }
  
  /** the scheduling mgrs for Map and Reduce tasks */ 
  protected TaskSchedulingMgr mapScheduler = new MapSchedulingMgr(this);
  protected TaskSchedulingMgr reduceScheduler = new ReduceSchedulingMgr(this);

  MemoryMatcher memoryMatcher = new MemoryMatcher(this);

  /** name of the default queue. */ 
  static final String DEFAULT_QUEUE_NAME = "default";
  
  /** how often does redistribution thread run (in msecs)*/
  private static long RECLAIM_CAPACITY_INTERVAL;
  /** we start killing tasks to reclaim capacity when we have so many 
   * heartbeats left. */
  private static final int HEARTBEATS_LEFT_BEFORE_KILLING = 3;

  static final Log LOG = LogFactory.getLog(CapacityTaskScheduler.class);
  protected JobQueuesManager jobQueuesManager;
  protected CapacitySchedulerConf rmConf;
  /** whether scheduler has started or not */
  private boolean started = false;
  
  /**
   * Used to distribute/reclaim excess capacity among queues
   */ 
  class ReclaimCapacity implements Runnable {
    public ReclaimCapacity() {
    }
    public void run() {
      while (true) {
        try {
          Thread.sleep(RECLAIM_CAPACITY_INTERVAL);
          if (stopReclaim) { 
            break;
          }
          reclaimCapacity();
        } catch (InterruptedException t) {
          break;
        } catch (Throwable t) {
          LOG.error("Error in redistributing capacity:\n" +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }
  private Thread reclaimCapacityThread = null;
  /** variable to indicate that thread should stop */
  private boolean stopReclaim = false;

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
    this.rmConf = conf;
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
        normalizeMemoryConfigValue(rmConf.getLimitMaxPmemForTasks());

    defaultMaxVmPerTask =
        normalizeMemoryConfigValue(conf.getLong(
            JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));

    defaultPercentOfPmemInVmem = rmConf.getDefaultPercentOfPmemInVmem();
    if (defaultPercentOfPmemInVmem < 0) {
      defaultPercentOfPmemInVmem = JobConf.DISABLED_MEMORY_LIMIT;
    }
  }

  @Override
  public synchronized void start() throws IOException {
    if (started) return;
    super.start();
    // initialize our queues from the config settings
    if (null == rmConf) {
      rmConf = new CapacitySchedulerConf();
    }

    initializeMemoryRelatedConf();
    
    RECLAIM_CAPACITY_INTERVAL = rmConf.getReclaimCapacityInterval();
    RECLAIM_CAPACITY_INTERVAL *= 1000;

    // read queue info from config file
    QueueManager queueManager = taskTrackerManager.getQueueManager();
    Set<String> queues = queueManager.getQueues();
    Set<String> queuesWithoutConfiguredGC = new HashSet<String>();
    float totalCapacity = 0.0f;
    for (String queueName: queues) {
      float gc = rmConf.getGuaranteedCapacity(queueName); 
      if(gc == -1.0) {
        queuesWithoutConfiguredGC.add(queueName);
      }else {
        totalCapacity += gc;
      }
      int ulMin = rmConf.getMinimumUserLimitPercent(queueName); 
      long reclaimTimeLimit = rmConf.getReclaimTimeLimit(queueName) * 1000;
      // reclaimTimeLimit is the time(in millisec) within which we need to
      // reclaim capacity. 
      // create queue scheduling objects for Map and Reduce
      mapScheduler.add(new QueueSchedulingInfo(queueName, gc, 
          ulMin, reclaimTimeLimit));
      reduceScheduler.add(new QueueSchedulingInfo(queueName, gc, 
          ulMin, reclaimTimeLimit));

      // create the queues of job objects
      boolean supportsPrio = rmConf.isPrioritySupported(queueName);
      jobQueuesManager.createQueue(queueName, supportsPrio);
      
      SchedulingInfo schedulingInfo = new SchedulingInfo(
          mapScheduler.getQueueSchedulingInfo(queueName),
          reduceScheduler.getQueueSchedulingInfo(queueName),supportsPrio,
          jobQueuesManager,rmConf.getSleepInterval());
      queueManager.setSchedulerInfo(queueName, schedulingInfo);
      
    }
    float remainingQuantityToAllocate = 100 - totalCapacity;
    float quantityToAllocate = 
      remainingQuantityToAllocate/queuesWithoutConfiguredGC.size();
    for(String queue: queuesWithoutConfiguredGC) {
      QueueSchedulingInfo schedulingInfo = 
        mapScheduler.getQueueSchedulingInfo(queue);
      schedulingInfo.guaranteedCapacityPercent = quantityToAllocate;
      schedulingInfo = reduceScheduler.getQueueSchedulingInfo(queue);
      schedulingInfo.guaranteedCapacityPercent = quantityToAllocate;
      rmConf.setGuaranteedCapacity(queue, quantityToAllocate);
    }
    if (totalCapacity > 100.0) {
      throw new IllegalArgumentException("Sum of queue capacities over 100% at "
                                         + totalCapacity);
    }    
    // Sanity check: there should be at least one queue. 
    if (0 == mapScheduler.getNumQueues()) {
      throw new IllegalStateException("System has no queue configured");
    }
    
    // check if there's a queue with the default name. If not, we quit.
    if (!mapScheduler.isQueuePresent(DEFAULT_QUEUE_NAME)) {
      throw new IllegalStateException("System has no default queue configured");
    }
    
    // listen to job changes
    taskTrackerManager.addJobInProgressListener(jobQueuesManager);

    //Start thread for initialization
    if (initializationPoller == null) {
      this.initializationPoller = new JobInitializationPoller(
          jobQueuesManager,rmConf,queues);
    }
    initializationPoller.init(queueManager.getQueues(), rmConf);
    initializationPoller.setDaemon(true);
    initializationPoller.start();
    // start thread for redistributing capacity
    this.reclaimCapacityThread = 
      new Thread(new ReclaimCapacity(),"reclaimCapacity");
    this.reclaimCapacityThread.start();
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
    // tell the reclaim thread to stop
    stopReclaim = true;
    started = false;
    initializationPoller.terminate();
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
  }

  void reclaimCapacity() {
    mapScheduler.reclaimCapacity();
    reduceScheduler.reclaimCapacity();
  }
  
  /**
   * provided for the test classes
   * lets you update the QSI objects and sorted collection
   */ 
  void updateQSIInfo() {
    mapScheduler.updateQSIObjects();
    mapScheduler.updateCollectionOfQSIs();
    reduceScheduler.updateQSIObjects();
    reduceScheduler.updateCollectionOfQSIs();
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
    LOG.debug("TT asking for task, max maps=" + taskTracker.getMaxMapTasks() + 
        ", run maps=" + taskTracker.countMapTasks() + ", max reds=" + 
        taskTracker.getMaxReduceTasks() + ", run reds=" + 
        taskTracker.countReduceTasks() + ", map cap=" + 
        mapScheduler.getClusterCapacity() + ", red cap = " + 
        reduceScheduler.getClusterCapacity());
    int maxMapTasks = taskTracker.getMaxMapTasks();
    int currentMapTasks = taskTracker.countMapTasks();
    int maxReduceTasks = taskTracker.getMaxReduceTasks();
    int currentReduceTasks = taskTracker.countReduceTasks();
    
    if ((maxReduceTasks - currentReduceTasks) > 
    (maxMapTasks - currentMapTasks)) {
      // get a reduce task first
      tlr = reduceScheduler.assignTasks(taskTracker);
      if (TaskLookUpStatus.TASK_FOUND == 
        tlr.getLookUpStatus()) {
        // found a task; return
        return Collections.singletonList(tlr.getTask());
      }
      else if (TaskLookUpStatus.NO_TASK_MATCHING_MEMORY_REQUIREMENTS == 
        tlr.getLookUpStatus()) {
        // return no task
        return null;
      }
      // if we didn't get any, look at map tasks, if TT has space
      else if ((TaskLookUpStatus.NO_TASK_IN_QUEUE == 
        tlr.getLookUpStatus()) && (maxMapTasks > currentMapTasks)) {
        tlr = mapScheduler.assignTasks(taskTracker);
        if (TaskLookUpStatus.TASK_FOUND == tlr.getLookUpStatus()) {
          return Collections.singletonList(tlr.getTask());
        }
      }
    }
    else {
      // get a map task first
      tlr = mapScheduler.assignTasks(taskTracker);
      if (TaskLookUpStatus.TASK_FOUND == 
        tlr.getLookUpStatus()) {
        // found a task; return
        return Collections.singletonList(tlr.getTask());
      }
      else if (TaskLookUpStatus.NO_TASK_MATCHING_MEMORY_REQUIREMENTS == 
        tlr.getLookUpStatus()) {
        // return no task
        return null;
      }
      // if we didn't get any, look at reduce tasks, if TT has space
      else if ((TaskLookUpStatus.NO_TASK_IN_QUEUE == 
        tlr.getLookUpStatus()) && (maxReduceTasks > currentReduceTasks)) {
        tlr = reduceScheduler.assignTasks(taskTracker);
        if (TaskLookUpStatus.TASK_FOUND == tlr.getLookUpStatus()) {
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
    // let our map and reduce schedulers know this, so they can update 
    // user-specific info
    mapScheduler.jobAdded(job);
    reduceScheduler.jobAdded(job);

    // Kill the job if it cannot run in the cluster because of invalid
    // resource requirements.
    String statusMsg = killJobIfInvalidRequirements(job);
    if (statusMsg != null) {
      throw new IOException(statusMsg);
    }
  }

  // called when a job completes
  synchronized void jobCompleted(JobInProgress job) {
    // let our map and reduce schedulers know this, so they can update 
    // user-specific info
    mapScheduler.jobRemoved(job);
    reduceScheduler.jobRemoved(job);
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
      jobQueuesManager.getJobs(queueName);
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
}

