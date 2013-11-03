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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * A scheduler that schedules resources between a set of queues. The scheduler
 * keeps track of the resources used by each queue, and attempts to maintain
 * fairness by scheduling tasks at queues whose allocations are farthest below
 * an ideal fair distribution.
 * 
 * The fair scheduler supports hierarchical queues. All queues descend from a
 * queue named "root". Available resources are distributed among the children
 * of the root queue in the typical fair scheduling fashion. Then, the children
 * distribute the resources assigned to them to their children in the same
 * fashion.  Applications may only be scheduled on leaf queues. Queues can be
 * specified as children of other queues by placing them as sub-elements of their
 * parents in the fair scheduler configuration file.
 * 
 * A queue's name starts with the names of its parents, with periods as
 * separators.  So a queue named "queue1" under the root named, would be 
 * referred to as "root.queue1", and a queue named "queue2" under a queue
 * named "parent1" would be referred to as "root.parent1.queue2".
 */
@LimitedPrivate("yarn")
@Unstable
@SuppressWarnings("unchecked")
public class FairScheduler implements ResourceScheduler {
  private boolean initialized;
  private FairSchedulerConfiguration conf;
  private RMContext rmContext;
  private Resource minimumAllocation;
  private Resource maximumAllocation;
  private Resource incrAllocation;
  private QueueManager queueMgr;
  private Clock clock;
  private boolean usePortForNodeName;

  private static final Log LOG = LogFactory.getLog(FairScheduler.class);
  
  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  
  // Value that container assignment methods return when a container is
  // reserved
  public static final Resource CONTAINER_RESERVED = Resources.createResource(-1);

  // How often fair shares are re-calculated (ms)
  protected long UPDATE_INTERVAL = 500;

  // Whether to use username in place of "default" queue name
  private volatile boolean userAsDefaultQueue = false;

  private final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();

  private static final Allocation EMPTY_ALLOCATION =
      new Allocation(EMPTY_CONTAINER_LIST, Resources.createResource(0));

  // Aggregate metrics
  FSQueueMetrics rootMetrics;

  // Time when we last updated preemption vars
  protected long lastPreemptionUpdateTime;
  // Time we last ran preemptTasksIfNecessary
  private long lastPreemptCheckTime;

  // This stores per-application scheduling information, indexed by
  // attempt ID's for fast lookup.
  @VisibleForTesting
  protected Map<ApplicationAttemptId, FSSchedulerApp> applications = 
      new ConcurrentHashMap<ApplicationAttemptId, FSSchedulerApp>();

  // Nodes in the cluster, indexed by NodeId
  private Map<NodeId, FSSchedulerNode> nodes = 
      new ConcurrentHashMap<NodeId, FSSchedulerNode>();

  // Aggregate capacity of the cluster
  private Resource clusterCapacity = 
      RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Resource.class);

  // How often tasks are preempted 
  protected long preemptionInterval; 
  
  // ms to wait before force killing stuff (must be longer than a couple
  // of heartbeats to give task-kill commands a chance to act).
  protected long waitTimeBeforeKill; 
  
  // Containers whose AMs have been warned that they will be preempted soon.
  private List<RMContainer> warnedContainers = new ArrayList<RMContainer>();
  
  protected boolean preemptionEnabled;
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected WeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected boolean continuousSchedulingEnabled; // Continuous Scheduling enabled or not
  protected int continuousSchedulingSleepMs; // Sleep time for each pass in continuous scheduling
  private Comparator nodeAvailableResourceComparator =
          new NodeAvailableResourceComparator(); // Node available resource comparator
  protected double nodeLocalityThreshold; // Cluster threshold for node locality
  protected double rackLocalityThreshold; // Cluster threshold for rack locality
  protected long nodeLocalityDelayMs; // Delay for node locality
  protected long rackLocalityDelayMs; // Delay for rack locality
  private FairSchedulerEventLog eventLog; // Machine-readable event log
  protected boolean assignMultiple; // Allocate multiple containers per
                                    // heartbeat
  protected int maxAssign; // Max containers to assign per heartbeat

  public FairScheduler() {
    clock = new SystemClock();
    queueMgr = new QueueManager(this);
  }

  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem < 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min should equal greater than 0"
        + ", max should be no smaller than min.");
    }

    // validate scheduler vcores allocation setting
    int minVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores < 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
        + "=" + minVcores
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
        + "=" + maxVcores + ", min should equal greater than 0"
        + ", max should be no smaller than min.");
    }
  }

  public FairSchedulerConfiguration getConf() {
    return conf;
  }

  public QueueManager getQueueManager() {
    return queueMgr;
  }

  private RMContainer getRMContainer(ContainerId containerId) {
    FSSchedulerApp application = 
        applications.get(containerId.getApplicationAttemptId());
    return (application == null) ? null : application.getRMContainer(containerId);
  }

  /**
   * A runnable which calls {@link FairScheduler#update()} every
   * <code>UPDATE_INTERVAL</code> milliseconds.
   */
  private class UpdateThread implements Runnable {
    public void run() {
      while (true) {
        try {
          Thread.sleep(UPDATE_INTERVAL);
          update();
          preemptTasksIfNecessary();
        } catch (Exception e) {
          LOG.error("Exception in fair scheduler UpdateThread", e);
        }
      }
    }
  }

  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and amount of used and
   * required resources per job.
   */
  protected synchronized void update() {
    queueMgr.reloadAllocsIfNecessary(); // Relaod alloc file
    updateRunnability(); // Set job runnability based on user/queue limits
    updatePreemptionVariables(); // Determine if any queues merit preemption

    FSQueue rootQueue = queueMgr.getRootQueue();

    // Recursively update demands for all queues
    rootQueue.updateDemand();

    rootQueue.setFairShare(clusterCapacity);
    // Recursively compute fair shares for all queues
    // and update metrics
    rootQueue.recomputeShares();
  }

  /**
   * Update the preemption fields for all QueueScheduables, i.e. the times since
   * each queue last was at its guaranteed share and at > 1/2 of its fair share
   * for each type of task.
   */
  private void updatePreemptionVariables() {
    long now = clock.getTime();
    lastPreemptionUpdateTime = now;
    for (FSLeafQueue sched : queueMgr.getLeafQueues()) {
      if (!isStarvedForMinShare(sched)) {
        sched.setLastTimeAtMinShare(now);
      }
      if (!isStarvedForFairShare(sched)) {
        sched.setLastTimeAtHalfFairShare(now);
      }
    }
  }

  /**
   * Is a queue below its min share for the given task type?
   */
  boolean isStarvedForMinShare(FSLeafQueue sched) {
    Resource desiredShare = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
      sched.getMinShare(), sched.getDemand());
    return Resources.lessThan(RESOURCE_CALCULATOR, clusterCapacity,
        sched.getResourceUsage(), desiredShare);
  }

  /**
   * Is a queue being starved for fair share for the given task type? This is
   * defined as being below half its fair share.
   */
  boolean isStarvedForFairShare(FSLeafQueue sched) {
    Resource desiredFairShare = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
        Resources.multiply(sched.getFairShare(), .5), sched.getDemand());
    return Resources.lessThan(RESOURCE_CALCULATOR, clusterCapacity,
        sched.getResourceUsage(), desiredFairShare);
  }

  /**
   * Check for queues that need tasks preempted, either because they have been
   * below their guaranteed share for minSharePreemptionTimeout or they have
   * been below half their fair share for the fairSharePreemptionTimeout. If
   * such queues exist, compute how many tasks of each type need to be preempted
   * and then select the right ones using preemptTasks.
   */
  protected synchronized void preemptTasksIfNecessary() {
    if (!preemptionEnabled) {
      return;
    }

    long curTime = clock.getTime();
    if (curTime - lastPreemptCheckTime < preemptionInterval) {
      return;
    }
    lastPreemptCheckTime = curTime;

    Resource resToPreempt = Resources.none();

    for (FSLeafQueue sched : queueMgr.getLeafQueues()) {
      resToPreempt = Resources.add(resToPreempt, resToPreempt(sched, curTime));
    }
    if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity, resToPreempt,
        Resources.none())) {
      preemptResources(queueMgr.getLeafQueues(), resToPreempt);
    }
  }

  /**
   * Preempt a quantity of resources from a list of QueueSchedulables. The
   * policy for this is to pick apps from queues that are over their fair share,
   * but make sure that no queue is placed below its fair share in the process.
   * We further prioritize preemption by choosing containers with lowest
   * priority to preempt.
   */
  protected void preemptResources(Collection<FSLeafQueue> scheds,
      Resource toPreempt) {
    if (scheds.isEmpty() || Resources.equals(toPreempt, Resources.none())) {
      return;
    }

    Map<RMContainer, FSSchedulerApp> apps = 
        new HashMap<RMContainer, FSSchedulerApp>();
    Map<RMContainer, FSLeafQueue> queues = 
        new HashMap<RMContainer, FSLeafQueue>();

    // Collect running containers from over-scheduled queues
    List<RMContainer> runningContainers = new ArrayList<RMContainer>();
    for (FSLeafQueue sched : scheds) {
      if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
          sched.getResourceUsage(), sched.getFairShare())) {
        for (AppSchedulable as : sched.getAppSchedulables()) {
          for (RMContainer c : as.getApp().getLiveContainers()) {
            runningContainers.add(c);
            apps.put(c, as.getApp());
            queues.put(c, sched);
          }
        }
      }
    }

    // Sort containers into reverse order of priority
    Collections.sort(runningContainers, new Comparator<RMContainer>() {
      public int compare(RMContainer c1, RMContainer c2) {
        int ret = c1.getContainer().getPriority().compareTo(
            c2.getContainer().getPriority());
        if (ret == 0) {
          return c2.getContainerId().compareTo(c1.getContainerId());
        }
        return ret;
      }
    });
    
    // Scan down the list of containers we've already warned and kill them
    // if we need to.  Remove any containers from the list that we don't need
    // or that are no longer running.
    Iterator<RMContainer> warnedIter = warnedContainers.iterator();
    Set<RMContainer> preemptedThisRound = new HashSet<RMContainer>();
    while (warnedIter.hasNext()) {
      RMContainer container = warnedIter.next();
      if (container.getState() == RMContainerState.RUNNING &&
          Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
              toPreempt, Resources.none())) {
        warnOrKillContainer(container, apps.get(container), queues.get(container));
        preemptedThisRound.add(container);
        Resources.subtractFrom(toPreempt, container.getContainer().getResource());
      } else {
        warnedIter.remove();
      }
    }

    // Scan down the rest of the containers until we've preempted enough, making
    // sure we don't preempt too many from any queue
    Iterator<RMContainer> runningIter = runningContainers.iterator();
    while (runningIter.hasNext() &&
        Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
            toPreempt, Resources.none())) {
      RMContainer container = runningIter.next();
      FSLeafQueue sched = queues.get(container);
      if (!preemptedThisRound.contains(container) &&
          Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
              sched.getResourceUsage(), sched.getFairShare())) {
        warnOrKillContainer(container, apps.get(container), sched);
        
        warnedContainers.add(container);
        Resources.subtractFrom(toPreempt, container.getContainer().getResource());
      }
    }
  }
  
  private void warnOrKillContainer(RMContainer container, FSSchedulerApp app,
      FSLeafQueue queue) {
    LOG.info("Preempting container (prio=" + container.getContainer().getPriority() +
        "res=" + container.getContainer().getResource() +
        ") from queue " + queue.getName());
    
    Long time = app.getContainerPreemptionTime(container);

    if (time != null) {
      // if we asked for preemption more than maxWaitTimeBeforeKill ms ago,
      // proceed with kill
      if (time + waitTimeBeforeKill < clock.getTime()) {
        ContainerStatus status =
          SchedulerUtils.createPreemptedContainerStatus(
            container.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER);

        // TODO: Not sure if this ever actually adds this to the list of cleanup
        // containers on the RMNode (see SchedulerNode.releaseContainer()).
        completedContainer(container, status, RMContainerEventType.KILL);
        LOG.info("Killing container" + container +
            " (after waiting for premption for " +
            (clock.getTime() - time) + "ms)");
      }
    } else {
      // track the request in the FSSchedulerApp itself
      app.addPreemption(container, clock.getTime());
    }
  }

  /**
   * Return the resource amount that this queue is allowed to preempt, if any.
   * If the queue has been below its min share for at least its preemption
   * timeout, it should preempt the difference between its current share and
   * this min share. If it has been below half its fair share for at least the
   * fairSharePreemptionTimeout, it should preempt enough tasks to get up to its
   * full fair share. If both conditions hold, we preempt the max of the two
   * amounts (this shouldn't happen unless someone sets the timeouts to be
   * identical for some reason).
   */
  protected Resource resToPreempt(FSLeafQueue sched, long curTime) {
    String queue = sched.getName();
    long minShareTimeout = queueMgr.getMinSharePreemptionTimeout(queue);
    long fairShareTimeout = queueMgr.getFairSharePreemptionTimeout();
    Resource resDueToMinShare = Resources.none();
    Resource resDueToFairShare = Resources.none();
    if (curTime - sched.getLastTimeAtMinShare() > minShareTimeout) {
      Resource target = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
          sched.getMinShare(), sched.getDemand());
      resDueToMinShare = Resources.max(RESOURCE_CALCULATOR, clusterCapacity,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    if (curTime - sched.getLastTimeAtHalfFairShare() > fairShareTimeout) {
      Resource target = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
          sched.getFairShare(), sched.getDemand());
      resDueToFairShare = Resources.max(RESOURCE_CALCULATOR, clusterCapacity,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    Resource resToPreempt = Resources.max(RESOURCE_CALCULATOR, clusterCapacity,
        resDueToMinShare, resDueToFairShare);
    if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
        resToPreempt, Resources.none())) {
      String message = "Should preempt " + resToPreempt + " res for queue "
          + sched.getName() + ": resDueToMinShare = " + resDueToMinShare
          + ", resDueToFairShare = " + resDueToFairShare;
      LOG.info(message);
    }
    return resToPreempt;
  }

  /**
   * This updates the runnability of all apps based on whether or not any
   * users/queues have exceeded their capacity.
   */
  private void updateRunnability() {
    List<AppSchedulable> apps = new ArrayList<AppSchedulable>();

    // Start by marking everything as not runnable
    for (FSLeafQueue leafQueue : queueMgr.getLeafQueues()) {
      for (AppSchedulable a : leafQueue.getAppSchedulables()) {
        a.setRunnable(false);
        apps.add(a);
      }
    }
    // Create a list of sorted jobs in order of start time and priority
    Collections.sort(apps, new FifoAppComparator());
    // Mark jobs as runnable in order of start time and priority, until
    // user or queue limits have been reached.
    Map<String, Integer> userApps = new HashMap<String, Integer>();
    Map<String, Integer> queueApps = new HashMap<String, Integer>();

    for (AppSchedulable app : apps) {
      String user = app.getApp().getUser();
      String queue = app.getApp().getQueueName();
      int userCount = userApps.containsKey(user) ? userApps.get(user) : 0;
      int queueCount = queueApps.containsKey(queue) ? queueApps.get(queue) : 0;
      if (userCount < queueMgr.getUserMaxApps(user) &&
          queueCount < queueMgr.getQueueMaxApps(queue)) {
        userApps.put(user, userCount + 1);
        queueApps.put(queue, queueCount + 1);
        app.setRunnable(true);
      }
    }
  }

  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return rmContext.getContainerTokenSecretManager();
  }

  // synchronized for sizeBasedWeight
  public synchronized ResourceWeights getAppWeight(AppSchedulable app) {
    if (!app.getRunnable()) {
      // Job won't launch tasks, but don't return 0 to avoid division errors
      return ResourceWeights.NEUTRAL;
    } else {
      double weight = 1.0;
      if (sizeBasedWeight) {
        // Set weight based on current memory demand
        weight = Math.log1p(app.getDemand().getMemory()) / Math.log(2);
      }
      weight *= app.getPriority().getPriority();
      if (weightAdjuster != null) {
        // Run weight through the user-supplied weightAdjuster
        weight = weightAdjuster.adjustWeight(app, weight);
      }
      return new ResourceWeights((float)weight);
    }
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  public Resource getIncrementResourceCapability() {
    return incrAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  public double getNodeLocalityThreshold() {
    return nodeLocalityThreshold;
  }

  public double getRackLocalityThreshold() {
    return rackLocalityThreshold;
  }

  public long getNodeLocalityDelayMs() {
    return nodeLocalityDelayMs;
  }

  public long getRackLocalityDelayMs() {
    return rackLocalityDelayMs;
  }

  public boolean isContinuousSchedulingEnabled() {
    return continuousSchedulingEnabled;
  }

  public synchronized int getContinuousSchedulingSleepMs() {
    return continuousSchedulingSleepMs;
  }

  public Resource getClusterCapacity() {
    return clusterCapacity;
  }

  public synchronized Clock getClock() {
    return clock;
  }

  protected synchronized void setClock(Clock clock) {
    this.clock = clock;
  }

  public FairSchedulerEventLog getEventLog() {
    return eventLog;
  }

  /**
   * Add a new application to the scheduler, with a given id, queue name, and
   * user. This will accept a new app even if the user or queue is above
   * configured limits, but the app will not be marked as runnable.
   */
  protected synchronized void addApplication(
      ApplicationAttemptId applicationAttemptId, String queueName, String user) {
    if (queueName == null || queueName.isEmpty()) {
      String message = "Reject application " + applicationAttemptId +
              " submitted by user " + user + " with an empty queue name.";
      LOG.info(message);
      rmContext.getDispatcher().getEventHandler().handle(
              new RMAppAttemptRejectedEvent(applicationAttemptId, message));
      return;
    }

    RMApp rmApp = rmContext.getRMApps().get(
        applicationAttemptId.getApplicationId());
    FSLeafQueue queue = assignToQueue(rmApp, queueName, user);

    FSSchedulerApp schedulerApp =
        new FSSchedulerApp(applicationAttemptId, user,
            queue, new ActiveUsersManager(getRootQueueMetrics()),
            rmContext);

    // Enforce ACLs
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(user);

    if (!queue.hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)
        && !queue.hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
      String msg = "User " + userUgi.getUserName() +
    	        " cannot submit applications to queue " + queue.getName();
      LOG.info(msg);
      rmContext.getDispatcher().getEventHandler().handle(
    	        new RMAppAttemptRejectedEvent(applicationAttemptId, msg));
      return;
    }

    queue.addApp(schedulerApp);
    queue.getMetrics().submitApp(user, applicationAttemptId.getAttemptId());

    applications.put(applicationAttemptId, schedulerApp);

    LOG.info("Application Submission: " + applicationAttemptId +
        ", user: "+ user +
        ", currently active: " + applications.size());

    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.APP_ACCEPTED));
  }
  
  @VisibleForTesting
  FSLeafQueue assignToQueue(RMApp rmApp, String queueName, String user) {
    // Potentially set queue to username if configured to do so
    if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME) &&
        userAsDefaultQueue) {
      queueName = user;
    }
    
    FSLeafQueue queue = queueMgr.getLeafQueue(queueName,
        conf.getAllowUndeclaredPools());
    if (queue == null) {
      // queue is not an existing or createable leaf queue
      queue = queueMgr.getLeafQueue(YarnConfiguration.DEFAULT_QUEUE_NAME, false);
    }
    
    if (rmApp != null) {
      rmApp.setQueue(queue.getName());
    } else {
      LOG.warn("Couldn't find RM app to set queue name on");
    }
    
    return queue;
  }

  private synchronized void removeApplication(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState) {
    LOG.info("Application " + applicationAttemptId + " is done." +
        " finalState=" + rmAppAttemptFinalState);

    FSSchedulerApp application = applications.get(applicationAttemptId);

    if (application == null) {
      LOG.info("Unknown application " + applicationAttemptId + " has completed!");
      return;
    }

    // Release all the running containers
    for (RMContainer rmContainer : application.getLiveContainers()) {
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(),
              SchedulerUtils.COMPLETED_APPLICATION),
              RMContainerEventType.KILL);
    }

    // Release all reserved containers
    for (RMContainer rmContainer : application.getReservedContainers()) {
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(),
              "Application Complete"),
          RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.
    application.stop(rmAppAttemptFinalState);

    // Inform the queue
    FSLeafQueue queue = queueMgr.getLeafQueue(application.getQueue()
        .getQueueName(), false);
    queue.removeApp(application);

    // Remove from our data-structure
    applications.remove(applicationAttemptId);
  }

  /**
   * Clean up a completed container.
   */
  private synchronized void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }

    Container container = rmContainer.getContainer();

    // Get the application for the finished container
    ApplicationAttemptId applicationAttemptId = container.getId().getApplicationAttemptId();
    FSSchedulerApp application = applications.get(applicationAttemptId);
    if (application == null) {
      LOG.info("Container " + container + " of" +
          " unknown application " + applicationAttemptId +
          " completed with event " + event);
      return;
    }

    // Get the node on which the container was allocated
    FSSchedulerNode node = nodes.get(container.getNodeId());

    if (rmContainer.getState() == RMContainerState.RESERVED) {
      application.unreserve(node, rmContainer.getReservedPriority());
      node.unreserveResource(application);
    } else {
      application.containerCompleted(rmContainer, containerStatus, event);
      node.releaseContainer(container);
      updateRootQueueMetrics();
    }

    LOG.info("Application " + applicationAttemptId +
        " released container " + container.getId() +
        " on node: " + node +
        " with event: " + event);
  }

  private synchronized void addNode(RMNode node) {
    nodes.put(node.getNodeID(), new FSSchedulerNode(node, usePortForNodeName));
    Resources.addTo(clusterCapacity, node.getTotalCapability());
    updateRootQueueMetrics();

    LOG.info("Added node " + node.getNodeAddress() +
        " cluster capacity: " + clusterCapacity);
  }

  private synchronized void removeNode(RMNode rmNode) {
    FSSchedulerNode node = nodes.get(rmNode.getNodeID());
    // This can occur when an UNHEALTHY node reconnects
    if (node == null) {
      return;
    }
    Resources.subtractFrom(clusterCapacity, rmNode.getTotalCapability());
    updateRootQueueMetrics();

    // Remove running containers
    List<RMContainer> runningContainers = node.getRunningContainers();
    for (RMContainer container : runningContainers) {
      completedContainer(container,
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(),
              SchedulerUtils.LOST_CONTAINER),
          RMContainerEventType.KILL);
    }

    // Remove reservations, if any
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      completedContainer(reservedContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              reservedContainer.getContainerId(),
              SchedulerUtils.LOST_CONTAINER),
          RMContainerEventType.KILL);
    }

    nodes.remove(rmNode.getNodeID());
    LOG.info("Removed node " + rmNode.getNodeAddress() +
        " cluster capacity: " + clusterCapacity);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {

    // Make sure this application exists
    FSSchedulerApp application = applications.get(appAttemptId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + appAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    SchedulerUtils.normalizeRequests(ask, new DominantResourceCalculator(),
        clusterCapacity, minimumAllocation, maximumAllocation, incrAllocation);

    // Release containers
    for (ContainerId releasedContainerId : release) {
      RMContainer rmContainer = getRMContainer(releasedContainerId);
      if (rmContainer == null) {
        RMAuditLogger.logFailure(application.getUser(),
            AuditConstants.RELEASE_CONTAINER,
            "Unauthorized access or invalid container", "FairScheduler",
            "Trying to release container not owned by app or with invalid id",
            application.getApplicationId(), releasedContainerId);
      }
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              releasedContainerId,
              SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
    }

    synchronized (application) {
      if (!ask.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("allocate: pre-update" +
              " applicationAttemptId=" + appAttemptId +
              " application=" + application.getApplicationId());
        }
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        LOG.debug("allocate: post-update");
        application.showRequests();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate:" +
            " applicationAttemptId=" + appAttemptId +
            " #ask=" + ask.size());

        LOG.debug("Preempting " + application.getPreemptionContainers().size()
            + " container(s)");
      }
      
      Set<ContainerId> preemptionContainerIds = new HashSet<ContainerId>();
      for (RMContainer container : application.getPreemptionContainers()) {
        preemptionContainerIds.add(container.getContainerId());
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      
      return new Allocation(application.pullNewlyAllocatedContainers(),
          application.getHeadroom(), preemptionContainerIds);
    }
  }

  /**
   * Process a container which has launched on a node, as reported by the node.
   */
  private void containerLaunchedOnNode(ContainerId containerId, FSSchedulerNode node) {
    // Get the application for the finished container
    ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
    FSSchedulerApp application = applications.get(applicationAttemptId);
    if (application == null) {
      LOG.info("Unknown application: " + applicationAttemptId +
          " launched container " + containerId +
          " on node: " + node);
      return;
    }

    application.containerLaunchedOnNode(containerId, node.getNodeID());
  }

  /**
   * Process a heartbeat update from a node.
   */
  private synchronized void nodeUpdate(RMNode nm) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeUpdate: " + nm + " cluster capacity: " + clusterCapacity);
    }
    eventLog.log("HEARTBEAT", nm.getHostName());
    FSSchedulerNode node = nodes.get(nm.getNodeID());

    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    } 
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      completedContainer(getRMContainer(containerId),
          completedContainer, RMContainerEventType.FINISHED);
    }

    if (continuousSchedulingEnabled) {
      if (!completedContainers.isEmpty()) {
        attemptScheduling(node);
      }
    } else {
      attemptScheduling(node);
    }
  }

  private void continuousScheduling() {
    while (true) {
      List<NodeId> nodeIdList = new ArrayList<NodeId>(nodes.keySet());
      Collections.sort(nodeIdList, nodeAvailableResourceComparator);

      // iterate all nodes
      for (NodeId nodeId : nodeIdList) {
        if (nodes.containsKey(nodeId)) {
          FSSchedulerNode node = nodes.get(nodeId);
          try {
            if (Resources.fitsIn(minimumAllocation,
                    node.getAvailableResource())) {
              attemptScheduling(node);
            }
          } catch (Throwable ex) {
            LOG.warn("Error while attempting scheduling for node " + node +
                    ": " + ex.toString(), ex);
          }
        }
      }
      try {
        Thread.sleep(getContinuousSchedulingSleepMs());
      } catch (InterruptedException e) {
        LOG.warn("Error while doing sleep in continuous scheduling: " +
                e.toString(), e);
      }
    }
  }

  /** Sort nodes by available resource */
  private class NodeAvailableResourceComparator implements Comparator<NodeId> {

    @Override
    public int compare(NodeId n1, NodeId n2) {
      return RESOURCE_CALCULATOR.compare(clusterCapacity,
              nodes.get(n2).getAvailableResource(),
              nodes.get(n1).getAvailableResource());
    }
  }
  
  private synchronized void attemptScheduling(FSSchedulerNode node) {
    // Assign new containers...
    // 1. Check for reserved applications
    // 2. Schedule if there are no reservations

    AppSchedulable reservedAppSchedulable = node.getReservedAppSchedulable();
    if (reservedAppSchedulable != null) {
      Priority reservedPriority = node.getReservedContainer().getReservedPriority();
      if (!reservedAppSchedulable.hasContainerForNode(reservedPriority, node)) {
        // Don't hold the reservation if app can no longer use it
        LOG.info("Releasing reservation that cannot be satisfied for application "
            + reservedAppSchedulable.getApp().getApplicationAttemptId()
            + " on node " + node);
        reservedAppSchedulable.unreserve(reservedPriority, node);
        reservedAppSchedulable = null;
      } else {
        // Reservation exists; try to fulfill the reservation
        LOG.info("Trying to fulfill reservation for application "
            + reservedAppSchedulable.getApp().getApplicationAttemptId()
            + " on node: " + node);

        node.getReservedAppSchedulable().assignReservedContainer(node);
      }
    }
    if (reservedAppSchedulable == null) {
      // No reservation, schedule at queue which is farthest below fair share
      int assignedContainers = 0;
      while (node.getReservedContainer() == null) {
        boolean assignedContainer = false;
        if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
              queueMgr.getRootQueue().assignContainer(node),
              Resources.none())) {
          assignedContainers++;
          assignedContainer = true;
        }
        if (!assignedContainer) { break; }
        if (!assignMultiple) { break; }
        if ((assignedContainers >= maxAssign) && (maxAssign > 0)) { break; }
      }
    }
    updateRootQueueMetrics();
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    FSSchedulerNode node = nodes.get(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }
  
  public FSSchedulerApp getSchedulerApp(ApplicationAttemptId appAttemptId) {
    return applications.get(appAttemptId);
  }
  
  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    if (!applications.containsKey(appAttemptId)) {
      LOG.error("Request for appInfo of unknown attempt" + appAttemptId);
      return null;
    }
    return new SchedulerAppReport(applications.get(appAttemptId));
  }
  
  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId) {
    FSSchedulerApp app = applications.get(appAttemptId);
    if (app == null) {
      LOG.error("Request for appInfo of unknown attempt" + appAttemptId);
      return null;
    }
    return app.getResourceUsageReport();
  }
  
  /**
   * Subqueue metrics might be a little out of date because fair shares are
   * recalculated at the update interval, but the root queue metrics needs to
   * be updated synchronously with allocations and completions so that cluster
   * metrics will be consistent.
   */
  private void updateRootQueueMetrics() {
    rootMetrics.setAvailableResourcesToQueue(
        Resources.subtract(
            clusterCapacity, rootMetrics.getAllocatedResources()));
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return rootMetrics;
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch (event.getType()) {
    case NODE_ADDED:
      if (!(event instanceof NodeAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
      break;
    case NODE_REMOVED:
      if (!(event instanceof NodeRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
      break;
    case NODE_UPDATE:
      if (!(event instanceof NodeUpdateSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode());
      break;
    case APP_ADDED:
      if (!(event instanceof AppAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent)event;
      String queue = appAddedEvent.getQueue();
      addApplication(appAddedEvent.getApplicationAttemptId(), queue,
          appAddedEvent.getUser());
      break;
    case APP_REMOVED:
      if (!(event instanceof AppRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      removeApplication(appRemovedEvent.getApplicationAttemptID(),
          appRemovedEvent.getFinalAttemptState());
      break;
    case CONTAINER_EXPIRED:
      if (!(event instanceof ContainerExpiredSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      ContainerExpiredSchedulerEvent containerExpiredEvent =
          (ContainerExpiredSchedulerEvent)event;
      ContainerId containerId = containerExpiredEvent.getContainerId();
      completedContainer(getRMContainer(containerId),
          SchedulerUtils.createAbnormalContainerStatus(
              containerId,
              SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
      break;
    default:
      LOG.error("Unknown event arrived at FairScheduler: " + event.toString());
    }
  }

  @Override
  public void recover(RMState state) throws Exception {
    // NOT IMPLEMENTED
  }

  @Override
  public synchronized void reinitialize(Configuration conf, RMContext rmContext)
      throws IOException {
    this.conf = new FairSchedulerConfiguration(conf);
    validateConf(this.conf);
    minimumAllocation = this.conf.getMinimumAllocation();
    maximumAllocation = this.conf.getMaximumAllocation();
    incrAllocation = this.conf.getIncrementAllocation();
    userAsDefaultQueue = this.conf.getUserAsDefaultQueue();
    continuousSchedulingEnabled = this.conf.isContinuousSchedulingEnabled();
    continuousSchedulingSleepMs =
            this.conf.getContinuousSchedulingSleepMs();
    nodeLocalityThreshold = this.conf.getLocalityThresholdNode();
    rackLocalityThreshold = this.conf.getLocalityThresholdRack();
    nodeLocalityDelayMs = this.conf.getLocalityDelayNodeMs();
    rackLocalityDelayMs = this.conf.getLocalityDelayRackMs();
    preemptionEnabled = this.conf.getPreemptionEnabled();
    assignMultiple = this.conf.getAssignMultiple();
    maxAssign = this.conf.getMaxAssign();
    sizeBasedWeight = this.conf.getSizeBasedWeight();
    preemptionInterval = this.conf.getPreemptionInterval();
    waitTimeBeforeKill = this.conf.getWaitTimeBeforeKill();
    usePortForNodeName = this.conf.getUsePortForNodeName();
    
    if (!initialized) {
      rootMetrics = FSQueueMetrics.forQueue("root", null, true, conf);
      this.rmContext = rmContext;
      this.eventLog = new FairSchedulerEventLog();
      eventLog.init(this.conf);

      initialized = true;

      try {
        queueMgr.initialize();
      } catch (Exception e) {
        throw new IOException("Failed to start FairScheduler", e);
      }

      Thread updateThread = new Thread(new UpdateThread());
      updateThread.setName("FairSchedulerUpdateThread");
      updateThread.setDaemon(true);
      updateThread.start();

      if (continuousSchedulingEnabled) {
        // start continuous scheduling thread
        Thread schedulingThread = new Thread(
          new Runnable() {
            @Override
            public void run() {
              continuousScheduling();
            }
          }
        );
        schedulingThread.setName("ContinuousScheduling");
        schedulingThread.setDaemon(true);
        schedulingThread.start();
      }
    } else {
      try {
        queueMgr.reloadAllocs();
      } catch (Exception e) {
        throw new IOException("Failed to initialize FairScheduler", e);
      }
    }
  }

  @Override
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
      boolean recursive) throws IOException {
    if (!queueMgr.exists(queueName)) {
      throw new IOException("queue " + queueName + " does not exist");
    }
    return queueMgr.getQueue(queueName).getQueueInfo(includeChildQueues,
        recursive);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    UserGroupInformation user = null;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      return new ArrayList<QueueUserACLInfo>();
    }

    return queueMgr.getRootQueue().getQueueUserAclInfo(user);
  }

  @Override
  public int getNumClusterNodes() {
    return nodes.size();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    FSQueue queue = getQueueManager().getQueue(queueName);
    if (queue == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ACL not found for queue access-type " + acl
            + " for queue " + queueName);
      }
      return false;
    }
    return queue.hasAccess(acl, callerUGI);
  }

}
