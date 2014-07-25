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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.ContainersAndNMTokensAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
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
import com.google.common.base.Preconditions;

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
public class FairScheduler extends
    AbstractYarnScheduler<FSSchedulerApp, FSSchedulerNode> {
  private FairSchedulerConfiguration conf;

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
  protected long updateInterval;
  private final int UPDATE_DEBUG_FREQUENCY = 5;
  private int updatesToSkipForDebug = UPDATE_DEBUG_FREQUENCY;

  private Thread updateThread;
  private Thread schedulingThread;
  // timeout to join when we stop this service
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  // Aggregate metrics
  FSQueueMetrics rootMetrics;

  // Time when we last updated preemption vars
  protected long lastPreemptionUpdateTime;
  // Time we last ran preemptTasksIfNecessary
  private long lastPreemptCheckTime;

  // Preemption related variables
  protected boolean preemptionEnabled;
  protected float preemptionUtilizationThreshold;

  // How often tasks are preempted
  protected long preemptionInterval; 
  
  // ms to wait before force killing stuff (must be longer than a couple
  // of heartbeats to give task-kill commands a chance to act).
  protected long waitTimeBeforeKill; 
  
  // Containers whose AMs have been warned that they will be preempted soon.
  private List<RMContainer> warnedContainers = new ArrayList<RMContainer>();
  
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected WeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected boolean continuousSchedulingEnabled; // Continuous Scheduling enabled or not
  protected int continuousSchedulingSleepMs; // Sleep time for each pass in continuous scheduling
  private Comparator<NodeId> nodeAvailableResourceComparator =
          new NodeAvailableResourceComparator(); // Node available resource comparator
  protected double nodeLocalityThreshold; // Cluster threshold for node locality
  protected double rackLocalityThreshold; // Cluster threshold for rack locality
  protected long nodeLocalityDelayMs; // Delay for node locality
  protected long rackLocalityDelayMs; // Delay for rack locality
  private FairSchedulerEventLog eventLog; // Machine-readable event log
  protected boolean assignMultiple; // Allocate multiple containers per
                                    // heartbeat
  protected int maxAssign; // Max containers to assign per heartbeat

  @VisibleForTesting
  final MaxRunningAppsEnforcer maxRunningEnforcer;

  private AllocationFileLoaderService allocsLoader;
  @VisibleForTesting
  AllocationConfiguration allocConf;
  
  public FairScheduler() {
    super(FairScheduler.class.getName());
    clock = new SystemClock();
    allocsLoader = new AllocationFileLoaderService();
    queueMgr = new QueueManager(this);
    maxRunningEnforcer = new MaxRunningAppsEnforcer(this);
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

  /**
   * A runnable which calls {@link FairScheduler#update()} every
   * <code>updateInterval</code> milliseconds.
   */
  private class UpdateThread implements Runnable {
    public void run() {
      while (true) {
        try {
          Thread.sleep(updateInterval);
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
    updatePreemptionVariables(); // Determine if any queues merit preemption

    FSQueue rootQueue = queueMgr.getRootQueue();

    // Recursively update demands for all queues
    rootQueue.updateDemand();

    rootQueue.setFairShare(clusterResource);
    // Recursively compute fair shares for all queues
    // and update metrics
    rootQueue.recomputeShares();

    if (LOG.isDebugEnabled()) {
      if (--updatesToSkipForDebug < 0) {
        updatesToSkipForDebug = UPDATE_DEBUG_FREQUENCY;
        LOG.debug("Cluster Capacity: " + clusterResource +
            "  Allocations: " + rootMetrics.getAllocatedResources() +
            "  Availability: " + Resource.newInstance(
            rootMetrics.getAvailableMB(),
            rootMetrics.getAvailableVirtualCores()) +
            "  Demand: " + rootQueue.getDemand());
      }
    }
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
    Resource desiredShare = Resources.min(RESOURCE_CALCULATOR, clusterResource,
      sched.getMinShare(), sched.getDemand());
    return Resources.lessThan(RESOURCE_CALCULATOR, clusterResource,
        sched.getResourceUsage(), desiredShare);
  }

  /**
   * Is a queue being starved for fair share for the given task type? This is
   * defined as being below half its fair share.
   */
  boolean isStarvedForFairShare(FSLeafQueue sched) {
    Resource desiredFairShare = Resources.min(RESOURCE_CALCULATOR, clusterResource,
        Resources.multiply(sched.getFairShare(), .5), sched.getDemand());
    return Resources.lessThan(RESOURCE_CALCULATOR, clusterResource,
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
    if (!shouldAttemptPreemption()) {
      return;
    }

    long curTime = clock.getTime();
    if (curTime - lastPreemptCheckTime < preemptionInterval) {
      return;
    }
    lastPreemptCheckTime = curTime;

    Resource resToPreempt = Resources.clone(Resources.none());
    for (FSLeafQueue sched : queueMgr.getLeafQueues()) {
      Resources.addTo(resToPreempt, resToPreempt(sched, curTime));
    }
    if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterResource, resToPreempt,
        Resources.none())) {
      preemptResources(resToPreempt);
    }
  }

  /**
   * Preempt a quantity of resources. Each round, we start from the root queue,
   * level-by-level, until choosing a candidate application.
   * The policy for prioritizing preemption for each queue depends on its
   * SchedulingPolicy: (1) fairshare/DRF, choose the ChildSchedulable that is
   * most over its fair share; (2) FIFO, choose the childSchedulable that is
   * latest launched.
   * Inside each application, we further prioritize preemption by choosing
   * containers with lowest priority to preempt.
   * We make sure that no queue is placed below its fair share in the process.
   */
  protected void preemptResources(Resource toPreempt) {
    if (Resources.equals(toPreempt, Resources.none())) {
      return;
    }

    // Scan down the list of containers we've already warned and kill them
    // if we need to.  Remove any containers from the list that we don't need
    // or that are no longer running.
    Iterator<RMContainer> warnedIter = warnedContainers.iterator();
    while (warnedIter.hasNext()) {
      RMContainer container = warnedIter.next();
      if ((container.getState() == RMContainerState.RUNNING ||
              container.getState() == RMContainerState.ALLOCATED) &&
          Resources.greaterThan(RESOURCE_CALCULATOR, clusterResource,
              toPreempt, Resources.none())) {
        warnOrKillContainer(container);
        Resources.subtractFrom(toPreempt, container.getContainer().getResource());
      } else {
        warnedIter.remove();
      }
    }

    try {
      // Reset preemptedResource for each app
      for (FSLeafQueue queue : getQueueManager().getLeafQueues()) {
        for (AppSchedulable app : queue.getRunnableAppSchedulables()) {
          app.getApp().resetPreemptedResources();
        }
      }

      while (Resources.greaterThan(RESOURCE_CALCULATOR, clusterResource,
          toPreempt, Resources.none())) {
        RMContainer container =
            getQueueManager().getRootQueue().preemptContainer();
        if (container == null) {
          break;
        } else {
          warnOrKillContainer(container);
          warnedContainers.add(container);
          Resources.subtractFrom(
              toPreempt, container.getContainer().getResource());
        }
      }
    } finally {
      // Clear preemptedResources for each app
      for (FSLeafQueue queue : getQueueManager().getLeafQueues()) {
        for (AppSchedulable app : queue.getRunnableAppSchedulables()) {
          app.getApp().clearPreemptedResources();
        }
      }
    }
  }
  
  protected void warnOrKillContainer(RMContainer container) {
    ApplicationAttemptId appAttemptId = container.getApplicationAttemptId();
    FSSchedulerApp app = getSchedulerApp(appAttemptId);
    FSLeafQueue queue = app.getQueue();
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

        recoverResourceRequestForContainer(container);
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
    long minShareTimeout = allocConf.getMinSharePreemptionTimeout(queue);
    long fairShareTimeout = allocConf.getFairSharePreemptionTimeout();
    Resource resDueToMinShare = Resources.none();
    Resource resDueToFairShare = Resources.none();
    if (curTime - sched.getLastTimeAtMinShare() > minShareTimeout) {
      Resource target = Resources.min(RESOURCE_CALCULATOR, clusterResource,
          sched.getMinShare(), sched.getDemand());
      resDueToMinShare = Resources.max(RESOURCE_CALCULATOR, clusterResource,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    if (curTime - sched.getLastTimeAtHalfFairShare() > fairShareTimeout) {
      Resource target = Resources.min(RESOURCE_CALCULATOR, clusterResource,
          sched.getFairShare(), sched.getDemand());
      resDueToFairShare = Resources.max(RESOURCE_CALCULATOR, clusterResource,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    Resource resToPreempt = Resources.max(RESOURCE_CALCULATOR, clusterResource,
        resDueToMinShare, resDueToFairShare);
    if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterResource,
        resToPreempt, Resources.none())) {
      String message = "Should preempt " + resToPreempt + " res for queue "
          + sched.getName() + ": resDueToMinShare = " + resDueToMinShare
          + ", resDueToFairShare = " + resDueToFairShare;
      LOG.info(message);
    }
    return resToPreempt;
  }

  public synchronized RMContainerTokenSecretManager
      getContainerTokenSecretManager() {
    return rmContext.getContainerTokenSecretManager();
  }

  // synchronized for sizeBasedWeight
  public synchronized ResourceWeights getAppWeight(AppSchedulable app) {
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
    ResourceWeights resourceWeights = app.getResourceWeights();
    resourceWeights.setWeight((float)weight);
    return resourceWeights;
  }

  public Resource getIncrementResourceCapability() {
    return incrAllocation;
  }

  private FSSchedulerNode getFSSchedulerNode(NodeId nodeId) {
    return nodes.get(nodeId);
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
  protected synchronized void addApplication(ApplicationId applicationId,
      String queueName, String user, boolean isAppRecovering) {
    if (queueName == null || queueName.isEmpty()) {
      String message = "Reject application " + applicationId +
              " submitted by user " + user + " with an empty queue name.";
      LOG.info(message);
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, message));
      return;
    }

    RMApp rmApp = rmContext.getRMApps().get(applicationId);
    FSLeafQueue queue = assignToQueue(rmApp, queueName, user);
    if (queue == null) {
      return;
    }

    // Enforce ACLs
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(user);

    if (!queue.hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)
        && !queue.hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
      String msg = "User " + userUgi.getUserName() +
              " cannot submit applications to queue " + queue.getName();
      LOG.info(msg);
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, msg));
      return;
    }
  
    SchedulerApplication<FSSchedulerApp> application =
        new SchedulerApplication<FSSchedulerApp>(queue, user);
    applications.put(applicationId, application);
    queue.getMetrics().submitApp(user);

    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName + ", currently num of applications: "
        + applications.size());
    if (isAppRecovering) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
      }
    } else {
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
    }
  }

  /**
   * Add a new application attempt to the scheduler.
   */
  protected synchronized void addApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
    SchedulerApplication<FSSchedulerApp> application =
        applications.get(applicationAttemptId.getApplicationId());
    String user = application.getUser();
    FSLeafQueue queue = (FSLeafQueue) application.getQueue();

    FSSchedulerApp attempt =
        new FSSchedulerApp(applicationAttemptId, user,
            queue, new ActiveUsersManager(getRootQueueMetrics()),
            rmContext);
    if (transferStateFromPreviousAttempt) {
      attempt.transferStateFromPreviousAttempt(application
        .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(attempt);

    boolean runnable = maxRunningEnforcer.canAppBeRunnable(queue, user);
    queue.addApp(attempt, runnable);
    if (runnable) {
      maxRunningEnforcer.trackRunnableApp(attempt);
    } else {
      maxRunningEnforcer.trackNonRunnableApp(attempt);
    }
    
    queue.getMetrics().submitAppAttempt(user);

    LOG.info("Added Application Attempt " + applicationAttemptId
        + " to scheduler from user: " + user);

    if (isAttemptRecovering) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(applicationAttemptId
            + " is recovering. Skipping notifying ATTEMPT_ADDED");
      }
    } else {
      rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
    }
  }

  /**
   * Helper method that attempts to assign the app to a queue. The method is
   * responsible to call the appropriate event-handler if the app is rejected.
   */
  @VisibleForTesting
  FSLeafQueue assignToQueue(RMApp rmApp, String queueName, String user) {
    FSLeafQueue queue = null;
    String appRejectMsg = null;

    try {
      QueuePlacementPolicy placementPolicy = allocConf.getPlacementPolicy();
      queueName = placementPolicy.assignAppToQueue(queueName, user);
      if (queueName == null) {
        appRejectMsg = "Application rejected by queue placement policy";
      } else {
        queue = queueMgr.getLeafQueue(queueName, true);
        if (queue == null) {
          appRejectMsg = queueName + " is not a leaf queue";
        }
      }
    } catch (IOException ioe) {
      appRejectMsg = "Error assigning app to queue " + queueName;
    }

    if (appRejectMsg != null && rmApp != null) {
      LOG.error(appRejectMsg);
      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppRejectedEvent(rmApp.getApplicationId(), appRejectMsg));
      return null;
    }

    if (rmApp != null) {
      rmApp.setQueue(queue.getName());
    } else {
      LOG.error("Couldn't find RM app to set queue name on");
    }
    return queue;
  }

  private synchronized void removeApplication(ApplicationId applicationId,
      RMAppState finalState) {
    SchedulerApplication<FSSchedulerApp> application =
        applications.get(applicationId);
    if (application == null){
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private synchronized void removeApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
    LOG.info("Application " + applicationAttemptId + " is done." +
        " finalState=" + rmAppAttemptFinalState);
    SchedulerApplication<FSSchedulerApp> application =
        applications.get(applicationAttemptId.getApplicationId());
    FSSchedulerApp attempt = getSchedulerApp(applicationAttemptId);

    if (attempt == null || application == null) {
      LOG.info("Unknown application " + applicationAttemptId + " has completed!");
      return;
    }

    // Release all the running containers
    for (RMContainer rmContainer : attempt.getLiveContainers()) {
      if (keepContainers
          && rmContainer.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + rmContainer.getContainerId());
        continue;
      }
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(),
              SchedulerUtils.COMPLETED_APPLICATION),
              RMContainerEventType.KILL);
    }

    // Release all reserved containers
    for (RMContainer rmContainer : attempt.getReservedContainers()) {
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(),
              "Application Complete"),
              RMContainerEventType.KILL);
    }
    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState);

    // Inform the queue
    FSLeafQueue queue = queueMgr.getLeafQueue(attempt.getQueue()
        .getQueueName(), false);
    boolean wasRunnable = queue.removeApp(attempt);

    if (wasRunnable) {
      maxRunningEnforcer.untrackRunnableApp(attempt);
      maxRunningEnforcer.updateRunnabilityOnAppRemoval(attempt,
          attempt.getQueue());
    } else {
      maxRunningEnforcer.untrackNonRunnableApp(attempt);
    }
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
    FSSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();
    if (application == null) {
      LOG.info("Container " + container + " of" +
          " unknown application attempt " + appId +
          " completed with event " + event);
      return;
    }

    // Get the node on which the container was allocated
    FSSchedulerNode node = getFSSchedulerNode(container.getNodeId());

    if (rmContainer.getState() == RMContainerState.RESERVED) {
      application.unreserve(node, rmContainer.getReservedPriority());
      node.unreserveResource(application);
    } else {
      application.containerCompleted(rmContainer, containerStatus, event);
      node.releaseContainer(container);
      updateRootQueueMetrics();
    }

    LOG.info("Application attempt " + application.getApplicationAttemptId()
        + " released container " + container.getId() + " on node: " + node
        + " with event: " + event);
  }

  private synchronized void addNode(RMNode node) {
    nodes.put(node.getNodeID(), new FSSchedulerNode(node, usePortForNodeName));
    Resources.addTo(clusterResource, node.getTotalCapability());
    updateRootQueueMetrics();

    LOG.info("Added node " + node.getNodeAddress() +
        " cluster capacity: " + clusterResource);
  }

  private synchronized void removeNode(RMNode rmNode) {
    FSSchedulerNode node = getFSSchedulerNode(rmNode.getNodeID());
    // This can occur when an UNHEALTHY node reconnects
    if (node == null) {
      return;
    }
    Resources.subtractFrom(clusterResource, rmNode.getTotalCapability());
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
        " cluster capacity: " + clusterResource);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {

    // Make sure this application exists
    FSSchedulerApp application = getSchedulerApp(appAttemptId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + appAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    SchedulerUtils.normalizeRequests(ask, new DominantResourceCalculator(),
        clusterResource, minimumAllocation, maximumAllocation, incrAllocation);

    // Set amResource for this app
    if (!application.getUnmanagedAM() && ask.size() == 1
        && application.getLiveContainers().isEmpty()) {
      application.setAMResource(ask.get(0).getCapability());
    }

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

        application.showRequests();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: post-update" +
            " applicationAttemptId=" + appAttemptId +
            " #ask=" + ask.size() +
            " reservation= " + application.getCurrentReservation());

        LOG.debug("Preempting " + application.getPreemptionContainers().size()
            + " container(s)");
      }
      
      Set<ContainerId> preemptionContainerIds = new HashSet<ContainerId>();
      for (RMContainer container : application.getPreemptionContainers()) {
        preemptionContainerIds.add(container.getContainerId());
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      ContainersAndNMTokensAllocation allocation =
          application.pullNewlyAllocatedContainersAndNMTokens();
      return new Allocation(allocation.getContainerList(),
        application.getHeadroom(), preemptionContainerIds, null, null,
        allocation.getNMTokenList());
    }
  }

  /**
   * Process a heartbeat update from a node.
   */
  private synchronized void nodeUpdate(RMNode nm) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeUpdate: " + nm + " cluster capacity: " + clusterResource);
    }
    eventLog.log("HEARTBEAT", nm.getHostName());
    FSSchedulerNode node = getFSSchedulerNode(nm.getNodeID());

    // Update resource if any change
    SchedulerUtils.updateResourceIfChanged(node, nm, clusterResource, LOG);
    
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

  void continuousSchedulingAttempt() {
    List<NodeId> nodeIdList = new ArrayList<NodeId>(nodes.keySet());
    // Sort the nodes by space available on them, so that we offer
    // containers on emptier nodes first, facilitating an even spread. This
    // requires holding the scheduler lock, so that the space available on a
    // node doesn't change during the sort.
    synchronized (this) {
      Collections.sort(nodeIdList, nodeAvailableResourceComparator);
    }

    // iterate all nodes
    for (NodeId nodeId : nodeIdList) {
      FSSchedulerNode node = getFSSchedulerNode(nodeId);
      try {
        if (node != null && Resources.fitsIn(minimumAllocation,
            node.getAvailableResource())) {
          attemptScheduling(node);
        }
      } catch (Throwable ex) {
        LOG.error("Error while attempting scheduling for node " + node +
            ": " + ex.toString(), ex);
      }
    }
  }

  /** Sort nodes by available resource */
  private class NodeAvailableResourceComparator implements Comparator<NodeId> {

    @Override
    public int compare(NodeId n1, NodeId n2) {
      if (!nodes.containsKey(n1)) {
        return 1;
      }
      if (!nodes.containsKey(n2)) {
        return -1;
      }
      return RESOURCE_CALCULATOR.compare(clusterResource,
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("Trying to fulfill reservation for application "
              + reservedAppSchedulable.getApp().getApplicationAttemptId()
              + " on node: " + node);
        }
        
        node.getReservedAppSchedulable().assignReservedContainer(node);
      }
    }
    if (reservedAppSchedulable == null) {
      // No reservation, schedule at queue which is farthest below fair share
      int assignedContainers = 0;
      while (node.getReservedContainer() == null) {
        boolean assignedContainer = false;
        if (!queueMgr.getRootQueue().assignContainer(node).equals(
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

  public FSSchedulerApp getSchedulerApp(ApplicationAttemptId appAttemptId) {
    return (FSSchedulerApp) super.getApplicationAttempt(appAttemptId);
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
            clusterResource, rootMetrics.getAllocatedResources()));
  }

  /**
   * Check if preemption is enabled and the utilization threshold for
   * preemption is met.
   *
   * @return true if preemption should be attempted, false otherwise.
   */
  private boolean shouldAttemptPreemption() {
    if (preemptionEnabled) {
      return (preemptionUtilizationThreshold < Math.max(
          (float) rootMetrics.getAllocatedMB() / clusterResource.getMemory(),
          (float) rootMetrics.getAllocatedVirtualCores() /
              clusterResource.getVirtualCores()));
    }
    return false;
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
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      addApplication(appAddedEvent.getApplicationId(),
        appAddedEvent.getQueue(), appAddedEvent.getUser(),
        appAddedEvent.getIsAppRecovering());
      break;
    case APP_REMOVED:
      if (!(event instanceof AppRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      removeApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
      break;
    case APP_ATTEMPT_ADDED:
      if (!(event instanceof AppAttemptAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
        appAttemptAddedEvent.getIsAttemptRecovering());
      break;
    case APP_ATTEMPT_REMOVED:
      if (!(event instanceof AppAttemptRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      removeApplicationAttempt(
          appAttemptRemovedEvent.getApplicationAttemptID(),
          appAttemptRemovedEvent.getFinalAttemptState(),
          appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
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

  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  private synchronized void initScheduler(Configuration conf)
      throws IOException {
    this.conf = new FairSchedulerConfiguration(conf);
    validateConf(this.conf);
    minimumAllocation = this.conf.getMinimumAllocation();
    maximumAllocation = this.conf.getMaximumAllocation();
    incrAllocation = this.conf.getIncrementAllocation();
    continuousSchedulingEnabled = this.conf.isContinuousSchedulingEnabled();
    continuousSchedulingSleepMs =
        this.conf.getContinuousSchedulingSleepMs();
    nodeLocalityThreshold = this.conf.getLocalityThresholdNode();
    rackLocalityThreshold = this.conf.getLocalityThresholdRack();
    nodeLocalityDelayMs = this.conf.getLocalityDelayNodeMs();
    rackLocalityDelayMs = this.conf.getLocalityDelayRackMs();
    preemptionEnabled = this.conf.getPreemptionEnabled();
    preemptionUtilizationThreshold =
        this.conf.getPreemptionUtilizationThreshold();
    assignMultiple = this.conf.getAssignMultiple();
    maxAssign = this.conf.getMaxAssign();
    sizeBasedWeight = this.conf.getSizeBasedWeight();
    preemptionInterval = this.conf.getPreemptionInterval();
    waitTimeBeforeKill = this.conf.getWaitTimeBeforeKill();
    usePortForNodeName = this.conf.getUsePortForNodeName();

    updateInterval = this.conf.getUpdateInterval();
    if (updateInterval < 0) {
      updateInterval = FairSchedulerConfiguration.DEFAULT_UPDATE_INTERVAL_MS;
      LOG.warn(FairSchedulerConfiguration.UPDATE_INTERVAL_MS
              + " is invalid, so using default value " +
              + FairSchedulerConfiguration.DEFAULT_UPDATE_INTERVAL_MS
              + " ms instead");
    }

    rootMetrics = FSQueueMetrics.forQueue("root", null, true, conf);
    // This stores per-application scheduling information
    this.applications =
        new ConcurrentHashMap<ApplicationId,SchedulerApplication<FSSchedulerApp>>();
    this.eventLog = new FairSchedulerEventLog();
    eventLog.init(this.conf);

    allocConf = new AllocationConfiguration(conf);
    try {
      queueMgr.initialize(conf);
    } catch (Exception e) {
      throw new IOException("Failed to start FairScheduler", e);
    }

    updateThread = new Thread(new UpdateThread());
    updateThread.setName("FairSchedulerUpdateThread");
    updateThread.setDaemon(true);

    if (continuousSchedulingEnabled) {
      // start continuous scheduling thread
      schedulingThread = new Thread(
          new Runnable() {
            @Override
            public void run() {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  continuousSchedulingAttempt();
                  Thread.sleep(getContinuousSchedulingSleepMs());
                } catch (InterruptedException e) {
                  LOG.error("Continuous scheduling thread interrupted. Exiting. ",
                      e);
                  return;
                }
              }
            }
          }
      );
      schedulingThread.setName("ContinuousScheduling");
      schedulingThread.setDaemon(true);
    }

    allocsLoader.init(conf);
    allocsLoader.setReloadListener(new AllocationReloadListener());
    // If we fail to load allocations file on initialize, we want to fail
    // immediately.  After a successful load, exceptions on future reloads
    // will just result in leaving things as they are.
    try {
      allocsLoader.reloadAllocations();
    } catch (Exception e) {
      throw new IOException("Failed to initialize FairScheduler", e);
    }
  }

  private synchronized void startSchedulerThreads() {
    Preconditions.checkNotNull(updateThread, "updateThread is null");
    Preconditions.checkNotNull(allocsLoader, "allocsLoader is null");
    updateThread.start();
    if (continuousSchedulingEnabled) {
      Preconditions.checkNotNull(schedulingThread, "schedulingThread is null");
      schedulingThread.start();
    }
    allocsLoader.start();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    initScheduler(conf);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    startSchedulerThreads();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    synchronized (this) {
      if (updateThread != null) {
        updateThread.interrupt();
        updateThread.join(THREAD_JOIN_TIMEOUT_MS);
      }
      if (continuousSchedulingEnabled) {
        if (schedulingThread != null) {
          schedulingThread.interrupt();
          schedulingThread.join(THREAD_JOIN_TIMEOUT_MS);
        }
      }
      if (allocsLoader != null) {
        allocsLoader.stop();
      }
    }

    super.serviceStop();
  }

  @Override
  public synchronized void reinitialize(Configuration conf, RMContext rmContext)
      throws IOException {
    try {
      allocsLoader.reloadAllocations();
    } catch (Exception e) {
      LOG.error("Failed to reload allocations file", e);
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
  
  public AllocationConfiguration getAllocationConfiguration() {
    return allocConf;
  }
  
  private class AllocationReloadListener implements
      AllocationFileLoaderService.Listener {

    @Override
    public void onReload(AllocationConfiguration queueInfo) {
      // Commit the reload; also create any queue defined in the alloc file
      // if it does not already exist, so it can be displayed on the web UI.
      synchronized (FairScheduler.this) {
        allocConf = queueInfo;
        allocConf.getDefaultSchedulingPolicy().initialize(clusterResource);
        queueMgr.updateAllocationConfiguration(allocConf);
      }
    }
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    FSQueue queue = queueMgr.getQueue(queueName);
    if (queue == null) {
      return null;
    }
    List<ApplicationAttemptId> apps = new ArrayList<ApplicationAttemptId>();
    queue.collectSchedulerApplications(apps);
    return apps;
  }

  @Override
  public synchronized String moveApplication(ApplicationId appId,
      String queueName) throws YarnException {
    SchedulerApplication<FSSchedulerApp> app = applications.get(appId);
    if (app == null) {
      throw new YarnException("App to be moved " + appId + " not found.");
    }
    FSSchedulerApp attempt = (FSSchedulerApp) app.getCurrentAppAttempt();
    // To serialize with FairScheduler#allocate, synchronize on app attempt
    synchronized (attempt) {
      FSLeafQueue oldQueue = (FSLeafQueue) app.getQueue();
      FSLeafQueue targetQueue = queueMgr.getLeafQueue(queueName, false);
      if (targetQueue == null) {
        throw new YarnException("Target queue " + queueName
            + " not found or is not a leaf queue.");
      }
      if (targetQueue == oldQueue) {
        return oldQueue.getQueueName();
      }
      
      if (oldQueue.getRunnableAppSchedulables().contains(
          attempt.getAppSchedulable())) {
        verifyMoveDoesNotViolateConstraints(attempt, oldQueue, targetQueue);
      }
      
      executeMove(app, attempt, oldQueue, targetQueue);
      return targetQueue.getQueueName();
    }
  }
  
  private void verifyMoveDoesNotViolateConstraints(FSSchedulerApp app,
      FSLeafQueue oldQueue, FSLeafQueue targetQueue) throws YarnException {
    String queueName = targetQueue.getQueueName();
    ApplicationAttemptId appAttId = app.getApplicationAttemptId();
    // When checking maxResources and maxRunningApps, only need to consider
    // queues before the lowest common ancestor of the two queues because the
    // total running apps in queues above will not be changed.
    FSQueue lowestCommonAncestor = findLowestCommonAncestorQueue(oldQueue,
        targetQueue);
    Resource consumption = app.getCurrentConsumption();
    
    // Check whether the move would go over maxRunningApps or maxShare
    FSQueue cur = targetQueue;
    while (cur != lowestCommonAncestor) {
      // maxRunningApps
      if (cur.getNumRunnableApps() == allocConf.getQueueMaxApps(cur.getQueueName())) {
        throw new YarnException("Moving app attempt " + appAttId + " to queue "
            + queueName + " would violate queue maxRunningApps constraints on"
            + " queue " + cur.getQueueName());
      }
      
      // maxShare
      if (!Resources.fitsIn(Resources.add(cur.getResourceUsage(), consumption),
          cur.getMaxShare())) {
        throw new YarnException("Moving app attempt " + appAttId + " to queue "
            + queueName + " would violate queue maxShare constraints on"
            + " queue " + cur.getQueueName());
      }
      
      cur = cur.getParent();
    }
  }
  
  /**
   * Helper for moveApplication, which has appropriate synchronization, so all
   * operations will be atomic.
   */
  private void executeMove(SchedulerApplication<FSSchedulerApp> app,
      FSSchedulerApp attempt, FSLeafQueue oldQueue, FSLeafQueue newQueue) {
    boolean wasRunnable = oldQueue.removeApp(attempt);
    // if app was not runnable before, it may be runnable now
    boolean nowRunnable = maxRunningEnforcer.canAppBeRunnable(newQueue,
        attempt.getUser());
    if (wasRunnable && !nowRunnable) {
      throw new IllegalStateException("Should have already verified that app "
          + attempt.getApplicationId() + " would be runnable in new queue");
    }
    
    if (wasRunnable) {
      maxRunningEnforcer.untrackRunnableApp(attempt);
    } else if (nowRunnable) {
      // App has changed from non-runnable to runnable
      maxRunningEnforcer.untrackNonRunnableApp(attempt);
    }
    
    attempt.move(newQueue); // This updates all the metrics
    app.setQueue(newQueue);
    newQueue.addApp(attempt, nowRunnable);
    
    if (nowRunnable) {
      maxRunningEnforcer.trackRunnableApp(attempt);
    }
    if (wasRunnable) {
      maxRunningEnforcer.updateRunnabilityOnAppRemoval(attempt, oldQueue);
    }
  }

  @VisibleForTesting
  FSQueue findLowestCommonAncestorQueue(FSQueue queue1, FSQueue queue2) {
    // Because queue names include ancestors, separated by periods, we can find
    // the lowest common ancestors by going from the start of the names until
    // there's a character that doesn't match.
    String name1 = queue1.getName();
    String name2 = queue2.getName();
    // We keep track of the last period we encounter to avoid returning root.apple
    // when the queues are root.applepie and root.appletart
    int lastPeriodIndex = -1;
    for (int i = 0; i < Math.max(name1.length(), name2.length()); i++) {
      if (name1.length() <= i || name2.length() <= i ||
          name1.charAt(i) != name2.charAt(i)) {
        return queueMgr.getQueue(name1.substring(0, lastPeriodIndex));
      } else if (name1.charAt(i) == '.') {
        lastPeriodIndex = i;
      }
    }
    return queue1; // names are identical
  }
}
