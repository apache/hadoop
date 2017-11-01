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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMCriticalThreadUncaughtExceptionHandler;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

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
 * specified as children of other queues by placing them as sub-elements of
 * their parents in the fair scheduler configuration file.
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
    AbstractYarnScheduler<FSAppAttempt, FSSchedulerNode> {
  private FairSchedulerConfiguration conf;

  private FSContext context;
  private YarnAuthorizationProvider authorizer;
  private Resource incrAllocation;
  private QueueManager queueMgr;
  private boolean usePortForNodeName;

  private static final Log LOG = LogFactory.getLog(FairScheduler.class);
  private static final Log STATE_DUMP_LOG =
      LogFactory.getLog(FairScheduler.class.getName() + ".statedump");

  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  private static final ResourceCalculator DOMINANT_RESOURCE_CALCULATOR =
      new DominantResourceCalculator();
  
  // Value that container assignment methods return when a container is
  // reserved
  public static final Resource CONTAINER_RESERVED = Resources.createResource(-1);

  private final int UPDATE_DEBUG_FREQUENCY = 25;
  private int updatesToSkipForDebug = UPDATE_DEBUG_FREQUENCY;

  @VisibleForTesting
  Thread schedulingThread;

  Thread preemptionThread;

  // Aggregate metrics
  FSQueueMetrics rootMetrics;
  FSOpDurations fsOpDurations;

  private float reservableNodesRatio; // percentage of available nodes
                                      // an app can be reserved on

  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  // Continuous Scheduling enabled or not
  protected boolean continuousSchedulingEnabled;
  // Sleep time for each pass in continuous scheduling
  protected volatile int continuousSchedulingSleepMs;
  // Node available resource comparator
  private Comparator<FSSchedulerNode> nodeAvailableResourceComparator =
          new NodeAvailableResourceComparator();
  protected double nodeLocalityThreshold; // Cluster threshold for node locality
  protected double rackLocalityThreshold; // Cluster threshold for rack locality
  protected long nodeLocalityDelayMs; // Delay for node locality
  protected long rackLocalityDelayMs; // Delay for rack locality
  private FairSchedulerEventLog eventLog; // Machine-readable event log
  protected boolean assignMultiple; // Allocate multiple containers per
                                    // heartbeat
  @VisibleForTesting
  boolean maxAssignDynamic;
  protected int maxAssign; // Max containers to assign per heartbeat

  @VisibleForTesting
  final MaxRunningAppsEnforcer maxRunningEnforcer;

  private AllocationFileLoaderService allocsLoader;
  @VisibleForTesting
  AllocationConfiguration allocConf;

  // Container size threshold for making a reservation.
  @VisibleForTesting
  Resource reservationThreshold;

  public FairScheduler() {
    super(FairScheduler.class.getName());
    context = new FSContext(this);
    allocsLoader = new AllocationFileLoaderService();
    queueMgr = new QueueManager(this);
    maxRunningEnforcer = new MaxRunningAppsEnforcer(this);
  }

  public FSContext getContext() {
    return context;
  }

  public boolean isAtLeastReservationThreshold(
      ResourceCalculator resourceCalculator, Resource resource) {
    return Resources.greaterThanOrEqual(resourceCalculator,
        getClusterResource(), resource, reservationThreshold);
  }

  private void validateConf(FairSchedulerConfiguration config) {
    // validate scheduler memory allocation setting
    int minMem = config.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = config.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem < 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration: "
        + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ".  Both values must be greater than or equal to 0"
        + "and the maximum allocation value must be greater than or equal to"
        + "the minimum allocation value.");
    }

    long incrementMem = config.getIncrementAllocation().getMemorySize();
    if (incrementMem <= 0) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
          + " allocation configuration: "
          + FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB
          + "=" + incrementMem + ". Values must be greater than 0.");
    }

    // validate scheduler vcores allocation setting
    int minVcores = config.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = config.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores < 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
        + " allocation configuration: "
        + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
        + "=" + minVcores
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
        + "=" + maxVcores + ".  Both values must be greater than or equal to 0"
          + "and the maximum allocation value must be greater than or equal to"
          + "the minimum allocation value.");
    }

    int incrementVcore = config.getIncrementAllocation().getVirtualCores();
    if (incrementVcore <= 0) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
          + " allocation configuration: "
          + FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES
          + "=" + incrementVcore + ". Values must be greater than 0.");
    }
  }

  public FairSchedulerConfiguration getConf() {
    return conf;
  }

  public int getNumNodesInRack(String rackName) {
    return nodeTracker.nodeCount(rackName);
  }

  public QueueManager getQueueManager() {
    return queueMgr;
  }

  /**
   * Thread which attempts scheduling resources continuously,
   * asynchronous to the node heartbeats.
   */
  private class ContinuousSchedulingThread extends Thread {

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          continuousSchedulingAttempt();
          Thread.sleep(getContinuousSchedulingSleepMs());
        } catch (InterruptedException e) {
          LOG.warn("Continuous scheduling thread interrupted. Exiting.", e);
          return;
        }
      }
    }
  }

  /**
   * Dump scheduler state including states of all queues.
   */
  private void dumpSchedulerState() {
    FSQueue rootQueue = queueMgr.getRootQueue();
    Resource clusterResource = getClusterResource();
    LOG.debug("FairScheduler state: Cluster Capacity: " + clusterResource +
        "  Allocations: " + rootMetrics.getAllocatedResources() +
        "  Availability: " + Resource.newInstance(
        rootMetrics.getAvailableMB(), rootMetrics.getAvailableVirtualCores()) +
        "  Demand: " + rootQueue.getDemand());

    STATE_DUMP_LOG.debug(rootQueue.dumpState());
  }

  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and amount of used and
   * required resources per job.
   */
  @VisibleForTesting
  @Override
  public void update() {
    // Storing start time for fsOpDurations
    long start = getClock().getTime();
    FSQueue rootQueue = queueMgr.getRootQueue();

    // Update demands and fairshares
    writeLock.lock();
    try {
      // Recursively update demands for all queues
      rootQueue.updateDemand();
      rootQueue.update(getClusterResource());

      // Update metrics
      updateRootQueueMetrics();
    } finally {
      writeLock.unlock();
    }

    readLock.lock();
    try {
      // Update starvation stats and identify starved applications
      if (shouldAttemptPreemption()) {
        for (FSLeafQueue queue : queueMgr.getLeafQueues()) {
          queue.updateStarvedApps();
        }
      }

      // Log debug information
      if (LOG.isDebugEnabled()) {
        if (--updatesToSkipForDebug < 0) {
          updatesToSkipForDebug = UPDATE_DEBUG_FREQUENCY;
          dumpSchedulerState();
        }
      }
    } finally {
      readLock.unlock();
    }
    fsOpDurations.addUpdateThreadRunDuration(getClock().getTime() - start);
  }

  public RMContainerTokenSecretManager
      getContainerTokenSecretManager() {
    return rmContext.getContainerTokenSecretManager();
  }

  public float getAppWeight(FSAppAttempt app) {
    double weight = 1.0;

    if (sizeBasedWeight) {
      readLock.lock();

      try {
        // Set weight based on current memory demand
        weight = Math.log1p(app.getDemand().getMemorySize()) / Math.log(2);
      } finally {
        readLock.unlock();
      }
    }

    return (float)weight * app.getPriority().getPriority();
  }

  public Resource getIncrementResourceCapability() {
    return incrAllocation;
  }

  private FSSchedulerNode getFSSchedulerNode(NodeId nodeId) {
    return nodeTracker.getNode(nodeId);
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

  public int getContinuousSchedulingSleepMs() {
    return continuousSchedulingSleepMs;
  }

  public FairSchedulerEventLog getEventLog() {
    return eventLog;
  }

  /**
   * Add a new application to the scheduler, with a given id, queue name, and
   * user. This will accept a new app even if the user or queue is above
   * configured limits, but the app will not be marked as runnable.
   */
  protected void addApplication(ApplicationId applicationId,
      String queueName, String user, boolean isAppRecovering) {
    if (queueName == null || queueName.isEmpty()) {
      String message =
          "Reject application " + applicationId + " submitted by user " + user
              + " with an empty queue name.";
      LOG.info(message);
      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
              message));
      return;
    }

    if (queueName.startsWith(".") || queueName.endsWith(".")) {
      String message =
          "Reject application " + applicationId + " submitted by user " + user
              + " with an illegal queue name " + queueName + ". "
              + "The queue name cannot start/end with period.";
      LOG.info(message);
      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
              message));
      return;
    }

    try {
      writeLock.lock();
      RMApp rmApp = rmContext.getRMApps().get(applicationId);
      FSLeafQueue queue = assignToQueue(rmApp, queueName, user);
      if (queue == null) {
        return;
      }

      // Enforce ACLs
      UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(
          user);

      if (!queue.hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi) && !queue
          .hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
        String msg = "User " + userUgi.getUserName()
            + " cannot submit applications to queue " + queue.getName()
            + "(requested queuename is " + queueName + ")";
        LOG.info(msg);
        rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED, msg));
        return;
      }

      SchedulerApplication<FSAppAttempt> application =
          new SchedulerApplication<FSAppAttempt>(queue, user);
      applications.put(applicationId, application);
      queue.getMetrics().submitApp(user);

        LOG.info("Accepted application " + applicationId + " from user: " + user
            + ", in queue: " + queue.getName()
            + ", currently num of applications: " + applications.size());
      if (isAppRecovering) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(applicationId
              + " is recovering. Skip notifying APP_ACCEPTED");
        }
      } else{
        rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Add a new application attempt to the scheduler.
   */
  protected void addApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
    try {
      writeLock.lock();
      SchedulerApplication<FSAppAttempt> application = applications.get(
          applicationAttemptId.getApplicationId());
      String user = application.getUser();
      FSLeafQueue queue = (FSLeafQueue) application.getQueue();

      FSAppAttempt attempt = new FSAppAttempt(this, applicationAttemptId, user,
          queue, new ActiveUsersManager(getRootQueueMetrics()), rmContext);
      if (transferStateFromPreviousAttempt) {
        attempt.transferStateFromPreviousAttempt(
            application.getCurrentAppAttempt());
      }
      application.setCurrentAppAttempt(attempt);

      boolean runnable = maxRunningEnforcer.canAppBeRunnable(queue, attempt);
      queue.addApp(attempt, runnable);
      if (runnable) {
        maxRunningEnforcer.trackRunnableApp(attempt);
      } else{
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
      } else{
        rmContext.getDispatcher().getEventHandler().handle(
            new RMAppAttemptEvent(applicationAttemptId,
                RMAppAttemptEventType.ATTEMPT_ADDED));
      }
    } finally {
      writeLock.unlock();
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
    } catch (IllegalStateException se) {
      appRejectMsg = "Unable to match app " + rmApp.getApplicationId() +
          " to a queue placement policy, and no valid terminal queue " +
          " placement rule is configured. Please contact an administrator " +
          " to confirm that the fair scheduler configuration contains a " +
          " valid terminal queue placement rule.";
    } catch (InvalidQueueNameException qne) {
      appRejectMsg = qne.getMessage();
    } catch (IOException ioe) {
      // IOException should only happen for a user without groups
      appRejectMsg = "Error assigning app to a queue: " + ioe.getMessage();
    }

    if (appRejectMsg != null && rmApp != null) {
      LOG.error(appRejectMsg);
      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppEvent(rmApp.getApplicationId(),
              RMAppEventType.APP_REJECTED, appRejectMsg));
      return null;
    }

    if (rmApp != null) {
      rmApp.setQueue(queue.getName());
    } else {
      LOG.error("Couldn't find RM app to set queue name on");
    }
    return queue;
  }

  private void removeApplication(ApplicationId applicationId,
      RMAppState finalState) {
    SchedulerApplication<FSAppAttempt> application = applications.remove(
        applicationId);
    if (application == null) {
      LOG.warn("Couldn't find application " + applicationId);
    } else{
      application.stop(finalState);
    }
  }

  private void removeApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
    try {
      writeLock.lock();
      LOG.info("Application " + applicationAttemptId + " is done. finalState="
              + rmAppAttemptFinalState);
      FSAppAttempt attempt = getApplicationAttempt(applicationAttemptId);

      if (attempt == null) {
        LOG.info(
            "Unknown application " + applicationAttemptId + " has completed!");
        return;
      }

      // Check if the attempt is already stopped and don't stop it twice.
      if (attempt.isStopped()) {
        LOG.info("Application " + applicationAttemptId + " has already been "
            + "stopped!");
        return;
      }

      // Release all the running containers
      for (RMContainer rmContainer : attempt.getLiveContainers()) {
        if (keepContainers && rmContainer.getState().equals(
            RMContainerState.RUNNING)) {
          // do not kill the running container in the case of work-preserving AM
          // restart.
          LOG.info("Skip killing " + rmContainer.getContainerId());
          continue;
        }
        super.completedContainer(rmContainer, SchedulerUtils
                .createAbnormalContainerStatus(rmContainer.getContainerId(),
                    SchedulerUtils.COMPLETED_APPLICATION),
            RMContainerEventType.KILL);
      }

      // Release all reserved containers
      for (RMContainer rmContainer : attempt.getReservedContainers()) {
        super.completedContainer(rmContainer, SchedulerUtils
            .createAbnormalContainerStatus(rmContainer.getContainerId(),
                "Application Complete"), RMContainerEventType.KILL);
      }
      // Clean up pending requests, metrics etc.
      attempt.stop(rmAppAttemptFinalState);

      // Inform the queue
      FSLeafQueue queue = queueMgr.getLeafQueue(
          attempt.getQueue().getQueueName(), false);
      boolean wasRunnable = queue.removeApp(attempt);

      if (wasRunnable) {
        maxRunningEnforcer.untrackRunnableApp(attempt);
        maxRunningEnforcer.updateRunnabilityOnAppRemoval(attempt,
            attempt.getQueue());
      } else{
        maxRunningEnforcer.untrackNonRunnableApp(attempt);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Clean up a completed container.
   */
  @Override
  protected void completedContainerInternal(
      RMContainer rmContainer, ContainerStatus containerStatus,
      RMContainerEventType event) {
    try {
      writeLock.lock();
      Container container = rmContainer.getContainer();

      // Get the application for the finished container
      FSAppAttempt application = getCurrentAttemptForContainer(
          container.getId());
      ApplicationId appId =
          container.getId().getApplicationAttemptId().getApplicationId();
      if (application == null) {
        LOG.info(
            "Container " + container + " of" + " finished application " + appId
                + " completed with event " + event);
        return;
      }

      // Get the node on which the container was allocated
      FSSchedulerNode node = getFSSchedulerNode(container.getNodeId());

      if (rmContainer.getState() == RMContainerState.RESERVED) {
        application.unreserve(rmContainer.getReservedSchedulerKey(), node);
      } else{
        application.containerCompleted(rmContainer, containerStatus, event);
        node.releaseContainer(rmContainer.getContainerId(), false);
        updateRootQueueMetrics();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Application attempt " + application.getApplicationAttemptId()
            + " released container " + container.getId() + " on node: " + node
            + " with event: " + event);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void addNode(List<NMContainerStatus> containerReports,
      RMNode node) {
    try {
      writeLock.lock();
      FSSchedulerNode schedulerNode = new FSSchedulerNode(node,
          usePortForNodeName);
      nodeTracker.addNode(schedulerNode);

      triggerUpdate();

      Resource clusterResource = getClusterResource();
      queueMgr.getRootQueue().setSteadyFairShare(clusterResource);
      queueMgr.getRootQueue().recomputeSteadyShares();
      LOG.info("Added node " + node.getNodeAddress() + " cluster capacity: "
          + clusterResource);

      recoverContainersOnNode(containerReports, node);
      updateRootQueueMetrics();
    } finally {
      writeLock.unlock();
    }
  }

  private void removeNode(RMNode rmNode) {
    try {
      writeLock.lock();
      NodeId nodeId = rmNode.getNodeID();
      FSSchedulerNode node = nodeTracker.getNode(nodeId);
      if (node == null) {
        LOG.error("Attempting to remove non-existent node " + nodeId);
        return;
      }

      // Remove running containers
      List<RMContainer> runningContainers =
          node.getCopiedListOfRunningContainers();
      for (RMContainer container : runningContainers) {
        super.completedContainer(container, SchedulerUtils
            .createAbnormalContainerStatus(container.getContainerId(),
                SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
      }

      // Remove reservations, if any
      RMContainer reservedContainer = node.getReservedContainer();
      if (reservedContainer != null) {
        super.completedContainer(reservedContainer, SchedulerUtils
            .createAbnormalContainerStatus(reservedContainer.getContainerId(),
                SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
      }

      nodeTracker.removeNode(nodeId);
      Resource clusterResource = getClusterResource();
      queueMgr.getRootQueue().setSteadyFairShare(clusterResource);
      queueMgr.getRootQueue().recomputeSteadyShares();
      updateRootQueueMetrics();
      triggerUpdate();

      LOG.info("Removed node " + rmNode.getNodeAddress() + " cluster capacity: "
          + clusterResource);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Resource getNormalizedResource(Resource requestedResource) {
    return SchedulerUtils.getNormalizedResource(requestedResource,
        DOMINANT_RESOURCE_CALCULATOR,
        minimumAllocation,
        getMaximumResourceCapability(),
        incrAllocation);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release,
      List<String> blacklistAdditions, List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {

    // Make sure this application exists
    FSAppAttempt application = getSchedulerApp(appAttemptId);
    if (application == null) {
      LOG.error("Calling allocate on removed or non existent application " +
          appAttemptId.getApplicationId());
      return EMPTY_ALLOCATION;
    }

    // The allocate may be the leftover from previous attempt, and it will
    // impact current attempt, such as confuse the request and allocation for
    // current attempt's AM container.
    // Note outside precondition check for the attempt id may be
    // outdated here, so double check it here is necessary.
    if (!application.getApplicationAttemptId().equals(appAttemptId)) {
      LOG.error("Calling allocate on previous or removed " +
          "or non existent application attempt " + appAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Handle promotions and demotions
    handleContainerUpdates(application, updateRequests);

    // Sanity check
    normalizeRequests(ask);

    // Record container allocation start time
    application.recordContainerRequestTime(getClock().getTime());

    // Release containers
    releaseContainers(release, application);

    ReentrantReadWriteLock.WriteLock lock = application.getWriteLock();
    lock.lock();
    try {
      if (!ask.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "allocate: pre-update" + " applicationAttemptId=" + appAttemptId
                  + " application=" + application.getApplicationId());
        }
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        application.showRequests();
      }
    } finally {
      lock.unlock();
    }

    Set<ContainerId> preemptionContainerIds =
        application.getPreemptionContainerIds();
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "allocate: post-update" + " applicationAttemptId=" + appAttemptId
              + " #ask=" + ask.size() + " reservation= " + application
              .getCurrentReservation());

      LOG.debug("Preempting " + preemptionContainerIds.size()
          + " container(s)");
    }

    application.updateBlacklist(blacklistAdditions, blacklistRemovals);

    List<Container> newlyAllocatedContainers =
        application.pullNewlyAllocatedContainers();
    // Record container allocation time
    if (!(newlyAllocatedContainers.isEmpty())) {
      application.recordContainerAllocationTime(getClock().getTime());
    }

    Resource headroom = application.getHeadroom();
    application.setApplicationHeadroomForMetrics(headroom);
    return new Allocation(newlyAllocatedContainers, headroom,
        preemptionContainerIds, null, null,
        application.pullUpdatedNMTokens(), null, null,
        application.pullNewlyPromotedContainers(),
        application.pullNewlyDemotedContainers());
  }

  @Override
  protected void nodeUpdate(RMNode nm) {
    try {
      writeLock.lock();
      long start = getClock().getTime();
      eventLog.log("HEARTBEAT", nm.getHostName());
      super.nodeUpdate(nm);

      FSSchedulerNode fsNode = getFSSchedulerNode(nm.getNodeID());
      attemptScheduling(fsNode);

      long duration = getClock().getTime() - start;
      fsOpDurations.addNodeUpdateDuration(duration);
    } finally {
      writeLock.unlock();
    }
  }

  void continuousSchedulingAttempt() throws InterruptedException {
    long start = getClock().getTime();
    List<FSSchedulerNode> nodeIdList;
    // Hold a lock to prevent comparator order changes due to changes of node
    // unallocated resources
    synchronized (this) {
      nodeIdList = nodeTracker.sortedNodeList(nodeAvailableResourceComparator);
    }

    // iterate all nodes
    for (FSSchedulerNode node : nodeIdList) {
      try {
        if (Resources.fitsIn(minimumAllocation,
            node.getUnallocatedResource())) {
          attemptScheduling(node);
        }
      } catch (Throwable ex) {
        LOG.error("Error while attempting scheduling for node " + node +
            ": " + ex.toString(), ex);
        if ((ex instanceof YarnRuntimeException) &&
            (ex.getCause() instanceof InterruptedException)) {
          // AsyncDispatcher translates InterruptedException to
          // YarnRuntimeException with cause InterruptedException.
          // Need to throw InterruptedException to stop schedulingThread.
          throw (InterruptedException)ex.getCause();
        }
      }
    }

    long duration = getClock().getTime() - start;
    fsOpDurations.addContinuousSchedulingRunDuration(duration);
  }

  /** Sort nodes by available resource */
  private class NodeAvailableResourceComparator
      implements Comparator<FSSchedulerNode> {

    @Override
    public int compare(FSSchedulerNode n1, FSSchedulerNode n2) {
      return RESOURCE_CALCULATOR.compare(getClusterResource(),
          n2.getUnallocatedResource(),
          n1.getUnallocatedResource());
    }
  }

  private boolean shouldContinueAssigning(int containers,
      Resource maxResourcesToAssign, Resource assignedResource) {
    if (!assignMultiple) {
      return false; // assignMultiple is not enabled. Allocate one at a time.
    }

    if (maxAssignDynamic) {
      // Using fitsIn to check if the resources assigned so far are less than
      // or equal to max resources to assign (half of remaining resources).
      // The "equal to" part can lead to allocating one extra container.
      return Resources.fitsIn(assignedResource, maxResourcesToAssign);
    } else {
      return maxAssign <= 0 || containers < maxAssign;
    }
  }

  /**
   * Assign preempted containers to the applications that have reserved
   * resources for preempted containers.
   * @param node Node to check
   */
  static void assignPreemptedContainers(FSSchedulerNode node) {
    for (Entry<FSAppAttempt, Resource> entry :
        node.getPreemptionList().entrySet()) {
      FSAppAttempt app = entry.getKey();
      Resource preemptionPending = Resources.clone(entry.getValue());
      while (!app.isStopped() && !Resources.isNone(preemptionPending)) {
        Resource assigned = app.assignContainer(node);
        if (Resources.isNone(assigned) ||
            assigned.equals(FairScheduler.CONTAINER_RESERVED)) {
          // Fail to assign, let's not try further
          break;
        }
        Resources.subtractFromNonNegative(preemptionPending, assigned);
      }
    }
  }

  @VisibleForTesting
  void attemptScheduling(FSSchedulerNode node) {
    try {
      writeLock.lock();
      if (rmContext.isWorkPreservingRecoveryEnabled() && !rmContext
          .isSchedulerReadyForAllocatingContainers()) {
        return;
      }

      final NodeId nodeID = node.getNodeID();
      if (!nodeTracker.exists(nodeID)) {
        // The node might have just been removed while this thread was waiting
        // on the synchronized lock before it entered this synchronized method
        LOG.info(
            "Skipping scheduling as the node " + nodeID + " has been removed");
        return;
      }

      // Assign new containers...
      // 1. Ensure containers are assigned to the apps that preempted
      // 2. Check for reserved applications
      // 3. Schedule if there are no reservations

      // Apps may wait for preempted containers
      // We have to satisfy these first to avoid cases, when we preempt
      // a container for A from B and C gets the preempted containers,
      // when C does not qualify for preemption itself.
      assignPreemptedContainers(node);
      FSAppAttempt reservedAppSchedulable = node.getReservedAppSchedulable();
      boolean validReservation = false;
      if (reservedAppSchedulable != null) {
        validReservation = reservedAppSchedulable.assignReservedContainer(node);
      }
      if (!validReservation) {
        // No reservation, schedule at queue which is farthest below fair share
        int assignedContainers = 0;
        Resource assignedResource = Resources.clone(Resources.none());
        Resource maxResourcesToAssign = Resources.multiply(
            node.getUnallocatedResource(), 0.5f);
        while (node.getReservedContainer() == null) {
          Resource assignment = queueMgr.getRootQueue().assignContainer(node);
          if (assignment.equals(Resources.none())) {
            break;
          }

          assignedContainers++;
          Resources.addTo(assignedResource, assignment);
          if (!shouldContinueAssigning(assignedContainers, maxResourcesToAssign,
              assignedResource)) {
            break;
          }
        }
      }
      updateRootQueueMetrics();
    } finally {
      writeLock.unlock();
    }
  }

  public FSAppAttempt getSchedulerApp(ApplicationAttemptId appAttemptId) {
    return super.getApplicationAttempt(appAttemptId);
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return RESOURCE_CALCULATOR;
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
            getClusterResource(), rootMetrics.getAllocatedResources()));
  }

  /**
   * Check if preemption is enabled and the utilization threshold for
   * preemption is met.
   *
   * @return true if preemption should be attempted, false otherwise.
   */
  private boolean shouldAttemptPreemption() {
    if (context.isPreemptionEnabled()) {
      return (context.getPreemptionUtilizationThreshold() < Math.max(
          (float) rootMetrics.getAllocatedMB() /
              getClusterResource().getMemorySize(),
          (float) rootMetrics.getAllocatedVirtualCores() /
              getClusterResource().getVirtualCores()));
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
      addNode(nodeAddedEvent.getContainerReports(),
          nodeAddedEvent.getAddedRMNode());
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
      String queueName =
          resolveReservationQueueName(appAddedEvent.getQueue(),
              appAddedEvent.getApplicationId(),
              appAddedEvent.getReservationID(),
              appAddedEvent.getIsAppRecovering());
      if (queueName != null) {
        addApplication(appAddedEvent.getApplicationId(),
            queueName, appAddedEvent.getUser(),
            appAddedEvent.getIsAppRecovering());
      }
      break;
    case APP_REMOVED:
      if (!(event instanceof AppRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      removeApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
      break;
    case NODE_RESOURCE_UPDATE:
      if (!(event instanceof NodeResourceUpdateSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = 
          (NodeResourceUpdateSchedulerEvent)event;
      updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
            nodeResourceUpdatedEvent.getResourceOption());
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
    case RELEASE_CONTAINER:
      if (!(event instanceof ReleaseContainerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      RMContainer container = ((ReleaseContainerEvent) event).getContainer();
      completedContainer(container,
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(),
              SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
      break;
    case CONTAINER_EXPIRED:
      if (!(event instanceof ContainerExpiredSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      ContainerExpiredSchedulerEvent containerExpiredEvent =
          (ContainerExpiredSchedulerEvent)event;
      ContainerId containerId = containerExpiredEvent.getContainerId();
      super.completedContainer(getRMContainer(containerId),
          SchedulerUtils.createAbnormalContainerStatus(
              containerId,
              SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
      break;
    default:
      LOG.error("Unknown event arrived at FairScheduler: " + event.toString());
    }
  }

  private String resolveReservationQueueName(String queueName,
      ApplicationId applicationId, ReservationId reservationID,
      boolean isRecovering) {
    try {
      readLock.lock();
      FSQueue queue = queueMgr.getQueue(queueName);
      if ((queue == null) || !allocConf.isReservable(queue.getQueueName())) {
        return queueName;
      }
      // Use fully specified name from now on (including root. prefix)
      queueName = queue.getQueueName();
      if (reservationID != null) {
        String resQName = queueName + "." + reservationID.toString();
        queue = queueMgr.getQueue(resQName);
        if (queue == null) {
          // reservation has terminated during failover
          if (isRecovering && allocConf.getMoveOnExpiry(queueName)) {
            // move to the default child queue of the plan
            return getDefaultQueueForPlanQueue(queueName);
          }
          String message = "Application " + applicationId
              + " submitted to a reservation which is not yet "
              + "currently active: " + resQName;
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                  message));
          return null;
        }
        if (!queue.getParent().getQueueName().equals(queueName)) {
          String message =
              "Application: " + applicationId + " submitted to a reservation "
                  + resQName + " which does not belong to the specified queue: "
                  + queueName;
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                  message));
          return null;
        }
        // use the reservation queue to run the app
        queueName = resQName;
      } else{
        // use the default child queue of the plan for unreserved apps
        queueName = getDefaultQueueForPlanQueue(queueName);
      }
      return queueName;
    } finally {
      readLock.unlock();
    }

  }

  private String getDefaultQueueForPlanQueue(String queueName) {
    String planName = queueName.substring(queueName.lastIndexOf(".") + 1);
    queueName = queueName + "." + planName + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    return queueName;
  }

  @Override
  public void recover(RMState state) throws Exception {
    // NOT IMPLEMENTED
  }

  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  private void initScheduler(Configuration conf) throws IOException {
    try {
      writeLock.lock();
      this.conf = new FairSchedulerConfiguration(conf);
      validateConf(this.conf);
      authorizer = YarnAuthorizationProvider.getInstance(conf);
      minimumAllocation = super.getMinimumAllocation();
      initMaximumResourceCapability(super.getMaximumAllocation());
      incrAllocation = this.conf.getIncrementAllocation();
      updateReservationThreshold();
      continuousSchedulingEnabled = this.conf.isContinuousSchedulingEnabled();
      continuousSchedulingSleepMs = this.conf.getContinuousSchedulingSleepMs();
      nodeLocalityThreshold = this.conf.getLocalityThresholdNode();
      rackLocalityThreshold = this.conf.getLocalityThresholdRack();
      nodeLocalityDelayMs = this.conf.getLocalityDelayNodeMs();
      rackLocalityDelayMs = this.conf.getLocalityDelayRackMs();
      assignMultiple = this.conf.getAssignMultiple();
      maxAssignDynamic = this.conf.isMaxAssignDynamic();
      maxAssign = this.conf.getMaxAssign();
      sizeBasedWeight = this.conf.getSizeBasedWeight();
      usePortForNodeName = this.conf.getUsePortForNodeName();
      reservableNodesRatio = this.conf.getReservableNodes();

      updateInterval = this.conf.getUpdateInterval();
      if (updateInterval < 0) {
        updateInterval = FairSchedulerConfiguration.DEFAULT_UPDATE_INTERVAL_MS;
        LOG.warn(FairSchedulerConfiguration.UPDATE_INTERVAL_MS
            + " is invalid, so using default value "
            + FairSchedulerConfiguration.DEFAULT_UPDATE_INTERVAL_MS
            + " ms instead");
      }

      rootMetrics = FSQueueMetrics.forQueue("root", null, true, conf);
      fsOpDurations = FSOpDurations.getInstance(true);

      // This stores per-application scheduling information
      this.applications = new ConcurrentHashMap<>();
      this.eventLog = new FairSchedulerEventLog();
      eventLog.init(this.conf);

      allocConf = new AllocationConfiguration(conf);
      try {
        queueMgr.initialize(conf);
      } catch (Exception e) {
        throw new IOException("Failed to start FairScheduler", e);
      }

      if (continuousSchedulingEnabled) {
        // start continuous scheduling thread
        schedulingThread = new ContinuousSchedulingThread();
        schedulingThread.setName("FairSchedulerContinuousScheduling");
        schedulingThread.setUncaughtExceptionHandler(
            new RMCriticalThreadUncaughtExceptionHandler(rmContext));
        schedulingThread.setDaemon(true);
      }

      if (this.conf.getPreemptionEnabled()) {
        createPreemptionThread();
      }
    } finally {
      writeLock.unlock();
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

  @VisibleForTesting
  protected void createPreemptionThread() {
    preemptionThread = new FSPreemptionThread(this);
    preemptionThread.setUncaughtExceptionHandler(
        new RMCriticalThreadUncaughtExceptionHandler(rmContext));
  }

  private void updateReservationThreshold() {
    Resource newThreshold = Resources.multiply(
        getIncrementResourceCapability(),
        this.conf.getReservationThresholdIncrementMultiple());

    reservationThreshold = newThreshold;
  }

  private void startSchedulerThreads() {
    try {
      writeLock.lock();
      Preconditions.checkNotNull(allocsLoader, "allocsLoader is null");
      if (continuousSchedulingEnabled) {
        Preconditions.checkNotNull(schedulingThread,
            "schedulingThread is null");
        schedulingThread.start();
      }
      if (preemptionThread != null) {
        preemptionThread.start();
      }
      allocsLoader.start();
    } finally {
      writeLock.unlock();
    }
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
    try {
      writeLock.lock();
      if (continuousSchedulingEnabled) {
        if (schedulingThread != null) {
          schedulingThread.interrupt();
          schedulingThread.join(THREAD_JOIN_TIMEOUT_MS);
        }
      }
      if (preemptionThread != null) {
        preemptionThread.interrupt();
        preemptionThread.join(THREAD_JOIN_TIMEOUT_MS);
      }
      if (allocsLoader != null) {
        allocsLoader.stop();
      }
    } finally {
      writeLock.unlock();
    }

    super.serviceStop();
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
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
    UserGroupInformation user;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      return new ArrayList<QueueUserACLInfo>();
    }

    return queueMgr.getRootQueue().getQueueUserAclInfo(user);
  }

  @Override
  public int getNumClusterNodes() {
    return nodeTracker.nodeCount();
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    try {
      readLock.lock();
      FSQueue queue = getQueueManager().getQueue(queueName);
      if (queue == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("ACL not found for queue access-type " + acl + " for queue "
              + queueName);
        }
        return false;
      }
      return queue.hasAccess(acl, callerUGI);
    } finally {
      readLock.unlock();
    }
  }
  
  public AllocationConfiguration getAllocationConfiguration() {
    return allocConf;
  }
  
  private class AllocationReloadListener implements
      AllocationFileLoaderService.Listener {

    @Override
    public void onReload(AllocationConfiguration queueInfo)
        throws IOException {
      // Commit the reload; also create any queue defined in the alloc file
      // if it does not already exist, so it can be displayed on the web UI.

      writeLock.lock();
      try {
        if (queueInfo == null) {
          authorizer.setPermission(allocsLoader.getDefaultPermissions(),
              UserGroupInformation.getCurrentUser());
        } else {
          allocConf = queueInfo;
          setQueueAcls(allocConf.getQueueAcls());
          allocConf.getDefaultSchedulingPolicy().initialize(getContext());
          queueMgr.updateAllocationConfiguration(allocConf);
          applyChildDefaults();
          maxRunningEnforcer.updateRunnabilityOnReload();
        }
      } finally {
        writeLock.unlock();
      }
    }
  }

  private void setQueueAcls(
      Map<String, Map<AccessType, AccessControlList>> queueAcls)
      throws IOException {
    authorizer.setPermission(allocsLoader.getDefaultPermissions(),
        UserGroupInformation.getCurrentUser());
    List<Permission> permissions = new ArrayList<>();
    for (Entry<String, Map<AccessType, AccessControlList>> queueAcl : queueAcls
        .entrySet()) {
      permissions.add(new Permission(new PrivilegedEntity(EntityType.QUEUE,
          queueAcl.getKey()), queueAcl.getValue()));
    }
    authorizer.setPermission(permissions,
        UserGroupInformation.getCurrentUser());
  }

  /**
   * After reloading the allocation config, the max resource settings for any
   * ad hoc queues will be missing. This method goes through the queue manager's
   * queue list and adds back the max resources settings for any ad hoc queues.
   * Note that the new max resource settings will be based on the new config.
   * The old settings are lost.
   */
  private void applyChildDefaults() {
    Collection<FSQueue> queues = queueMgr.getQueues();
    Set<String> configuredLeafQueues =
        allocConf.getConfiguredQueues().get(FSQueueType.LEAF);
    Set<String> configuredParentQueues =
        allocConf.getConfiguredQueues().get(FSQueueType.PARENT);

    for (FSQueue queue : queues) {
      // If the queue is ad hoc and not root, apply the child defaults
      if ((queue.getParent() != null) &&
          !configuredLeafQueues.contains(queue.getName()) &&
          !configuredParentQueues.contains(queue.getName())) {
        ConfigurableResource max = queue.getParent().
            getMaxChildQueueResource();

        if (max != null) {
          queue.setMaxShare(max);
        }
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
  public String moveApplication(ApplicationId appId,
      String queueName) throws YarnException {
    try {
      writeLock.lock();
      SchedulerApplication<FSAppAttempt> app = applications.get(appId);
      if (app == null) {
        throw new YarnException("App to be moved " + appId + " not found.");
      }
      FSAppAttempt attempt = (FSAppAttempt) app.getCurrentAppAttempt();
      // To serialize with FairScheduler#allocate, synchronize on app attempt

      try {
        attempt.getWriteLock().lock();
        FSLeafQueue oldQueue = (FSLeafQueue) app.getQueue();
        // Check if the attempt is already stopped: don't move stopped app
        // attempt. The attempt has already been removed from all queues.
        if (attempt.isStopped()) {
          LOG.info("Application " + appId + " is stopped and can't be moved!");
          throw new YarnException("Application " + appId
              + " is stopped and can't be moved!");
        }
        String destQueueName = handleMoveToPlanQueue(queueName);
        FSLeafQueue targetQueue = queueMgr.getLeafQueue(destQueueName, false);
        if (targetQueue == null) {
          throw new YarnException("Target queue " + queueName
              + " not found or is not a leaf queue.");
        }
        if (targetQueue == oldQueue) {
          return oldQueue.getQueueName();
        }

        if (oldQueue.isRunnableApp(attempt)) {
          verifyMoveDoesNotViolateConstraints(attempt, oldQueue, targetQueue);
        }

        executeMove(app, attempt, oldQueue, targetQueue);
        return targetQueue.getQueueName();
      } finally {
        attempt.getWriteLock().unlock();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void preValidateMoveApplication(ApplicationId appId, String newQueue)
      throws YarnException {
    try {
      writeLock.lock();
      SchedulerApplication<FSAppAttempt> app = applications.get(appId);
      if (app == null) {
        throw new YarnException("App to be moved " + appId + " not found.");
      }

      FSAppAttempt attempt = app.getCurrentAppAttempt();
      // To serialize with FairScheduler#allocate, synchronize on app attempt

      try {
        attempt.getWriteLock().lock();
        FSLeafQueue oldQueue = (FSLeafQueue) app.getQueue();
        String destQueueName = handleMoveToPlanQueue(newQueue);
        FSLeafQueue targetQueue = queueMgr.getLeafQueue(destQueueName, false);
        if (targetQueue == null) {
          throw new YarnException("Target queue " + newQueue
              + " not found or is not a leaf queue.");
        }

        if (oldQueue.isRunnableApp(attempt)) {
          verifyMoveDoesNotViolateConstraints(attempt, oldQueue, targetQueue);
        }
      } finally {
        attempt.getWriteLock().unlock();
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void verifyMoveDoesNotViolateConstraints(FSAppAttempt app,
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
      if (cur.getNumRunnableApps() == cur.getMaxRunningApps()) {
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
  private void executeMove(SchedulerApplication<FSAppAttempt> app,
      FSAppAttempt attempt, FSLeafQueue oldQueue, FSLeafQueue newQueue)
      throws YarnException {
    // Check current runs state. Do not remove the attempt from the queue until
    // after the check has been performed otherwise it could remove the app
    // from a queue without moving it to a new queue.
    boolean wasRunnable = oldQueue.isRunnableApp(attempt);
    // if app was not runnable before, it may be runnable now
    boolean nowRunnable = maxRunningEnforcer.canAppBeRunnable(newQueue,
        attempt);
    if (wasRunnable && !nowRunnable) {
      throw new YarnException("Should have already verified that app "
          + attempt.getApplicationId() + " would be runnable in new queue");
    }

    // Now it is safe to remove from the queue.
    oldQueue.removeApp(attempt);

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
  
  /**
   * Process resource update on a node and update Queue.
   */
  @Override
  public void updateNodeResource(RMNode nm,
      ResourceOption resourceOption) {
    try {
      writeLock.lock();
      super.updateNodeResource(nm, resourceOption);
      updateRootQueueMetrics();
      queueMgr.getRootQueue().setSteadyFairShare(getClusterResource());
      queueMgr.getRootQueue().recomputeSteadyShares();
    } finally {
      writeLock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes() {
    return EnumSet
      .of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU);
  }

  @Override
  public Set<String> getPlanQueues() throws YarnException {
    Set<String> planQueues = new HashSet<String>();
    for (FSQueue fsQueue : queueMgr.getQueues()) {
      String queueName = fsQueue.getName();
      if (allocConf.isReservable(queueName)) {
        planQueues.add(queueName);
      }
    }
    return planQueues;
  }

  @Override
  public void setEntitlement(String queueName,
      QueueEntitlement entitlement) throws YarnException {

    FSLeafQueue reservationQueue = queueMgr.getLeafQueue(queueName, false);
    if (reservationQueue == null) {
      throw new YarnException("Target queue " + queueName
          + " not found or is not a leaf queue.");
    }

    reservationQueue.setWeights(entitlement.getCapacity());

    // TODO Does MaxCapacity need to be set for fairScheduler ?
  }

  /**
   * Only supports removing empty leaf queues
   * @param queueName name of queue to remove
   * @throws YarnException if queue to remove is either not a leaf or if its
   * not empty
   */
  @Override
  public void removeQueue(String queueName) throws YarnException {
    FSLeafQueue reservationQueue = queueMgr.getLeafQueue(queueName, false);
    if (reservationQueue != null) {
      if (!queueMgr.removeLeafQueue(queueName)) {
        throw new YarnException("Could not remove queue " + queueName + " as " +
            "its either not a leaf queue or its not empty");
      }
    }
  }

  private String handleMoveToPlanQueue(String targetQueueName) {
    FSQueue dest = queueMgr.getQueue(targetQueueName);
    if (dest != null && allocConf.isReservable(dest.getQueueName())) {
      // use the default child reservation queue of the plan
      targetQueueName = getDefaultQueueForPlanQueue(targetQueueName);
    }
    return targetQueueName;
  }

  public float getReservableNodesRatio() {
    return reservableNodesRatio;
  }

  long getNMHeartbeatInterval() {
    return nmHeartbeatInterval;
  }

  ReadLock getSchedulerReadLock() {
    return this.readLock;
  }

  @Override
  public long checkAndGetApplicationLifetime(String queueName, long lifetime) {
    // Lifetime is the application lifetime by default.
    return lifetime;
  }
}
