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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMCriticalThreadUncaughtExceptionHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitorManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerNMDoneChangeResourceEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerRecoverEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeFinishedContainersPulledByAMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;


@SuppressWarnings("unchecked")
@Private
@Unstable
public abstract class AbstractYarnScheduler
    <T extends SchedulerApplicationAttempt, N extends SchedulerNode>
    extends AbstractService implements ResourceScheduler {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractYarnScheduler.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  protected final ClusterNodeTracker<N> nodeTracker =
      new ClusterNodeTracker<>();

  protected Resource minimumAllocation;

  protected volatile RMContext rmContext;

  private volatile Priority maxClusterLevelAppPriority;

  protected ActivitiesManager activitiesManager;
  protected SchedulerHealth schedulerHealth = new SchedulerHealth();
  protected volatile long lastNodeUpdateTime;

  // timeout to join when we stop this service
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  private volatile Clock clock;

  /**
   * To enable the update thread, subclasses should set updateInterval to a
   * positive value during {@link #serviceInit(Configuration)}.
   */
  protected long updateInterval = -1L;
  @VisibleForTesting
  Thread updateThread;
  private final Object updateThreadMonitor = new Object();
  private Timer releaseCache;
  private boolean autoCorrectContainerAllocation;

  /*
   * All schedulers which are inheriting AbstractYarnScheduler should use
   * concurrent version of 'applications' map.
   */
  protected ConcurrentMap<ApplicationId, SchedulerApplication<T>> applications;
  protected int nmExpireInterval;
  protected long nmHeartbeatInterval;
  private long skipNodeInterval;

  private final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
    EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  protected final ReentrantReadWriteLock.ReadLock readLock;

  /*
   * Use writeLock for any of operations below:
   * - queue change (hierarchy / configuration / container allocation)
   * - application(add/remove/allocate-container, but not include container
   *   finish)
   * - node (add/remove/change-resource/container-allocation, but not include
   *   container finish)
   */
  protected final ReentrantReadWriteLock.WriteLock writeLock;

  // If set to true, then ALL container updates will be automatically sent to
  // the NM in the next heartbeat.
  private boolean autoUpdateContainers = false;

  protected SchedulingMonitorManager schedulingMonitorManager =
      new SchedulingMonitorManager();

  private boolean migration;

  /**
   * Construct the service.
   *
   * @param name service name
   */
  public AbstractYarnScheduler(String name) {
    super(name);
    clock = SystemClock.getInstance();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    migration =
        conf.getBoolean(FairSchedulerConfiguration.MIGRATION_MODE, false);

    nmExpireInterval =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    nmHeartbeatInterval =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    skipNodeInterval = YarnConfiguration.getSkipNodeInterval(conf);
    autoCorrectContainerAllocation =
        conf.getBoolean(YarnConfiguration.RM_SCHEDULER_AUTOCORRECT_CONTAINER_ALLOCATION,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_AUTOCORRECT_CONTAINER_ALLOCATION);
    long configuredMaximumAllocationWaitTime =
        conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
          YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
    nodeTracker.setConfiguredMaxAllocationWaitTime(
        configuredMaximumAllocationWaitTime);
    maxClusterLevelAppPriority = getMaxPriorityFromConf(conf);
    if (!migration) {
      this.releaseCache = new Timer("Pending Container Clear Timer");
    }

    autoUpdateContainers =
        conf.getBoolean(YarnConfiguration.RM_AUTO_UPDATE_CONTAINERS,
            YarnConfiguration.DEFAULT_RM_AUTO_UPDATE_CONTAINERS);

    if (updateInterval > 0) {
      updateThread = new UpdateThread();
      updateThread.setName("SchedulerUpdateThread");
      updateThread.setUncaughtExceptionHandler(
          new RMCriticalThreadUncaughtExceptionHandler(rmContext));
      updateThread.setDaemon(true);
    }
    super.serviceInit(conf);

  }

  @Override
  protected void serviceStart() throws Exception {
    if (!migration) {
      if (updateThread != null) {
        updateThread.start();
      }
      schedulingMonitorManager.startAll();
      createReleaseCache();
    }

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (updateThread != null) {
      updateThread.interrupt();
      updateThread.join(THREAD_JOIN_TIMEOUT_MS);
    }

    //Stop Timer
    if (releaseCache != null) {
      releaseCache.cancel();
      releaseCache = null;
    }
    schedulingMonitorManager.stop();
    super.serviceStop();
  }

  @VisibleForTesting
  public ClusterNodeTracker<N> getNodeTracker() {
    return nodeTracker;
  }

  @VisibleForTesting
  public SchedulingMonitorManager getSchedulingMonitorManager() {
    return schedulingMonitorManager;
  }

  /*
   * YARN-3136 removed synchronized lock for this method for performance
   * purposes
   */
  public List<Container> getTransferredContainers(
      ApplicationAttemptId currentAttempt) {
    ApplicationId appId = currentAttempt.getApplicationId();
    SchedulerApplication<T> app = applications.get(appId);
    List<Container> containerList = new ArrayList<Container>();
    if (app == null) {
      return containerList;
    }
    Collection<RMContainer> liveContainers = app.getCurrentAppAttempt()
        .pullContainersToTransfer();
    ContainerId amContainerId = null;
    // For UAM, amContainer would be null
    if (rmContext.getRMApps().get(appId).getCurrentAppAttempt()
        .getMasterContainer() != null) {
      amContainerId = rmContext.getRMApps().get(appId).getCurrentAppAttempt()
          .getMasterContainer().getId();
    }
    for (RMContainer rmContainer : liveContainers) {
      if (!rmContainer.getContainerId().equals(amContainerId)) {
        containerList.add(rmContainer.getContainer());
      }
    }
    return containerList;
  }

  public Map<ApplicationId, SchedulerApplication<T>>
      getSchedulerApplications() {
    return applications;
  }

  /**
   * Add blacklisted NodeIds to the list that is passed.
   *
   * @param app application attempt.
   * @return blacklisted NodeIds.
   */
  public List<N> getBlacklistedNodes(final SchedulerApplicationAttempt app) {

    NodeFilter nodeFilter = new NodeFilter() {
      @Override
      public boolean accept(SchedulerNode node) {
        return SchedulerAppUtils.isPlaceBlacklisted(app, node, LOG);
      }
    };
    return nodeTracker.getNodes(nodeFilter);
  }

  public List<N> getNodes(final NodeFilter filter) {
    return nodeTracker.getNodes(filter);
  }

  public boolean shouldContainersBeAutoUpdated() {
    return this.autoUpdateContainers;
  }

  @Override
  public Resource getClusterResource() {
    return nodeTracker.getClusterCapacity();
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return nodeTracker.getMaxAllowedAllocation();
  }

  @Override
  public Resource getMaximumResourceCapability(String queueName) {
    return getMaximumResourceCapability();
  }

  protected void initMaximumResourceCapability(Resource maximumAllocation) {
    nodeTracker.setConfiguredMaxAllocation(maximumAllocation);
  }

  public SchedulerHealth getSchedulerHealth() {
    return this.schedulerHealth;
  }

  protected void setLastNodeUpdateTime(long time) {
    this.lastNodeUpdateTime = time;
  }

  public long getLastNodeUpdateTime() {
    return lastNodeUpdateTime;
  }

  public long getSkipNodeInterval(){
    return skipNodeInterval;
  }

  protected void containerLaunchedOnNode(
      ContainerId containerId, SchedulerNode node) {
    readLock.lock();
    try {
      // Get the application for the finished container
      SchedulerApplicationAttempt application =
          getCurrentAttemptForContainer(containerId);
      if (application == null) {
        LOG.info("Unknown application " + containerId.getApplicationAttemptId()
            .getApplicationId() + " launched container " + containerId
            + " on node: " + node);
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
        return;
      }

      application.containerLaunchedOnNode(containerId, node.getNodeID());
      node.containerStarted(containerId);
    } finally {
      readLock.unlock();
    }
  }

  protected void containerIncreasedOnNode(ContainerId containerId,
      SchedulerNode node, Container increasedContainerReportedByNM) {
    /*
     * No lock is required, as this method is protected by scheduler's writeLock
     */
    // Get the application for the finished container
    SchedulerApplicationAttempt application = getCurrentAttemptForContainer(
        containerId);
    if (application == null) {
      LOG.info("Unknown application " + containerId.getApplicationAttemptId()
          .getApplicationId() + " increased container " + containerId
          + " on node: " + node);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
      return;
    }

    RMContainer rmContainer = getRMContainer(containerId);
    if (rmContainer == null) {
      // Some unknown container sneaked into the system. Kill it.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
      return;
    }
    rmContainer.handle(new RMContainerNMDoneChangeResourceEvent(containerId,
        increasedContainerReportedByNM.getResource()));

  }

  // TODO: Rename it to getCurrentApplicationAttempt
  public T getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    SchedulerApplication<T> app = applications.get(
        applicationAttemptId.getApplicationId());
    return app == null ? null : app.getCurrentAppAttempt();
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      LOG.debug("Request for appInfo of unknown attempt {}", appAttemptId);
      return null;
    }
    return new SchedulerAppReport(attempt);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      LOG.debug("Request for appInfo of unknown attempt {}", appAttemptId);
      return null;
    }
    return attempt.getResourceUsageReport();
  }

  public T getCurrentAttemptForContainer(ContainerId containerId) {
    return getApplicationAttempt(containerId.getApplicationAttemptId());
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    SchedulerApplicationAttempt attempt =
        getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    return nodeTracker.getNodeReport(nodeId);
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support moving apps between queues");
  }

  @Override
  public void preValidateMoveApplication(ApplicationId appId,
      String newQueue) throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support pre-validation of moving apps between queues");
  }

  public void removeQueue(String queueName) throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support removing queues");
  }

  @Override
  public void addQueue(Queue newQueue) throws YarnException, IOException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support this operation");
  }

  @Override
  public void setEntitlement(String queue, QueueEntitlement entitlement)
      throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support this operation");
  }

  private void killOrphanContainerOnNode(RMNode node,
      NMContainerStatus container) {
    if (!container.getContainerState().equals(ContainerState.COMPLETE)) {
      LOG.warn("Killing container " + container + " for unknown application");
      this.rmContext.getDispatcher().getEventHandler().handle(
        new RMNodeCleanContainerEvent(node.getNodeID(),
          container.getContainerId()));
    }
  }

  public void recoverContainersOnNode(List<NMContainerStatus> containerReports,
      RMNode nm) {
    writeLock.lock();
    try {
      if (!rmContext.isWorkPreservingRecoveryEnabled()
          || containerReports == null || (containerReports != null
          && containerReports.isEmpty())) {
        return;
      }

      for (NMContainerStatus container : containerReports) {
        ApplicationId appId =
            container.getContainerId().getApplicationAttemptId()
                .getApplicationId();
        RMApp rmApp = rmContext.getRMApps().get(appId);
        if (rmApp == null) {
          killOrphanContainerOnNode(nm, container);
          continue;
        }

        SchedulerApplication<T> schedulerApp = applications.get(appId);
        if (schedulerApp == null) {
          killOrphanContainerOnNode(nm, container);
          continue;
        }

        LOG.info("Recovering container " + container);
        SchedulerApplicationAttempt schedulerAttempt =
            schedulerApp.getCurrentAppAttempt();

        if (!rmApp.getApplicationSubmissionContext()
            .getKeepContainersAcrossApplicationAttempts()) {
          // Do not recover containers for stopped attempt or previous attempt.
          if (schedulerAttempt.isStopped() || !schedulerAttempt
              .getApplicationAttemptId().equals(
                  container.getContainerId().getApplicationAttemptId())) {
            LOG.info("Skip recovering container " + container
                + " for already stopped attempt.");
            killOrphanContainerOnNode(nm, container);
            continue;
          }
        }

        Queue queue = schedulerApp.getQueue();
        //To make sure we don't face ambiguity, CS queues should be referenced
        //by their full queue names
        String queueName =  queue instanceof CSQueue ?
            ((CSQueue)queue).getQueuePath() : queue.getQueueName();

        // create container
        RMContainer rmContainer = recoverAndCreateContainer(container, nm,
            queueName);

        // recover RMContainer
        rmContainer.handle(
            new RMContainerRecoverEvent(container.getContainerId(), container));

        // recover scheduler node
        SchedulerNode schedulerNode = nodeTracker.getNode(nm.getNodeID());
        schedulerNode.recoverContainer(rmContainer);

        // recover queue: update headroom etc.
        Queue queueToRecover = schedulerAttempt.getQueue();
        queueToRecover.recoverContainer(getClusterResource(), schedulerAttempt,
            rmContainer);

        // recover scheduler attempt
        final boolean recovered = schedulerAttempt.recoverContainer(
            schedulerNode, rmContainer);

        if (recovered && rmContainer.getExecutionType() ==
            ExecutionType.OPPORTUNISTIC) {
          OpportunisticSchedulerMetrics.getMetrics()
              .incrAllocatedOppContainers(1);
        }
        // set master container for the current running AMContainer for this
        // attempt.
        RMAppAttempt appAttempt = rmApp.getCurrentAppAttempt();
        if (appAttempt != null) {
          Container masterContainer = appAttempt.getMasterContainer();

          // Mark current running AMContainer's RMContainer based on the master
          // container ID stored in AppAttempt.
          if (masterContainer != null && masterContainer.getId().equals(
              rmContainer.getContainerId())) {
            ((RMContainerImpl) rmContainer).setAMContainer(true);
          }
        }

        if (schedulerAttempt.getPendingRelease().remove(
            container.getContainerId())) {
          // release the container
          rmContainer.handle(
              new RMContainerFinishedEvent(container.getContainerId(),
                  SchedulerUtils
                      .createAbnormalContainerStatus(container.getContainerId(),
                          SchedulerUtils.RELEASED_CONTAINER),
                  RMContainerEventType.RELEASED));
          LOG.info(container.getContainerId() + " is released by application.");
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Autocorrect container resourceRequests by decrementing the number of newly allocated containers
   * from the current container request. This also updates the newlyAllocatedContainers to be within
   * the limits of the current container resourceRequests.
   * ResourceRequests locality/resourceName is not considered while autocorrecting the container
   * request, hence when there are two types of resourceRequest which is same except for the
   * locality/resourceName, it is counted as same {@link ContainerObjectType} and the container
   * ask and number of newly allocated container is decremented accordingly.
   * For example when a client requests for 4 containers with locality/resourceName
   * as "node1", AMRMClientaugments the resourceRequest into two
   * where R1(numContainer=4,locality=*) and R2(numContainer=4,locality=node1),
   * if Yarn allocated 6 containers previously, it will release 2 containers as well as
   * update the container ask to 0.
   *
   * If there is a client which directly calls Yarn (without AMRMClient) with
   * two where R1(numContainer=4,locality=*) and R2(numContainer=4,locality=node1)
   * the autocorrection may not work as expected. The use case of such client is very rare.
   *
   * <p>
   * This method is called from {@link AbstractYarnScheduler#allocate} method. It is package private
   * to be used within the scheduler package only.
   * @param resourceRequests List of resources to be allocated
   * @param application ApplicationAttempt
   */
  @VisibleForTesting
  protected void autoCorrectContainerAllocation(List<ResourceRequest> resourceRequests,
      SchedulerApplicationAttempt application) {

    // if there is no resourceRequests for containers or no newly allocated container from
    // the previous request there is nothing to do.
    if (!autoCorrectContainerAllocation || resourceRequests.isEmpty() ||
        application.newlyAllocatedContainers.isEmpty()) {
      return;
    }

    // iterate newlyAllocatedContainers and form a mapping of container type
    // and number of its occurrence.
    Map<ContainerObjectType, List<RMContainer>> allocatedContainerMap = new HashMap<>();
    for (RMContainer rmContainer : application.newlyAllocatedContainers) {
      Container container = rmContainer.getContainer();
      ContainerObjectType containerObjectType = new ContainerObjectType(
          container.getAllocationRequestId(), container.getPriority(),
          container.getExecutionType(), container.getResource());
      allocatedContainerMap.computeIfAbsent(containerObjectType,
          k -> new ArrayList<>()).add(rmContainer);
    }

    Map<ContainerObjectType, Integer> extraContainerAllocatedMap = new HashMap<>();
    // iterate through resourceRequests and update the request by
    // decrementing the already allocated containers.
    for (ResourceRequest request : resourceRequests) {
      ContainerObjectType containerObjectType =
          new ContainerObjectType(request.getAllocationRequestId(),
              request.getPriority(), request.getExecutionTypeRequest().getExecutionType(),
              request.getCapability());
      int numContainerAllocated = allocatedContainerMap.getOrDefault(containerObjectType,
          Collections.emptyList()).size();
      if (numContainerAllocated > 0) {
        int numContainerAsk = request.getNumContainers();
        int updatedContainerRequest = numContainerAsk - numContainerAllocated;
        if (updatedContainerRequest < 0) {
          // add an entry to extra allocated map
          extraContainerAllocatedMap.put(containerObjectType, Math.abs(updatedContainerRequest));
          LOG.debug("{} container of the resource type: {} will be released",
              Math.abs(updatedContainerRequest), request);
          // if newlyAllocatedContainer count is more than the current container
          // resourceRequests, reset it to 0.
          updatedContainerRequest = 0;
        }

        // update the request
        LOG.debug("Updating container resourceRequests from {} to {} for the resource type: {}",
            numContainerAsk, updatedContainerRequest, request);
        request.setNumContainers(updatedContainerRequest);
      }
    }

    // Iterate over the entries in extraContainerAllocatedMap
    for (Map.Entry<ContainerObjectType, Integer> entry : extraContainerAllocatedMap.entrySet()) {
      ContainerObjectType containerObjectType = entry.getKey();
      int extraContainers = entry.getValue();

      // Get the list of allocated containers for the current ContainerObjectType
      List<RMContainer> allocatedContainers = allocatedContainerMap.get(containerObjectType);
      if (allocatedContainers != null) {
        for (RMContainer rmContainer : allocatedContainers) {
          if (extraContainers > 0) {
            // Change the state of the container from ALLOCATED to EXPIRED since it is not required.
            LOG.debug("Removing extra container:{}", rmContainer.getContainer());
            completedContainer(rmContainer, SchedulerUtils.createAbnormalContainerStatus(
                rmContainer.getContainerId(), SchedulerUtils.EXPIRED_CONTAINER),
                RMContainerEventType.EXPIRE);
            application.newlyAllocatedContainers.remove(rmContainer);
            extraContainers--;
          }
        }
      }
    }
  }

  private RMContainer recoverAndCreateContainer(NMContainerStatus status,
      RMNode node, String queueName) {
    Container container =
        Container.newInstance(status.getContainerId(), node.getNodeID(),
          node.getHttpAddress(), status.getAllocatedResource(),
          status.getPriority(), null);
    container.setVersion(status.getVersion());
    container.setExecutionType(status.getExecutionType());
    container.setAllocationRequestId(status.getAllocationRequestId());
    container.setAllocationTags(status.getAllocationTags());
    ApplicationAttemptId attemptId =
        container.getId().getApplicationAttemptId();
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), attemptId, node.getNodeID(),
        applications.get(attemptId.getApplicationId()).getUser(), rmContext,
        status.getCreationTime(), status.getNodeLabelExpression());
    ((RMContainerImpl) rmContainer).setQueueName(queueName);
    return rmContainer;
  }

  /**
   * Recover resource request back from RMContainer when a container is
   * preempted before AM pulled the same. If container is pulled by
   * AM, then RMContainer will not have resource request to recover.
   * @param rmContainer rmContainer
   */
  private void recoverResourceRequestForContainer(RMContainer rmContainer) {
    ContainerRequest containerRequest = rmContainer.getContainerRequest();

    // If container state is moved to ACQUIRED, request will be empty.
    if (containerRequest == null) {
      return;
    }

    // when auto correct container allocation is enabled, there can be a case when extra containers
    // go to expired state from allocated state. When such scenario happens do not re-attempt the
    // container request since this is expected.
    if (autoCorrectContainerAllocation &&
        RMContainerState.EXPIRED.equals(rmContainer.getState())) {
      return;
    }

    // Add resource request back to Scheduler ApplicationAttempt.

    // We lookup the application-attempt here again using
    // getCurrentApplicationAttempt() because there is only one app-attempt at
    // any point in the scheduler. But in corner cases, AMs can crash,
    // corresponding containers get killed and recovered to the same-attempt,
    // but because the app-attempt is extinguished right after, the recovered
    // requests don't serve any purpose, but that's okay.
    SchedulerApplicationAttempt schedulerAttempt =
        getCurrentAttemptForContainer(rmContainer.getContainerId());
    if (schedulerAttempt != null) {
      schedulerAttempt.recoverResourceRequestsForContainer(containerRequest);
    }
  }

  protected void createReleaseCache() {
    // Cleanup the cache after nm expire interval.
    releaseCache.schedule(new TimerTask() {
      @Override
      public void run() {
        clearPendingContainerCache();
        LOG.info("Release request cache is cleaned up");
      }
    }, nmExpireInterval);
  }

  @VisibleForTesting
  public void clearPendingContainerCache() {
    for (SchedulerApplication<T> app : applications.values()) {
      T attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        for (ContainerId containerId : attempt.getPendingRelease()) {
          RMAuditLogger.logFailure(app.getUser(),
              AuditConstants.RELEASE_CONTAINER,
              "Unauthorized access or invalid container", "Scheduler",
              "Trying to release container not owned by app "
                  + "or with invalid id.", attempt.getApplicationId(),
              containerId, null);
        }
        attempt.getPendingRelease().clear();
      }
    }
  }

  @VisibleForTesting
  @Private
  // clean up a completed container
  public void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {

    if (rmContainer == null) {
      LOG.info("Container " + containerStatus.getContainerId()
          + " completed with event " + event
          + ", but corresponding RMContainer doesn't exist.");
      return;
    }

    if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
      completedContainerInternal(rmContainer, containerStatus, event);
      completeOustandingUpdatesWhichAreReserved(
          rmContainer, containerStatus, event);
    } else {
      ContainerId containerId = rmContainer.getContainerId();
      // Inform the container
      rmContainer.handle(
          new RMContainerFinishedEvent(containerId, containerStatus, event));
      SchedulerApplicationAttempt schedulerAttempt =
          getCurrentAttemptForContainer(containerId);
      if (schedulerAttempt != null) {
        if (schedulerAttempt.removeRMContainer(containerId)) {
          OpportunisticSchedulerMetrics.getMetrics()
              .incrReleasedOppContainers(1);
        }
      }
      LOG.debug("Completed container: {} in state: {} event:{}",
          rmContainer.getContainerId(), rmContainer.getState(), event);

      SchedulerNode node = getSchedulerNode(rmContainer.getNodeId());
      if (node != null) {
        node.releaseContainer(rmContainer.getContainerId(), false);
      }
    }

    // If the container is getting killed in ACQUIRED state, the requester (AM
    // for regular containers and RM itself for AM container) will not know what
    // happened. Simply add the ResourceRequest back again so that requester
    // doesn't need to do anything conditionally.
    recoverResourceRequestForContainer(rmContainer);
  }

  // Optimization:
  // Check if there are in-flight container updates and complete the
  // associated temp containers. These are removed when the app completes,
  // but removing them when the actual container completes would allow the
  // scheduler to reallocate those resources sooner.
  private void completeOustandingUpdatesWhichAreReserved(
      RMContainer rmContainer, ContainerStatus containerStatus,
      RMContainerEventType event) {
    N schedulerNode = getSchedulerNode(rmContainer.getNodeId());
    if (schedulerNode != null) {
      RMContainer resContainer = schedulerNode.getReservedContainer();
      if (resContainer != null && resContainer.getReservedSchedulerKey() != null) {
        ContainerId containerToUpdate = resContainer
            .getReservedSchedulerKey().getContainerToUpdate();
        if (containerToUpdate != null &&
            containerToUpdate.equals(containerStatus.getContainerId())) {
          completedContainerInternal(resContainer,
              ContainerStatus.newInstance(resContainer.getContainerId(),
                  containerStatus.getState(), containerStatus
                      .getDiagnostics(),
                  containerStatus.getExitStatus()), event);
        }
      }
    }
  }

  // clean up a completed container
  protected abstract void completedContainerInternal(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event);

  protected void releaseContainers(List<ContainerId> containers,
      SchedulerApplicationAttempt attempt) {
    for (ContainerId containerId : containers) {
      RMContainer rmContainer = getRMContainer(containerId);
      if (rmContainer == null) {
        if (System.currentTimeMillis() - ResourceManager.getClusterTimeStamp()
            < nmExpireInterval) {
          LOG.info(containerId + " doesn't exist. Add the container"
              + " to the release request cache as it maybe on recovery.");
          attempt.getPendingRelease().add(containerId);
        } else {
          RMAuditLogger.logFailure(attempt.getUser(),
            AuditConstants.RELEASE_CONTAINER,
            "Unauthorized access or invalid container", "Scheduler",
            "Trying to release container not owned by app or with invalid id.",
            attempt.getApplicationId(), containerId, null);
        }
      }
      completedContainer(rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(containerId,
          SchedulerUtils.RELEASED_CONTAINER), RMContainerEventType.RELEASED);
    }
  }

  @Override
  public N getSchedulerNode(NodeId nodeId) {
    return nodeTracker.getNode(nodeId);
  }

  @Override
  public void moveAllApps(String sourceQueue, String destQueue)
      throws YarnException {
    writeLock.lock();
    try {
      // check if destination queue is a valid leaf queue
      try {
        getQueueInfo(destQueue, false, false);
      } catch (IOException e) {
        LOG.warn(e.toString());
        throw new YarnException(e);
      }

      // generate move events for each pending/running app
      for (ApplicationAttemptId appAttemptId : getAppsFromQueue(sourceQueue)) {
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppManagerEvent(appAttemptId.getApplicationId(),
                destQueue, RMAppManagerEventType.APP_MOVE));
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void killAllAppsInQueue(String queueName)
      throws YarnException {
    writeLock.lock();
    try {
      // generate kill events for each pending/running app
      for (ApplicationAttemptId app : getAppsFromQueue(queueName)) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(app.getApplicationId(), RMAppEventType.KILL,
                "Application killed due to expiry of reservation queue "
                    + queueName + "."));
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Process resource update on a node.
   *
   * @param nm RMNode.
   * @param resourceOption resourceOption.
   */
  public void updateNodeResource(RMNode nm,
      ResourceOption resourceOption) {
    writeLock.lock();
    try {
      SchedulerNode node = getSchedulerNode(nm.getNodeID());
      if (node == null) {
        LOG.info("Node: " + nm.getNodeID() + " has already been taken out of " +
            "scheduling. Skip updating its resource");
        return;
      }
      Resource newResource = resourceOption.getResource();
      final int timeout = resourceOption.getOverCommitTimeout();
      Resource oldResource = node.getTotalResource();
      if (!oldResource.equals(newResource)) {
        // Notify NodeLabelsManager about this change
        rmContext.getNodeLabelManager().updateNodeResource(nm.getNodeID(),
            newResource);

        // Log resource change
        LOG.info("Update resource on node: {} from: {}, to: {} in {} ms",
            node.getNodeName(), oldResource, newResource, timeout);

        nodeTracker.removeNode(nm.getNodeID());

        // update resource to node
        node.updateTotalResource(newResource);
        node.setOvercommitTimeOut(timeout);
        signalContainersIfOvercommitted(node, timeout == 0);

        nodeTracker.addNode((N) node);
      } else{
        // Log resource change
        LOG.warn("Update resource on node: " + node.getNodeName()
            + " with the same resource: " + newResource);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes() {
    return EnumSet.of(SchedulerResourceTypes.MEMORY);
  }

  @Override
  public Set<String> getPlanQueues() throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support reservations");
  }

  /**
   * By default placement constraint is disabled. Schedulers which support
   * placement constraint can override this value.
   * @return enabled or not
   */
  public boolean placementConstraintEnabled() {
    return false;
  }

  protected void refreshMaximumAllocation(Resource newMaxAlloc) {
    nodeTracker.setConfiguredMaxAllocation(newMaxAlloc);
  }

  @Override
  public List<ResourceRequest> getPendingResourceRequestsForAttempt(
      ApplicationAttemptId attemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(attemptId);
    if (attempt != null) {
      return attempt.getAppSchedulingInfo().getAllResourceRequests();
    }
    return null;
  }

  @Override
  public List<SchedulingRequest> getPendingSchedulingRequestsForAttempt(
      ApplicationAttemptId attemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(attemptId);
    if (attempt != null) {
      return attempt.getAppSchedulingInfo().getAllSchedulingRequests();
    }
    return null;
  }

  @Override
  public Priority checkAndGetApplicationPriority(
          Priority priorityRequestedByApp, UserGroupInformation user,
          String queuePath, ApplicationId applicationId) throws YarnException {
    // Dummy Implementation till Application Priority changes are done in
    // specific scheduler.
    return Priority.newInstance(0);
  }

  @Override
  public Priority updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId, SettableFuture<Object> future,
      UserGroupInformation user)
      throws YarnException {
    // Dummy Implementation till Application Priority changes are done in
    // specific scheduler.
    return Priority.newInstance(0);
  }

  @Override
  public Priority getMaxClusterLevelAppPriority() {
    return maxClusterLevelAppPriority;
  }

  private Priority getMaxPriorityFromConf(Configuration conf) {
    return Priority.newInstance(conf.getInt(
        YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        YarnConfiguration.DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY));
  }

  @Override
  public void setClusterMaxPriority(Configuration conf)
      throws YarnException {
    try {
      maxClusterLevelAppPriority = getMaxPriorityFromConf(conf);
    } catch (NumberFormatException e) {
      throw new YarnException(e);
    }
    LOG.info("Updated the cluste max priority to maxClusterLevelAppPriority = "
        + maxClusterLevelAppPriority);
  }

  /**
   * Sanity check increase/decrease request, and return
   * SchedulerContainerResourceChangeRequest according to given
   * UpdateContainerRequest.
   *
   * <pre>
   * - Returns non-null value means validation succeeded
   * - Throw exception when any other error happens
   * </pre>
   */
  private SchedContainerChangeRequest createSchedContainerChangeRequest(
      UpdateContainerRequest request, boolean increase)
      throws YarnException {
    ContainerId containerId = request.getContainerId();
    RMContainer rmContainer = getRMContainer(containerId);
    if (null == rmContainer) {
      String msg =
          "Failed to get rmContainer for "
              + (increase ? "increase" : "decrease")
              + " request, with container-id=" + containerId;
      throw new InvalidResourceRequestException(msg);
    }
    SchedulerNode schedulerNode =
        getSchedulerNode(rmContainer.getAllocatedNode());
    return new SchedContainerChangeRequest(
        this.rmContext, schedulerNode, rmContainer, request.getCapability());
  }

  protected List<SchedContainerChangeRequest>
      createSchedContainerChangeRequests(
          List<UpdateContainerRequest> changeRequests,
          boolean increase) {
    List<SchedContainerChangeRequest> schedulerChangeRequests =
        new ArrayList<SchedContainerChangeRequest>();
    for (UpdateContainerRequest r : changeRequests) {
      SchedContainerChangeRequest sr = null;
      try {
        sr = createSchedContainerChangeRequest(r, increase);
      } catch (YarnException e) {
        LOG.warn("Error happens when checking increase request, Ignoring.."
            + " exception=", e);
        continue;
      }
      schedulerChangeRequests.add(sr);
    }
    return schedulerChangeRequests;
  }

  public ActivitiesManager getActivitiesManager() {
    return this.activitiesManager;
  }

  public Clock getClock() {
    return clock;
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @Lock(Lock.NoLock.class)
  public SchedulerNode getNode(NodeId nodeId) {
    return nodeTracker.getNode(nodeId);
  }

  /**
   * Get lists of new containers from NodeManager and process them.
   * @param nm The RMNode corresponding to the NodeManager
   * @param schedulerNode schedulerNode
   * @return list of completed containers
   */
  private List<ContainerStatus> updateNewContainerInfo(RMNode nm,
      SchedulerNode schedulerNode) {
    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers =
        new ArrayList<>();
    List<ContainerStatus> completedContainers =
        new ArrayList<>();
    List<Map.Entry<ApplicationId, ContainerStatus>> updateExistContainers =
        new ArrayList<>();

    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers
          .addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
      updateExistContainers.addAll(containerInfo.getUpdateContainers());
    }

    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(),
          schedulerNode);
    }

    // Processing the newly increased containers
    List<Container> newlyIncreasedContainers =
        nm.pullNewlyIncreasedContainers();
    for (Container container : newlyIncreasedContainers) {
      containerIncreasedOnNode(container.getId(), schedulerNode, container);
    }

    // Processing the update exist containers
    for (Map.Entry<ApplicationId, ContainerStatus> c : updateExistContainers) {
      SchedulerApplication<T> app = applications.get(c.getKey());
      ContainerId containerId = c.getValue().getContainerId();
      if (app == null || app.getCurrentAppAttempt() == null) {
        continue;
      }
      RMContainer rmContainer
          = app.getCurrentAppAttempt().getRMContainer(containerId);
      if (rmContainer == null) {
        continue;
      }
      // exposed ports are already set for the container, skip
      if (rmContainer.getExposedPorts() != null &&
          rmContainer.getExposedPorts().size() > 0) {
        continue;
      }

      String strExposedPorts = c.getValue().getExposedPorts();
      if (null != strExposedPorts && !strExposedPorts.isEmpty()) {
        Gson gson = new Gson();
        Map<String, List<Map<String, String>>> exposedPorts =
            gson.fromJson(strExposedPorts,
            new TypeToken<Map<String, List<Map<String, String>>>>()
                {}.getType());
        LOG.info("update exist container " + containerId.getContainerId()
            + ", strExposedPorts = " + strExposedPorts);
        rmContainer.setExposedPorts(exposedPorts);
      }
    }

    return completedContainers;
  }

  /**
   * Process completed container list.
   * @param completedContainers Extracted list of completed containers
   * @param releasedResources Reference resource object for completed containers
   * @param nodeId NodeId corresponding to the NodeManager
   * @param schedulerNode schedulerNode
   * @return The total number of released containers
   */
  private int updateCompletedContainers(List<ContainerStatus> completedContainers,
      Resource releasedResources, NodeId nodeId, SchedulerNode schedulerNode) {
    int releasedContainers = 0;
    List<ContainerId> untrackedContainerIdList = new ArrayList<ContainerId>();
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: {}", containerId);
      RMContainer container = getRMContainer(containerId);
      completedContainer(container,
          completedContainer, RMContainerEventType.FINISHED);
      if (schedulerNode != null) {
        schedulerNode.releaseContainer(containerId, true);
      }

      if (container != null) {
        releasedContainers++;
        Resource ars = container.getAllocatedResource();
        if (ars != null) {
          Resources.addTo(releasedResources, ars);
        }
        Resource rrs = container.getReservedResource();
        if (rrs != null) {
          Resources.addTo(releasedResources, rrs);
        }
      } else {
        // Add containers which are untracked by RM.
        untrackedContainerIdList.add(containerId);
      }
    }

    // Acknowledge NM to remove RM-untracked-containers from NM context.
    if (!untrackedContainerIdList.isEmpty()) {
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMNodeFinishedContainersPulledByAMEvent(nodeId,
              untrackedContainerIdList));
    }

    return releasedContainers;
  }

  /**
   * Update schedulerHealth information.
   * @param releasedResources Reference resource object for completed containers
   * @param releasedContainers Count of released containers
   */
  protected void updateSchedulerHealthInformation(Resource releasedResources,
      int releasedContainers) {

    schedulerHealth.updateSchedulerReleaseDetails(getLastNodeUpdateTime(),
        releasedResources);
    schedulerHealth.updateSchedulerReleaseCounts(releasedContainers);
  }

  /**
   * Update container and utilization information on the NodeManager.
   * @param nm The NodeManager to update
   * @param schedulerNode schedulerNode
   */
  protected void updateNodeResourceUtilization(RMNode nm,
      SchedulerNode schedulerNode) {
    // Updating node resource utilization
    schedulerNode.setAggregatedContainersUtilization(
        nm.getAggregatedContainersUtilization());
    schedulerNode.setNodeUtilization(nm.getNodeUtilization());
  }

  /**
   * Process a heartbeat update from a node.
   * @param nm The RMNode corresponding to the NodeManager
   */
  protected void nodeUpdate(RMNode nm) {
    LOG.debug("nodeUpdate: {} cluster capacity: {}",
        nm, getClusterResource());

    // Process new container information
    // NOTICE: it is possible to not find the NodeID as a node can be
    // decommissioned at the same time. Skip updates if node is null.
    SchedulerNode schedulerNode = getNode(nm.getNodeID());
    List<ContainerStatus> completedContainers = updateNewContainerInfo(nm,
        schedulerNode);

    // Notify Scheduler Node updated.
    if (schedulerNode != null) {
      schedulerNode.notifyNodeUpdate();
    }

    // Process completed containers
    Resource releasedResources = Resource.newInstance(0, 0);
    int releasedContainers = updateCompletedContainers(completedContainers,
        releasedResources, nm.getNodeID(), schedulerNode);

    // If the node is decommissioning, send an update to have the total
    // resource equal to the used resource, so no available resource to
    // schedule.
    if (nm.getState() == NodeState.DECOMMISSIONING && schedulerNode != null
        && schedulerNode.getTotalResource().compareTo(
            schedulerNode.getAllocatedResource()) != 0) {
      this.rmContext
          .getDispatcher()
          .getEventHandler()
          .handle(
              new RMNodeResourceUpdateEvent(nm.getNodeID(), ResourceOption
                  .newInstance(schedulerNode.getAllocatedResource(), 0)));
    }

    updateSchedulerHealthInformation(releasedResources, releasedContainers);
    if (schedulerNode != null) {
      updateNodeResourceUtilization(nm, schedulerNode);
    }

    if (schedulerNode != null) {
      signalContainersIfOvercommitted(schedulerNode, true);
    }

    // Now node data structures are up-to-date and ready for scheduling.
    if(LOG.isDebugEnabled()) {
      LOG.debug(
          "Node being looked for scheduling " + nm + " availableResource: " +
              (schedulerNode == null ? "unknown (decommissioned)" :
                  schedulerNode.getUnallocatedResource()));
    }
  }

  /**
   * Check if the node is overcommitted and needs to remove containers. If
   * it is overcommitted, it will kill or preempt (notify the AM to stop them)
   * containers. It also takes into account the overcommit timeout. It only
   * notifies the application to preempt a container if the timeout hasn't
   * passed. If the timeout has passed, it tries to kill the containers. If
   * there is no timeout, it doesn't do anything and just prevents new
   * allocations.
   *
   * This action is taken when the change of resources happens (to preempt
   * containers or killing them if specified) or when the node heart beats
   * (for killing only).
   *
   * @param schedulerNode The node to check whether is overcommitted.
   * @param kill If the container should be killed or just notify the AM.
   */
  private void signalContainersIfOvercommitted(
      SchedulerNode schedulerNode, boolean kill) {

    // If there is no time out, we don't do anything
    if (!schedulerNode.isOvercommitTimeOutSet()) {
      return;
    }

    SchedulerEventType eventType =
        SchedulerEventType.MARK_CONTAINER_FOR_PREEMPTION;
    if (kill) {
      eventType = SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE;

      // If it hasn't timed out yet, don't kill
      if (!schedulerNode.isOvercommitTimedOut()) {
        return;
      }
    }

    // Check if the node is overcommitted (negative resources)
    ResourceCalculator rc = getResourceCalculator();
    Resource unallocated = Resource.newInstance(
        schedulerNode.getUnallocatedResource());
    if (Resources.fitsIn(rc, ZERO_RESOURCE, unallocated)) {
      return;
    }

    LOG.info("{} is overcommitted ({}), preempt/kill containers",
        schedulerNode.getNodeID(), unallocated);
    for (RMContainer container : schedulerNode.getContainersToKill()) {
      LOG.info("Send {} to {} to free up {}", eventType,
          container.getContainerId(), container.getAllocatedResource());
      ApplicationAttemptId appId = container.getApplicationAttemptId();
      ContainerPreemptEvent event =
          new ContainerPreemptEvent(appId, container, eventType);
      this.rmContext.getDispatcher().getEventHandler().handle(event);
      Resources.addTo(unallocated, container.getAllocatedResource());

      if (Resources.fitsIn(rc, ZERO_RESOURCE, unallocated)) {
        LOG.debug("Enough unallocated resources {}", unallocated);
        break;
      }
    }
  }

  @Override
  public Resource getNormalizedResource(Resource requestedResource,
                                        Resource maxResourceCapability) {
    return SchedulerUtils.getNormalizedResource(requestedResource,
        getResourceCalculator(),
        getMinimumResourceCapability(),
        maxResourceCapability,
        getMinimumResourceCapability());
  }

  /**
   * Normalize a list of resource requests.
   *
   * @param asks resource requests
   */
  protected void normalizeResourceRequests(List<ResourceRequest> asks) {
    normalizeResourceRequests(asks, null);
  }

  /**
   * Normalize a list of resource requests
   * using queue maximum resource allocations.
   * @param asks resource requests
   * @param queueName queue Name.
   */
  protected void normalizeResourceRequests(List<ResourceRequest> asks,
      String queueName) {
    Resource maxAllocation = getMaximumResourceCapability(queueName);
    for (ResourceRequest ask : asks) {
      ask.setCapability(
          getNormalizedResource(ask.getCapability(), maxAllocation));
    }
  }

  protected void handleContainerUpdates(
      SchedulerApplicationAttempt appAttempt, ContainerUpdates updates) {
    List<UpdateContainerRequest> promotionRequests =
        updates.getPromotionRequests();
    if (promotionRequests != null && !promotionRequests.isEmpty()) {
      LOG.info("Promotion Update requests : " + promotionRequests);
      // Promotion is technically an increase request from
      // 0 resources to target resources.
      handleIncreaseRequests(appAttempt, promotionRequests);
    }
    List<UpdateContainerRequest> increaseRequests =
        updates.getIncreaseRequests();
    if (increaseRequests != null && !increaseRequests.isEmpty()) {
      LOG.info("Resource increase requests : " + increaseRequests);
      handleIncreaseRequests(appAttempt, increaseRequests);
    }
    List<UpdateContainerRequest> demotionRequests =
        updates.getDemotionRequests();
    if (demotionRequests != null && !demotionRequests.isEmpty()) {
      LOG.info("Demotion Update requests : " + demotionRequests);
      // Demotion is technically a decrease request from initial
      // to 0 resources
      handleDecreaseRequests(appAttempt, demotionRequests);
    }
    List<UpdateContainerRequest> decreaseRequests =
        updates.getDecreaseRequests();
    if (decreaseRequests != null && !decreaseRequests.isEmpty()) {
      LOG.info("Resource decrease requests : " + decreaseRequests);
      handleDecreaseRequests(appAttempt, decreaseRequests);
    }
  }

  private void handleIncreaseRequests(
      SchedulerApplicationAttempt applicationAttempt,
      List<UpdateContainerRequest> updateContainerRequests) {
    for (UpdateContainerRequest uReq : updateContainerRequests) {
      RMContainer rmContainer =
          rmContext.getScheduler().getRMContainer(uReq.getContainerId());
      // Check if this is a container update
      // And not in the middle of a Demotion
      if (rmContainer != null) {
        // Check if this is an executionType change request
        // If so, fix the rr to make it look like a normal rr
        // with relaxLocality=false and numContainers=1
        SchedulerNode schedulerNode = rmContext.getScheduler()
            .getSchedulerNode(rmContainer.getContainer().getNodeId());

        // Add only if no outstanding promote requests exist.
        if (!applicationAttempt.getUpdateContext()
            .checkAndAddToOutstandingIncreases(
                rmContainer, schedulerNode, uReq)) {
          applicationAttempt.addToUpdateContainerErrors(
              UpdateContainerError.newInstance(
              RMServerUtils.UPDATE_OUTSTANDING_ERROR, uReq));
        }
      } else {
        LOG.warn("Cannot promote non-existent (or completed) Container ["
            + uReq.getContainerId() + "]");
      }
    }
  }

  private void handleDecreaseRequests(SchedulerApplicationAttempt appAttempt,
      List<UpdateContainerRequest> demotionRequests) {
    OpportunisticContainerContext oppCntxt =
        appAttempt.getOpportunisticContainerContext();
    for (UpdateContainerRequest uReq : demotionRequests) {
      RMContainer rmContainer =
          rmContext.getScheduler().getRMContainer(uReq.getContainerId());
      if (rmContainer != null) {
        SchedulerNode schedulerNode = rmContext.getScheduler()
            .getSchedulerNode(rmContainer.getContainer().getNodeId());
        if (appAttempt.getUpdateContext()
            .checkAndAddToOutstandingDecreases(uReq, schedulerNode,
                rmContainer.getContainer())) {
          if (ContainerUpdateType.DEMOTE_EXECUTION_TYPE ==
              uReq.getContainerUpdateType()) {
            RMContainer demotedRMContainer =
                createDemotedRMContainer(appAttempt, oppCntxt, rmContainer);
            if (demotedRMContainer != null) {
              OpportunisticSchedulerMetrics.getMetrics()
                  .incrAllocatedOppContainers(1);
              appAttempt.addToNewlyDemotedContainers(
                      uReq.getContainerId(), demotedRMContainer);
            }
          } else {
            RMContainer demotedRMContainer = createDecreasedRMContainer(
                appAttempt, uReq, rmContainer);
            appAttempt.addToNewlyDecreasedContainers(
                uReq.getContainerId(), demotedRMContainer);
          }
        } else {
          appAttempt.addToUpdateContainerErrors(
              UpdateContainerError.newInstance(
              RMServerUtils.UPDATE_OUTSTANDING_ERROR, uReq));
        }
      } else {
        LOG.warn("Cannot demote/decrease non-existent (or completed) " +
            "Container [" + uReq.getContainerId() + "]");
      }
    }
  }

  private RMContainer createDecreasedRMContainer(
      SchedulerApplicationAttempt appAttempt, UpdateContainerRequest uReq,
      RMContainer rmContainer) {
    SchedulerRequestKey sk =
        SchedulerRequestKey.extractFrom(rmContainer.getContainer());
    Container decreasedContainer = BuilderUtils.newContainer(
        ContainerId.newContainerId(appAttempt.getApplicationAttemptId(),
            appAttempt.getNewContainerId()),
        rmContainer.getContainer().getNodeId(),
        rmContainer.getContainer().getNodeHttpAddress(),
        Resources.none(),
        sk.getPriority(), null, rmContainer.getExecutionType(),
        sk.getAllocationRequestId());
    decreasedContainer.setVersion(rmContainer.getContainer().getVersion());
    RMContainer newRmContainer = new RMContainerImpl(decreasedContainer,
        sk, appAttempt.getApplicationAttemptId(),
        decreasedContainer.getNodeId(), appAttempt.getUser(), rmContext,
        rmContainer.isRemotelyAllocated());
    appAttempt.addRMContainer(decreasedContainer.getId(), rmContainer);
    ((AbstractYarnScheduler) rmContext.getScheduler()).getNode(
        decreasedContainer.getNodeId()).allocateContainer(newRmContainer);
    return newRmContainer;
  }

  private RMContainer createDemotedRMContainer(
      SchedulerApplicationAttempt appAttempt,
      OpportunisticContainerContext oppCntxt,
      RMContainer rmContainer) {
    SchedulerRequestKey sk =
        SchedulerRequestKey.extractFrom(rmContainer.getContainer());
    Container demotedContainer = BuilderUtils.newContainer(
        ContainerId.newContainerId(appAttempt.getApplicationAttemptId(),
            oppCntxt.getContainerIdGenerator().generateContainerId()),
        rmContainer.getContainer().getNodeId(),
        rmContainer.getContainer().getNodeHttpAddress(),
        rmContainer.getContainer().getResource(),
        sk.getPriority(), null, ExecutionType.OPPORTUNISTIC,
        sk.getAllocationRequestId());
    demotedContainer.setVersion(rmContainer.getContainer().getVersion());
    return SchedulerUtils.createOpportunisticRmContainer(
        rmContext, demotedContainer, false);
  }

  /**
   * Rollback container update after expiry.
   * @param containerId ContainerId.
   */
  protected void rollbackContainerUpdate(
      ContainerId containerId) {
    RMContainer rmContainer = getRMContainer(containerId);
    if (rmContainer == null) {
      LOG.info("Cannot rollback resource for container " + containerId
          + ". The container does not exist.");
      return;
    }
    T app = getCurrentAttemptForContainer(containerId);
    if (getCurrentAttemptForContainer(containerId) == null) {
      LOG.info("Cannot rollback resource for container " + containerId
          + ". The application that the container "
          + "belongs to does not exist.");
      return;
    }

    if (Resources.fitsIn(rmContainer.getLastConfirmedResource(),
        rmContainer.getContainer().getResource())) {
      LOG.info("Roll back resource for container " + containerId);
      handleDecreaseRequests(app, Arrays.asList(
          UpdateContainerRequest.newInstance(
              rmContainer.getContainer().getVersion(),
              rmContainer.getContainerId(),
              ContainerUpdateType.DECREASE_RESOURCE,
              rmContainer.getLastConfirmedResource(), null)));
    }
  }

  @Override
  public List<NodeId> getNodeIds(String resourceName) {
    return nodeTracker.getNodeIdsByResourceName(resourceName);
  }

  /**
   * To be used to release a container via a Scheduler Event rather than
   * in the same thread.
   * @param container Container.
   */
  public void asyncContainerRelease(RMContainer container) {
    this.rmContext.getDispatcher().getEventHandler().handle(
        new ReleaseContainerEvent(container));
  }

  /*
   * Get a Resource object with for the minimum allocation possible.
   *
   * @return a Resource object with the minimum allocation for the scheduler
   */
  public Resource getMinimumAllocation() {
    Resource ret = ResourceUtils.getResourceTypesMinimumAllocation();
    LOG.info("Minimum allocation = " + ret);
    return ret;
  }

  /**
   * Get a Resource object with for the maximum allocation possible.
   *
   * @return a Resource object with the maximum allocation for the scheduler
   */

  public Resource getMaximumAllocation() {
    Resource ret = ResourceUtils.getResourceTypesMaximumAllocation();
    LOG.info("Maximum allocation = " + ret);
    return ret;
  }

  @Override
  public long checkAndGetApplicationLifetime(String queueName, long lifetime,
                                             RMAppImpl app) {
    // Lifetime is the application lifetime by default.
    return lifetime;
  }

  @Override
  public long getMaximumApplicationLifetime(String queueName) {
    return -1;
  }

  /**
   * Kill a RMContainer. This is meant to be called in tests only to simulate
   * AM container failures.
   * @param container the container to kill
   */
  @VisibleForTesting
  public abstract void killContainer(RMContainer container);

  /**
   * Update internal state of the scheduler.  This can be useful for scheduler
   * implementations that maintain some state that needs to be periodically
   * updated; for example, metrics or queue resources.  It will be called by the
   * {@link UpdateThread} every {@link #updateInterval}.  By default, it will
   * not run; subclasses should set {@link #updateInterval} to a
   * positive value during {@link #serviceInit(Configuration)} if they want to
   * enable the thread.
   */
  @VisibleForTesting
  public void update() {
    // do nothing by default
  }

  /**
   * Thread which calls {@link #update()} every
   * <code>updateInterval</code> milliseconds.
   */
  private class UpdateThread extends Thread {
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          synchronized (updateThreadMonitor) {
            updateThreadMonitor.wait(updateInterval);
          }
          update();
        } catch (InterruptedException ie) {
          LOG.warn("Scheduler UpdateThread interrupted. Exiting.");
          return;
        } catch (Exception e) {
          LOG.error("Exception in scheduler UpdateThread", e);
        }
      }
    }
  }

  /**
   * Allows {@link UpdateThread} to start processing without waiting till
   * {@link #updateInterval}.
   */
  protected void triggerUpdate() {
    synchronized (updateThreadMonitor) {
      updateThreadMonitor.notify();
    }
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws IOException {
    try {
      LOG.info("Reinitializing SchedulingMonitorManager ...");
      schedulingMonitorManager.reinitialize(rmContext, conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * Default implementation. Always returns false.
   * @param appAttempt ApplicationAttempt.
   * @param schedulingRequest SchedulingRequest.
   * @param schedulerNode SchedulerNode.
   * @return Success or not.
   */
  @Override
  public boolean attemptAllocationOnNode(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode) {
    return false;
  }

  @Override
  public void resetSchedulerMetrics() {
    // reset scheduler metrics
  }

  /**
   * Gets the apps from a given queue.
   *
   * Mechanics:
   * 1. Get all {@link ApplicationAttemptId}s in the given queue by
   * {@link #getAppsInQueue(String)} method.
   * 2. Always need to check validity for the given queue by the returned
   * values.
   *
   * @param queueName queue name
   * @return a collection of app attempt ids in the given queue, it maybe empty.
   * @throws YarnException if {@link #getAppsInQueue(String)} return null, will
   * throw this exception.
   */
  private List<ApplicationAttemptId> getAppsFromQueue(String queueName)
      throws YarnException {
    List<ApplicationAttemptId> apps = getAppsInQueue(queueName);
    if (apps == null) {
      throw new YarnException("The specified queue: " + queueName
          + " doesn't exist");
    }
    return apps;
  }

  /**
   * ContainerObjectType is a container object with the following properties.
   * Namely allocationId, priority, executionType and resourceType.
   */
  protected class ContainerObjectType extends Object {
    private final long allocationId;
    private final Priority priority;
    private final ExecutionType executionType;
    private final Resource resource;

    public ContainerObjectType(long allocationId, Priority priority,
        ExecutionType executionType, Resource resource) {
      this.allocationId = allocationId;
      this.priority = priority;
      this.executionType = executionType;
      this.resource = resource;
    }

    public long getAllocationId() {
      return allocationId;
    }

    public Priority getPriority() {
      return priority;
    }

    public ExecutionType getExecutionType() {
      return executionType;
    }

    public Resource getResource() {
      return resource;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
          .append(allocationId)
          .append(priority)
          .append(executionType)
          .append(resource)
          .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj.getClass() != this.getClass()) {
        return false;
      }

      ContainerObjectType other = (ContainerObjectType) obj;
      return new EqualsBuilder()
          .append(allocationId, other.getAllocationId())
          .append(priority, other.getPriority())
          .append(executionType, other.getExecutionType())
          .append(resource, other.getResource())
          .isEquals();
    }

    @Override
    public String toString() {
      return "{ContainerObjectType: "
          + ", Priority: " + getPriority()
          + ", Allocation Id: " + getAllocationId()
          + ", Execution Type: " + getExecutionType()
          + ", Resource: " + getResource()
          + "}";
    }
  }
}
