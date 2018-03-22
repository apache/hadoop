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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerUpdatesAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeUpdateContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ConcurrentHashMultiset;

/**
 * Represents an application attempt from the viewpoint of the scheduler.
 * Each running app attempt in the RM corresponds to one instance
 * of this class.
 */
@Private
@Unstable
public class SchedulerApplicationAttempt implements SchedulableEntity {
  
  private static final Log LOG = LogFactory
    .getLog(SchedulerApplicationAttempt.class);

  private FastDateFormat fdf =
      FastDateFormat.getInstance("EEE MMM dd HH:mm:ss Z yyyy");

  private static final long MEM_AGGREGATE_ALLOCATION_CACHE_MSECS = 3000;
  protected long lastMemoryAggregateAllocationUpdateTime = 0;
  private Map<String, Long> lastResourceSecondsMap = new HashMap<>();
  protected final AppSchedulingInfo appSchedulingInfo;
  protected ApplicationAttemptId attemptId;
  protected Map<ContainerId, RMContainer> liveContainers =
      new ConcurrentHashMap<>();
  protected final Map<SchedulerRequestKey, Map<NodeId, RMContainer>>
      reservedContainers = new HashMap<>();

  private final ConcurrentHashMultiset<SchedulerRequestKey> reReservations =
      ConcurrentHashMultiset.create();
  
  private volatile Resource resourceLimit = Resource.newInstance(0, 0);
  private boolean unmanagedAM = true;
  private boolean amRunning = false;
  private LogAggregationContext logAggregationContext;

  private volatile Priority appPriority = null;
  private boolean isAttemptRecovering;

  protected ResourceUsage attemptResourceUsage = new ResourceUsage();
  /** Resource usage of opportunistic containers. */
  protected ResourceUsage attemptOpportunisticResourceUsage =
      new ResourceUsage();
  /** Scheduled by a remote scheduler. */
  protected ResourceUsage attemptResourceUsageAllocatedRemotely =
      new ResourceUsage();
  private AtomicLong firstAllocationRequestSentTime = new AtomicLong(0);
  private AtomicLong firstContainerAllocatedTime = new AtomicLong(0);

  protected List<RMContainer> newlyAllocatedContainers = new ArrayList<>();
  protected List<RMContainer> tempContainerToKill = new ArrayList<>();
  protected Map<ContainerId, RMContainer> newlyPromotedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyDemotedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyDecreasedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyIncreasedContainers = new HashMap<>();
  protected Set<NMToken> updatedNMTokens = new HashSet<>();

  protected List<UpdateContainerError> updateContainerErrors = new ArrayList<>();

  //Keeps track of recovered containers from previous attempt which haven't
  //been reported to the AM.
  private List<Container> recoveredPreviousAttemptContainers =
      new ArrayList<>();

  // This pendingRelease is used in work-preserving recovery scenario to keep
  // track of the AM's outstanding release requests. RM on recovery could
  // receive the release request form AM before it receives the container status
  // from NM for recovery. In this case, the to-be-recovered containers reported
  // by NM should not be recovered.
  private Set<ContainerId> pendingRelease = null;

  private OpportunisticContainerContext oppContainerContext;

  /**
   * Count how many times the application has been given an opportunity to
   * schedule a task at each priority. Each time the scheduler asks the
   * application for a task at this priority, it is incremented, and each time
   * the application successfully schedules a task (at rack or node local), it
   * is reset to 0.
   */
  private ConcurrentHashMultiset<SchedulerRequestKey> schedulingOpportunities =
      ConcurrentHashMultiset.create();

  /**
   * Count how many times the application has been given an opportunity to
   * schedule a non-partitioned resource request at each priority. Each time the
   * scheduler asks the application for a task at this priority, it is
   * incremented, and each time the application successfully schedules a task,
   * it is reset to 0 when schedule any task at corresponding priority.
   */
  private ConcurrentHashMultiset<SchedulerRequestKey>
      missedNonPartitionedReqSchedulingOpportunity =
      ConcurrentHashMultiset.create();
  
  // Time of the last container scheduled at the current allowed level
  protected Map<SchedulerRequestKey, Long> lastScheduledContainer =
      new ConcurrentHashMap<>();

  protected volatile Queue queue;
  protected volatile boolean isStopped = false;

  protected String appAMNodePartitionName = CommonNodeLabelsManager.NO_LABEL;

  protected final RMContext rmContext;

  private RMAppAttempt appAttempt;

  protected ReentrantReadWriteLock.ReadLock readLock;
  protected ReentrantReadWriteLock.WriteLock writeLock;

  private Map<String, String> applicationSchedulingEnvs = new HashMap<>();

  // Not confirmed allocation resource, will be used to avoid too many proposal
  // rejected because of duplicated allocation
  private AtomicLong unconfirmedAllocatedMem = new AtomicLong();
  private AtomicInteger unconfirmedAllocatedVcores = new AtomicInteger();

  public SchedulerApplicationAttempt(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext) {
    Preconditions.checkNotNull(rmContext, "RMContext should not be null");
    this.rmContext = rmContext;
    this.queue = queue;
    this.pendingRelease = Collections.newSetFromMap(
        new ConcurrentHashMap<ContainerId, Boolean>());
    this.attemptId = applicationAttemptId;
    if (rmContext.getRMApps() != null &&
        rmContext.getRMApps()
            .containsKey(applicationAttemptId.getApplicationId())) {
      RMApp rmApp = rmContext.getRMApps().get(applicationAttemptId.getApplicationId());
      ApplicationSubmissionContext appSubmissionContext =
          rmApp
              .getApplicationSubmissionContext();
      appAttempt = rmApp.getCurrentAppAttempt();
      if (appSubmissionContext != null) {
        unmanagedAM = appSubmissionContext.getUnmanagedAM();
        this.logAggregationContext =
            appSubmissionContext.getLogAggregationContext();
      }
      applicationSchedulingEnvs = rmApp.getApplicationSchedulingEnvs();
    }

    this.appSchedulingInfo = new AppSchedulingInfo(applicationAttemptId, user,
        queue, abstractUsersManager, rmContext.getEpoch(), attemptResourceUsage,
        applicationSchedulingEnvs, rmContext);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public void setOpportunisticContainerContext(
      OpportunisticContainerContext oppContext) {
    this.oppContainerContext = oppContext;
  }

  public OpportunisticContainerContext
      getOpportunisticContainerContext() {
    return this.oppContainerContext;
  }

  /**
   * Get the live containers of the application.
   * @return live containers of the application
   */
  public Collection<RMContainer> getLiveContainers() {
    try {
      readLock.lock();
      return new ArrayList<>(liveContainers.values());
    } finally {
      readLock.unlock();
    }
  }

  public AppSchedulingInfo getAppSchedulingInfo() {
    return this.appSchedulingInfo;
  }

  public ContainerUpdateContext getUpdateContext() {
    return this.appSchedulingInfo.getUpdateContext();
  }

  /**
   * Is this application pending?
   * @return true if it is else false.
   */
  public boolean isPending() {
    return appSchedulingInfo.isPending();
  }
  
  /**
   * Get {@link ApplicationAttemptId} of the application master.
   * @return <code>ApplicationAttemptId</code> of the application master
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return appSchedulingInfo.getApplicationAttemptId();
  }
  
  public ApplicationId getApplicationId() {
    return appSchedulingInfo.getApplicationId();
  }
  
  public String getUser() {
    return appSchedulingInfo.getUser();
  }

  public Set<ContainerId> getPendingRelease() {
    return this.pendingRelease;
  }

  public long getNewContainerId() {
    return appSchedulingInfo.getNewContainerId();
  }

  public Collection<SchedulerRequestKey> getSchedulerKeys() {
    return appSchedulingInfo.getSchedulerKeys();
  }

  public PendingAsk getPendingAsk(
      SchedulerRequestKey schedulerKey, String resourceName) {
    try {
      readLock.lock();
      return appSchedulingInfo.getPendingAsk(schedulerKey, resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey) {
    return getOutstandingAsksCount(schedulerKey, ResourceRequest.ANY);
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey,
      String resourceName) {
    try {
      readLock.lock();
      AppPlacementAllocator ap = appSchedulingInfo.getAppPlacementAllocator(
          schedulerKey);
      return ap == null ? 0 : ap.getOutstandingAsksCount(resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public String getQueueName() {
    return appSchedulingInfo.getQueueName();
  }
  
  public Resource getAMResource() {
    return attemptResourceUsage.getAMUsed();
  }

  public Resource getAMResource(String label) {
    return attemptResourceUsage.getAMUsed(label);
  }

  public void setAMResource(Resource amResource) {
    attemptResourceUsage.setAMUsed(amResource);
  }

  public void setAMResource(String label, Resource amResource) {
    attemptResourceUsage.setAMUsed(label, amResource);
  }

  public boolean isAmRunning() {
    return amRunning;
  }

  public void setAmRunning(boolean bool) {
    amRunning = bool;
  }

  public boolean getUnmanagedAM() {
    return unmanagedAM;
  }

  public RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  public void addRMContainer(
      ContainerId id, RMContainer rmContainer) {
    try {
      writeLock.lock();
      if (!getApplicationAttemptId().equals(
          rmContainer.getApplicationAttemptId()) &&
          !liveContainers.containsKey(id)) {
        LOG.info("recovered container " + id +
            " from previous attempt " + rmContainer.getApplicationAttemptId());
        recoveredPreviousAttemptContainers.add(rmContainer.getContainer());
      }
      liveContainers.put(id, rmContainer);
      if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
        this.attemptOpportunisticResourceUsage.incUsed(
            rmContainer.getAllocatedResource());
      }
      if (rmContainer.isRemotelyAllocated()) {
        this.attemptResourceUsageAllocatedRemotely.incUsed(
            rmContainer.getAllocatedResource());
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void removeRMContainer(ContainerId containerId) {
    try {
      writeLock.lock();
      RMContainer rmContainer = liveContainers.remove(containerId);
      if (rmContainer != null) {
        if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
          this.attemptOpportunisticResourceUsage
              .decUsed(rmContainer.getAllocatedResource());
        }
        if (rmContainer.isRemotelyAllocated()) {
          this.attemptResourceUsageAllocatedRemotely
              .decUsed(rmContainer.getAllocatedResource());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  protected void resetReReservations(
      SchedulerRequestKey schedulerKey) {
    reReservations.setCount(schedulerKey, 0);
  }

  protected void addReReservation(
      SchedulerRequestKey schedulerKey) {
    try {
      reReservations.add(schedulerKey);
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
    }
  }

  public int getReReservations(SchedulerRequestKey schedulerKey) {
    return reReservations.count(schedulerKey);
  }

  /**
   * Get total current reservations.
   * Used only by unit tests
   * @return total current reservations
   */
  @Stable
  @Private
  public Resource getCurrentReservation() {
    return attemptResourceUsage.getReserved();
  }
  
  public Queue getQueue() {
    return queue;
  }
  
  public boolean updateResourceRequests(
      List<ResourceRequest> requests) {
    try {
      writeLock.lock();
      if (!isStopped) {
        return appSchedulingInfo.updateResourceRequests(requests, false);
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean updateSchedulingRequests(
      List<SchedulingRequest> requests) {
    if (requests == null) {
      return false;
    }

    try {
      writeLock.lock();
      if (!isStopped) {
        return appSchedulingInfo.updateSchedulingRequests(requests, false);
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }
  
  public void recoverResourceRequestsForContainer(
      ContainerRequest containerRequest) {
    try {
      writeLock.lock();
      if (!isStopped) {
        appSchedulingInfo.updateResourceRequests(
            containerRequest.getResourceRequests(), true);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public void stop(RMAppAttemptState rmAppAttemptFinalState) {
    try {
      writeLock.lock();
      // Cleanup all scheduling information
      isStopped = true;
      appSchedulingInfo.stop();
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isStopped() {
    return isStopped;
  }

  /**
   * Get the list of reserved containers
   * @return All of the reserved containers.
   */
  public List<RMContainer> getReservedContainers() {
    List<RMContainer> list = new ArrayList<>();
    try {
      readLock.lock();
      for (Entry<SchedulerRequestKey, Map<NodeId, RMContainer>> e :
          this.reservedContainers.entrySet()) {
        list.addAll(e.getValue().values());
      }
      return list;
    } finally {
      readLock.unlock();
    }

  }
  
  public boolean reserveIncreasedContainer(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Resource reservedResource) {
    try {
      writeLock.lock();
      if (commonReserve(node, schedulerKey, rmContainer, reservedResource)) {
        attemptResourceUsage.incReserved(node.getPartition(), reservedResource);
        // succeeded
        return true;
      }

      return false;
    } finally {
      writeLock.unlock();
    }

  }
  
  private boolean commonReserve(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Resource reservedResource) {
    try {
      rmContainer.handle(new RMContainerReservedEvent(rmContainer
          .getContainerId(), reservedResource, node.getNodeID(), schedulerKey));
    } catch (InvalidStateTransitionException e) {
      // We reach here could be caused by container already finished, return
      // false indicate it fails
      return false;
    }
    
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(schedulerKey);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(schedulerKey, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Application attempt " + getApplicationAttemptId()
          + " reserved container " + rmContainer + " on node " + node
          + ". This attempt currently has " + reservedContainers.size()
          + " reserved containers at priority " + schedulerKey.getPriority()
          + "; currentReservation " + reservedResource);
    }
    
    return true;
  }
  
  public RMContainer reserve(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Container container) {
    try {
      writeLock.lock();
      // Create RMContainer if necessary
      if (rmContainer == null) {
        rmContainer = new RMContainerImpl(container, schedulerKey,
            getApplicationAttemptId(), node.getNodeID(),
            appSchedulingInfo.getUser(), rmContext);
      }
      if (rmContainer.getState() == RMContainerState.NEW) {
        attemptResourceUsage.incReserved(node.getPartition(),
            container.getResource());
        ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

        // Reset the re-reservation count
        resetReReservations(schedulerKey);
      } else{
        // Note down the re-reservation
        addReReservation(schedulerKey);
      }

      commonReserve(node, schedulerKey, rmContainer, container.getResource());

      return rmContainer;
    } finally {
      writeLock.unlock();
    }

  }

  public void setHeadroom(Resource globalLimit) {
    this.resourceLimit = Resources.componentwiseMax(globalLimit,
        Resources.none());
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   */
  public Resource getHeadroom() {
    return resourceLimit;
  }
  
  public int getNumReservedContainers(
      SchedulerRequestKey schedulerKey) {
    try {
      readLock.lock();
      Map<NodeId, RMContainer> map = this.reservedContainers.get(
          schedulerKey);
      return (map == null) ? 0 : map.size();
    } finally {
      readLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public void containerLaunchedOnNode(ContainerId containerId,
      NodeId nodeId) {
    try {
      writeLock.lock();
      // Inform the container
      RMContainer rmContainer = getRMContainer(containerId);
      if (rmContainer == null) {
        // Some unknown container sneaked into the system. Kill it.
        rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeCleanContainerEvent(nodeId, containerId));
        return;
      }

      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED));
    } finally {
      writeLock.unlock();
    }
  }
  
  public void showRequests() {
    if (LOG.isDebugEnabled()) {
      try {
        readLock.lock();
        for (SchedulerRequestKey schedulerKey : getSchedulerKeys()) {
          AppPlacementAllocator ap = getAppPlacementAllocator(schedulerKey);
          if (ap != null &&
              ap.getOutstandingAsksCount(ResourceRequest.ANY) > 0) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " headRoom=" + getHeadroom() + " currentConsumption="
                + attemptResourceUsage.getUsed().getMemorySize());
            ap.showRequests();
          }
        }
      } finally {
        readLock.unlock();
      }
    }
  }
  
  public Resource getCurrentConsumption() {
    return attemptResourceUsage.getUsed();
  }
  
  private Container updateContainerAndNMToken(RMContainer rmContainer,
      ContainerUpdateType updateType) {
    Container container = rmContainer.getContainer();
    ContainerType containerType = ContainerType.TASK;
    if (updateType != null) {
      container.setVersion(container.getVersion() + 1);
    }
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    if (isWaitingForAMContainer()) {
      containerType = ContainerType.APPLICATION_MASTER;
    }
    try {
      // create container token and NMToken altogether.
      container.setContainerToken(rmContext.getContainerTokenSecretManager()
          .createContainerToken(container.getId(), container.getVersion(),
              container.getNodeId(), getUser(), container.getResource(),
              container.getPriority(), rmContainer.getCreationTime(),
              this.logAggregationContext, rmContainer.getNodeLabelExpression(),
              containerType, container.getExecutionType(),
              container.getAllocationRequestId(),
              rmContainer.getAllocationTags()));
      container.setAllocationTags(rmContainer.getAllocationTags());
      updateNMToken(container);
    } catch (IllegalArgumentException e) {
      // DNS might be down, skip returning this container.
      LOG.error("Error trying to assign container token and NM token to"
          + " an updated container " + container.getId(), e);
      return null;
    }

    if (updateType == null) {
      // This is a newly allocated container
      rmContainer.handle(new RMContainerEvent(
          rmContainer.getContainerId(), RMContainerEventType.ACQUIRED));
    } else {
      // Resource increase is handled as follows:
      // If the AM does not use the updated token to increase the container
      // for a configured period of time, the RM will automatically rollback
      // the update by performing a container decrease. This rollback (which
      // essentially is another resource decrease update) is notified to the
      // NM heartbeat response. If autoUpdate flag is set, then AM does not
      // need to do anything - same code path as resource decrease.
      //
      // Resource Decrease is always automatic: the AM never has to do
      // anything. It is always via NM heartbeat response.
      //
      // ExecutionType updates (both Promotion and Demotion) are either
      // always automatic (if the flag is set) or the AM has to explicitly
      // call updateContainer() on the NM. There is no expiry
      boolean autoUpdate =
          ContainerUpdateType.DECREASE_RESOURCE == updateType ||
              ((AbstractYarnScheduler)rmContext.getScheduler())
                  .shouldContainersBeAutoUpdated();
      if (autoUpdate) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeUpdateContainerEvent(rmContainer.getNodeId(),
                Collections.singletonMap(
                    rmContainer.getContainer(), updateType)));
      } else {
        rmContainer.handle(new RMContainerUpdatesAcquiredEvent(
            rmContainer.getContainerId(),
            ContainerUpdateType.INCREASE_RESOURCE == updateType));
      }
    }
    return container;
  }

  public void updateNMTokens(Collection<Container> containers) {
    for (Container container : containers) {
      updateNMToken(container);
    }
  }

  private void updateNMToken(Container container) {
    NMToken nmToken =
        rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(),
            getApplicationAttemptId(), container);
    if (nmToken != null) {
      updatedNMTokens.add(nmToken);
    }
  }

  /**
   * Called when AM registers. These containers are reported to the AM in the
   * <code>
   * RegisterApplicationMasterResponse#containersFromPreviousAttempts
   * </code>.
   */
  List<RMContainer> pullContainersToTransfer() {
    try {
      writeLock.lock();
      recoveredPreviousAttemptContainers.clear();
      return new ArrayList<>(liveContainers.values());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Called when AM heartbeats. These containers were recovered by the RM after
   * the AM had registered. They are reported to the AM in the
   * <code>AllocateResponse#containersFromPreviousAttempts</code>.
   */
  public List<Container> pullPreviousAttemptContainers() {
    try {
      writeLock.lock();
      if (recoveredPreviousAttemptContainers.isEmpty()) {
        return null;
      }
      List<Container> returnContainerList = new ArrayList<>
          (recoveredPreviousAttemptContainers);
      recoveredPreviousAttemptContainers.clear();
      return returnContainerList;
    } finally {
      writeLock.unlock();
    }
  }

  // Create container token and update NMToken altogether, if either of them fails for
  // some reason like DNS unavailable, do not return this container and keep it
  // in the newlyAllocatedContainers waiting to be refetched.
  public List<Container> pullNewlyAllocatedContainers() {
    try {
      writeLock.lock();
      List<Container> returnContainerList = new ArrayList<Container>(
          newlyAllocatedContainers.size());

      Iterator<RMContainer> i = newlyAllocatedContainers.iterator();
      while (i.hasNext()) {
        RMContainer rmContainer = i.next();
        Container updatedContainer =
            updateContainerAndNMToken(rmContainer, null);
        // Only add container to return list when it's not null.
        // updatedContainer could be null when generate token failed, it can be
        // caused by DNS resolving failed.
        if (updatedContainer != null) {
          returnContainerList.add(updatedContainer);
          i.remove();
        }
      }
      return returnContainerList;
    } finally {
      writeLock.unlock();
    }
  }

  public synchronized void addToNewlyDemotedContainers(ContainerId containerId,
      RMContainer rmContainer) {
    newlyDemotedContainers.put(containerId, rmContainer);
  }

  public synchronized void addToNewlyDecreasedContainers(
      ContainerId containerId, RMContainer rmContainer) {
    newlyDecreasedContainers.put(containerId, rmContainer);
  }

  protected synchronized void addToUpdateContainerErrors(
      UpdateContainerError error) {
    updateContainerErrors.add(error);
  }

  protected synchronized void addToNewlyAllocatedContainers(
      SchedulerNode node, RMContainer rmContainer) {
    ContainerId matchedContainerId =
        getUpdateContext().matchContainerToOutstandingIncreaseReq(
            node, rmContainer.getAllocatedSchedulerKey(), rmContainer);
    if (matchedContainerId != null) {
      if (ContainerUpdateContext.UNDEFINED == matchedContainerId) {
        // This is a spurious allocation (relaxLocality = false
        // resulted in the Container being allocated on an NM on the same host
        // but not on the NM running the container to be updated. Can
        // happen if more than one NM exists on the same host.. usually
        // occurs when using MiniYARNCluster to test).
        tempContainerToKill.add(rmContainer);
      } else {
        RMContainer existingContainer = getRMContainer(matchedContainerId);
        // If this container was already GUARANTEED, then it is an
        // increase, else its a promotion
        if (existingContainer == null ||
            EnumSet.of(RMContainerState.COMPLETED, RMContainerState.KILLED,
                RMContainerState.EXPIRED, RMContainerState.RELEASED).contains(
                    existingContainer.getState())) {
          tempContainerToKill.add(rmContainer);
        } else {
          if (ExecutionType.GUARANTEED == existingContainer.getExecutionType()) {
            newlyIncreasedContainers.put(matchedContainerId, rmContainer);
          } else {
            newlyPromotedContainers.put(matchedContainerId, rmContainer);
          }
        }
      }
    } else {
      newlyAllocatedContainers.add(rmContainer);
    }
  }

  public List<Container> pullNewlyPromotedContainers() {
    return pullNewlyUpdatedContainers(newlyPromotedContainers,
        ContainerUpdateType.PROMOTE_EXECUTION_TYPE);
  }

  public List<Container> pullNewlyDemotedContainers() {
    return pullNewlyUpdatedContainers(newlyDemotedContainers,
        ContainerUpdateType.DEMOTE_EXECUTION_TYPE);
  }

  public List<Container> pullNewlyIncreasedContainers() {
    return pullNewlyUpdatedContainers(newlyIncreasedContainers,
        ContainerUpdateType.INCREASE_RESOURCE);
  }

  public List<Container> pullNewlyDecreasedContainers() {
    return pullNewlyUpdatedContainers(newlyDecreasedContainers,
        ContainerUpdateType.DECREASE_RESOURCE);
  }

  public List<UpdateContainerError> pullUpdateContainerErrors() {
    List<UpdateContainerError> errors =
        new ArrayList<>(updateContainerErrors);
    updateContainerErrors.clear();
    return errors;
  }

  /**
   * A container is promoted if its executionType is changed from
   * OPPORTUNISTIC to GUARANTEED. It id demoted if the change is from
   * GUARANTEED to OPPORTUNISTIC.
   * @return Newly Promoted and Demoted containers
   */
  private List<Container> pullNewlyUpdatedContainers(
      Map<ContainerId, RMContainer> newlyUpdatedContainers,
      ContainerUpdateType updateTpe) {
    List<Container> updatedContainers = new ArrayList<>();
    if (oppContainerContext == null &&
        (ContainerUpdateType.DEMOTE_EXECUTION_TYPE == updateTpe
            || ContainerUpdateType.PROMOTE_EXECUTION_TYPE == updateTpe)) {
      return updatedContainers;
    }
    try {
      writeLock.lock();
      Iterator<Map.Entry<ContainerId, RMContainer>> i =
          newlyUpdatedContainers.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<ContainerId, RMContainer> entry = i.next();
        ContainerId matchedContainerId = entry.getKey();
        RMContainer tempRMContainer = entry.getValue();

        RMContainer existingRMContainer =
            getRMContainer(matchedContainerId);
        if (existingRMContainer != null) {
          // swap containers
          existingRMContainer = getUpdateContext().swapContainer(
              tempRMContainer, existingRMContainer, updateTpe);
          getUpdateContext().removeFromOutstandingUpdate(
              tempRMContainer.getAllocatedSchedulerKey(),
              existingRMContainer.getContainer());
          Container updatedContainer = updateContainerAndNMToken(
              existingRMContainer, updateTpe);
          updatedContainers.add(updatedContainer);
        }
        tempContainerToKill.add(tempRMContainer);
        i.remove();
      }
      // Release all temporary containers
      Iterator<RMContainer> tempIter = tempContainerToKill.iterator();
      while (tempIter.hasNext()) {
        RMContainer c = tempIter.next();
        // Mark container for release (set RRs to null, so RM does not think
        // it is a recoverable container)
        ((RMContainerImpl) c).setContainerRequest(null);

        // Release this container async-ly so as to prevent
        // 'LeafQueue::completedContainer()' from trying to acquire a lock
        // on the app and queue which can contended for in the reverse order
        // by the Scheduler thread.
        ((AbstractYarnScheduler)rmContext.getScheduler())
            .asyncContainerRelease(c);
        tempIter.remove();
      }
      return updatedContainers;
    } finally {
      writeLock.unlock();
    }
  }

  public List<NMToken> pullUpdatedNMTokens() {
    try {
      writeLock.lock();
      List <NMToken> returnList = new ArrayList<>(updatedNMTokens);
      updatedNMTokens.clear();
      return returnList;
    } finally {
      writeLock.unlock();
    }

  }

  public boolean isWaitingForAMContainer() {
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    return (!unmanagedAM && appAttempt.getMasterContainer() == null);
  }

  public void updateBlacklist(List<String> blacklistAdditions,
      List<String> blacklistRemovals) {
    try {
      writeLock.lock();
      if (!isStopped) {
        if (isWaitingForAMContainer()) {
          // The request is for the AM-container, and the AM-container is
          // launched by the system. So, update the places that are blacklisted
          // by system (as opposed to those blacklisted by the application).
          this.appSchedulingInfo.updatePlacesBlacklistedBySystem(
              blacklistAdditions, blacklistRemovals);
        } else{
          this.appSchedulingInfo.updatePlacesBlacklistedByApp(
              blacklistAdditions, blacklistRemovals);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isPlaceBlacklisted(String resourceName) {
    try {
      readLock.lock();
      boolean forAMContainer = isWaitingForAMContainer();
      return this.appSchedulingInfo.isPlaceBlacklisted(resourceName,
          forAMContainer);
    } finally {
      readLock.unlock();
    }
  }

  public int addMissedNonPartitionedRequestSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    try {
      return missedNonPartitionedReqSchedulingOpportunity.add(
          schedulerKey, 1) + 1;
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
      return Integer.MAX_VALUE;
    }
  }

  public void
      resetMissedNonPartitionedRequestSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    missedNonPartitionedReqSchedulingOpportunity.setCount(schedulerKey, 0);
  }

  
  public void addSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    try {
      schedulingOpportunities.add(schedulerKey, 1);
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
    }
  }
  
  public void subtractSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    this.schedulingOpportunities.removeExactly(schedulerKey, 1);
  }

  /**
   * Return the number of times the application has been given an opportunity
   * to schedule a task at the given priority since the last time it
   * successfully did so.
   * @param schedulerKey Scheduler Key
   * @return number of scheduling opportunities
   */
  public int getSchedulingOpportunities(
      SchedulerRequestKey schedulerKey) {
    return schedulingOpportunities.count(schedulerKey);
  }
  
  /**
   * Should be called when an application has successfully scheduled a
   * container, or when the scheduling locality threshold is relaxed.
   * Reset various internal counters which affect delay scheduling
   *
   * @param schedulerKey The priority of the container scheduled.
   */
  public void resetSchedulingOpportunities(
      SchedulerRequestKey schedulerKey) {
    resetSchedulingOpportunities(schedulerKey, System.currentTimeMillis());
  }

  // used for continuous scheduling
  public void resetSchedulingOpportunities(SchedulerRequestKey schedulerKey,
      long currentTimeMs) {
    lastScheduledContainer.put(schedulerKey, currentTimeMs);
    schedulingOpportunities.setCount(schedulerKey, 0);
  }

  @VisibleForTesting
  void setSchedulingOpportunities(SchedulerRequestKey schedulerKey, int count) {
    schedulingOpportunities.setCount(schedulerKey, count);
  }

  private AggregateAppResourceUsage getRunningAggregateAppResourceUsage() {
    long currentTimeMillis = System.currentTimeMillis();
    // Don't walk the whole container list if the resources were computed
    // recently.
    if ((currentTimeMillis - lastMemoryAggregateAllocationUpdateTime)
        > MEM_AGGREGATE_ALLOCATION_CACHE_MSECS) {
      Map<String, Long> resourceSecondsMap = new HashMap<>();
      for (RMContainer rmContainer : this.liveContainers.values()) {
        long usedMillis = currentTimeMillis - rmContainer.getCreationTime();
        Resource resource = rmContainer.getContainer().getResource();
        for (ResourceInformation entry : resource.getResources()) {
          long value = RMServerUtils
              .getOrDefault(resourceSecondsMap, entry.getName(), 0L);
          value += entry.getValue() * usedMillis
              / DateUtils.MILLIS_PER_SECOND;
          resourceSecondsMap.put(entry.getName(), value);
        }
      }

      lastMemoryAggregateAllocationUpdateTime = currentTimeMillis;
      lastResourceSecondsMap = resourceSecondsMap;
    }
    return new AggregateAppResourceUsage(lastResourceSecondsMap);
  }

  public ApplicationResourceUsageReport getResourceUsageReport() {
    try {
      writeLock.lock();
      AggregateAppResourceUsage runningResourceUsage =
          getRunningAggregateAppResourceUsage();
      Resource usedResourceClone = Resources.clone(
          attemptResourceUsage.getAllUsed());
      Resource reservedResourceClone = Resources.clone(
          attemptResourceUsage.getReserved());
      Resource cluster = rmContext.getScheduler().getClusterResource();
      ResourceCalculator calc =
          rmContext.getScheduler().getResourceCalculator();
      Map<String, Long> preemptedResourceSecondsMaps = new HashMap<>();
      preemptedResourceSecondsMaps
          .put(ResourceInformation.MEMORY_MB.getName(), 0L);
      preemptedResourceSecondsMaps
          .put(ResourceInformation.VCORES.getName(), 0L);
      float queueUsagePerc = 0.0f;
      float clusterUsagePerc = 0.0f;
      if (!calc.isInvalidDivisor(cluster)) {
        float queueCapacityPerc = queue.getQueueInfo(false, false)
            .getCapacity();
        if (queueCapacityPerc != 0) {
          queueUsagePerc = calc.divide(cluster, usedResourceClone,
              Resources.multiply(cluster, queueCapacityPerc)) * 100;
        }
        clusterUsagePerc =
            calc.divide(cluster, usedResourceClone, cluster) * 100;
      }
      return ApplicationResourceUsageReport
          .newInstance(liveContainers.size(), reservedContainers.size(),
              usedResourceClone, reservedResourceClone,
              Resources.add(usedResourceClone, reservedResourceClone),
              runningResourceUsage.getResourceUsageSecondsMap(), queueUsagePerc,
              clusterUsagePerc, preemptedResourceSecondsMaps);
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  public Map<ContainerId, RMContainer> getLiveContainersMap() {
    return this.liveContainers;
  }

  public Map<SchedulerRequestKey, Long>
      getLastScheduledContainer() {
    return this.lastScheduledContainer;
  }

  public void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    try {
      writeLock.lock();
      this.liveContainers = appAttempt.getLiveContainersMap();
      // this.reReservations = appAttempt.reReservations;
      this.attemptResourceUsage.copyAllUsed(appAttempt.attemptResourceUsage);
      this.setHeadroom(appAttempt.resourceLimit);
      // this.currentReservation = appAttempt.currentReservation;
      // this.newlyAllocatedContainers = appAttempt.newlyAllocatedContainers;
      // this.schedulingOpportunities = appAttempt.schedulingOpportunities;
      this.lastScheduledContainer = appAttempt.getLastScheduledContainer();
      this.appSchedulingInfo.transferStateFromPreviousAppSchedulingInfo(
          appAttempt.appSchedulingInfo);
    } finally {
      writeLock.unlock();
    }
  }
  
  public void move(Queue newQueue) {
    try {
      writeLock.lock();
      QueueMetrics oldMetrics = queue.getMetrics();
      QueueMetrics newMetrics = newQueue.getMetrics();
      String newQueueName = newQueue.getQueueName();
      String user = getUser();

      for (RMContainer liveContainer : liveContainers.values()) {
        Resource resource = liveContainer.getContainer().getResource();
        ((RMContainerImpl) liveContainer).setQueueName(newQueueName);
        oldMetrics.releaseResources(liveContainer.getNodeLabelExpression(),
            user, 1, resource);
        newMetrics.allocateResources(liveContainer.getNodeLabelExpression(),
            user, 1, resource, false);
      }
      for (Map<NodeId, RMContainer> map : reservedContainers.values()) {
        for (RMContainer reservedContainer : map.values()) {
          ((RMContainerImpl) reservedContainer).setQueueName(newQueueName);
          Resource resource = reservedContainer.getReservedResource();
          oldMetrics.unreserveResource(
              reservedContainer.getNodeLabelExpression(), user, resource);
          newMetrics.reserveResource(
              reservedContainer.getNodeLabelExpression(), user, resource);
        }
      }

      if (!isStopped) {
        appSchedulingInfo.move(newQueue);
      }
      this.queue = newQueue;
    } finally {
      writeLock.unlock();
    }
  }

  public void recoverContainer(SchedulerNode node,
      RMContainer rmContainer) {
    try {
      writeLock.lock();
      // recover app scheduling info
      appSchedulingInfo.recoverContainer(rmContainer, node.getPartition());

      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return;
      }
      LOG.info("SchedulerAttempt " + getApplicationAttemptId()
          + " is recovering container " + rmContainer.getContainerId());
      addRMContainer(rmContainer.getContainerId(), rmContainer);
      if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
        attemptResourceUsage.incUsed(node.getPartition(),
            rmContainer.getContainer().getResource());
      }

      // resourceLimit: updated when LeafQueue#recoverContainer#allocateResource
      // is called.
      // newlyAllocatedContainers.add(rmContainer);
      // schedulingOpportunities
      // lastScheduledContainer
    } finally {
      writeLock.unlock();
    }
  }

  public void incNumAllocatedContainers(NodeType containerType,
      NodeType requestType) {
    if (containerType == null || requestType == null) {
      // Sanity check
      return;
    }

    RMAppAttempt attempt =
        rmContext.getRMApps().get(attemptId.getApplicationId())
          .getCurrentAppAttempt();
    if (attempt != null) {
      attempt.getRMAppAttemptMetrics().incNumAllocatedContainers(containerType,
        requestType);
    }
  }

  public void setApplicationHeadroomForMetrics(Resource headroom) {
    RMAppAttempt attempt =
        rmContext.getRMApps().get(attemptId.getApplicationId())
            .getCurrentAppAttempt();
    if (attempt != null) {
      attempt.getRMAppAttemptMetrics().setApplicationAttemptHeadRoom(
          Resources.clone(headroom));
    }
  }

  public void recordContainerRequestTime(long value) {
    firstAllocationRequestSentTime.compareAndSet(0, value);
  }

  public void recordContainerAllocationTime(long value) {
    if (firstContainerAllocatedTime.compareAndSet(0, value)) {
      long timediff = firstContainerAllocatedTime.longValue() -
          firstAllocationRequestSentTime.longValue();
      if (timediff > 0) {
        queue.getMetrics().addAppAttemptFirstContainerAllocationDelay(timediff);
      }
    }
  }

  public Set<String> getBlacklistedNodes() {
    return this.appSchedulingInfo.getBlackListCopy();
  }
  
  @Private
  public boolean hasPendingResourceRequest(ResourceCalculator rc,
      String nodePartition, Resource cluster,
      SchedulingMode schedulingMode) {
    // We need to consider unconfirmed allocations
    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      nodePartition = RMNodeLabelsManager.NO_LABEL;
    }

    Resource pending = attemptResourceUsage.getPending(nodePartition);

    // TODO, need consider node partition here
    // To avoid too many allocation-proposals rejected for non-default
    // partition allocation
    if (StringUtils.equals(nodePartition, RMNodeLabelsManager.NO_LABEL)) {
      pending = Resources.subtract(pending, Resources
          .createResource(unconfirmedAllocatedMem.get(),
              unconfirmedAllocatedVcores.get()));
    }

    if (Resources.greaterThan(rc, cluster, pending, Resources.none())) {
      return true;
    }

    return false;
  }

  /*
   * Note that the behavior of appAttemptResourceUsage is different from queue's
   * For queue, used = actual-used + reserved
   * For app, used = actual-used.
   *
   * TODO (wangda): Need to make behaviors of queue/app's resource usage
   * consistent
   */
  @VisibleForTesting
  public ResourceUsage getAppAttemptResourceUsage() {
    return this.attemptResourceUsage;
  }

  @Override
  public Priority getPriority() {
    return appPriority;
  }

  public void setPriority(Priority appPriority) {
    this.appPriority = appPriority;
  }

  @Override
  public String getId() {
    return getApplicationId().toString();
  }
  
  @Override
  public int compareInputOrderTo(SchedulableEntity other) {
    if (other instanceof SchedulerApplicationAttempt) {
      return getApplicationId().compareTo(
        ((SchedulerApplicationAttempt)other).getApplicationId());
    }
    return 1;//let other types go before this, if any
  }
  
  @Override
  public ResourceUsage getSchedulingResourceUsage() {
    return attemptResourceUsage;
  }

  public void setAppAMNodePartitionName(String partitionName) {
    this.appAMNodePartitionName = partitionName;
  }

  public String getAppAMNodePartitionName() {
    return appAMNodePartitionName;
  }

  public void updateAMContainerDiagnostics(AMState state,
      String diagnosticMessage) {
    if (!isWaitingForAMContainer()) {
      return;
    }
    StringBuilder diagnosticMessageBldr = new StringBuilder();
    diagnosticMessageBldr.append("[");
    diagnosticMessageBldr.append(fdf.format(System.currentTimeMillis()));
    diagnosticMessageBldr.append("] ");
    switch (state) {
    case INACTIVATED:
      diagnosticMessageBldr.append(state.diagnosticMessage);
      if (diagnosticMessage != null) {
        diagnosticMessageBldr.append(diagnosticMessage);
      }
      getPendingAppDiagnosticMessage(diagnosticMessageBldr);
      break;
    case ACTIVATED:
      diagnosticMessageBldr.append(state.diagnosticMessage);
      if (diagnosticMessage != null) {
        diagnosticMessageBldr.append(diagnosticMessage);
      }
      getActivedAppDiagnosticMessage(diagnosticMessageBldr);
      break;
    default:
      // UNMANAGED , ASSIGNED
      diagnosticMessageBldr.append(state.diagnosticMessage);
      break;
    }
    appAttempt.updateAMLaunchDiagnostics(diagnosticMessageBldr.toString());
  }

  protected void getPendingAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    // Give the specific information which might be applicable for the
    // respective scheduler
    // like partitionAMResourcelimit,UserAMResourceLimit, queue'AMResourceLimit
  }

  protected void getActivedAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    // Give the specific information which might be applicable for the
    // respective scheduler
    // queue's resource usage for specific partition
  }

  public ReentrantReadWriteLock.WriteLock getWriteLock() {
    return writeLock;
  }

  @Override
  public boolean isRecovering() {
    return isAttemptRecovering;
  }

  protected void setAttemptRecovering(boolean isRecovering) {
    this.isAttemptRecovering = isRecovering;
  }

  public <N extends SchedulerNode> AppPlacementAllocator<N> getAppPlacementAllocator(
      SchedulerRequestKey schedulerRequestKey) {
    return appSchedulingInfo.getAppPlacementAllocator(schedulerRequestKey);
  }
  public void incUnconfirmedRes(Resource res) {
    unconfirmedAllocatedMem.addAndGet(res.getMemorySize());
    unconfirmedAllocatedVcores.addAndGet(res.getVirtualCores());
  }

  public void decUnconfirmedRes(Resource res) {
    unconfirmedAllocatedMem.addAndGet(-res.getMemorySize());
    unconfirmedAllocatedVcores.addAndGet(-res.getVirtualCores());
  }

  @Override
  public int hashCode() {
    return getApplicationAttemptId().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (! (o instanceof SchedulerApplicationAttempt)) {
      return false;
    }

    SchedulerApplicationAttempt other = (SchedulerApplicationAttempt) o;
    return (this == other ||
        this.getApplicationAttemptId().equals(other.getApplicationAttemptId()));
  }

  /**
   * Different state for Application Master, user can see this state from web UI
   */
  public enum AMState {
    UNMANAGED("User launched the Application Master, since it's unmanaged. "),
    INACTIVATED("Application is added to the scheduler and is not yet activated. "),
    ACTIVATED("Application is Activated, waiting for resources to be assigned for AM. "),
    ASSIGNED("Scheduler has assigned a container for AM, waiting for AM "
        + "container to be launched"),
    LAUNCHED("AM container is launched, waiting for AM container to Register "
        + "with RM")
    ;

    private String diagnosticMessage;

    AMState(String diagnosticMessage) {
      this.diagnosticMessage = diagnosticMessage;
    }

    public String getDiagnosticMessage() {
      return diagnosticMessage;
    }
  }

  public Map<String, String> getApplicationSchedulingEnvs() {
    return this.applicationSchedulingEnvs;
  }
}