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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerChangeResourceEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerUpdatesAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

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
  private long lastMemorySeconds = 0;
  private long lastVcoreSeconds = 0;

  protected final AppSchedulingInfo appSchedulingInfo;
  protected ApplicationAttemptId attemptId;
  protected Map<ContainerId, RMContainer> liveContainers =
      new HashMap<ContainerId, RMContainer>();
  protected final Map<Priority, Map<NodeId, RMContainer>> reservedContainers = 
      new HashMap<Priority, Map<NodeId, RMContainer>>();

  private final Multiset<Priority> reReservations = HashMultiset.create();
  
  private Resource resourceLimit = Resource.newInstance(0, 0);
  private boolean unmanagedAM = true;
  private boolean amRunning = false;
  private LogAggregationContext logAggregationContext;

  private volatile Priority appPriority = null;
  private boolean isAttemptRecovering;

  protected ResourceUsage attemptResourceUsage = new ResourceUsage();
  private AtomicLong firstAllocationRequestSentTime = new AtomicLong(0);
  private AtomicLong firstContainerAllocatedTime = new AtomicLong(0);

  protected List<RMContainer> newlyAllocatedContainers = new ArrayList<>();
  protected Map<ContainerId, RMContainer> newlyDecreasedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyIncreasedContainers = new HashMap<>();
  protected Set<NMToken> updatedNMTokens = new HashSet<>();

  // This pendingRelease is used in work-preserving recovery scenario to keep
  // track of the AM's outstanding release requests. RM on recovery could
  // receive the release request form AM before it receives the container status
  // from NM for recovery. In this case, the to-be-recovered containers reported
  // by NM should not be recovered.
  private Set<ContainerId> pendingRelease = null;

  /**
   * Count how many times the application has been given an opportunity to
   * schedule a task at each priority. Each time the scheduler asks the
   * application for a task at this priority, it is incremented, and each time
   * the application successfully schedules a task (at rack or node local), it
   * is reset to 0.
   */
  Multiset<Priority> schedulingOpportunities = HashMultiset.create();
  
  /**
   * Count how many times the application has been given an opportunity to
   * schedule a non-partitioned resource request at each priority. Each time the
   * scheduler asks the application for a task at this priority, it is
   * incremented, and each time the application successfully schedules a task,
   * it is reset to 0 when schedule any task at corresponding priority.
   */
  Multiset<Priority> missedNonPartitionedRequestSchedulingOpportunity =
      HashMultiset.create();
  
  // Time of the last container scheduled at the current allowed level
  protected Map<Priority, Long> lastScheduledContainer =
      new HashMap<Priority, Long>();

  protected Queue queue;
  protected boolean isStopped = false;

  protected String appAMNodePartitionName = CommonNodeLabelsManager.NO_LABEL;

  protected final RMContext rmContext;

  private RMAppAttempt appAttempt;

  public SchedulerApplicationAttempt(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    Preconditions.checkNotNull(rmContext, "RMContext should not be null");
    this.rmContext = rmContext;
    this.appSchedulingInfo = 
        new AppSchedulingInfo(applicationAttemptId, user, queue,  
            activeUsersManager, rmContext.getEpoch(), attemptResourceUsage);
    this.queue = queue;
    this.pendingRelease = new HashSet<ContainerId>();
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
    }
  }
  
  /**
   * Get the live containers of the application.
   * @return live containers of the application
   */
  public synchronized Collection<RMContainer> getLiveContainers() {
    return new ArrayList<RMContainer>(liveContainers.values());
  }

  public AppSchedulingInfo getAppSchedulingInfo() {
    return this.appSchedulingInfo;
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

  public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
    return appSchedulingInfo.getResourceRequests(priority);
  }

  public Set<ContainerId> getPendingRelease() {
    return this.pendingRelease;
  }

  public long getNewContainerId() {
    return appSchedulingInfo.getNewContainerId();
  }

  public Collection<Priority> getPriorities() {
    return appSchedulingInfo.getPriorities();
  }
  
  public synchronized ResourceRequest getResourceRequest(Priority priority,
      String resourceName) {
    return this.appSchedulingInfo.getResourceRequest(priority, resourceName);
  }

  public synchronized int getTotalRequiredResources(Priority priority) {
    ResourceRequest request = getResourceRequest(priority, ResourceRequest.ANY);
    return request == null ? 0 : request.getNumContainers();
  }

  public synchronized Resource getResource(Priority priority) {
    return appSchedulingInfo.getResource(priority);
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

  public synchronized RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  protected synchronized void resetReReservations(Priority priority) {
    reReservations.setCount(priority, 0);
  }

  protected synchronized void addReReservation(Priority priority) {
    reReservations.add(priority);
  }

  public synchronized int getReReservations(Priority priority) {
    return reReservations.count(priority);
  }

  /**
   * Get total current reservations.
   * Used only by unit tests
   * @return total current reservations
   */
  @Stable
  @Private
  public synchronized Resource getCurrentReservation() {
    return attemptResourceUsage.getReserved();
  }
  
  public Queue getQueue() {
    return queue;
  }
  
  public synchronized boolean updateResourceRequests(
      List<ResourceRequest> requests) {
    if (!isStopped) {
      return appSchedulingInfo.updateResourceRequests(requests, false);
    }
    return false;
  }
  
  public synchronized void recoverResourceRequestsForContainer(
      List<ResourceRequest> requests) {
    if (!isStopped) {
      appSchedulingInfo.updateResourceRequests(requests, true);
    }
  }
  
  public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
    // Cleanup all scheduling information
    isStopped = true;
    appSchedulingInfo.stop();
  }

  public synchronized boolean isStopped() {
    return isStopped;
  }

  /**
   * Get the list of reserved containers
   * @return All of the reserved containers.
   */
  public synchronized List<RMContainer> getReservedContainers() {
    List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
    for (Map.Entry<Priority, Map<NodeId, RMContainer>> e : 
      this.reservedContainers.entrySet()) {
      reservedContainers.addAll(e.getValue().values());
    }
    return reservedContainers;
  }
  
  public synchronized boolean reserveIncreasedContainer(SchedulerNode node,
      Priority priority, RMContainer rmContainer, Resource reservedResource) {
    if (commonReserve(node, priority, rmContainer, reservedResource)) {
      attemptResourceUsage.incReserved(node.getPartition(),
          reservedResource);
      // succeeded
      return true;
    }
    
    return false;
  }
  
  private synchronized boolean commonReserve(SchedulerNode node,
      Priority priority, RMContainer rmContainer, Resource reservedResource) {
    try {
      rmContainer.handle(new RMContainerReservedEvent(rmContainer
          .getContainerId(), reservedResource, node.getNodeID(), priority));
    } catch (InvalidStateTransitionException e) {
      // We reach here could be caused by container already finished, return
      // false indicate it fails
      return false;
    }
    
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(priority, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Application attempt " + getApplicationAttemptId()
          + " reserved container " + rmContainer + " on node " + node
          + ". This attempt currently has " + reservedContainers.size()
          + " reserved containers at priority " + priority
          + "; currentReservation " + reservedResource);
    }
    
    return true;
  }
  
  public synchronized RMContainer reserve(SchedulerNode node,
      Priority priority, RMContainer rmContainer, Container container) {
    // Create RMContainer if necessary
    if (rmContainer == null) {
      rmContainer =
          new RMContainerImpl(container, getApplicationAttemptId(),
              node.getNodeID(), appSchedulingInfo.getUser(), rmContext);
      attemptResourceUsage.incReserved(node.getPartition(),
          container.getResource());
      ((RMContainerImpl)rmContainer).setQueueName(this.getQueueName());

      // Reset the re-reservation count
      resetReReservations(priority);
    } else {
      // Note down the re-reservation
      addReReservation(priority);
    }
    
    commonReserve(node, priority, rmContainer, container.getResource());

    return rmContainer;
  }
  
  /**
   * Has the application reserved the given <code>node</code> at the
   * given <code>priority</code>?
   * @param node node to be checked
   * @param priority priority of reserved container
   * @return true is reserved, false if not
   */
  public synchronized boolean isReserved(SchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers != null) {
      return reservedContainers.containsKey(node.getNodeID());
    }
    return false;
  }
  
  public synchronized void setHeadroom(Resource globalLimit) {
    this.resourceLimit = globalLimit; 
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   */
  public synchronized Resource getHeadroom() {
    // Corner case to deal with applications being slightly over-limit
    if (resourceLimit.getMemorySize() < 0) {
      resourceLimit.setMemory(0);
    }
    
    return resourceLimit;
  }
  
  public synchronized int getNumReservedContainers(Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    return (reservedContainers == null) ? 0 : reservedContainers.size();
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void containerLaunchedOnNode(ContainerId containerId,
      NodeId nodeId) {
    // Inform the container
    RMContainer rmContainer = getRMContainer(containerId);
    if (rmContainer == null) {
      // Some unknown container sneaked into the system. Kill it.
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeCleanContainerEvent(nodeId, containerId));
      return;
    }

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.LAUNCHED));
  }
  
  public synchronized void showRequests() {
    if (LOG.isDebugEnabled()) {
      for (Priority priority : getPriorities()) {
        Map<String, ResourceRequest> requests = getResourceRequests(priority);
        if (requests != null) {
          LOG.debug("showRequests:" + " application=" + getApplicationId()
              + " headRoom=" + getHeadroom() + " currentConsumption="
              + attemptResourceUsage.getUsed().getMemorySize());
          for (ResourceRequest request : requests.values()) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " request=" + request);
          }
        }
      }
    }
  }
  
  public Resource getCurrentConsumption() {
    return attemptResourceUsage.getUsed();
  }
  
  private Container updateContainerAndNMToken(RMContainer rmContainer,
      boolean newContainer, boolean increasedContainer) {
    Container container = rmContainer.getContainer();
    ContainerType containerType = ContainerType.TASK;
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    if (isWaitingForAMContainer()) {
      containerType = ContainerType.APPLICATION_MASTER;
    }
    try {
      // create container token and NMToken altogether.
      container.setContainerToken(rmContext.getContainerTokenSecretManager()
          .createContainerToken(container.getId(), container.getNodeId(),
              getUser(), container.getResource(), container.getPriority(),
              rmContainer.getCreationTime(), this.logAggregationContext,
              rmContainer.getNodeLabelExpression(), containerType));
      NMToken nmToken =
          rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(),
              getApplicationAttemptId(), container);
      if (nmToken != null) {
        updatedNMTokens.add(nmToken);
      }
    } catch (IllegalArgumentException e) {
      // DNS might be down, skip returning this container.
      LOG.error("Error trying to assign container token and NM token to"
          + " an updated container " + container.getId(), e);
      return null;
    }
    
    if (newContainer) {
      rmContainer.handle(new RMContainerEvent(
          rmContainer.getContainerId(), RMContainerEventType.ACQUIRED));
    } else {
      rmContainer.handle(new RMContainerUpdatesAcquiredEvent(
          rmContainer.getContainerId(), increasedContainer));
    }
    return container;
  }

  // Create container token and update NMToken altogether, if either of them fails for
  // some reason like DNS unavailable, do not return this container and keep it
  // in the newlyAllocatedContainers waiting to be refetched.
  public synchronized List<Container> pullNewlyAllocatedContainers() {
    List<Container> returnContainerList =
        new ArrayList<Container>(newlyAllocatedContainers.size());
    for (Iterator<RMContainer> i = newlyAllocatedContainers.iterator(); i
        .hasNext();) {
      RMContainer rmContainer = i.next();
      Container updatedContainer =
          updateContainerAndNMToken(rmContainer, true, false);
      // Only add container to return list when it's not null. updatedContainer
      // could be null when generate token failed, it can be caused by DNS
      // resolving failed.
      if (updatedContainer != null) {
        returnContainerList.add(updatedContainer);
        i.remove();
      }
    }
    return returnContainerList;
  }
  
  private synchronized List<Container> pullNewlyUpdatedContainers(
      Map<ContainerId, RMContainer> updatedContainerMap, boolean increase) {
    List<Container> returnContainerList =
        new ArrayList<Container>(updatedContainerMap.size());
    for (Iterator<Entry<ContainerId, RMContainer>> i =
        updatedContainerMap.entrySet().iterator(); i.hasNext();) {
      RMContainer rmContainer = i.next().getValue();
      Container updatedContainer =
          updateContainerAndNMToken(rmContainer, false, increase);
      if (updatedContainer != null) {
        returnContainerList.add(updatedContainer);
        i.remove();
      }
    }
    return returnContainerList;
  }

  public synchronized List<Container> pullNewlyIncreasedContainers() {
    return pullNewlyUpdatedContainers(newlyIncreasedContainers, true);
  }
  
  public synchronized List<Container> pullNewlyDecreasedContainers() {
    return pullNewlyUpdatedContainers(newlyDecreasedContainers, false);
  }
  
  public synchronized List<NMToken> pullUpdatedNMTokens() {
    List<NMToken> returnList = new ArrayList<NMToken>(updatedNMTokens);
    updatedNMTokens.clear();
    return returnList;
  }

  public boolean isWaitingForAMContainer() {
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    return (!unmanagedAM && appAttempt.getMasterContainer() == null);
  }

  // Blacklist used for user containers
  public synchronized void updateBlacklist(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    if (!isStopped) {
      this.appSchedulingInfo.updateBlacklist(
          blacklistAdditions, blacklistRemovals);
    }
  }

  // Blacklist used for AM containers
  public synchronized void updateAMBlacklist(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    if (!isStopped) {
      this.appSchedulingInfo.updateAMBlacklist(
          blacklistAdditions, blacklistRemovals);
    }
  }

  public boolean isBlacklisted(String resourceName) {
    boolean useAMBlacklist = isWaitingForAMContainer();
    return this.appSchedulingInfo.isBlacklisted(resourceName, useAMBlacklist);
  }

  public synchronized int addMissedNonPartitionedRequestSchedulingOpportunity(
      Priority priority) {
    missedNonPartitionedRequestSchedulingOpportunity.add(priority);
    return missedNonPartitionedRequestSchedulingOpportunity.count(priority);
  }

  public synchronized void
      resetMissedNonPartitionedRequestSchedulingOpportunity(Priority priority) {
    missedNonPartitionedRequestSchedulingOpportunity.setCount(priority, 0);
  }

  
  public synchronized void addSchedulingOpportunity(Priority priority) {
    int count = schedulingOpportunities.count(priority);
    if (count < Integer.MAX_VALUE) {
      schedulingOpportunities.setCount(priority, count + 1);
    }
  }
  
  public synchronized void subtractSchedulingOpportunity(Priority priority) {
    int count = schedulingOpportunities.count(priority) - 1;
    this.schedulingOpportunities.setCount(priority, Math.max(count,  0));
  }

  /**
   * Return the number of times the application has been given an opportunity
   * to schedule a task at the given priority since the last time it
   * successfully did so.
   */
  public synchronized int getSchedulingOpportunities(Priority priority) {
    return schedulingOpportunities.count(priority);
  }
  
  /**
   * Should be called when an application has successfully scheduled a container,
   * or when the scheduling locality threshold is relaxed.
   * Reset various internal counters which affect delay scheduling
   *
   * @param priority The priority of the container scheduled.
   */
  public synchronized void resetSchedulingOpportunities(Priority priority) {
    resetSchedulingOpportunities(priority, System.currentTimeMillis());
  }

  // used for continuous scheduling
  public synchronized void resetSchedulingOpportunities(Priority priority,
      long currentTimeMs) {
    lastScheduledContainer.put(priority, currentTimeMs);
    schedulingOpportunities.setCount(priority, 0);
  }

  @VisibleForTesting
  void setSchedulingOpportunities(Priority priority, int count) {
    schedulingOpportunities.setCount(priority, count);
  }

  synchronized AggregateAppResourceUsage getRunningAggregateAppResourceUsage() {
    long currentTimeMillis = System.currentTimeMillis();
    // Don't walk the whole container list if the resources were computed
    // recently.
    if ((currentTimeMillis - lastMemoryAggregateAllocationUpdateTime)
        > MEM_AGGREGATE_ALLOCATION_CACHE_MSECS) {
      long memorySeconds = 0;
      long vcoreSeconds = 0;
      for (RMContainer rmContainer : this.liveContainers.values()) {
        long usedMillis = currentTimeMillis - rmContainer.getCreationTime();
        Resource resource = rmContainer.getContainer().getResource();
        memorySeconds += resource.getMemorySize() * usedMillis /
            DateUtils.MILLIS_PER_SECOND;
        vcoreSeconds += resource.getVirtualCores() * usedMillis  
            / DateUtils.MILLIS_PER_SECOND;
      }

      lastMemoryAggregateAllocationUpdateTime = currentTimeMillis;
      lastMemorySeconds = memorySeconds;
      lastVcoreSeconds = vcoreSeconds;
    }
    return new AggregateAppResourceUsage(lastMemorySeconds, lastVcoreSeconds);
  }

  public synchronized ApplicationResourceUsageReport getResourceUsageReport() {
    AggregateAppResourceUsage runningResourceUsage =
        getRunningAggregateAppResourceUsage();
    Resource usedResourceClone =
        Resources.clone(attemptResourceUsage.getAllUsed());
    Resource reservedResourceClone =
        Resources.clone(attemptResourceUsage.getReserved());
    Resource cluster = rmContext.getScheduler().getClusterResource();
    ResourceCalculator calc = rmContext.getScheduler().getResourceCalculator();
    float queueUsagePerc = 0.0f;
    float clusterUsagePerc = 0.0f;
    if (!calc.isInvalidDivisor(cluster)) {
      queueUsagePerc =
          calc.divide(cluster, usedResourceClone, Resources.multiply(cluster,
              queue.getQueueInfo(false, false).getCapacity())) * 100;
      clusterUsagePerc = calc.divide(cluster, usedResourceClone, cluster) * 100;
    }
    return ApplicationResourceUsageReport.newInstance(liveContainers.size(),
        reservedContainers.size(), usedResourceClone, reservedResourceClone,
        Resources.add(usedResourceClone, reservedResourceClone),
        runningResourceUsage.getMemorySeconds(),
        runningResourceUsage.getVcoreSeconds(), queueUsagePerc,
        clusterUsagePerc);
  }

  public synchronized Map<ContainerId, RMContainer> getLiveContainersMap() {
    return this.liveContainers;
  }

  public synchronized Resource getResourceLimit() {
    return this.resourceLimit;
  }

  public synchronized Map<Priority, Long> getLastScheduledContainer() {
    return this.lastScheduledContainer;
  }

  public synchronized void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    this.liveContainers = appAttempt.getLiveContainersMap();
    // this.reReservations = appAttempt.reReservations;
    this.attemptResourceUsage.copyAllUsed(appAttempt.attemptResourceUsage);
    this.resourceLimit = appAttempt.getResourceLimit();
    // this.currentReservation = appAttempt.currentReservation;
    // this.newlyAllocatedContainers = appAttempt.newlyAllocatedContainers;
    // this.schedulingOpportunities = appAttempt.schedulingOpportunities;
    this.lastScheduledContainer = appAttempt.getLastScheduledContainer();
    this.appSchedulingInfo
      .transferStateFromPreviousAppSchedulingInfo(appAttempt.appSchedulingInfo);
  }
  
  public synchronized void move(Queue newQueue) {
    QueueMetrics oldMetrics = queue.getMetrics();
    QueueMetrics newMetrics = newQueue.getMetrics();
    String newQueueName = newQueue.getQueueName();
    String user = getUser();
    for (RMContainer liveContainer : liveContainers.values()) {
      Resource resource = liveContainer.getContainer().getResource();
      ((RMContainerImpl)liveContainer).setQueueName(newQueueName);
      oldMetrics.releaseResources(user, 1, resource);
      newMetrics.allocateResources(user, 1, resource, false);
    }
    for (Map<NodeId, RMContainer> map : reservedContainers.values()) {
      for (RMContainer reservedContainer : map.values()) {
        ((RMContainerImpl)reservedContainer).setQueueName(newQueueName);
        Resource resource = reservedContainer.getReservedResource();
        oldMetrics.unreserveResource(user, resource);
        newMetrics.reserveResource(user, resource);
      }
    }

    appSchedulingInfo.move(newQueue);
    this.queue = newQueue;
  }

  public synchronized void recoverContainer(SchedulerNode node,
      RMContainer rmContainer) {
    // recover app scheduling info
    appSchedulingInfo.recoverContainer(rmContainer);

    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    LOG.info("SchedulerAttempt " + getApplicationAttemptId()
      + " is recovering container " + rmContainer.getContainerId());
    liveContainers.put(rmContainer.getContainerId(), rmContainer);
    attemptResourceUsage.incUsed(node.getPartition(), rmContainer
        .getContainer().getResource());
    
    // resourceLimit: updated when LeafQueue#recoverContainer#allocateResource
    // is called.
    // newlyAllocatedContainers.add(rmContainer);
    // schedulingOpportunities
    // lastScheduledContainer
  }

  public void incNumAllocatedContainers(NodeType containerType,
      NodeType requestType) {
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
    return SchedulerUtils.hasPendingResourceRequest(rc,
        this.attemptResourceUsage, nodePartition, cluster,
        schedulingMode);
  }
  
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
  
  public synchronized boolean removeIncreaseRequest(NodeId nodeId,
      Priority priority, ContainerId containerId) {
    return appSchedulingInfo.removeIncreaseRequest(nodeId, priority,
        containerId);
  }
  
  public synchronized boolean updateIncreaseRequests(
      List<SchedContainerChangeRequest> increaseRequests) {
    return appSchedulingInfo.updateIncreaseRequests(increaseRequests);
  }
  
  private synchronized void changeContainerResource(
      SchedContainerChangeRequest changeRequest, boolean increase) {
    if (increase) {
      appSchedulingInfo.increaseContainer(changeRequest);
    } else {
      appSchedulingInfo.decreaseContainer(changeRequest);
    }

    RMContainer changedRMContainer = changeRequest.getRMContainer(); 
    changedRMContainer.handle(
        new RMContainerChangeResourceEvent(changeRequest.getContainerId(),
            changeRequest.getTargetCapacity(), increase));

    // remove pending and not pulled by AM newly-increased/decreased-containers
    // and add the new one
    if (increase) {
      newlyDecreasedContainers.remove(changeRequest.getContainerId());
      newlyIncreasedContainers.put(changeRequest.getContainerId(),
          changedRMContainer);
    } else {
      newlyIncreasedContainers.remove(changeRequest.getContainerId());
      newlyDecreasedContainers.put(changeRequest.getContainerId(),
          changedRMContainer);
    }
  }
  
  public synchronized void decreaseContainer(
      SchedContainerChangeRequest decreaseRequest) {
    changeContainerResource(decreaseRequest, false);
  }
  
  public synchronized void increaseContainer(
      SchedContainerChangeRequest increaseRequest) {
    changeContainerResource(increaseRequest, true);
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

  @Override
  public boolean isRecovering() {
    return isAttemptRecovering;
  }

  protected void setAttemptRecovering(boolean isRecovering) {
    this.isAttemptRecovering = isRecovering;
  }

  public static enum AMState {
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
}