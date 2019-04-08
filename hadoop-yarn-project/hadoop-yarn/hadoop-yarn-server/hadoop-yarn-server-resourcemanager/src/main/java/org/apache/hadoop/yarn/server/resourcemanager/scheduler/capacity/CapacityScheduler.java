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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.AppNameMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementFactory;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

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
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueInvalidException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.CSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.FileBasedCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.KillableContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceAllocationCommitter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAttributesUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .QueueManagementChangeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QUEUE_MAPPING;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class CapacityScheduler extends
    AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode> implements
    PreemptableResourceScheduler, CapacitySchedulerContext, Configurable,
    ResourceAllocationCommitter, MutableConfScheduler {

  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");
  private static final Logger LOG =
      LoggerFactory.getLogger(CapacityScheduler.class);

  private CapacitySchedulerQueueManager queueManager;

  // timeout to join when we stop this service
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  private PreemptionManager preemptionManager = new PreemptionManager();

  private volatile boolean isLazyPreemptionEnabled = false;

  private int offswitchPerHeartbeatLimit;

  private boolean assignMultipleEnabled;

  private int maxAssignPerHeartbeat;

  private CSConfigurationProvider csConfProvider;

  @Override
  public void setConf(Configuration conf) {
      yarnConf = conf;
  }

  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }

    // validate scheduler vcores allocation setting
    int minVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores <= 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
        + "=" + minVcores
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
        + "=" + maxVcores + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }

  @Override
  public Configuration getConf() {
    return yarnConf;
  }

  private CapacitySchedulerConfiguration conf;
  private Configuration yarnConf;

  private ResourceCalculator calculator;
  private boolean usePortForNodeName;

  private boolean scheduleAsynchronously;
  private List<AsyncScheduleThread> asyncSchedulerThreads;
  private ResourceCommitterService resourceCommitterService;
  private RMNodeLabelsManager labelManager;
  private AppPriorityACLsManager appPriorityACLManager;
  private boolean multiNodePlacementEnabled;

  private static boolean printedVerboseLoggingForAsyncScheduling = false;

  /**
   * EXPERT
   */
  private long asyncScheduleInterval;
  private static final String ASYNC_SCHEDULER_INTERVAL =
      CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
          + ".scheduling-interval-ms";
  private static final long DEFAULT_ASYNC_SCHEDULER_INTERVAL = 5;
  private long asyncMaxPendingBacklogs;

  public CapacityScheduler() {
    super(CapacityScheduler.class.getName());
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return getRootQueue().getMetrics();
  }

  public CSQueue getRootQueue() {
    return queueManager.getRootQueue();
  }

  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.rmContext.getContainerTokenSecretManager();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return calculator;
  }

  @VisibleForTesting
  public void setResourceCalculator(ResourceCalculator rc) {
    this.calculator = rc;
  }

  @Override
  public int getNumClusterNodes() {
    return nodeTracker.nodeCount();
  }

  @Override
  public RMContext getRMContext() {
    return this.rmContext;
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @VisibleForTesting
  void initScheduler(Configuration configuration) throws
      IOException {
    writeLock.lock();
    try {
      String confProviderStr = configuration.get(
          YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          YarnConfiguration.DEFAULT_CONFIGURATION_STORE);
      switch (confProviderStr) {
      case YarnConfiguration.FILE_CONFIGURATION_STORE:
        this.csConfProvider =
            new FileBasedCSConfigurationProvider(rmContext);
        break;
      case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
      case YarnConfiguration.LEVELDB_CONFIGURATION_STORE:
      case YarnConfiguration.ZK_CONFIGURATION_STORE:
      case YarnConfiguration.FS_CONFIGURATION_STORE:
        this.csConfProvider = new MutableCSConfigurationProvider(rmContext);
        break;
      default:
        throw new IOException("Invalid configuration store class: " +
            confProviderStr);
      }
      this.csConfProvider.init(configuration);
      this.conf = this.csConfProvider.loadConfiguration(configuration);
      validateConf(this.conf);
      this.minimumAllocation = super.getMinimumAllocation();
      initMaximumResourceCapability(super.getMaximumAllocation());
      this.calculator = this.conf.getResourceCalculator();
      if (this.calculator instanceof DefaultResourceCalculator
          && ResourceUtils.getNumberOfKnownResourceTypes() > 2) {
        throw new YarnRuntimeException("RM uses DefaultResourceCalculator which"
            + " used only memory as resource-type but invalid resource-types"
            + " specified " + ResourceUtils.getResourceTypes() + ". Use"
            + " DominantResourceCalculator instead to make effective use of"
            + " these resource-types");
      }
      this.usePortForNodeName = this.conf.getUsePortForNodeName();
      this.applications = new ConcurrentHashMap<>();
      this.labelManager = rmContext.getNodeLabelManager();
      this.appPriorityACLManager = new AppPriorityACLsManager(conf);
      this.queueManager = new CapacitySchedulerQueueManager(yarnConf,
          this.labelManager, this.appPriorityACLManager);
      this.queueManager.setCapacitySchedulerContext(this);

      this.activitiesManager = new ActivitiesManager(rmContext);
      activitiesManager.init(conf);
      initializeQueues(this.conf);
      this.isLazyPreemptionEnabled = conf.getLazyPreemptionEnabled();

      scheduleAsynchronously = this.conf.getScheduleAynschronously();
      asyncScheduleInterval = this.conf.getLong(ASYNC_SCHEDULER_INTERVAL,
          DEFAULT_ASYNC_SCHEDULER_INTERVAL);

      this.assignMultipleEnabled = this.conf.getAssignMultipleEnabled();
      this.maxAssignPerHeartbeat = this.conf.getMaxAssignPerHeartbeat();

      // number of threads for async scheduling
      int maxAsyncSchedulingThreads = this.conf.getInt(
          CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
          1);
      maxAsyncSchedulingThreads = Math.max(maxAsyncSchedulingThreads, 1);

      if (scheduleAsynchronously) {
        asyncSchedulerThreads = new ArrayList<>();
        for (int i = 0; i < maxAsyncSchedulingThreads; i++) {
          asyncSchedulerThreads.add(new AsyncScheduleThread(this));
        }
        resourceCommitterService = new ResourceCommitterService(this);
        asyncMaxPendingBacklogs = this.conf.getInt(
            CapacitySchedulerConfiguration.
                SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_PENDING_BACKLOGS,
            CapacitySchedulerConfiguration.
                DEFAULT_SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_PENDING_BACKLOGS);
      }

      // Setup how many containers we can allocate for each round
      offswitchPerHeartbeatLimit = this.conf.getOffSwitchPerHeartbeatLimit();

      // Register CS specific multi-node policies to common MultiNodeManager
      // which will add to a MultiNodeSorter which gives a pre-sorted list of
      // nodes to scheduler's allocation.
      multiNodePlacementEnabled = this.conf.getMultiNodePlacementEnabled();
      if(rmContext.getMultiNodeSortingManager() != null) {
        rmContext.getMultiNodeSortingManager().registerMultiNodePolicyNames(
            multiNodePlacementEnabled,
            this.conf.getMultiNodePlacementPolicies());
      }

      LOG.info("Initialized CapacityScheduler with " + "calculator="
          + getResourceCalculator().getClass() + ", " + "minimumAllocation=<"
          + getMinimumResourceCapability() + ">, " + "maximumAllocation=<"
          + getMaximumResourceCapability() + ">, " + "asynchronousScheduling="
          + scheduleAsynchronously + ", " + "asyncScheduleInterval="
          + asyncScheduleInterval + "ms" + ",multiNodePlacementEnabled="
          + multiNodePlacementEnabled);
    } finally {
      writeLock.unlock();
    }
  }

  private void startSchedulerThreads() {
    writeLock.lock();
    try {
      activitiesManager.start();
      if (scheduleAsynchronously) {
        Preconditions.checkNotNull(asyncSchedulerThreads,
            "asyncSchedulerThreads is null");
        for (Thread t : asyncSchedulerThreads) {
          t.start();
        }

        resourceCommitterService.start();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Configuration configuration = new Configuration(conf);
    super.serviceInit(conf);
    initScheduler(configuration);
    // Initialize SchedulingMonitorManager
    schedulingMonitorManager.initialize(rmContext, conf);
  }

  @Override
  public void serviceStart() throws Exception {
    startSchedulerThreads();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    writeLock.lock();
    try {
      this.activitiesManager.stop();
      if (scheduleAsynchronously && asyncSchedulerThreads != null) {
        for (Thread t : asyncSchedulerThreads) {
          t.interrupt();
          t.join(THREAD_JOIN_TIMEOUT_MS);
        }
        resourceCommitterService.interrupt();
        resourceCommitterService.join(THREAD_JOIN_TIMEOUT_MS);
      }
    } finally {
      writeLock.unlock();
    }

    if (isConfigurationMutable()) {
      ((MutableConfigurationProvider) csConfProvider).close();
    }
    super.serviceStop();
  }

  @Override
  public void reinitialize(Configuration newConf, RMContext rmContext)
      throws IOException {
    writeLock.lock();
    try {
      Configuration configuration = new Configuration(newConf);
      CapacitySchedulerConfiguration oldConf = this.conf;
      this.conf = csConfProvider.loadConfiguration(configuration);
      validateConf(this.conf);
      try {
        LOG.info("Re-initializing queues...");
        refreshMaximumAllocation(
            ResourceUtils.fetchMaximumAllocationFromConfig(this.conf));
        reinitializeQueues(this.conf);
      } catch (Throwable t) {
        this.conf = oldConf;
        refreshMaximumAllocation(
            ResourceUtils.fetchMaximumAllocationFromConfig(this.conf));
        throw new IOException("Failed to re-init queues : " + t.getMessage(),
            t);
      }

      // update lazy preemption
      this.isLazyPreemptionEnabled = this.conf.getLazyPreemptionEnabled();

      // Setup how many containers we can allocate for each round
      offswitchPerHeartbeatLimit = this.conf.getOffSwitchPerHeartbeatLimit();

      super.reinitialize(newConf, rmContext);
    } finally {
      writeLock.unlock();
    }
  }

  long getAsyncScheduleInterval() {
    return asyncScheduleInterval;
  }

  private final static Random random = new Random(System.currentTimeMillis());

  private static boolean shouldSkipNodeSchedule(FiCaSchedulerNode node,
      CapacityScheduler cs, boolean printVerboseLog) {
    // Skip node which missed 2 heartbeats since the node might be dead and
    // we should not continue allocate containers on that.
    long timeElapsedFromLastHeartbeat =
        Time.monotonicNow() - node.getLastHeartbeatMonotonicTime();
    if (timeElapsedFromLastHeartbeat > cs.nmHeartbeatInterval * 2) {
      if (printVerboseLog && LOG.isDebugEnabled()) {
        LOG.debug("Skip scheduling on node because it haven't heartbeated for "
            + timeElapsedFromLastHeartbeat / 1000.0f + " secs");
      }
      return true;
    }
    return false;
  }

  /**
   * Schedule on all nodes by starting at a random point.
   * @param cs
   */
  static void schedule(CapacityScheduler cs) throws InterruptedException{
    // First randomize the start point
    int current = 0;
    Collection<FiCaSchedulerNode> nodes = cs.nodeTracker.getAllNodes();

    // If nodes size is 0 (when there are no node managers registered,
    // we can return from here itself.
    int nodeSize = nodes.size();
    if(nodeSize == 0) {
      return;
    }
    int start = random.nextInt(nodeSize);

    // To avoid too verbose DEBUG logging, only print debug log once for
    // every 10 secs.
    boolean printSkipedNodeLogging = false;
    if (Time.monotonicNow() / 1000 % 10 == 0) {
      printSkipedNodeLogging = (!printedVerboseLoggingForAsyncScheduling);
    } else {
      printedVerboseLoggingForAsyncScheduling = false;
    }

    // Allocate containers of node [start, end)
    for (FiCaSchedulerNode node : nodes) {
      if (current++ >= start) {
        if (shouldSkipNodeSchedule(node, cs, printSkipedNodeLogging)) {
          continue;
        }
        cs.allocateContainersToNode(node.getNodeID(), false);
      }
    }

    current = 0;

    // Allocate containers of node [0, start)
    for (FiCaSchedulerNode node : nodes) {
      if (current++ > start) {
        break;
      }
      if (shouldSkipNodeSchedule(node, cs, printSkipedNodeLogging)) {
        continue;
      }
      cs.allocateContainersToNode(node.getNodeID(), false);
    }

    if (printSkipedNodeLogging) {
      printedVerboseLoggingForAsyncScheduling = true;
    }

    Thread.sleep(cs.getAsyncScheduleInterval());
  }

  static class AsyncScheduleThread extends Thread {

    private final CapacityScheduler cs;
    private AtomicBoolean runSchedules = new AtomicBoolean(false);

    public AsyncScheduleThread(CapacityScheduler cs) {
      this.cs = cs;
      setDaemon(true);
    }

    @Override
    public void run() {
      int debuggingLogCounter = 0;
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (!runSchedules.get()) {
            Thread.sleep(100);
          } else {
            // Don't run schedule if we have some pending backlogs already
            if (cs.getAsyncSchedulingPendingBacklogs()
                > cs.asyncMaxPendingBacklogs) {
              Thread.sleep(1);
            } else{
              schedule(cs);
              if(LOG.isDebugEnabled()) {
                // Adding a debug log here to ensure that the thread is alive
                // and running fine.
                if (debuggingLogCounter++ > 10000) {
                  debuggingLogCounter = 0;
                  LOG.debug("AsyncScheduleThread[" + getName() + "] is running!");
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          // keep interrupt signal
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("AsyncScheduleThread[" + getName() + "] exited!");
    }

    public void beginSchedule() {
      runSchedules.set(true);
    }

    public void suspendSchedule() {
      runSchedules.set(false);
    }

  }

  static class ResourceCommitterService extends Thread {
    private final CapacityScheduler cs;
    private BlockingQueue<ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode>>
        backlogs = new LinkedBlockingQueue<>();

    public ResourceCommitterService(CapacityScheduler cs) {
      this.cs = cs;
      setDaemon(true);
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request =
              backlogs.take();
          cs.writeLock.lock();
          try {
            cs.tryCommit(cs.getClusterResource(), request, true);
          } finally {
            cs.writeLock.unlock();
          }

        } catch (InterruptedException e) {
          LOG.error(e.toString());
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("ResourceCommitterService exited!");
    }

    public void addNewCommitRequest(
        ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> proposal) {
      backlogs.add(proposal);
    }

    public int getPendingBacklogs() {
      return backlogs.size();
    }
  }

  @VisibleForTesting
  public PlacementRule getUserGroupMappingPlacementRule() throws IOException {
    readLock.lock();
    try {
      UserGroupMappingPlacementRule ugRule = new UserGroupMappingPlacementRule();
      ugRule.initialize(this);
      return ugRule;
    } finally {
      readLock.unlock();
    }
  }

  public PlacementRule getAppNameMappingPlacementRule() throws IOException {
    readLock.lock();
    try {
      AppNameMappingPlacementRule anRule = new AppNameMappingPlacementRule();
      anRule.initialize(this);
      return anRule;
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public void updatePlacementRules() throws IOException {
    // Initialize placement rules
    Collection<String> placementRuleStrs = conf.getStringCollection(
        YarnConfiguration.QUEUE_PLACEMENT_RULES);
    List<PlacementRule> placementRules = new ArrayList<>();
    Set<String> distingushRuleSet = new HashSet<>();
    // fail the case if we get duplicate placementRule add in
    for (String pls : placementRuleStrs) {
      if (!distingushRuleSet.add(pls)) {
        throw new IOException("Invalid PlacementRule inputs which "
            + "contains duplicate rule strings");
      }
    }

    // add UserGroupMappingPlacementRule if absent
    distingushRuleSet.add(YarnConfiguration.USER_GROUP_PLACEMENT_RULE);

    placementRuleStrs = new ArrayList<>(distingushRuleSet);

    for (String placementRuleStr : placementRuleStrs) {
      switch (placementRuleStr) {
      case YarnConfiguration.USER_GROUP_PLACEMENT_RULE:
        PlacementRule ugRule = getUserGroupMappingPlacementRule();
        if (null != ugRule) {
          placementRules.add(ugRule);
        }
        break;
      case YarnConfiguration.APP_NAME_PLACEMENT_RULE:
        PlacementRule anRule = getAppNameMappingPlacementRule();
        if (null != anRule) {
          placementRules.add(anRule);
        }
        break;
      default:
        boolean isMappingNotEmpty;
        try {
          PlacementRule rule = PlacementFactory.getPlacementRule(
              placementRuleStr, conf);
          if (null != rule) {
            try {
              isMappingNotEmpty = rule.initialize(this);
            } catch (IOException ie) {
              throw new IOException(ie);
            }
            if (isMappingNotEmpty) {
              placementRules.add(rule);
            }
          }
        } catch (ClassNotFoundException cnfe) {
          throw new IOException(cnfe);
        }
      }
    }

    rmContext.getQueuePlacementManager().updateRules(placementRules);
  }

  @Lock(CapacityScheduler.class)
  private void initializeQueues(CapacitySchedulerConfiguration conf)
    throws IOException {

    this.queueManager.initializeQueues(conf);

    updatePlacementRules();

    // Notify Preemption Manager
    preemptionManager.refreshQueues(null, this.getRootQueue());
  }

  @Lock(CapacityScheduler.class)
  private void reinitializeQueues(CapacitySchedulerConfiguration newConf)
  throws IOException {
    this.queueManager.reinitializeQueues(newConf);
    updatePlacementRules();

    // Notify Preemption Manager
    preemptionManager.refreshQueues(null, this.getRootQueue());
  }

  @Override
  public CSQueue getQueue(String queueName) {
    if (queueName == null) {
      return null;
    }
    return this.queueManager.getQueue(queueName);
  }

  private void addApplicationOnRecovery(ApplicationId applicationId,
      String queueName, String user,
      Priority priority, ApplicationPlacementContext placementContext) {
    writeLock.lock();
    try {
      //check if the queue needs to be auto-created during recovery
      CSQueue queue = getOrCreateQueueFromPlacementContext(applicationId, user,
           queueName, placementContext, true);

      if (queue == null) {
        //During a restart, this indicates a queue was removed, which is
        //not presently supported
        if (!getConfiguration().shouldAppFailFast(getConfig())) {
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppEvent(applicationId, RMAppEventType.KILL,
                  "Application killed on recovery as it"
                      + " was submitted to queue " + queueName
                      + " which no longer exists after restart."));
          return;
        } else{
          String queueErrorMsg = "Queue named " + queueName + " missing "
              + "during application recovery."
              + " Queue removal during recovery is not presently "
              + "supported by the capacity scheduler, please "
              + "restart with all queues configured"
              + " which were present before shutdown/restart.";
          LOG.error(FATAL, queueErrorMsg);
          throw new QueueInvalidException(queueErrorMsg);
        }
      }
      if (!(queue instanceof LeafQueue)) {
        // During RM restart, this means leaf queue was converted to a parent
        // queue, which is not supported for running apps.
        if (!getConfiguration().shouldAppFailFast(getConfig())) {
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppEvent(applicationId, RMAppEventType.KILL,
                  "Application killed on recovery as it was "
                      + "submitted to queue " + queueName
                      + " which is no longer a leaf queue after restart."));
          return;
        } else{
          String queueErrorMsg = "Queue named " + queueName
              + " is no longer a leaf queue during application recovery."
              + " Changing a leaf queue to a parent queue during recovery is"
              + " not presently supported by the capacity scheduler. Please"
              + " restart with leaf queues before shutdown/restart continuing"
              + " as leaf queues.";
          LOG.error(FATAL, queueErrorMsg);
          throw new QueueInvalidException(queueErrorMsg);
        }
      }
      // When recovering apps in this queue but queue is in STOPPED state,
      // that means its previous state was DRAINING. So we auto transit
      // the state to DRAINING for recovery.
      if (queue.getState() == QueueState.STOPPED) {
        ((LeafQueue) queue).recoverDrainingState();
      }
      // Submit to the queue
      try {
        queue.submitApplication(applicationId, user, queueName);
      } catch (AccessControlException ace) {
        // Ignore the exception for recovered app as the app was previously
        // accepted.
      }
      queue.getMetrics().submitApp(user);
      SchedulerApplication<FiCaSchedulerApp> application =
          new SchedulerApplication<FiCaSchedulerApp>(queue, user, priority);
      applications.put(applicationId, application);
      LOG.info("Accepted application " + applicationId + " from user: " + user
          + ", in queue: " + queueName);
      LOG.debug(
          applicationId + " is recovering. Skip notifying APP_ACCEPTED");
    } finally {
      writeLock.unlock();
    }
  }

  private CSQueue getOrCreateQueueFromPlacementContext(ApplicationId
      applicationId, String user, String queueName,
      ApplicationPlacementContext placementContext,
       boolean isRecovery) {

    CSQueue queue = getQueue(queueName);

    if (queue == null) {
      if (placementContext != null && placementContext.hasParentQueue()) {
        try {
          return autoCreateLeafQueue(placementContext);
        } catch (YarnException | IOException e) {
          if (isRecovery) {
            if (!getConfiguration().shouldAppFailFast(getConfig())) {
              LOG.error("Could not auto-create leaf queue " + queueName +
                  " due to : ", e);
              this.rmContext.getDispatcher().getEventHandler().handle(
                  new RMAppEvent(applicationId, RMAppEventType.KILL,
                      "Application killed on recovery"
                          + " as it was submitted to queue " + queueName
                          + " which could not be auto-created"));
            } else{
              String queueErrorMsg =
                  "Queue named " + queueName + " could not be "
                      + "auto-created during application recovery.";
              LOG.error(FATAL, queueErrorMsg, e);
              throw new QueueInvalidException(queueErrorMsg);
            }
          } else{
            LOG.error("Could not auto-create leaf queue due to : ", e);
            final String message =
                "Application " + applicationId + " submission by user : "
                    + user
                    + " to  queue : " + queueName + " failed : " + e
                    .getMessage();
            this.rmContext.getDispatcher().getEventHandler().handle(
                new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                    message));
          }
        }
      }
    }
    return queue;
  }

  private void addApplication(ApplicationId applicationId, String queueName,
      String user, Priority priority,
      ApplicationPlacementContext placementContext) {
    writeLock.lock();
    try {
      if (isSystemAppsLimitReached()) {
        String message = "Maximum system application limit reached,"
            + "cannot accept submission of application: " + applicationId;
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                message));
        return;
      }

      //Could be a potential auto-created leaf queue
      CSQueue queue = getOrCreateQueueFromPlacementContext(applicationId, user,
            queueName, placementContext, false);

      if (queue == null) {
        final String message =
            "Application " + applicationId + " submitted by user " + user
                + " to unknown queue: " + queueName;

        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                message));
        return;
      }

      if (!(queue instanceof LeafQueue)) {
        String message =
            "Application " + applicationId + " submitted by user : " + user
                + " to non-leaf queue : " + queueName;
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                message));
        return;
      } else if (queue instanceof AutoCreatedLeafQueue && queue
          .getParent() instanceof ManagedParentQueue) {

        //If queue already exists and auto-queue creation was not required,
        //placement context should not be null
        if (placementContext == null) {
          String message =
              "Application " + applicationId + " submission by user : " + user
                  + " to specified queue : " + queueName + "  is prohibited. "
                  + "Verify automatic queue mapping for user exists in " +
                  QUEUE_MAPPING;
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                  message));
          return;
          // For a queue which exists already and
          // not auto-created above, then its parent queue should match
          // the parent queue specified in queue mapping
        } else if (!queue.getParent().getQueueName().equals(
            placementContext.getParentQueue())) {
          String message =
              "Auto created Leaf queue " + placementContext.getQueue() + " "
                  + "already exists under queue : " + queue
                  .getParent().getQueuePath()
                  + ".But Queue mapping configuration " +
                   CapacitySchedulerConfiguration.QUEUE_MAPPING + " has been "
                  + "updated to a different parent queue : "
                  + placementContext.getParentQueue()
                  + " for the specified user : " + user;
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                  message));
          return;
        }
      }

      // Submit to the queue
      try {
        queue.submitApplication(applicationId, user, queueName);
      } catch (AccessControlException ace) {
        LOG.info("Failed to submit application " + applicationId + " to queue "
            + queueName + " from user " + user, ace);
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED,
                ace.toString()));
        return;
      }
      // update the metrics
      queue.getMetrics().submitApp(user);
      SchedulerApplication<FiCaSchedulerApp> application =
          new SchedulerApplication<FiCaSchedulerApp>(queue, user, priority);
      applications.put(applicationId, application);
      LOG.info("Accepted application " + applicationId + " from user: " + user
          + ", in queue: " + queueName);
      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
    } finally {
      writeLock.unlock();
    }
  }

  private void addApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
    writeLock.lock();
    try {
      SchedulerApplication<FiCaSchedulerApp> application = applications.get(
          applicationAttemptId.getApplicationId());
      if (application == null) {
        LOG.warn("Application " + applicationAttemptId.getApplicationId()
            + " cannot be found in scheduler.");
        return;
      }
      CSQueue queue = (CSQueue) application.getQueue();

      FiCaSchedulerApp attempt = new FiCaSchedulerApp(applicationAttemptId,
          application.getUser(), queue, queue.getAbstractUsersManager(),
          rmContext, application.getPriority(), isAttemptRecovering,
          activitiesManager);
      if (transferStateFromPreviousAttempt) {
        attempt.transferStateFromPreviousAttempt(
            application.getCurrentAppAttempt());
      }
      application.setCurrentAppAttempt(attempt);

      // Update attempt priority to the latest to avoid race condition i.e
      // SchedulerApplicationAttempt is created with old priority but it is not
      // set to SchedulerApplication#setCurrentAppAttempt.
      // Scenario would occur is
      // 1. SchdulerApplicationAttempt is created with old priority.
      // 2. updateApplicationPriority() updates SchedulerApplication. Since
      // currentAttempt is null, it just return.
      // 3. ScheduelerApplcationAttempt is set in
      // SchedulerApplication#setCurrentAppAttempt.
      attempt.setPriority(application.getPriority());

      queue.submitApplicationAttempt(attempt, application.getUser());
      LOG.info("Added Application Attempt " + applicationAttemptId
          + " to scheduler from user " + application.getUser() + " in queue "
          + queue.getQueueName());
      if (isAttemptRecovering) {
        LOG.debug("{} is recovering. Skipping notifying ATTEMPT_ADDED",
            applicationAttemptId);
      } else{
        rmContext.getDispatcher().getEventHandler().handle(
            new RMAppAttemptEvent(applicationAttemptId,
                RMAppAttemptEventType.ATTEMPT_ADDED));
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void doneApplication(ApplicationId applicationId,
      RMAppState finalState) {
    writeLock.lock();
    try {
      SchedulerApplication<FiCaSchedulerApp> application = applications.get(
          applicationId);
      if (application == null) {
        // The AppRemovedSchedulerEvent maybe sent on recovery for completed
        // apps, ignore it.
        LOG.warn("Couldn't find application " + applicationId);
        return;
      }
      CSQueue queue = (CSQueue) application.getQueue();
      if (!(queue instanceof LeafQueue)) {
        LOG.error("Cannot finish application " + "from non-leaf queue: " + queue
            .getQueueName());
      } else{
        queue.finishApplication(applicationId, application.getUser());
      }
      application.stop(finalState);
      applications.remove(applicationId);
    } finally {
      writeLock.unlock();
    }
  }

  private void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
    writeLock.lock();
    try {
      LOG.info("Application Attempt " + applicationAttemptId + " is done."
          + " finalState=" + rmAppAttemptFinalState);

      FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
      SchedulerApplication<FiCaSchedulerApp> application = applications.get(
          applicationAttemptId.getApplicationId());

      if (application == null || attempt == null) {
        LOG.info(
            "Unknown application " + applicationAttemptId + " has completed!");
        return;
      }

      // Release all the allocated, acquired, running containers
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
      String queueName = attempt.getQueue().getQueueName();
      CSQueue queue = this.getQueue(queueName);
      if (!(queue instanceof LeafQueue)) {
        LOG.error(
            "Cannot finish application " + "from non-leaf queue: " + queueName);
      } else{
        queue.finishApplicationAttempt(attempt, queue.getQueueName());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Normalize a list of SchedulingRequest.
   *
   * @param asks scheduling request
   */
  private void normalizeSchedulingRequests(List<SchedulingRequest> asks) {
    if (asks == null) {
      return;
    }
    Resource maxAllocation = getMaximumResourceCapability();
    for (SchedulingRequest ask: asks) {
      ResourceSizing sizing = ask.getResourceSizing();
      if (sizing != null && sizing.getResources() != null) {
        sizing.setResources(
            getNormalizedResource(sizing.getResources(), maxAllocation));
      }
    }
  }

  @Override
  @Lock(Lock.NoLock.class)
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
      List<ContainerId> release, List<String> blacklistAdditions,
      List<String> blacklistRemovals, ContainerUpdates updateRequests) {
    FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      LOG.error("Calling allocate on removed or non existent application " +
          applicationAttemptId.getApplicationId());
      return EMPTY_ALLOCATION;
    }

    // The allocate may be the leftover from previous attempt, and it will
    // impact current attempt, such as confuse the request and allocation for
    // current attempt's AM container.
    // Note outside precondition check for the attempt id may be
    // outdated here, so double check it here is necessary.
    if (!application.getApplicationAttemptId().equals(applicationAttemptId)) {
      LOG.error("Calling allocate on previous or removed " +
          "or non existent application attempt " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Handle all container updates
    handleContainerUpdates(application, updateRequests);

    // Release containers
    releaseContainers(release, application);

    LeafQueue updateDemandForQueue = null;

    // Sanity check for new allocation requests
    normalizeResourceRequests(ask);

    // Normalize scheduling requests
    normalizeSchedulingRequests(schedulingRequests);

    Allocation allocation;

    // make sure we aren't stopping/removing the application
    // when the allocate comes in
    application.getWriteLock().lock();
    try {
      if (application.isStopped()) {
        return EMPTY_ALLOCATION;
      }

      // Process resource requests
      if (!ask.isEmpty() || (schedulingRequests != null && !schedulingRequests
          .isEmpty())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "allocate: pre-update " + applicationAttemptId + " ask size ="
                  + ask.size());
          application.showRequests();
        }

        // Update application requests
        if (application.updateResourceRequests(ask) || application
            .updateSchedulingRequests(schedulingRequests)) {
          updateDemandForQueue = (LeafQueue) application.getQueue();
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("allocate: post-update");
          application.showRequests();
        }
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);

      allocation = application.getAllocation(getResourceCalculator(),
          getClusterResource(), getMinimumResourceCapability());
    } finally {
      application.getWriteLock().unlock();
    }

    if (updateDemandForQueue != null && !application
        .isWaitingForAMContainer()) {
      updateDemandForQueue.getOrderingPolicy().demandUpdated(application);
    }

    LOG.debug("Allocation for application {} : {} with cluster resource : {}",
        applicationAttemptId, allocation, getClusterResource());
    return allocation;
  }

  @Override
  @Lock(Lock.NoLock.class)
  public QueueInfo getQueueInfo(String queueName,
      boolean includeChildQueues, boolean recursive)
  throws IOException {
    CSQueue queue = null;
    queue = this.getQueue(queueName);
    if (queue == null) {
      throw new IOException("Unknown queue: " + queueName);
    }
    return queue.getQueueInfo(includeChildQueues, recursive);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    UserGroupInformation user = null;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      // should never happen
      return new ArrayList<QueueUserACLInfo>();
    }

    return getRootQueue().getQueueUserAclInfo(user);
  }

  @Override
  protected void nodeUpdate(RMNode rmNode) {
    long begin = System.nanoTime();
    readLock.lock();
    try {
      setLastNodeUpdateTime(Time.now());
      super.nodeUpdate(rmNode);
    } finally {
      readLock.unlock();
    }

    // Try to do scheduling
    if (!scheduleAsynchronously) {
      writeLock.lock();
      try {
        // reset allocation and reservation stats before we start doing any
        // work
        updateSchedulerHealth(lastNodeUpdateTime, rmNode.getNodeID(),
            CSAssignment.NULL_ASSIGNMENT);

        allocateContainersToNode(rmNode.getNodeID(), true);
      } finally {
        writeLock.unlock();
      }
    }

    long latency = System.nanoTime() - begin;
    CapacitySchedulerMetrics.getMetrics().addNodeUpdate(latency);
  }

  /**
   * Process resource update on a node.
   */
  private void updateNodeAndQueueResource(RMNode nm,
      ResourceOption resourceOption) {
    writeLock.lock();
    try {
      updateNodeResource(nm, resourceOption);
      Resource clusterResource = getClusterResource();
      getRootQueue().updateClusterResource(clusterResource,
          new ResourceLimits(clusterResource));
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Process node labels update on a node.
   */
  private void updateLabelsOnNode(NodeId nodeId,
      Set<String> newLabels) {
    FiCaSchedulerNode node = nodeTracker.getNode(nodeId);
    if (null == node) {
      return;
    }

    // Get new partition, we have only one partition per node
    String newPartition;
    if (newLabels.isEmpty()) {
      newPartition = RMNodeLabelsManager.NO_LABEL;
    } else{
      newPartition = newLabels.iterator().next();
    }

    // old partition as well
    String oldPartition = node.getPartition();

    // Update resources of these containers
    for (RMContainer rmContainer : node.getCopiedListOfRunningContainers()) {
      FiCaSchedulerApp application = getApplicationAttempt(
          rmContainer.getApplicationAttemptId());
      if (null != application) {
        application.nodePartitionUpdated(rmContainer, oldPartition,
            newPartition);
      } else{
        LOG.warn("There's something wrong, some RMContainers running on"
            + " a node, but we cannot find SchedulerApplicationAttempt "
            + "for it. Node=" + node.getNodeID() + " applicationAttemptId="
            + rmContainer.getApplicationAttemptId());
        continue;
      }
    }

    // Unreserve container on this node
    RMContainer reservedContainer = node.getReservedContainer();
    if (null != reservedContainer) {
      killReservedContainer(reservedContainer);
    }

    // Update node labels after we've done this
    node.updateLabels(newLabels);
  }

  private void updateSchedulerHealth(long now, NodeId nodeId,
      CSAssignment assignment) {
    List<AssignmentInformation.AssignmentDetails> allocations =
        assignment.getAssignmentInformation().getAllocationDetails();
    List<AssignmentInformation.AssignmentDetails> reservations =
        assignment.getAssignmentInformation().getReservationDetails();
    // Get nodeId from allocated container if incoming argument is null.
    NodeId updatedNodeid = (nodeId == null)
        ? allocations.get(allocations.size() - 1).rmContainer.getNodeId()
        : nodeId;

    if (!allocations.isEmpty()) {
      ContainerId allocatedContainerId =
          allocations.get(allocations.size() - 1).containerId;
      String allocatedQueue = allocations.get(allocations.size() - 1).queue;
      schedulerHealth.updateAllocation(now, updatedNodeid, allocatedContainerId,
        allocatedQueue);
    }
    if (!reservations.isEmpty()) {
      ContainerId reservedContainerId =
          reservations.get(reservations.size() - 1).containerId;
      String reservedQueue = reservations.get(reservations.size() - 1).queue;
      schedulerHealth.updateReservation(now, updatedNodeid, reservedContainerId,
        reservedQueue);
    }
    schedulerHealth.updateSchedulerReservationCounts(assignment
      .getAssignmentInformation().getNumReservations());
    schedulerHealth.updateSchedulerAllocationCounts(assignment
      .getAssignmentInformation().getNumAllocations());
    schedulerHealth.updateSchedulerRunDetails(now, assignment
      .getAssignmentInformation().getAllocated(), assignment
      .getAssignmentInformation().getReserved());
  }

  private boolean canAllocateMore(CSAssignment assignment, int offswitchCount,
      int assignedContainers) {
    // Current assignment shouldn't be empty
    if (assignment == null
            || Resources.equals(assignment.getResource(), Resources.none())) {
      return false;
    }

    // offswitch assignment should be under threshold
    if (offswitchCount >= offswitchPerHeartbeatLimit) {
      return false;
    }

    // And it should not be a reserved container
    if (assignment.getAssignmentInformation().getNumReservations() > 0) {
      return false;
    }

    // assignMultipleEnabled should be ON,
    // and assignedContainers should be under threshold
    return assignMultipleEnabled
        && (maxAssignPerHeartbeat == -1
            || assignedContainers < maxAssignPerHeartbeat);
  }

  private CandidateNodeSet<FiCaSchedulerNode> getCandidateNodeSet(
      FiCaSchedulerNode node) {
    CandidateNodeSet<FiCaSchedulerNode> candidates = null;
    candidates = new SimpleCandidateNodeSet<>(node);
    if (multiNodePlacementEnabled) {
      Map<NodeId, FiCaSchedulerNode> nodesByPartition = new HashMap<>();
      List<FiCaSchedulerNode> nodes = nodeTracker
          .getNodesPerPartition(node.getPartition());
      if (nodes != null && !nodes.isEmpty()) {
        nodes.forEach(n -> nodesByPartition.put(n.getNodeID(), n));
        candidates = new SimpleCandidateNodeSet<FiCaSchedulerNode>(
            nodesByPartition, node.getPartition());
      }
    }
    return candidates;
  }

  /**
   * We need to make sure when doing allocation, Node should be existed
   * And we will construct a {@link CandidateNodeSet} before proceeding
   */
  private void allocateContainersToNode(NodeId nodeId,
      boolean withNodeHeartbeat) {
    FiCaSchedulerNode node = getNode(nodeId);
    if (null != node) {
      int offswitchCount = 0;
      int assignedContainers = 0;

      CandidateNodeSet<FiCaSchedulerNode> candidates = getCandidateNodeSet(
          node);
      CSAssignment assignment = allocateContainersToNode(candidates,
          withNodeHeartbeat);
      // Only check if we can allocate more container on the same node when
      // scheduling is triggered by node heartbeat
      if (null != assignment && withNodeHeartbeat) {
        if (assignment.getType() == NodeType.OFF_SWITCH) {
          offswitchCount++;
        }

        if (Resources.greaterThan(calculator, getClusterResource(),
            assignment.getResource(), Resources.none())) {
          assignedContainers++;
        }

        while (canAllocateMore(assignment, offswitchCount,
            assignedContainers)) {
          // Try to see if it is possible to allocate multiple container for
          // the same node heartbeat
          assignment = allocateContainersToNode(candidates, true);

          if (null != assignment
              && assignment.getType() == NodeType.OFF_SWITCH) {
            offswitchCount++;
          }

          if (null != assignment
              && Resources.greaterThan(calculator, getClusterResource(),
                  assignment.getResource(), Resources.none())) {
            assignedContainers++;
          }
        }

        if (offswitchCount >= offswitchPerHeartbeatLimit) {
          LOG.debug("Assigned maximum number of off-switch containers: {},"
              + " assignments so far: {}", offswitchCount, assignment);
        }
      }
    }
  }

  /*
   * Logics of allocate container on a single node (Old behavior)
   */
  private CSAssignment allocateContainerOnSingleNode(
      CandidateNodeSet<FiCaSchedulerNode> candidates, FiCaSchedulerNode node,
      boolean withNodeHeartbeat) {
    LOG.debug("Trying to schedule on node: {}, available: {}",
        node.getNodeName(), node.getUnallocatedResource());

    // Backward compatible way to make sure previous behavior which allocation
    // driven by node heartbeat works.
    if (getNode(node.getNodeID()) != node) {
      LOG.error("Trying to schedule on a removed node, please double check, "
          + "nodeId=" + node.getNodeID());
      return null;
    }

    CSAssignment assignment;

    // Assign new containers...
    // 1. Check for reserved applications
    // 2. Schedule if there are no reservations
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp reservedApplication = getCurrentAttemptForContainer(
          reservedContainer.getContainerId());
      if (reservedApplication == null) {
        LOG.error(
            "Trying to schedule for a finished app, please double check. nodeId="
                + node.getNodeID() + " container=" + reservedContainer
                .getContainerId());
        return null;
      }

      // Try to fulfill the reservation
      LOG.debug("Trying to fulfill reservation for application {} on node: {}",
          reservedApplication.getApplicationId(), node.getNodeID());

      LeafQueue queue = ((LeafQueue) reservedApplication.getQueue());
      assignment = queue.assignContainers(getClusterResource(), candidates,
          // TODO, now we only consider limits for parent for non-labeled
          // resources, should consider labeled resources as well.
          new ResourceLimits(labelManager
              .getResourceByLabel(RMNodeLabelsManager.NO_LABEL,
                  getClusterResource())),
          SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);

      if (assignment.isFulfilledReservation()) {
        if (withNodeHeartbeat) {
          // Only update SchedulerHealth in sync scheduling, existing
          // Data structure of SchedulerHealth need to be updated for
          // Async mode
          updateSchedulerHealth(lastNodeUpdateTime, node.getNodeID(),
              assignment);
        }

        schedulerHealth.updateSchedulerFulfilledReservationCounts(1);

        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            queue.getParent().getQueueName(), queue.getQueueName(),
            ActivityState.ACCEPTED, ActivityDiagnosticConstant.EMPTY);
        ActivitiesLogger.NODE.finishAllocatedNodeAllocation(activitiesManager,
            node, reservedContainer.getContainerId(),
            AllocationState.ALLOCATED_FROM_RESERVED);
      } else{
        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            queue.getParent().getQueueName(), queue.getQueueName(),
            ActivityState.ACCEPTED, ActivityDiagnosticConstant.EMPTY);
        ActivitiesLogger.NODE.finishAllocatedNodeAllocation(activitiesManager,
            node, reservedContainer.getContainerId(), AllocationState.SKIPPED);
      }

      assignment.setSchedulingMode(
          SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      submitResourceCommitRequest(getClusterResource(), assignment);
    }

    // Do not schedule if there are any reservations to fulfill on the node
    if (node.getReservedContainer() != null) {
      LOG.debug("Skipping scheduling since node {} is reserved by"
          + " application {}", node.getNodeID(), node.getReservedContainer().
          getContainerId().getApplicationAttemptId());
      return null;
    }

    // First check if we can schedule
    // When this time look at one node only, try schedule if the node
    // has any available or killable resource
    if (calculator.computeAvailableContainers(Resources
            .add(node.getUnallocatedResource(), node.getTotalKillableResources()),
        minimumAllocation) <= 0) {
      LOG.debug("This node or node partition doesn't have available or" +
          " preemptible resource");
      return null;
    }

    return allocateOrReserveNewContainers(candidates, withNodeHeartbeat);
  }

  private CSAssignment allocateOrReserveNewContainers(
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      boolean withNodeHeartbeat) {
    CSAssignment assignment = getRootQueue().assignContainers(
        getClusterResource(), candidates, new ResourceLimits(labelManager
            .getResourceByLabel(candidates.getPartition(),
                getClusterResource())),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);

    assignment.setSchedulingMode(SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    submitResourceCommitRequest(getClusterResource(), assignment);

    if (Resources.greaterThan(calculator, getClusterResource(),
        assignment.getResource(), Resources.none())) {
      FiCaSchedulerNode node = CandidateNodeSetUtils.getSingleNode(candidates);
      NodeId nodeId = null;
      if (node != null) {
        nodeId = node.getNodeID();
      }
      if (withNodeHeartbeat) {
        updateSchedulerHealth(lastNodeUpdateTime, nodeId, assignment);
      }
      return assignment;
    }

    // Only do non-exclusive allocation when node has node-labels.
    if (StringUtils.equals(candidates.getPartition(),
        RMNodeLabelsManager.NO_LABEL)) {
      return null;
    }

    // Only do non-exclusive allocation when the node-label supports that
    try {
      if (rmContext.getNodeLabelManager().isExclusiveNodeLabel(
          candidates.getPartition())) {
        return null;
      }
    } catch (IOException e) {
      LOG.warn(
          "Exception when trying to get exclusivity of node label=" + candidates
          .getPartition(), e);
      return null;
    }

    // Try to use NON_EXCLUSIVE
    assignment = getRootQueue().assignContainers(getClusterResource(),
        candidates,
        // TODO, now we only consider limits for parent for non-labeled
        // resources, should consider labeled resources as well.
        new ResourceLimits(labelManager
            .getResourceByLabel(RMNodeLabelsManager.NO_LABEL,
                getClusterResource())),
        SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY);
    assignment.setSchedulingMode(SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY);
    submitResourceCommitRequest(getClusterResource(), assignment);

    return assignment;
  }

  /*
   * New behavior, allocate containers considering multiple nodes
   */
  private CSAssignment allocateContainersOnMultiNodes(
      CandidateNodeSet<FiCaSchedulerNode> candidates) {
    // When this time look at multiple nodes, try schedule if the
    // partition has any available resource or killable resource
    if (getRootQueue().getQueueCapacities().getUsedCapacity(
        candidates.getPartition()) >= 1.0f
        && preemptionManager.getKillableResource(
        CapacitySchedulerConfiguration.ROOT, candidates.getPartition())
        == Resources.none()) {
      LOG.debug("This node or this node partition doesn't have available or"
          + "killable resource");
      return null;
    }

    return allocateOrReserveNewContainers(candidates, false);
  }

  @VisibleForTesting
  CSAssignment allocateContainersToNode(
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      boolean withNodeHeartbeat) {
    if (rmContext.isWorkPreservingRecoveryEnabled() && !rmContext
        .isSchedulerReadyForAllocatingContainers()) {
      return null;
    }

    long startTime = System.nanoTime();

    // Backward compatible way to make sure previous behavior which allocation
    // driven by node heartbeat works.
    FiCaSchedulerNode node = CandidateNodeSetUtils.getSingleNode(candidates);

    // We have two different logics to handle allocation on single node / multi
    // nodes.
    CSAssignment assignment;
    if (!multiNodePlacementEnabled) {
      ActivitiesLogger.NODE.startNodeUpdateRecording(activitiesManager,
          node.getNodeID());
      assignment = allocateContainerOnSingleNode(candidates,
          node, withNodeHeartbeat);
      ActivitiesLogger.NODE.finishNodeUpdateRecording(activitiesManager,
          node.getNodeID());
    } else{
      ActivitiesLogger.NODE.startNodeUpdateRecording(activitiesManager,
          ActivitiesManager.EMPTY_NODE_ID);
      assignment = allocateContainersOnMultiNodes(candidates);
      ActivitiesLogger.NODE.finishNodeUpdateRecording(activitiesManager,
          ActivitiesManager.EMPTY_NODE_ID);
    }

    if (assignment != null && assignment.getAssignmentInformation() != null
        && assignment.getAssignmentInformation().getNumAllocations() > 0) {
      long allocateTime = System.nanoTime() - startTime;
      CapacitySchedulerMetrics.getMetrics().addAllocate(allocateTime);
    }
    return assignment;
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch(event.getType()) {
    case NODE_ADDED:
    {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
      recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
        nodeAddedEvent.getAddedRMNode());
    }
    break;
    case NODE_REMOVED:
    {
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
    }
    break;
    case NODE_RESOURCE_UPDATE:
    {
      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
          (NodeResourceUpdateSchedulerEvent)event;
      updateNodeAndQueueResource(nodeResourceUpdatedEvent.getRMNode(),
        nodeResourceUpdatedEvent.getResourceOption());
    }
    break;
    case NODE_LABELS_UPDATE:
    {
      NodeLabelsUpdateSchedulerEvent labelUpdateEvent =
          (NodeLabelsUpdateSchedulerEvent) event;

      updateNodeLabelsAndQueueResource(labelUpdateEvent);
    }
    break;
    case NODE_ATTRIBUTES_UPDATE:
    {
      NodeAttributesUpdateSchedulerEvent attributeUpdateEvent =
          (NodeAttributesUpdateSchedulerEvent) event;

      updateNodeAttributes(attributeUpdateEvent);
    }
    break;
    case NODE_UPDATE:
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode());
    }
    break;
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      String queueName = resolveReservationQueueName(appAddedEvent.getQueue(),
          appAddedEvent.getApplicationId(), appAddedEvent.getReservationID(),
          appAddedEvent.getIsAppRecovering());
      if (queueName != null) {
        if (!appAddedEvent.getIsAppRecovering()) {
          addApplication(appAddedEvent.getApplicationId(), queueName,
              appAddedEvent.getUser(), appAddedEvent.getApplicatonPriority(),
              appAddedEvent.getPlacementContext());
        } else {
          addApplicationOnRecovery(appAddedEvent.getApplicationId(), queueName,
              appAddedEvent.getUser(), appAddedEvent.getApplicatonPriority(),
              appAddedEvent.getPlacementContext());
        }
      }
    }
    break;
    case APP_REMOVED:
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      doneApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
    }
    break;
    case APP_ATTEMPT_ADDED:
    {
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
        appAttemptAddedEvent.getIsAttemptRecovering());
    }
    break;
    case APP_ATTEMPT_REMOVED:
    {
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      doneApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
        appAttemptRemovedEvent.getFinalAttemptState(),
        appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
    }
    break;
    case CONTAINER_EXPIRED:
    {
      ContainerExpiredSchedulerEvent containerExpiredEvent =
          (ContainerExpiredSchedulerEvent) event;
      ContainerId containerId = containerExpiredEvent.getContainerId();
      if (containerExpiredEvent.isIncrease()) {
        rollbackContainerUpdate(containerId);
      } else {
        completedContainer(getRMContainer(containerId),
            SchedulerUtils.createAbnormalContainerStatus(
                containerId,
                SchedulerUtils.EXPIRED_CONTAINER),
            RMContainerEventType.EXPIRE);
      }
    }
    break;
    case RELEASE_CONTAINER:
    {
      RMContainer container = ((ReleaseContainerEvent) event).getContainer();
      completedContainer(container,
          SchedulerUtils.createAbnormalContainerStatus(
            container.getContainerId(),
            SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
    }
    break;
    case KILL_RESERVED_CONTAINER:
    {
      ContainerPreemptEvent killReservedContainerEvent =
          (ContainerPreemptEvent) event;
      RMContainer container = killReservedContainerEvent.getContainer();
      killReservedContainer(container);
    }
    break;
    case MARK_CONTAINER_FOR_PREEMPTION:
    {
      ContainerPreemptEvent preemptContainerEvent =
          (ContainerPreemptEvent)event;
      ApplicationAttemptId aid = preemptContainerEvent.getAppId();
      RMContainer containerToBePreempted = preemptContainerEvent.getContainer();
      markContainerForPreemption(aid, containerToBePreempted);
    }
    break;
    case MARK_CONTAINER_FOR_KILLABLE:
    {
      ContainerPreemptEvent containerKillableEvent = (ContainerPreemptEvent)event;
      RMContainer killableContainer = containerKillableEvent.getContainer();
      markContainerForKillable(killableContainer);
    }
    break;
    case MARK_CONTAINER_FOR_NONKILLABLE:
    {
      if (isLazyPreemptionEnabled) {
        ContainerPreemptEvent cancelKillContainerEvent =
            (ContainerPreemptEvent) event;
        markContainerForNonKillable(cancelKillContainerEvent.getContainer());
      }
    }
    break;
    case MANAGE_QUEUE:
    {
      QueueManagementChangeEvent queueManagementChangeEvent =
          (QueueManagementChangeEvent) event;
      ParentQueue parentQueue = queueManagementChangeEvent.getParentQueue();
      try {
        final List<QueueManagementChange> queueManagementChanges =
            queueManagementChangeEvent.getQueueManagementChanges();
        ((ManagedParentQueue) parentQueue)
            .validateAndApplyQueueManagementChanges(queueManagementChanges);
      } catch (SchedulerDynamicEditException sde) {
        LOG.error("Queue Management Change event cannot be applied for "
            + "parent queue : " + parentQueue.getQueueName(), sde);
      } catch (IOException ioe) {
        LOG.error("Queue Management Change event cannot be applied for "
            + "parent queue : " + parentQueue.getQueueName(), ioe);
      }
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  private void updateNodeAttributes(
      NodeAttributesUpdateSchedulerEvent attributeUpdateEvent) {
    writeLock.lock();
    try {
      for (Entry<String, Set<NodeAttribute>> entry : attributeUpdateEvent
          .getUpdatedNodeToAttributes().entrySet()) {
        String hostname = entry.getKey();
        Set<NodeAttribute> attributes = entry.getValue();
        List<NodeId> nodeIds = nodeTracker.getNodeIdsByResourceName(hostname);
        updateAttributesOnNode(nodeIds, attributes);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void updateAttributesOnNode(List<NodeId> nodeIds,
      Set<NodeAttribute> attributes) {
    nodeIds.forEach((k) -> {
      SchedulerNode node = nodeTracker.getNode(k);
      node.updateNodeAttributes(attributes);
    });
  }

  /**
   * Process node labels update.
   */
  private void updateNodeLabelsAndQueueResource(
      NodeLabelsUpdateSchedulerEvent labelUpdateEvent) {
    writeLock.lock();
    try {
      Set<String> updateLabels = new HashSet<String>();
      for (Entry<NodeId, Set<String>> entry : labelUpdateEvent
          .getUpdatedNodeToLabels().entrySet()) {
        NodeId id = entry.getKey();
        Set<String> labels = entry.getValue();
        FiCaSchedulerNode node = nodeTracker.getNode(id);

        if (node != null) {
          // Update old partition to list.
          updateLabels.add(node.getPartition());
        }
        updateLabelsOnNode(id, labels);
        updateLabels.addAll(labels);
      }
      refreshLabelToNodeCache(updateLabels);
      Resource clusterResource = getClusterResource();
      getRootQueue().updateClusterResource(clusterResource,
          new ResourceLimits(clusterResource));
    } finally {
      writeLock.unlock();
    }
  }

  private void refreshLabelToNodeCache(Set<String> updateLabels) {
    Map<String, Set<NodeId>> labelMapping = labelManager
        .getLabelsToNodes(updateLabels);
    for (String label : updateLabels) {
      Set<NodeId> nodes = labelMapping.get(label);
      if (nodes == null) {
        continue;
      }
      nodeTracker.updateNodesPerPartition(label, nodes);
    }
  }

  private void addNode(RMNode nodeManager) {
    writeLock.lock();
    try {
      FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager,
          usePortForNodeName, nodeManager.getNodeLabels());
      nodeTracker.addNode(schedulerNode);

      // update this node to node label manager
      if (labelManager != null) {
        labelManager.activateNode(nodeManager.getNodeID(),
            schedulerNode.getTotalResource());
      }

      // recover attributes from store if any.
      if (rmContext.getNodeAttributesManager() != null) {
        rmContext.getNodeAttributesManager()
            .refreshNodeAttributesToScheduler(schedulerNode.getNodeID());
      }

      Resource clusterResource = getClusterResource();
      getRootQueue().updateClusterResource(clusterResource,
          new ResourceLimits(clusterResource));

      LOG.info(
          "Added node " + nodeManager.getNodeAddress() + " clusterResource: "
              + clusterResource);

      if (scheduleAsynchronously && getNumClusterNodes() == 1) {
        for (AsyncScheduleThread t : asyncSchedulerThreads) {
          t.beginSchedule();
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void removeNode(RMNode nodeInfo) {
    writeLock.lock();
    try {
      // update this node to node label manager
      if (labelManager != null) {
        labelManager.deactivateNode(nodeInfo.getNodeID());
      }

      NodeId nodeId = nodeInfo.getNodeID();
      FiCaSchedulerNode node = nodeTracker.getNode(nodeId);
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
      getRootQueue().updateClusterResource(clusterResource,
          new ResourceLimits(clusterResource));
      int numNodes = nodeTracker.nodeCount();

      if (scheduleAsynchronously && numNodes == 0) {
        for (AsyncScheduleThread t : asyncSchedulerThreads) {
          t.suspendSchedule();
        }
      }

      LOG.info(
          "Removed node " + nodeInfo.getNodeAddress() + " clusterResource: "
              + getClusterResource());
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  protected void completedContainerInternal(
      RMContainer rmContainer, ContainerStatus containerStatus,
      RMContainerEventType event) {
    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();

    // Get the application for the finished container
    FiCaSchedulerApp application = getCurrentAttemptForContainer(
        container.getId());
    ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();
    if (application == null) {
      LOG.info(
          "Container " + container + " of" + " finished application " + appId
              + " completed with event " + event);
      return;
    }

    // Get the node on which the container was allocated
    FiCaSchedulerNode node = getNode(container.getNodeId());
    if (null == node) {
      LOG.info("Container " + container + " of" + " removed node " + container
          .getNodeId() + " completed with event " + event);
      return;
    }

    // Inform the queue
    LeafQueue queue = (LeafQueue) application.getQueue();
    queue.completedContainer(getClusterResource(), application, node,
        rmContainer, containerStatus, event, null, true);
  }

  @Lock(Lock.NoLock.class)
  @VisibleForTesting
  @Override
  public FiCaSchedulerApp getApplicationAttempt(
      ApplicationAttemptId applicationAttemptId) {
    return super.getApplicationAttempt(applicationAttemptId);
  }

  @Lock(Lock.NoLock.class)
  public FiCaSchedulerNode getNode(NodeId nodeId) {
    return nodeTracker.getNode(nodeId);
  }

  @Lock(Lock.NoLock.class)
  public List<FiCaSchedulerNode> getAllNodes() {
    return nodeTracker.getAllNodes();
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void recover(RMState state) throws Exception {
    // NOT IMPLEMENTED
  }

  @Override
  public void killReservedContainer(RMContainer container) {
    LOG.debug("{}:{}", SchedulerEventType.KILL_RESERVED_CONTAINER, container);

    // To think: What happens if this is no longer a reserved container, for
    // e.g if the reservation became an allocation.
    super.completedContainer(container,
        SchedulerUtils.createAbnormalContainerStatus(
            container.getContainerId(),
            SchedulerUtils.UNRESERVED_CONTAINER),
        RMContainerEventType.KILL);
  }

  @Override
  public void markContainerForPreemption(ApplicationAttemptId aid,
      RMContainer cont) {
    LOG.debug("{}: appAttempt:{} container:{}",
        SchedulerEventType.MARK_CONTAINER_FOR_PREEMPTION, aid, cont);
    FiCaSchedulerApp app = getApplicationAttempt(aid);
    if (app != null) {
      app.markContainerForPreemption(cont.getContainerId());
    }
  }

  @VisibleForTesting
  @Override
  public void killContainer(RMContainer container) {
    markContainerForKillable(container);
  }

  public void markContainerForKillable(
      RMContainer killableContainer) {
    writeLock.lock();
    try {
      LOG.debug("{}: container {}",
          SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE, killableContainer);

      if (!isLazyPreemptionEnabled) {
        super.completedContainer(killableContainer, SchedulerUtils
            .createPreemptedContainerStatus(killableContainer.getContainerId(),
                SchedulerUtils.PREEMPTED_CONTAINER), RMContainerEventType.KILL);
      } else {
        FiCaSchedulerNode node = getSchedulerNode(
            killableContainer.getAllocatedNode());

        FiCaSchedulerApp application = getCurrentAttemptForContainer(
            killableContainer.getContainerId());

        node.markContainerToKillable(killableContainer.getContainerId());

        // notify PreemptionManager
        // Get the application for the finished container
        if (null != application) {
          String leafQueueName = application.getCSLeafQueue().getQueueName();
          getPreemptionManager().addKillableContainer(
              new KillableContainer(killableContainer, node.getPartition(),
                  leafQueueName));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void markContainerForNonKillable(
      RMContainer nonKillableContainer) {
    writeLock.lock();
    try {
      LOG.debug("{}: container {}", SchedulerEventType.
          MARK_CONTAINER_FOR_NONKILLABLE, nonKillableContainer);

      FiCaSchedulerNode node = getSchedulerNode(
          nonKillableContainer.getAllocatedNode());

      FiCaSchedulerApp application = getCurrentAttemptForContainer(
          nonKillableContainer.getContainerId());

      node.markContainerToNonKillable(nonKillableContainer.getContainerId());

      // notify PreemptionManager
      // Get the application for the finished container
      if (null != application) {
        String leafQueueName = application.getCSLeafQueue().getQueueName();
        getPreemptionManager().removeKillableContainer(
            new KillableContainer(nonKillableContainer, node.getPartition(),
                leafQueueName));
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      LOG.debug("ACL not found for queue access-type {} for queue {}",
          acl, queueName);
      return false;
    }
    return queue.hasAccess(acl, callerUGI);
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      return null;
    }
    List<ApplicationAttemptId> apps = new ArrayList<ApplicationAttemptId>();
    queue.collectSchedulerApplications(apps);
    return apps;
  }

  public boolean isSystemAppsLimitReached() {
    if (getRootQueue().getNumApplications() < conf
        .getMaximumSystemApplications()) {
      return false;
    }
    return true;
  }

  private String getDefaultReservationQueueName(String planQueueName) {
    return planQueueName + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
  }

  private String resolveReservationQueueName(String queueName,
      ApplicationId applicationId, ReservationId reservationID,
      boolean isRecovering) {
    readLock.lock();
    try {
      CSQueue queue = getQueue(queueName);
      // Check if the queue is a plan queue
      if ((queue == null) || !(queue instanceof PlanQueue)) {
        return queueName;
      }
      if (reservationID != null) {
        String resQName = reservationID.toString();
        queue = getQueue(resQName);
        if (queue == null) {
          // reservation has terminated during failover
          if (isRecovering && conf.getMoveOnExpiry(
              getQueue(queueName).getQueuePath())) {
            // move to the default child queue of the plan
            return getDefaultReservationQueueName(queueName);
          }
          String message = "Application " + applicationId
              + " submitted to a reservation which is not currently active: "
              + resQName;
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
        queueName = getDefaultReservationQueueName(queueName);
      }
      return queueName;
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public void removeQueue(String queueName)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      LOG.info("Removing queue: " + queueName);
      CSQueue q = this.getQueue(queueName);
      if (!(AbstractAutoCreatedLeafQueue.class.isAssignableFrom(
          q.getClass()))) {
        throw new SchedulerDynamicEditException(
            "The queue that we are asked " + "to remove (" + queueName
                + ") is not a AutoCreatedLeafQueue or ReservationQueue");
      }
      AbstractAutoCreatedLeafQueue disposableLeafQueue =
          (AbstractAutoCreatedLeafQueue) q;
      // at this point we should have no more apps
      if (disposableLeafQueue.getNumApplications() > 0) {
        throw new SchedulerDynamicEditException(
            "The queue " + queueName + " is not empty " + disposableLeafQueue
                .getApplications().size() + " active apps "
                + disposableLeafQueue.getPendingApplications().size()
                + " pending apps");
      }

      ((AbstractManagedParentQueue) disposableLeafQueue.getParent())
          .removeChildQueue(q);
      this.queueManager.removeQueue(queueName);
      LOG.info(
          "Removal of AutoCreatedLeafQueue " + queueName + " has succeeded");
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addQueue(Queue queue)
      throws SchedulerDynamicEditException, IOException {
    writeLock.lock();
    try {
      if (queue == null) {
        throw new SchedulerDynamicEditException(
            "Queue specified is null. Should be an implementation of "
                + "AbstractAutoCreatedLeafQueue");
      } else if (!(AbstractAutoCreatedLeafQueue.class
          .isAssignableFrom(queue.getClass()))) {
        throw new SchedulerDynamicEditException(
            "Queue is not an implementation of "
                + "AbstractAutoCreatedLeafQueue : " + queue.getClass());
      }

      AbstractAutoCreatedLeafQueue newQueue =
          (AbstractAutoCreatedLeafQueue) queue;

      if (newQueue.getParent() == null || !(AbstractManagedParentQueue.class.
          isAssignableFrom(newQueue.getParent().getClass()))) {
        throw new SchedulerDynamicEditException(
            "ParentQueue for " + newQueue + " is not properly set"
                + " (should be set and be a PlanQueue or ManagedParentQueue)");
      }

      AbstractManagedParentQueue parent =
          (AbstractManagedParentQueue) newQueue.getParent();
      String queuename = newQueue.getQueueName();
      parent.addChildQueue(newQueue);
      this.queueManager.addQueue(queuename, newQueue);

      LOG.info("Creation of AutoCreatedLeafQueue " + newQueue + " succeeded");
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void setEntitlement(String inQueue, QueueEntitlement entitlement)
      throws YarnException {
    writeLock.lock();
    try {
      LeafQueue queue = this.queueManager.getAndCheckLeafQueue(inQueue);
      AbstractManagedParentQueue parent =
          (AbstractManagedParentQueue) queue.getParent();

      if (!(AbstractAutoCreatedLeafQueue.class.isAssignableFrom(
          queue.getClass()))) {
        throw new SchedulerDynamicEditException(
            "Entitlement can not be" + " modified dynamically since queue "
                + inQueue + " is not a AutoCreatedLeafQueue");
      }

      if (parent == null || !(AbstractManagedParentQueue.class.isAssignableFrom(
          parent.getClass()))) {
        throw new SchedulerDynamicEditException(
            "The parent of AutoCreatedLeafQueue " + inQueue
                + " must be a PlanQueue/ManagedParentQueue");
      }

      AbstractAutoCreatedLeafQueue newQueue =
          (AbstractAutoCreatedLeafQueue) queue;
      parent.validateQueueEntitlementChange(newQueue, entitlement);

      newQueue.setEntitlement(entitlement);

      LOG.info("Set entitlement for AutoCreatedLeafQueue " + inQueue + "  to "
          + queue.getCapacity() + " request was (" + entitlement.getCapacity()
          + ")");
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String moveApplication(ApplicationId appId,
      String targetQueueName) throws YarnException {
    writeLock.lock();
    try {
      SchedulerApplication<FiCaSchedulerApp> application =
          applications.get(appId);
      if (application == null) {
        throw new YarnException("App to be moved " + appId + " not found.");
      }
      String sourceQueueName = application.getQueue().getQueueName();
      LeafQueue source =
          this.queueManager.getAndCheckLeafQueue(sourceQueueName);
      String destQueueName = handleMoveToPlanQueue(targetQueueName);
      LeafQueue dest = this.queueManager.getAndCheckLeafQueue(destQueueName);

      String user = application.getUser();
      try {
        dest.submitApplication(appId, user, destQueueName);
      } catch (AccessControlException e) {
        throw new YarnException(e);
      }

      FiCaSchedulerApp app = application.getCurrentAppAttempt();
      if (app != null) {
        // Move all live containers even when stopped.
        // For transferStateFromPreviousAttempt required
        for (RMContainer rmContainer : app.getLiveContainers()) {
          source.detachContainer(getClusterResource(), app, rmContainer);
          // attach the Container to another queue
          dest.attachContainer(getClusterResource(), app, rmContainer);
        }
        if (!app.isStopped()) {
          source.finishApplicationAttempt(app, sourceQueueName);
          // Submit to a new queue
          dest.submitApplicationAttempt(app, user);
        }
        // Finish app & update metrics
        app.move(dest);
      }
      source.appFinished();
      // Detach the application..
      source.getParent().finishApplication(appId, user);
      application.setQueue(dest);
      LOG.info("App: " + appId + " successfully moved from " + sourceQueueName
          + " to: " + destQueueName);
      return targetQueueName;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void preValidateMoveApplication(ApplicationId appId,
      String newQueue) throws YarnException {
    writeLock.lock();
    try {
      SchedulerApplication<FiCaSchedulerApp> application =
          applications.get(appId);
      if (application == null) {
        throw new YarnException("App to be moved " + appId + " not found.");
      }
      String sourceQueueName = application.getQueue().getQueueName();
      this.queueManager.getAndCheckLeafQueue(sourceQueueName);
      String destQueueName = handleMoveToPlanQueue(newQueue);
      LeafQueue dest = this.queueManager.getAndCheckLeafQueue(destQueueName);
      // Validation check - ACLs, submission limits for user & queue
      String user = application.getUser();
      // Check active partition only when attempt is available
      FiCaSchedulerApp appAttempt =
          getApplicationAttempt(ApplicationAttemptId.newInstance(appId, 0));
      if (null != appAttempt) {
        checkQueuePartition(appAttempt, dest);
      }
      try {
        dest.validateSubmitApplication(appId, user, destQueueName);
      } catch (AccessControlException e) {
        throw new YarnException(e);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Check application can be moved to queue with labels enabled. All labels in
   * application life time will be checked
   *
   * @param app
   * @param dest
   * @throws YarnException
   */
  private void checkQueuePartition(FiCaSchedulerApp app, LeafQueue dest)
      throws YarnException {
    if (!YarnConfiguration.areNodeLabelsEnabled(conf)) {
      return;
    }
    Set<String> targetqueuelabels = dest.getAccessibleNodeLabels();
    AppSchedulingInfo schedulingInfo = app.getAppSchedulingInfo();
    Set<String> appLabelexpressions = schedulingInfo.getRequestedPartitions();
    // default partition access always available remove empty label
    appLabelexpressions.remove(RMNodeLabelsManager.NO_LABEL);
    Set<String> nonAccessiblelabels = new HashSet<String>();
    for (String label : appLabelexpressions) {
      if (!SchedulerUtils.checkQueueLabelExpression(targetqueuelabels, label,
          null)) {
        nonAccessiblelabels.add(label);
      }
    }
    if (nonAccessiblelabels.size() > 0) {
      throw new YarnException(
          "Specified queue=" + dest.getQueueName() + " can't satisfy following "
              + "apps label expressions =" + nonAccessiblelabels
              + " accessible node labels =" + targetqueuelabels);
    }
  }

  /** {@inheritDoc} */
  @Override
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes() {
    if (calculator.getClass().getName()
      .equals(DefaultResourceCalculator.class.getName())) {
      return EnumSet.of(SchedulerResourceTypes.MEMORY);
    }
    return EnumSet.of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU);
  }

  @Override
  public Resource getMaximumResourceCapability(String queueName) {
    if(queueName == null || queueName.isEmpty()) {
      return getMaximumResourceCapability();
    }
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      LOG.error("Unknown queue: " + queueName);
      return getMaximumResourceCapability();
    }
    if (!(queue instanceof LeafQueue)) {
      LOG.error("queue " + queueName + " is not an leaf queue");
      return getMaximumResourceCapability();
    }

    // queue.getMaxAllocation returns *configured* maximum allocation.
    // getMaximumResourceCapability() returns maximum allocation considers
    // per-node maximum resources. So return (component-wise) min of the two.

    Resource queueMaxAllocation = ((LeafQueue)queue).getMaximumAllocation();
    Resource clusterMaxAllocationConsiderNodeMax =
        getMaximumResourceCapability();

    return Resources.componentwiseMin(queueMaxAllocation,
        clusterMaxAllocationConsiderNodeMax);
  }

  private String handleMoveToPlanQueue(String targetQueueName) {
    CSQueue dest = getQueue(targetQueueName);
    if (dest != null && dest instanceof PlanQueue) {
      // use the default child reservation queue of the plan
      targetQueueName = targetQueueName + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    }
    return targetQueueName;
  }

  @Override
  public Set<String> getPlanQueues() {
    Set<String> ret = new HashSet<String>();
    for (Map.Entry<String, CSQueue> l : queueManager.getQueues().entrySet()) {
      if (l.getValue() instanceof PlanQueue) {
        ret.add(l.getKey());
      }
    }
    return ret;
  }

  @Override
  public Priority checkAndGetApplicationPriority(
      Priority priorityRequestedByApp, UserGroupInformation user,
      String queueName, ApplicationId applicationId) throws YarnException {
    readLock.lock();
    try {
      Priority appPriority = priorityRequestedByApp;

      // Verify the scenario where priority is null from submissionContext.
      if (null == appPriority) {
        // Verify whether submitted user has any default priority set. If so,
        // user's default priority will get precedence over queue default.
        // for updateApplicationPriority call flow, this check is done in
        // CientRMService itself.
        appPriority = this.appPriorityACLManager.getDefaultPriority(queueName,
            user);

        // Get the default priority for the Queue. If Queue is non-existent,
        // then
        // use default priority. Do it only if user doesn't have any default.
        if (null == appPriority) {
          appPriority = this.queueManager.getDefaultPriorityForQueue(queueName);
        }

        LOG.info(
            "Application '" + applicationId + "' is submitted without priority "
                + "hence considering default queue/cluster priority: "
                + appPriority.getPriority());
      }

      // Verify whether submitted priority is lesser than max priority
      // in the cluster. If it is out of found, defining a max cap.
      if (appPriority.getPriority() > getMaxClusterLevelAppPriority()
          .getPriority()) {
        appPriority = Priority
            .newInstance(getMaxClusterLevelAppPriority().getPriority());
      }

      // Lets check for ACLs here.
      if (!appPriorityACLManager.checkAccess(user, queueName, appPriority)) {
        throw new YarnException(new AccessControlException(
            "User " + user + " does not have permission to submit/update "
                + applicationId + " for " + appPriority));
      }

      LOG.info("Priority '" + appPriority.getPriority()
          + "' is acceptable in queue : " + queueName + " for application: "
          + applicationId);

      return appPriority;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Priority updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId, SettableFuture<Object> future,
      UserGroupInformation user)
      throws YarnException {
    writeLock.lock();
    try {
      Priority appPriority = null;
      SchedulerApplication<FiCaSchedulerApp> application = applications
          .get(applicationId);

      if (application == null) {
        throw new YarnException("Application '" + applicationId
            + "' is not present, hence could not change priority.");
      }

      RMApp rmApp = rmContext.getRMApps().get(applicationId);

      appPriority = checkAndGetApplicationPriority(newPriority, user,
          rmApp.getQueue(), applicationId);

      if (application.getPriority().equals(appPriority)) {
        future.set(null);
        return appPriority;
      }

      // Update new priority in Submission Context to update to StateStore.
      rmApp.getApplicationSubmissionContext().setPriority(appPriority);

      // Update to state store
      ApplicationStateData appState = ApplicationStateData.newInstance(
          rmApp.getSubmitTime(), rmApp.getStartTime(),
          rmApp.getApplicationSubmissionContext(), rmApp.getUser(),
          rmApp.getCallerContext());
      appState.setApplicationTimeouts(rmApp.getApplicationTimeouts());
      rmContext.getStateStore().updateApplicationStateSynchronously(appState,
          false, future);

      // As we use iterator over a TreeSet for OrderingPolicy, once we change
      // priority then reinsert back to make order correct.
      LeafQueue queue = (LeafQueue) getQueue(rmApp.getQueue());
      queue.updateApplicationPriority(application, appPriority);

      LOG.info("Priority '" + appPriority + "' is updated in queue :"
          + rmApp.getQueue() + " for application: " + applicationId
          + " for the user: " + rmApp.getUser());
      return appPriority;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public PreemptionManager getPreemptionManager() {
    return preemptionManager;
  }

  @Override
  public ResourceUsage getClusterResourceUsage() {
    return getRootQueue().getQueueResourceUsage();
  }

  private SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> getSchedulerContainer(
      RMContainer rmContainer, boolean allocated) {
    if (null == rmContainer) {
      return null;
    }

    FiCaSchedulerApp app = getApplicationAttempt(
        rmContainer.getApplicationAttemptId());
    if (null == app) { return null; }

    NodeId nodeId;
    // Get nodeId
    if (rmContainer.getState() == RMContainerState.RESERVED) {
      nodeId = rmContainer.getReservedNode();
    } else {
      nodeId = rmContainer.getNodeId();
    }

    FiCaSchedulerNode node = getNode(nodeId);
    if (null == node) {
      return null;
    }
    return new SchedulerContainer<>(app, node, rmContainer,
        // TODO, node partition should come from CSAssignment to avoid partition
        // get updated before submitting the commit
        node.getPartition(), allocated);
  }

  private List<SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>>
      getSchedulerContainersToRelease(
      CSAssignment csAssignment) {
    List<SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>> list = null;

    if (csAssignment.getContainersToKill() != null && !csAssignment
        .getContainersToKill().isEmpty()) {
      list = new ArrayList<>();
      for (RMContainer rmContainer : csAssignment.getContainersToKill()) {
        SchedulerContainer schedulerContainer =
            getSchedulerContainer(rmContainer, false);
        if (schedulerContainer != null) {
          list.add(schedulerContainer);
        }
      }
    }

    if (csAssignment.getExcessReservation() != null) {
      if (null == list) {
        list = new ArrayList<>();
      }
      SchedulerContainer schedulerContainer =
          getSchedulerContainer(csAssignment.getExcessReservation(), false);
      if (schedulerContainer != null) {
        list.add(schedulerContainer);
      }
    }

    if (list != null && list.isEmpty()) {
      list = null;
    }
    return list;
  }

  @VisibleForTesting
  public void submitResourceCommitRequest(Resource cluster,
      CSAssignment csAssignment) {
    ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request =
        createResourceCommitRequest(csAssignment);

    if (null == request) {
      return;
    }

    if (scheduleAsynchronously) {
      // Submit to a commit thread and commit it async-ly
      resourceCommitterService.addNewCommitRequest(request);
    } else{
      // Otherwise do it sync-ly.
      tryCommit(cluster, request, true);
    }
  }

  @Override
  public boolean attemptAllocationOnNode(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode) {
    if (schedulingRequest.getResourceSizing() != null) {
      if (schedulingRequest.getResourceSizing().getNumAllocations() > 1) {
        LOG.warn("The SchedulingRequest has requested more than 1 allocation," +
            " but only 1 will be attempted !!");
      }
      if (!appAttempt.isStopped()) {
        ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode>
            resourceCommitRequest = createResourceCommitRequest(
            appAttempt, schedulingRequest, schedulerNode);

        // Validate placement constraint is satisfied before
        // committing the request.
        try {
          if (!PlacementConstraintsUtil.canSatisfyConstraints(
              appAttempt.getApplicationId(),
              schedulingRequest, schedulerNode,
              rmContext.getPlacementConstraintManager(),
              rmContext.getAllocationTagsManager())) {
            LOG.info("Failed to allocate container for application "
                + appAttempt.getApplicationId() + " on node "
                + schedulerNode.getNodeName()
                + " because this allocation violates the"
                + " placement constraint.");
            return false;
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Unable to allocate container", e);
          return false;
        }
        return tryCommit(getClusterResource(), resourceCommitRequest, false);
      }
    }
    return false;
  }

  // This assumes numContainers = 1 for the request.
  private ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode>
      createResourceCommitRequest(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode) {
    ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> allocated =
        null;
    Resource resource = schedulingRequest.getResourceSizing().getResources();
    if (Resources.greaterThan(calculator, getClusterResource(),
        resource, Resources.none())) {
      ContainerId cId =
          ContainerId.newContainerId(appAttempt.getApplicationAttemptId(),
              appAttempt.getAppSchedulingInfo().getNewContainerId());
      Container container = BuilderUtils.newContainer(
          cId, schedulerNode.getNodeID(), schedulerNode.getHttpAddress(),
          resource, schedulingRequest.getPriority(), null,
          ExecutionType.GUARANTEED,
          schedulingRequest.getAllocationRequestId());
      RMContainer rmContainer = new RMContainerImpl(container,
          SchedulerRequestKey.extractFrom(container),
          appAttempt.getApplicationAttemptId(), container.getNodeId(),
          appAttempt.getUser(), rmContext, false);
      ((RMContainerImpl)rmContainer).setAllocationTags(
          new HashSet<>(schedulingRequest.getAllocationTags()));

      SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
          schedulerContainer = getSchedulerContainer(rmContainer, true);
      if (schedulerContainer == null) {
        allocated = null;
      } else {
        allocated = new ContainerAllocationProposal<>(schedulerContainer,
            null, null, NodeType.NODE_LOCAL, NodeType.NODE_LOCAL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, resource);
      }
    }

    if (null != allocated) {
      List<ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>>
          allocationsList = new ArrayList<>();
      allocationsList.add(allocated);

      return new ResourceCommitRequest<>(allocationsList, null, null);
    }
    return null;
  }

  @VisibleForTesting
  public ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode>
      createResourceCommitRequest(CSAssignment csAssignment) {
    ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> allocated =
        null;
    ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> reserved =
        null;
    List<SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>> released =
        null;

    if (Resources.greaterThan(calculator, getClusterResource(),
        csAssignment.getResource(), Resources.none())) {
      // Allocated something
      List<AssignmentInformation.AssignmentDetails> allocations =
          csAssignment.getAssignmentInformation().getAllocationDetails();
      if (!allocations.isEmpty()) {
        RMContainer rmContainer = allocations.get(0).rmContainer;
        SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
            schedulerContainer = getSchedulerContainer(rmContainer, true);
        if (schedulerContainer == null) {
          allocated = null;
          // Decrease unconfirmed resource if app is alive
          FiCaSchedulerApp app = getApplicationAttempt(
              rmContainer.getApplicationAttemptId());
          if (app != null) {
            app.decUnconfirmedRes(rmContainer.getAllocatedResource());
          }
        } else {
          allocated = new ContainerAllocationProposal<>(schedulerContainer,
              getSchedulerContainersToRelease(csAssignment),
              getSchedulerContainer(
                  csAssignment.getFulfilledReservedContainer(), false),
              csAssignment.getType(), csAssignment.getRequestLocalityType(),
              csAssignment.getSchedulingMode() != null ?
                  csAssignment.getSchedulingMode() :
                  SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY,
              csAssignment.getResource());
        }
      }

      // Reserved something
      List<AssignmentInformation.AssignmentDetails> reservation =
          csAssignment.getAssignmentInformation().getReservationDetails();
      if (!reservation.isEmpty()) {
        RMContainer rmContainer = reservation.get(0).rmContainer;
        SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
            schedulerContainer = getSchedulerContainer(rmContainer, false);
        if (schedulerContainer == null) {
          reserved = null;
        } else {
          reserved = new ContainerAllocationProposal<>(schedulerContainer,
              getSchedulerContainersToRelease(csAssignment),
              getSchedulerContainer(
                  csAssignment.getFulfilledReservedContainer(), false),
              csAssignment.getType(), csAssignment.getRequestLocalityType(),
              csAssignment.getSchedulingMode() != null ?
                  csAssignment.getSchedulingMode() :
                  SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY,
              csAssignment.getResource());
        }
      }
    }

    // When we don't need to allocate/reserve anything, we can feel free to
    // kill all to-release containers in the request.
    if (null == allocated && null == reserved) {
      released = getSchedulerContainersToRelease(csAssignment);
    }

    if (null != allocated || null != reserved || (null != released && !released
        .isEmpty())) {
      List<ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>>
          allocationsList = null;
      if (allocated != null) {
        allocationsList = new ArrayList<>();
        allocationsList.add(allocated);
      }

      List<ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>>
          reservationsList = null;
      if (reserved != null) {
        reservationsList = new ArrayList<>();
        reservationsList.add(reserved);
      }

      return new ResourceCommitRequest<>(allocationsList, reservationsList,
          released);
    }

    return null;
  }

  @Override
  public boolean tryCommit(Resource cluster, ResourceCommitRequest r,
      boolean updatePending) {
    long commitStart = System.nanoTime();
    ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request =
        (ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode>) r;

    ApplicationAttemptId attemptId = null;

    // We need to update unconfirmed allocated resource of application when
    // any container allocated.
    boolean updateUnconfirmedAllocatedResource =
        request.getContainersToAllocate() != null && !request
            .getContainersToAllocate().isEmpty();

    // find the application to accept and apply the ResourceCommitRequest
    if (request.anythingAllocatedOrReserved()) {
      ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> c =
          request.getFirstAllocatedOrReservedContainer();
      attemptId =
          c.getAllocatedOrReservedContainer().getSchedulerApplicationAttempt()
              .getApplicationAttemptId();
    } else {
      if (!request.getContainersToRelease().isEmpty()) {
        attemptId = request.getContainersToRelease().get(0)
            .getSchedulerApplicationAttempt().getApplicationAttemptId();
      }
    }

    LOG.debug("Try to commit allocation proposal={}", request);

    boolean isSuccess = false;
    if (attemptId != null) {
      FiCaSchedulerApp app = getApplicationAttempt(attemptId);
      // Required sanity check for attemptId - when async-scheduling enabled,
      // proposal might be outdated if AM failover just finished
      // and proposal queue was not be consumed in time
      if (app != null && attemptId.equals(app.getApplicationAttemptId())) {
        if (app.accept(cluster, request, updatePending)
            && app.apply(cluster, request, updatePending)) {
          long commitSuccess = System.nanoTime() - commitStart;
          CapacitySchedulerMetrics.getMetrics()
              .addCommitSuccess(commitSuccess);
          LOG.info("Allocation proposal accepted");
          isSuccess = true;
        } else{
          long commitFailed = System.nanoTime() - commitStart;
          CapacitySchedulerMetrics.getMetrics()
              .addCommitFailure(commitFailed);
          LOG.info("Failed to accept allocation proposal");
        }

        LOG.debug("Allocation proposal accepted={}, proposal={}", isSuccess,
            request);

        // Update unconfirmed allocated resource.
        if (updateUnconfirmedAllocatedResource) {
          app.decUnconfirmedRes(request.getTotalAllocatedResource());
        }
      }
    }
    return isSuccess;
  }

  public int getAsyncSchedulingPendingBacklogs() {
    if (scheduleAsynchronously) {
      return resourceCommitterService.getPendingBacklogs();
    }
    return 0;
  }

  @Override
  public CapacitySchedulerQueueManager getCapacitySchedulerQueueManager() {
    return this.queueManager;
  }

  /**
   * Try to move a reserved container to a targetNode.
   * If the targetNode is reserved by another application (other than this one).
   * The previous reservation will be cancelled.
   *
   * @param toBeMovedContainer reserved container will be moved
   * @param targetNode targetNode
   * @return true if move succeeded. Return false if the targetNode is reserved by
   *         a different container or move failed because of any other reasons.
   */
  public boolean moveReservedContainer(RMContainer toBeMovedContainer,
      FiCaSchedulerNode targetNode) {
    writeLock.lock();
    try {
      LOG.debug("Trying to move container={} to node={}",
          toBeMovedContainer, targetNode.getNodeID());

      FiCaSchedulerNode sourceNode = getNode(toBeMovedContainer.getNodeId());
      if (null == sourceNode) {
        LOG.debug("Failed to move reservation, cannot find source node={}",
            toBeMovedContainer.getNodeId());
        return false;
      }

      // Target node updated?
      if (getNode(targetNode.getNodeID()) != targetNode) {
        LOG.debug("Failed to move reservation, node updated or removed,"
            + " moving cancelled.");
        return false;
      }

      // Target node's reservation status changed?
      if (targetNode.getReservedContainer() != null) {
        LOG.debug("Target node's reservation status changed,"
            + " moving cancelled.");
        return false;
      }

      FiCaSchedulerApp app = getApplicationAttempt(
          toBeMovedContainer.getApplicationAttemptId());
      if (null == app) {
        LOG.debug("Cannot find to-be-moved container's application={}",
            toBeMovedContainer.getApplicationAttemptId());
        return false;
      }

      // finally, move the reserved container
      return app.moveReservation(toBeMovedContainer, sourceNode, targetNode);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public long checkAndGetApplicationLifetime(String queueName,
      long lifetimeRequestedByApp) {
    readLock.lock();
    try {
      CSQueue queue = getQueue(queueName);
      if (queue == null || !(queue instanceof LeafQueue)) {
        return lifetimeRequestedByApp;
      }

      long defaultApplicationLifetime =
          ((LeafQueue) queue).getDefaultApplicationLifetime();
      long maximumApplicationLifetime =
          ((LeafQueue) queue).getMaximumApplicationLifetime();

      // check only for maximum, that's enough because default can't
      // exceed maximum
      if (maximumApplicationLifetime <= 0) {
        return lifetimeRequestedByApp;
      }

      if (lifetimeRequestedByApp <= 0) {
        return defaultApplicationLifetime;
      } else if (lifetimeRequestedByApp > maximumApplicationLifetime) {
        return maximumApplicationLifetime;
      }
      return lifetimeRequestedByApp;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getMaximumApplicationLifetime(String queueName) {
    CSQueue queue = getQueue(queueName);
    if (queue == null || !(queue instanceof LeafQueue)) {
      LOG.error("Unknown queue: " + queueName);
      return -1;
    }
    // In seconds
    return ((LeafQueue) queue).getMaximumApplicationLifetime();
  }

  @Override
  public boolean isConfigurationMutable() {
    return csConfProvider instanceof MutableConfigurationProvider;
  }

  @Override
  public MutableConfigurationProvider getMutableConfProvider() {
    if (isConfigurationMutable()) {
      return (MutableConfigurationProvider) csConfProvider;
    }
    return null;
  }

  private LeafQueue autoCreateLeafQueue(
      ApplicationPlacementContext placementContext)
      throws IOException, YarnException {

    AutoCreatedLeafQueue autoCreatedLeafQueue = null;

    String leafQueueName = placementContext.getQueue();
    String parentQueueName = placementContext.getParentQueue();

    if (!StringUtils.isEmpty(parentQueueName)) {
      CSQueue parentQueue = getQueue(parentQueueName);

      if (parentQueue != null && conf.isAutoCreateChildQueueEnabled(
          parentQueue.getQueuePath())) {

        ManagedParentQueue autoCreateEnabledParentQueue =
            (ManagedParentQueue) parentQueue;
        autoCreatedLeafQueue = new AutoCreatedLeafQueue(this, leafQueueName,
            autoCreateEnabledParentQueue);

        addQueue(autoCreatedLeafQueue);

      } else{
        throw new SchedulerDynamicEditException(
            "Could not auto-create leaf queue for " + leafQueueName
                + ". Queue mapping specifies an invalid parent queue "
                + "which does not exist "
                + parentQueueName);
      }
    } else{
      throw new SchedulerDynamicEditException(
          "Could not auto-create leaf queue for " + leafQueueName
              + ". Queue mapping does not specify"
              + " which parent queue it needs to be created under.");
    }
    return autoCreatedLeafQueue;
  }

  @Override
  public void resetSchedulerMetrics() {
    CapacitySchedulerMetrics.destroy();
  }
}
