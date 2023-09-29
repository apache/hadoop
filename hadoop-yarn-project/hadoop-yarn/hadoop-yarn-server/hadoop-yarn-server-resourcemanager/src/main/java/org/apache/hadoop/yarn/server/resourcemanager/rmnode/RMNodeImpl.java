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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.AllocationExpirationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils.ContainerIdComparator;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * This class is used to keep track of all the applications/containers
 * running on a node.
 *
 */
@Private
@Unstable
@SuppressWarnings("unchecked")
public class RMNodeImpl implements RMNode, EventHandler<RMNodeEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMNodeImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private final ConcurrentLinkedQueue<UpdatedContainerInfo> nodeUpdateQueue;
  private volatile boolean nextHeartBeat = true;

  private final NodeId nodeId;
  private final RMContext context;
  private final String hostName;
  private final int commandPort;
  private int httpPort;
  private final String nodeAddress; // The containerManager address
  private String httpAddress;
  /* Snapshot of total resources before receiving decommissioning command */
  private volatile Resource originalTotalCapability;
  private volatile Resource totalCapability;
  private volatile Resource allocatedContainerResource =
      Resource.newInstance(Resources.none());
  private volatile boolean updatedCapability = false;
  private final Node node;

  private String healthReport;
  private long lastHealthReportTime;
  private String nodeManagerVersion;
  private Integer decommissioningTimeout;

  private long timeStamp;
  /* Aggregated resource utilization for the containers. */
  private ResourceUtilization containersUtilization;
  /* Resource utilization for the node. */
  private ResourceUtilization nodeUtilization;

  /* Track last increment made to Utilization metrics*/
  private Resource lastUtilIncr = Resources.none();

  /** Physical resources in the node. */
  private volatile Resource physicalResource;

  /* Container Queue Information for the node.. Used by Distributed Scheduler */
  private OpportunisticContainersStatus opportunisticContainersStatus;

  private final ContainerAllocationExpirer containerAllocationExpirer;
  /* set of containers that have just launched */
  private final Set<ContainerId> launchedContainers =
    new HashSet<ContainerId>();

  /* track completed container globally */
  private final Set<ContainerId> completedContainers =
      new HashSet<ContainerId>();

  /* set of containers that need to be cleaned */
  private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(
      new ContainerIdComparator());

  /* set of containers that need to be signaled */
  private final List<SignalContainerRequest> containersToSignal =
      new ArrayList<SignalContainerRequest>();

  /*
   * set of containers to notify NM to remove them from its context. Currently,
   * this includes containers that were notified to AM about their completion
   */
  private final Set<ContainerId> containersToBeRemovedFromNM =
      new HashSet<ContainerId>();

  /* the list of applications that have finished and need to be purged */
  private final List<ApplicationId> finishedApplications =
      new ArrayList<ApplicationId>();

  /* the list of applications that are running on this node */
  private final List<ApplicationId> runningApplications =
      new ArrayList<ApplicationId>();
  
  private final Map<ContainerId, Container> toBeUpdatedContainers =
      new HashMap<>();

  /*
   * Because the Docker container's Ip, Port Mapping and other properties
   * are generated after the container is launched, need to update the
   * container property information to the applications in the RM.
   */
  private final Map<ContainerId, ContainerStatus> updatedExistContainers =
      new HashMap<>();

  // NOTE: This is required for backward compatibility.
  private final Map<ContainerId, Container> toBeDecreasedContainers =
      new HashMap<>();

  private final Map<ContainerId, Container> nmReportedIncreasedContainers =
      new HashMap<>();

  private NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);

  private static final StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent> stateMachineFactory 
                 = new StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent>(NodeState.NEW)
      //Transitions from NEW state
      .addTransition(NodeState.NEW,
          EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          RMNodeEventType.STARTED, new AddNodeTransition())
      .addTransition(NodeState.NEW, NodeState.NEW,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.NEW, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.NEW, NodeState.NEW,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      //Transitions from RUNNING state
      .addTransition(NodeState.RUNNING,
          EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.RUNNING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.RUNNING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.RUNNING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.RUNNING, EnumSet.of(NodeState.RUNNING),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenRunningTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.UPDATE_CONTAINER,
          new UpdateContainersTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.SIGNAL_CONTAINER, new SignalContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      //Transitions from REBOOTED state
      .addTransition(NodeState.REBOOTED, NodeState.REBOOTED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())

      //Transitions from DECOMMISSIONED state
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

       //Transitions from DECOMMISSIONING state
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.RUNNING,
          RMNodeEventType.RECOMMISSION,
          new RecommissionNodeTransition(NodeState.RUNNING))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenRunningTransition())
      .addTransition(NodeState.DECOMMISSIONING,
          EnumSet.of(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.DECOMMISSIONING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())

      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.DECOMMISSIONING, EnumSet.of(
          NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())

      //Transitions from LOST state
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      //Transitions from UNHEALTHY state
      .addTransition(NodeState.UNHEALTHY,
          EnumSet.of(NodeState.UNHEALTHY, NodeState.RUNNING),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenUnHealthyTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.UNHEALTHY,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.UNHEALTHY, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.UNHEALTHY, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.UNHEALTHY, EnumSet.of(NodeState.UNHEALTHY),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.SIGNAL_CONTAINER, new SignalContainerTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      //Transitions from SHUTDOWN state
      .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      // create the topology tables
      .installTopology();

  private final StateMachine<NodeState, RMNodeEventType,
                             RMNodeEvent> stateMachine;

  public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
      int cmPort, int httpPort, Node node, Resource capability,
      String nodeManagerVersion) {
    this(nodeId, context, hostName, cmPort, httpPort, node, capability,
        nodeManagerVersion, null);
  }

  public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
      int cmPort, int httpPort, Node node, Resource capability,
      String nodeManagerVersion, Resource physResource) {
    this.nodeId = nodeId;
    this.context = context;
    this.hostName = hostName;
    this.commandPort = cmPort;
    this.httpPort = httpPort;
    this.totalCapability = capability; 
    this.nodeAddress = hostName + ":" + cmPort;
    this.httpAddress = hostName + ":" + httpPort;
    this.node = node;
    this.healthReport = "Healthy";
    this.lastHealthReportTime = System.currentTimeMillis();
    this.nodeManagerVersion = nodeManagerVersion;
    this.timeStamp = 0;
    // If physicalResource is not available, capability is a reasonable guess
    this.physicalResource = physResource==null ? capability : physResource;

    this.latestNodeHeartBeatResponse.setResponseId(0);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);

    this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();

    this.containerAllocationExpirer = context.getContainerAllocationExpirer();
  }

  @Override
  public String toString() {
    return this.nodeId.toString();
  }

  @Override
  public String getHostName() {
    return hostName;
  }

  @Override
  public int getCommandPort() {
    return commandPort;
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  // Test only
  public void setHttpPort(int port) {
    this.httpPort = port;
  }

  @Override
  public NodeId getNodeID() {
    return this.nodeId;
  }

  @Override
  public String getNodeAddress() {
    return this.nodeAddress;
  }

  @Override
  public String getHttpAddress() {
    return this.httpAddress;
  }

  @Override
  public Resource getTotalCapability() {
    return this.totalCapability;
  }

  @Override
  public Resource getAllocatedContainerResource() {
    return this.allocatedContainerResource;
  }

  @Override
  public boolean isUpdatedCapability() {
    return this.updatedCapability;
  }

  @Override
  public void resetUpdatedCapability() {
    this.updatedCapability = false;
  }

  @Override
  public String getRackName() {
    return node.getNetworkLocation();
  }
  
  @Override
  public Node getNode() {
    return this.node;
  }
  
  @Override
  public String getHealthReport() {
    this.readLock.lock();

    try {
      return this.healthReport;
    } finally {
      this.readLock.unlock();
    }
  }
  
  public void setHealthReport(String healthReport) {
    this.writeLock.lock();

    try {
      this.healthReport = healthReport;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  public void setLastHealthReportTime(long lastHealthReportTime) {
    this.writeLock.lock();

    try {
      this.lastHealthReportTime = lastHealthReportTime;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Override
  public long getLastHealthReportTime() {
    this.readLock.lock();

    try {
      return this.lastHealthReportTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getNodeManagerVersion() {
    return nodeManagerVersion;
  }

  @Override
  public ResourceUtilization getAggregatedContainersUtilization() {
    this.readLock.lock();

    try {
      return this.containersUtilization;
    } finally {
      this.readLock.unlock();
    }
  }

  public void setAggregatedContainersUtilization(
      ResourceUtilization containersUtilization) {
    this.writeLock.lock();

    try {
      this.containersUtilization = containersUtilization;
    } finally {
      this.writeLock.unlock();
    }
  }

  private void clearContributionToUtilizationMetrics() {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    metrics.decrUtilizedMB(lastUtilIncr.getMemorySize());
    metrics.decrUtilizedVirtualCores(lastUtilIncr.getVirtualCores());
    lastUtilIncr = Resources.none();
  }

  private void updateClusterUtilizationMetrics() {
    // Update cluster utilization metrics
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    Resource prevIncr = lastUtilIncr;

    if (this.nodeUtilization == null) {
      lastUtilIncr = Resources.none();
    } else {
      /* Scale memory contribution based on configured node size */
      long newmem = (long)((float)this.nodeUtilization.getPhysicalMemory()
          / Math.max(1.0f, this.getPhysicalResource().getMemorySize())
          * this.getTotalCapability().getMemorySize());
      lastUtilIncr =
          Resource.newInstance(newmem,
              (int) (this.nodeUtilization.getCPU()
                  / Math.max(1.0f, this.getPhysicalResource().getVirtualCores())
                  * this.getTotalCapability().getVirtualCores()));
    }
    metrics.incrUtilizedMB(lastUtilIncr.getMemorySize() -
        prevIncr.getMemorySize());
    metrics.incrUtilizedVirtualCores(lastUtilIncr.getVirtualCores() -
        prevIncr.getVirtualCores());
  }

  @Override
  public ResourceUtilization getNodeUtilization() {
    this.readLock.lock();

    try {
      return this.nodeUtilization;
    } finally {
      this.readLock.unlock();
    }
  }

  public void setNodeUtilization(ResourceUtilization nodeUtilization) {
    this.writeLock.lock();

    try {
      this.nodeUtilization = nodeUtilization;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Resource getPhysicalResource() {
    return this.physicalResource;
  }

  public void setPhysicalResource(Resource physicalResource) {
    this.physicalResource = physicalResource;
  }

  @Override
  public NodeState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ApplicationId> getAppsToCleanup() {
    this.readLock.lock();

    try {
      return new ArrayList<ApplicationId>(this.finishedApplications);
    } finally {
      this.readLock.unlock();
    }

  }
  
  @Override
  public List<ApplicationId> getRunningApps() {
    this.readLock.lock();
    try {
      return new ArrayList<ApplicationId>(this.runningApplications);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerId> getContainersToCleanUp() {

    this.readLock.lock();

    try {
      return new ArrayList<ContainerId>(this.containersToClean);
    } finally {
      this.readLock.unlock();
    }
  };

  @Override
  public void setAndUpdateNodeHeartbeatResponse(
      NodeHeartbeatResponse response) {
    this.writeLock.lock();

    try {
      response.addAllContainersToCleanup(
          new ArrayList<ContainerId>(this.containersToClean));
      response.addAllApplicationsToCleanup(this.finishedApplications);
      response.addContainersToBeRemovedFromNM(
          new ArrayList<ContainerId>(this.containersToBeRemovedFromNM));
      response.addAllContainersToSignal(this.containersToSignal);
      this.completedContainers.removeAll(this.containersToBeRemovedFromNM);
      this.containersToClean.clear();
      this.finishedApplications.clear();
      this.containersToSignal.clear();
      this.containersToBeRemovedFromNM.clear();

      response.addAllContainersToUpdate(toBeUpdatedContainers.values());
      toBeUpdatedContainers.clear();

      // NOTE: This is required for backward compatibility.
      response.addAllContainersToDecrease(toBeDecreasedContainers.values());
      toBeDecreasedContainers.clear();

      // Synchronously update the last response in rmNode with updated
      // responseId
      this.latestNodeHeartBeatResponse = response;
    } finally {
      this.writeLock.unlock();
    }
  };

  @VisibleForTesting
  public Collection<Container> getToBeUpdatedContainers() {
    return toBeUpdatedContainers.values();
  }

  @Override
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {
    this.readLock.lock();
    try {
      return this.latestNodeHeartBeatResponse;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void resetLastNodeHeartBeatResponse() {
    this.writeLock.lock();
    try {
      latestNodeHeartBeatResponse.setResponseId(0);
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public long calculateHeartBeatInterval(long defaultInterval, long minInterval,
      long maxInterval, float speedupFactor, float slowdownFactor) {

    long newInterval = defaultInterval;

    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    float clusterUtil = metrics.getUtilizedVirtualCores()
        / Math.max(1.0f, metrics.getCapabilityVirtualCores());

    if (this.nodeUtilization != null && this.getPhysicalResource() != null) {
      // getCPU() returns utilization normalized to 1 cpu. getVirtualCores() on
      // a physicalResource returns number of physical cores. So,
      // nodeUtil will be CPU utilization of entire node.
      float nodeUtil = this.nodeUtilization.getCPU()
          / Math.max(1.0f, this.getPhysicalResource().getVirtualCores());

      // sanitize
      nodeUtil = Math.min(1.0f, Math.max(0.0f, nodeUtil));
      clusterUtil = Math.min(1.0f, Math.max(0.0f, clusterUtil));

      if (nodeUtil > clusterUtil) {
        // Slow down - 20% more CPU utilization means slow down by 20% * factor
        newInterval = (long) (defaultInterval
            * (1.0f + (nodeUtil - clusterUtil) * slowdownFactor));
      } else {
        // Speed up - 20% less CPU utilization means speed up by 20% * factor
        newInterval = (long) (defaultInterval
            * (1.0f - (clusterUtil - nodeUtil) * speedupFactor));
      }
      newInterval =
          Math.min(maxInterval, Math.max(minInterval, newInterval));

      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting heartbeatinterval to: " + newInterval
            + " node:" + this.nodeId + " nodeUtil: " + nodeUtil
            + " clusterUtil: " + clusterUtil);
      }
    }
    return newInterval;
  }

  public void handle(RMNodeEvent event) {
    LOG.debug("Processing {} of type {}", event.getNodeId(), event.getType());
    writeLock.lock();
    try {
      NodeState oldState = getState();
      try {
         stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + 
            " on Node  " + this.nodeId + " oldState " + oldState);
      }
      if (oldState != getState()) {
        LOG.info(nodeId + " Node Transitioned from " + oldState + " to "
                 + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  private void updateMetricsForRejoinedNode(NodeState previousNodeState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    // Update utilization metrics
    this.updateClusterUtilizationMetrics();

    switch (previousNodeState) {
    case LOST:
      metrics.decrNumLostNMs();
      break;
    case REBOOTED:
      metrics.decrNumRebootedNMs();
      break;
    case DECOMMISSIONED:
      metrics.decrDecommisionedNMs();
      break;
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.decrNumShutdownNMs();
      break;
    case DECOMMISSIONING:
      metrics.decrDecommissioningNMs();
      break;
    default:
      LOG.debug("Unexpected previous node state");
    }
  }

  // Update metrics when moving to Decommissioning state
  private void updateMetricsForGracefulDecommission(NodeState initialState,
      NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    switch (initialState) {
    case UNHEALTHY :
      metrics.decrNumUnhealthyNMs();
      break;
    case RUNNING :
      metrics.decrNumActiveNodes();
      break;
    case DECOMMISSIONING :
      metrics.decrDecommissioningNMs();
      break;
    default :
      LOG.warn("Unexpected initial state");
    }

    switch (finalState) {
    case DECOMMISSIONING :
      metrics.incrDecommissioningNMs();
      break;
    case RUNNING :
      metrics.incrNumActiveNodes();
      break;
    default :
      LOG.warn("Unexpected final state");
    }
  }

  private void updateMetricsForDeactivatedNode(NodeState initialState,
                                               NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    // Update utilization metrics
    clearContributionToUtilizationMetrics();

    switch (initialState) {
    case RUNNING:
      metrics.decrNumActiveNodes();
      break;
    case DECOMMISSIONING:
      metrics.decrDecommissioningNMs();
      break;
    case DECOMMISSIONED:
      metrics.decrDecommisionedNMs();
      break;
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    case NEW:
      break;
    default:
      LOG.warn("Unexpected initial state");
    }

    switch (finalState) {
    case DECOMMISSIONED:
      metrics.incrDecommisionedNMs();
      break;
    case LOST:
      metrics.incrNumLostNMs();
      break;
    case REBOOTED:
      metrics.incrNumRebootedNMs();
      break;
    case UNHEALTHY:
      metrics.incrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.incrNumShutdownNMs();
      break;
    default:
      LOG.warn("Unexpected final state");
    }
  }

  private static void handleRunningAppOnNode(RMNodeImpl rmNode,
      RMContext context, ApplicationId appId, NodeId nodeId) {
    RMApp app = context.getRMApps().get(appId);

    // if we failed getting app by appId, maybe something wrong happened, just
    // add the app to the finishedApplications list so that the app can be
    // cleaned up on the NM
    if (null == app) {
      LOG.warn("Cannot get RMApp by appId=" + appId
          + ", just added it to finishedApplications list for cleanup");
      rmNode.finishedApplications.add(appId);
      rmNode.runningApplications.remove(appId);
      return;
    }

    // Add running applications back due to Node add or Node reconnection.
    rmNode.runningApplications.add(appId);
    context.getDispatcher().getEventHandler()
        .handle(new RMAppRunningOnNodeEvent(appId, nodeId));
  }
  
  private static void updateNodeResourceFromEvent(RMNodeImpl rmNode,
      RMNodeResourceUpdateEvent event){
    ResourceOption resourceOption = event.getResourceOption();
    // Set resource on RMNode
    rmNode.totalCapability = resourceOption.getResource();
    rmNode.updatedCapability = true;
  }

  private static NodeHealthStatus updateRMNodeFromStatusEvents(
      RMNodeImpl rmNode, RMNodeStatusEvent statusEvent) {
    // Switch the last heartbeatresponse.
    NodeHealthStatus remoteNodeHealthStatus = statusEvent.getNodeHealthStatus();
    rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
    rmNode.setLastHealthReportTime(remoteNodeHealthStatus
        .getLastHealthReportTime());
    rmNode.setAggregatedContainersUtilization(statusEvent
        .getAggregatedContainersUtilization());
    rmNode.setNodeUtilization(statusEvent.getNodeUtilization());
    return remoteNodeHealthStatus;
  }

  public static class AddNodeTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler
      RMNodeStartedEvent startEvent = (RMNodeStartedEvent) event;
      List<NMContainerStatus> containers = null;

      NodeId nodeId = rmNode.nodeId;
      RMNode previousRMNode =
          rmNode.context.getInactiveRMNodes().remove(nodeId);
      if (previousRMNode != null) {
        rmNode.updateMetricsForRejoinedNode(previousRMNode.getState());
      } else {
        NodeId unknownNodeId =
            NodesListManager.createUnknownNodeId(nodeId.getHost());
        previousRMNode =
            rmNode.context.getInactiveRMNodes().remove(unknownNodeId);
        if (previousRMNode != null) {
          ClusterMetrics.getMetrics().decrDecommisionedNMs();
        }
        containers = startEvent.getNMContainerStatuses();
        final Resource allocatedResource = Resource.newInstance(
            Resources.none());
        if (containers != null && !containers.isEmpty()) {
          for (NMContainerStatus container : containers) {
            if (container.getContainerState() == ContainerState.NEW ||
                container.getContainerState() == ContainerState.RUNNING) {
              Resources.addTo(allocatedResource,
                  container.getAllocatedResource());
              if (container.getContainerState() == ContainerState.RUNNING) {
                rmNode.launchedContainers.add(container.getContainerId());
              }
            }
          }
        }

        rmNode.allocatedContainerResource = allocatedResource;
      }

      if (null != startEvent.getRunningApplications()) {
        for (ApplicationId appId : startEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }
      }

      NodeState nodeState;
      NodeStatus nodeStatus =
          startEvent.getNodeStatus();

      if (nodeStatus == null) {
        nodeState = NodeState.RUNNING;
        reportNodeRunning(rmNode, containers);
      } else {
        RMNodeStatusEvent rmNodeStatusEvent =
            new RMNodeStatusEvent(nodeId, nodeStatus);

        NodeHealthStatus nodeHealthStatus =
            updateRMNodeFromStatusEvents(rmNode, rmNodeStatusEvent);

        if (nodeHealthStatus.getIsNodeHealthy()) {
          nodeState = NodeState.RUNNING;
          reportNodeRunning(rmNode, containers);
        } else {
          nodeState = NodeState.UNHEALTHY;
          reportNodeUnusable(rmNode, nodeState);
        }
      }

      List<LogAggregationReport> logAggregationReportsForApps =
          startEvent.getLogAggregationReportsForApps();
      if (logAggregationReportsForApps != null
          && !logAggregationReportsForApps.isEmpty()) {
        rmNode.handleLogAggregationStatus(logAggregationReportsForApps);
      }

      return nodeState;
    }
  }

  public static class ReconnectNodeTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeReconnectEvent reconnectEvent = (RMNodeReconnectEvent) event;
      RMNode newNode = reconnectEvent.getReconnectedNode();
      rmNode.nodeManagerVersion = newNode.getNodeManagerVersion();
      List<ApplicationId> runningApps = reconnectEvent.getRunningApplications();
      boolean noRunningApps = 
          (runningApps == null) || (runningApps.size() == 0);
      
      // No application running on the node, so send node-removal event with 
      // cleaning up old container info.
      if (noRunningApps) {
        if (rmNode.getState() == NodeState.DECOMMISSIONING) {
          // When node in decommissioning, and no running apps on this node,
          // it will return as decommissioned state.
          deactivateNode(rmNode, NodeState.DECOMMISSIONED);
          return NodeState.DECOMMISSIONED;
        }
        rmNode.nodeUpdateQueue.clear();
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeRemovedSchedulerEvent(rmNode));

        if (rmNode.getHttpPort() == newNode.getHttpPort()) {
          if (!rmNode.getTotalCapability().equals(
              newNode.getTotalCapability())) {
            rmNode.totalCapability = newNode.getTotalCapability();
          }
          if (rmNode.getState().equals(NodeState.RUNNING)) {
            // Only add old node if old state is RUNNING
            rmNode.context.getDispatcher().getEventHandler().handle(
                new NodeAddedSchedulerEvent(rmNode));
          }
        }

      } else {
        rmNode.httpPort = newNode.getHttpPort();
        rmNode.httpAddress = newNode.getHttpAddress();
        boolean isCapabilityChanged = false;
        if (!rmNode.getTotalCapability().equals(
            newNode.getTotalCapability())) {
          rmNode.totalCapability = newNode.getTotalCapability();
          isCapabilityChanged = true;
        }
      
        handleNMContainerStatus(reconnectEvent.getNMContainerStatuses(), rmNode);

        for (ApplicationId appId : reconnectEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }

        if (isCapabilityChanged
            && rmNode.getState().equals(NodeState.RUNNING)) {
          // Update scheduler node's capacity for reconnect node.
          rmNode.context
              .getDispatcher()
              .getEventHandler()
              .handle(
                  new NodeResourceUpdateSchedulerEvent(rmNode, ResourceOption
                      .newInstance(newNode.getTotalCapability(), -1)));
        }

      }
      return rmNode.getState();
    }

    private void handleNMContainerStatus(
        List<NMContainerStatus> nmContainerStatuses, RMNodeImpl rmnode) {
      if (nmContainerStatuses != null) {
        List<ContainerStatus> containerStatuses =
            new ArrayList<ContainerStatus>();
        for (NMContainerStatus nmContainerStatus : nmContainerStatuses) {
          containerStatuses.add(createContainerStatus(nmContainerStatus));
        }
        rmnode.handleContainerStatus(containerStatuses);
      }
    }

    private ContainerStatus createContainerStatus(
        NMContainerStatus remoteContainer) {
      ContainerStatus cStatus =
          ContainerStatus.newInstance(remoteContainer.getContainerId(),
              remoteContainer.getContainerState(),
              remoteContainer.getDiagnostics(),
              remoteContainer.getContainerExitStatus());
      return cStatus;
    }
  }
  
  public static class UpdateNodeResourceWhenRunningTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeResourceUpdateEvent updateEvent = (RMNodeResourceUpdateEvent)event;
      updateNodeResourceFromEvent(rmNode, updateEvent);
      // Notify new resourceOption to scheduler
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeResourceUpdateSchedulerEvent(rmNode, updateEvent.getResourceOption()));
    }
  }
  
  public static class UpdateNodeResourceWhenUnusableTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // The node is not usable, only log a warn message
      LOG.warn("Try to update resource on a "+ rmNode.getState().toString() +
          " node: "+rmNode.toString());
      updateNodeResourceFromEvent(rmNode, (RMNodeResourceUpdateEvent)event);
      // No need to notify scheduler as schedulerNode is not function now
      // and can sync later from RMnode.
    }
  }
  
  public static class CleanUpAppTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      ApplicationId appId = ((RMNodeCleanAppEvent) event).getAppId();
      rmNode.finishedApplications.add(appId);
      rmNode.runningApplications.remove(appId);
    }
  }

  public static class CleanUpContainerTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToClean.add(((
          RMNodeCleanContainerEvent) event).getContainerId());
    }
  }

  public static class AddContainersToBeRemovedFromNMTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToBeRemovedFromNM.addAll(((
          RMNodeFinishedContainersPulledByAMEvent) event).getContainers());
    }
  }

  /**
   * Transition to Update a container.
   */
  public static class UpdateContainersTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {
 
    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeUpdateContainerEvent de = (RMNodeUpdateContainerEvent) event;

      for (Map.Entry<Container, ContainerUpdateType> e :
          de.getToBeUpdatedContainers().entrySet()) {
        // NOTE: This is required for backward compatibility.
        if (ContainerUpdateType.DECREASE_RESOURCE == e.getValue()) {
          rmNode.toBeDecreasedContainers.put(e.getKey().getId(), e.getKey());
        }
        rmNode.toBeUpdatedContainers.put(e.getKey().getId(), e.getKey());
      }
    }
  }

  public static class DeactivateNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    private final NodeState finalState;
    public DeactivateNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeImpl.deactivateNode(rmNode, finalState);
    }
  }

  /**
   * Put a node in deactivated (decommissioned or shutdown) status.
   * @param rmNode RMNode.
   * @param finalState NodeState.
   */
  public static void deactivateNode(RMNodeImpl rmNode, NodeState finalState) {

    if (rmNode.getNodeID().getPort() == -1) {
      rmNode.updateMetricsForDeactivatedNode(rmNode.getState(), finalState);
      return;
    }
    reportNodeUnusable(rmNode, finalState);

    // Deactivate the node
    rmNode.context.getRMNodes().remove(rmNode.nodeId);
    LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
        + finalState);
    rmNode.context.getInactiveRMNodes().put(rmNode.nodeId, rmNode);
    if (rmNode.context.getNodesListManager().isUntrackedNode(rmNode.hostName)) {
      rmNode.setUntrackedTimeStamp(Time.monotonicNow());
    }
  }

  /**
   * Report node is RUNNING.
   * @param rmNode RMNode.
   * @param containers NMContainerStatus List.
   */
  public static void reportNodeRunning(RMNodeImpl rmNode,
      List<NMContainerStatus> containers) {
    rmNode.context.getDispatcher().getEventHandler()
        .handle(new NodeAddedSchedulerEvent(rmNode, containers));
    rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_USABLE, rmNode));
    // Increment activeNodes explicitly because this is a new node.
    ClusterMetrics.getMetrics().incrNumActiveNodes();
  }

  /**
   * Report node is UNUSABLE and update metrics.
   * @param rmNode RMNode.
   * @param finalState NodeState.
   */
  public static void reportNodeUnusable(RMNodeImpl rmNode,
      NodeState finalState) {
    // Inform the scheduler
    rmNode.nodeUpdateQueue.clear();
    // If the current state is NodeState.UNHEALTHY
    // Then node is already been removed from the
    // Scheduler
    NodeState initialState = rmNode.getState();
    if (!initialState.equals(NodeState.UNHEALTHY)) {
      rmNode.context.getDispatcher().getEventHandler()
        .handle(new NodeRemovedSchedulerEvent(rmNode));
    }
    rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_UNUSABLE, rmNode));

    //Update the metrics
    rmNode.updateMetricsForDeactivatedNode(initialState, finalState);
  }

  /**
   * The transition to put node in decommissioning state.
   */
  public static class DecommissioningNodeTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {
    private final NodeState initState;
    private final NodeState finalState;

    public DecommissioningNodeTransition(NodeState initState,
        NodeState finalState) {
      this.initState = initState;
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      Integer timeout = null;
      if (RMNodeDecommissioningEvent.class.isInstance(event)) {
        RMNodeDecommissioningEvent e = ((RMNodeDecommissioningEvent) event);
        timeout = e.getDecommissioningTimeout();
      }
      // Pick up possible updates on decommissioningTimeout.
      if (rmNode.getState() == NodeState.DECOMMISSIONING) {
        if (!Objects.equals(rmNode.getDecommissioningTimeout(), timeout)) {
          LOG.info("Update " + rmNode.getNodeID() +
                   " DecommissioningTimeout to be " + timeout);
          rmNode.decommissioningTimeout = timeout;
        } else {
          LOG.info(rmNode.getNodeID() + " is already DECOMMISSIONING");
        }
        return;
      }
      LOG.info("Put Node " + rmNode.nodeId + " in DECOMMISSIONING.");
      // Update NM metrics during graceful decommissioning.
      rmNode.updateMetricsForGracefulDecommission(initState, finalState);
      rmNode.decommissioningTimeout = timeout;
      // Notify NodesListManager to notify all RMApp so that each
      // Application Master could take any required actions.
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodesListManagerEvent(
              NodesListManagerEventType.NODE_DECOMMISSIONING, rmNode));
      if (rmNode.originalTotalCapability == null){
        rmNode.originalTotalCapability =
            Resources.clone(rmNode.totalCapability);
        LOG.info("Preserve original total capability: "
            + rmNode.originalTotalCapability);
      }
    }
  }

  public static class RecommissionNodeTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    private final NodeState finalState;
    public RecommissionNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Restore the original total capability
      if (rmNode.originalTotalCapability != null) {
        rmNode.totalCapability = rmNode.originalTotalCapability;
        rmNode.originalTotalCapability = null;
        rmNode.updatedCapability = true;
      }
      LOG.info("Node " + rmNode.nodeId + " in DECOMMISSIONING is " +
          "recommissioned back to RUNNING.");
      rmNode
          .updateMetricsForGracefulDecommission(rmNode.getState(), finalState);
      //update the scheduler with the restored original total capability
      rmNode.context
          .getDispatcher()
          .getEventHandler()
          .handle(
              new NodeResourceUpdateSchedulerEvent(rmNode, ResourceOption
                  .newInstance(rmNode.totalCapability, 0)));

      // Notify NodesListManager to notify all RMApp that this node has been
      // recommissioned so that each Application Master can take any required
      // actions.
      rmNode.context.getDispatcher().getEventHandler().handle(
              new NodesListManagerEvent(
                      NodesListManagerEventType.NODE_USABLE, rmNode));
    }
  }

  /**
   * Status update transition when node is healthy.
   */
  public static class StatusUpdateWhenHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {
    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {

      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;
      rmNode.setOpportunisticContainersStatus(
          statusEvent.getOpportunisticContainersStatus());
      NodeHealthStatus remoteNodeHealthStatus = updateRMNodeFromStatusEvents(
          rmNode, statusEvent);
      rmNode.updateClusterUtilizationMetrics();
      NodeState initialState = rmNode.getState();
      boolean isNodeDecommissioning =
          initialState.equals(NodeState.DECOMMISSIONING);
      if (isNodeDecommissioning) {
        List<ApplicationId> keepAliveApps = statusEvent.getKeepAliveAppIds();
        // hasScheduledAMContainers solves the following race condition -
        // 1. launch AM container on a node with 0 containers.
        // 2. gracefully decommission this node.
        // 3. Node heartbeats to RM. In StatusUpdateWhenHealthyTransition,
        //    rmNode.runningApplications will be empty as it is updated after
        //    call to RMNodeImpl.deactivateNode. This will cause the node to be
        //    deactivated even though container is running on it and hence kill
        //    all containers running on it.
        // In order to avoid such race conditions the ground truth is retrieved
        // from the scheduler before deactivating a DECOMMISSIONING node.
        // Only AM containers are considered as AM container reattempts can
        // cause application failures if max attempts is set to 1.
        if (rmNode.runningApplications.isEmpty() &&
            (keepAliveApps == null || keepAliveApps.isEmpty()) &&
            !hasScheduledAMContainers(rmNode)) {
          LOG.info("No containers running on " + rmNode.nodeId + ". "
              + "Attempting to deactivate decommissioning node.");
          RMNodeImpl.deactivateNode(rmNode, NodeState.DECOMMISSIONED);
          return NodeState.DECOMMISSIONED;
        }
      }

      if (!remoteNodeHealthStatus.getIsNodeHealthy()) {
        LOG.info("Node " + rmNode.nodeId +
            " reported UNHEALTHY with details: " +
            remoteNodeHealthStatus.getHealthReport());
        // if a node in decommissioning receives an unhealthy report,
        // it will stay in decommissioning.
        if (isNodeDecommissioning) {
          return NodeState.DECOMMISSIONING;
        } else {
          reportNodeUnusable(rmNode, NodeState.UNHEALTHY);
          return NodeState.UNHEALTHY;
        }
      }

      rmNode.handleContainerStatus(statusEvent.getContainers());
      rmNode.handleReportedIncreasedContainers(
          statusEvent.getNMReportedIncreasedContainers());

      List<LogAggregationReport> logAggregationReportsForApps =
          statusEvent.getLogAggregationReportsForApps();
      if (logAggregationReportsForApps != null
          && !logAggregationReportsForApps.isEmpty()) {
        rmNode.handleLogAggregationStatus(logAggregationReportsForApps);
      }

      if(rmNode.nextHeartBeat) {
        rmNode.nextHeartBeat = false;
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeUpdateSchedulerEvent(rmNode));
      }

      // Update DTRenewer in secure mode to keep these apps alive. Today this is
      // needed for log-aggregation to finish long after the apps are gone.
      if (UserGroupInformation.isSecurityEnabled()) {
        rmNode.context.getDelegationTokenRenewer().updateKeepAliveApplications(
          statusEvent.getKeepAliveAppIds());
      }

      return initialState;
    }

    /**
     * Checks if the scheduler has scheduled any AMs on the given node.
     * @return true if node has any AM scheduled on it.
     */
    private boolean hasScheduledAMContainers(RMNodeImpl rmNode) {
      return rmNode.context.getScheduler()
          .getSchedulerNode(rmNode.getNodeID())
          .getCopiedListOfRunningContainers()
          .stream().anyMatch(RMContainer::isAMContainer);
    }
  }

  public static class StatusUpdateWhenUnHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent)event;

      // Switch the last heartbeatresponse.
      NodeHealthStatus remoteNodeHealthStatus = updateRMNodeFromStatusEvents(
          rmNode, statusEvent);
      if (remoteNodeHealthStatus.getIsNodeHealthy()) {
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeAddedSchedulerEvent(rmNode));
        rmNode.context.getDispatcher().getEventHandler().handle(
                new NodesListManagerEvent(
                    NodesListManagerEventType.NODE_USABLE, rmNode));
        // ??? how about updating metrics before notifying to ensure that
        // notifiers get update metadata because they will very likely query it
        // upon notification
        // Update metrics
        ClusterMetrics.getMetrics().incrNumActiveNodes();
        rmNode.updateMetricsForRejoinedNode(NodeState.UNHEALTHY);
        return NodeState.RUNNING;
      }

      return NodeState.UNHEALTHY;
    }
  }

  public static class SignalContainerTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToSignal.add(((
          RMNodeSignalContainerEvent) event).getSignalRequest());
    }
  }

  @Override
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    List<UpdatedContainerInfo> latestContainerInfoList = 
        new ArrayList<UpdatedContainerInfo>();
    UpdatedContainerInfo containerInfo;
    while ((containerInfo = nodeUpdateQueue.poll()) != null) {
      latestContainerInfoList.add(containerInfo);
    }
    this.nextHeartBeat = true;
    return latestContainerInfoList;
  }

  @VisibleForTesting
  public void setNextHeartBeat(boolean nextHeartBeat) {
    this.nextHeartBeat = nextHeartBeat;
  }
  
  @VisibleForTesting
  public int getQueueSize() {
    return nodeUpdateQueue.size();
  }

  // For test only.
  @VisibleForTesting
  public Map<ContainerId, ContainerStatus> getUpdatedExistContainers() {
    return this.updatedExistContainers;
  }
  // For test only.
  @VisibleForTesting
  public Set<ContainerId> getLaunchedContainers() {
    return this.launchedContainers;
  }

  @VisibleForTesting
  public Set<ContainerId> getCompletedContainers() {
    return this.completedContainers;
  }

  @Override
  public Set<String> getNodeLabels() {
    RMNodeLabelsManager nlm = context.getNodeLabelManager();
    if (nlm == null || nlm.getLabelsOnNode(nodeId) == null) {
      return CommonNodeLabelsManager.EMPTY_STRING_SET;
    }
    return nlm.getLabelsOnNode(nodeId);
  }
  
  private void handleReportedIncreasedContainers(
      List<Container> reportedIncreasedContainers) {
    for (Container container : reportedIncreasedContainers) {
      ContainerId containerId = container.getId();

      // Don't bother with containers already scheduled for cleanup, or for
      // applications already killed. The scheduler doens't need to know any
      // more about this container
      if (containersToClean.contains(containerId)) {
        LOG.info("Container " + containerId + " already scheduled for "
            + "cleanup, no further processing");
        continue;
      }

      ApplicationId containerAppId =
          containerId.getApplicationAttemptId().getApplicationId();

      if (finishedApplications.contains(containerAppId)) {
        LOG.info("Container " + containerId
            + " belongs to an application that is already killed,"
            + " no further processing");
        continue;
      }
      
      this.nmReportedIncreasedContainers.put(containerId, container);
    }
  }

  private void handleContainerStatus(List<ContainerStatus> containerStatuses) {
    // Filter the map to only obtain just launched containers and finished
    // containers.
    List<ContainerStatus> newlyLaunchedContainers =
        new ArrayList<ContainerStatus>();
    List<ContainerStatus> newlyCompletedContainers =
        new ArrayList<ContainerStatus>();
    List<Map.Entry<ApplicationId, ContainerStatus>> needUpdateContainers =
        new ArrayList<Map.Entry<ApplicationId, ContainerStatus>>();
    int numRemoteRunningContainers = 0;
    final Resource allocatedResource = Resource.newInstance(Resources.none());

    for (ContainerStatus remoteContainer : containerStatuses) {
      ContainerId containerId = remoteContainer.getContainerId();

      // Don't bother with containers already scheduled for cleanup, or for
      // applications already killed. The scheduler doens't need to know any
      // more about this container
      if (containersToClean.contains(containerId)) {
        LOG.info("Container " + containerId + " already scheduled for "
            + "cleanup, no further processing");
        continue;
      }

      ApplicationId containerAppId =
          containerId.getApplicationAttemptId().getApplicationId();

      if (finishedApplications.contains(containerAppId)) {
        LOG.info("Container " + containerId
            + " belongs to an application that is already killed,"
            + " no further processing");
        continue;
      } else if (!runningApplications.contains(containerAppId)) {
        LOG.debug("Container {} is the first container get launched for"
            + " application {}", containerId, containerAppId);
        handleRunningAppOnNode(this, context, containerAppId, nodeId);
      }

      // Process running containers
      if (remoteContainer.getState() == ContainerState.RUNNING) {
        ++numRemoteRunningContainers;
        if (!launchedContainers.contains(containerId)) {
          // Just launched container. RM knows about it the first time.
          launchedContainers.add(containerId);
          newlyLaunchedContainers.add(remoteContainer);
          // Unregister from containerAllocationExpirer.
          containerAllocationExpirer
              .unregister(new AllocationExpirationInfo(containerId));
        }

        // Check if you need to update the exist container status
        boolean needUpdate = false;
        if (!updatedExistContainers.containsKey(containerId)) {
          needUpdate = true;
        } else {
          ContainerStatus pContainer = updatedExistContainers.get(containerId);
          if (null != pContainer) {
            String preExposedPorts = pContainer.getExposedPorts();
            if (null != preExposedPorts &&
                !preExposedPorts.equals(remoteContainer.getExposedPorts())) {
              needUpdate = true;
            }
          }
        }
        if (needUpdate) {
          updatedExistContainers.put(containerId, remoteContainer);
          needUpdateContainers.add(new DefaultMapEntry(containerAppId,
              remoteContainer));
        }
      } else {
        // A finished container
        launchedContainers.remove(containerId);
        updatedExistContainers.remove(containerId);
        if (completedContainers.add(containerId)) {
          newlyCompletedContainers.add(remoteContainer);
        }
        // Unregister from containerAllocationExpirer.
        containerAllocationExpirer
            .unregister(new AllocationExpirationInfo(containerId));
      }

      if ((remoteContainer.getState() == ContainerState.RUNNING ||
          remoteContainer.getState() == ContainerState.NEW) &&
          remoteContainer.getCapability() != null) {
        Resources.addTo(allocatedResource, remoteContainer.getCapability());
      }
    }

    allocatedContainerResource = allocatedResource;

    List<ContainerStatus> lostContainers =
        findLostContainers(numRemoteRunningContainers, containerStatuses);
    for (ContainerStatus remoteContainer : lostContainers) {
      ContainerId containerId = remoteContainer.getContainerId();
      updatedExistContainers.remove(containerId);
      if (completedContainers.add(containerId)) {
        newlyCompletedContainers.add(remoteContainer);
      }
    }

    if (newlyLaunchedContainers.size() != 0
        || newlyCompletedContainers.size() != 0
        || needUpdateContainers.size() != 0) {
      nodeUpdateQueue.add(new UpdatedContainerInfo(newlyLaunchedContainers,
          newlyCompletedContainers, needUpdateContainers));
    }
  }

  private List<ContainerStatus> findLostContainers(int numRemoteRunning,
      List<ContainerStatus> containerStatuses) {
    if (numRemoteRunning >= launchedContainers.size()) {
      return Collections.emptyList();
    }
    Set<ContainerId> nodeContainers =
        new HashSet<ContainerId>(numRemoteRunning);
    List<ContainerStatus> lostContainers = new ArrayList<ContainerStatus>(
        launchedContainers.size() - numRemoteRunning);
    for (ContainerStatus remoteContainer : containerStatuses) {
      if (remoteContainer.getState() == ContainerState.RUNNING
          && remoteContainer.getExecutionType() == ExecutionType.GUARANTEED) {
        nodeContainers.add(remoteContainer.getContainerId());
      }
    }
    Iterator<ContainerId> iter = launchedContainers.iterator();
    while (iter.hasNext()) {
      ContainerId containerId = iter.next();
      if (!nodeContainers.contains(containerId)) {
        String diag = "Container " + containerId
            + " was running but not reported from " + nodeId;
        LOG.warn(diag);
        lostContainers.add(SchedulerUtils.createAbnormalContainerStatus(
            containerId, diag));
        iter.remove();
      }
    }
    return lostContainers;
  }

  private void handleLogAggregationStatus(
      List<LogAggregationReport> logAggregationReportsForApps) {
    for (LogAggregationReport report : logAggregationReportsForApps) {
      RMApp rmApp = this.context.getRMApps().get(report.getApplicationId());
      if (rmApp != null) {
        ((RMAppImpl)rmApp).aggregateLogReport(this.nodeId, report);
      }
    }
  }

  @Override
  public List<Container> pullNewlyIncreasedContainers() {
    writeLock.lock();
    try {
      if (nmReportedIncreasedContainers.isEmpty()) {
        return Collections.emptyList();
      } else {
        List<Container> container =
            new ArrayList<Container>(nmReportedIncreasedContainers.values());
        nmReportedIncreasedContainers.clear();
        return container;
      }
      
    } finally {
      writeLock.unlock();
    }
   }

  public Resource getOriginalTotalCapability() {
    return this.originalTotalCapability;
  }

  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    this.readLock.lock();

    try {
      return this.opportunisticContainersStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  public void setOpportunisticContainersStatus(
      OpportunisticContainersStatus opportunisticContainersStatus) {
    this.writeLock.lock();

    try {
      this.opportunisticContainersStatus = opportunisticContainersStatus;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public long getUntrackedTimeStamp() {
    return this.timeStamp;
  }

  @Override
  public void setUntrackedTimeStamp(long ts) {
    this.timeStamp = ts;
  }

  @Override
  public Integer getDecommissioningTimeout() {
    return decommissioningTimeout;
  }

  @Override
  public Map<String, Long> getAllocationTagsWithCount() {
    return context.getAllocationTagsManager()
        .getAllocationTagsWithCount(getNodeID());
  }

  @Override
  public RMContext getRMContext() {
    return this.context;
  }

  @Override
  public Set<NodeAttribute> getAllNodeAttributes() {
    NodeAttributesManager attrMgr = context.getNodeAttributesManager();
    Map<NodeAttribute, AttributeValue> nodeattrs =
        attrMgr.getAttributesForNode(hostName);
    return nodeattrs.keySet();
  }

  @VisibleForTesting
  public Set<ContainerId> getContainersToBeRemovedFromNM() {
    return containersToBeRemovedFromNM;
  }
}
