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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils.ContainerIdComparator;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used to keep track of all the applications/containers
 * running on a node.
 *
 */
@Private
@Unstable
@SuppressWarnings("unchecked")
public class RMNodeImpl implements RMNode, EventHandler<RMNodeEvent> {

  private static final Log LOG = LogFactory.getLog(RMNodeImpl.class);

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
  private final int httpPort;
  private final String nodeAddress; // The containerManager address
  private final String httpAddress;
  private final Resource totalCapability;
  private final Node node;
  private final NodeHealthStatus nodeHealthStatus = recordFactory
      .newRecordInstance(NodeHealthStatus.class);
  
  /* set of containers that have just launched */
  private final Map<ContainerId, ContainerStatus> justLaunchedContainers = 
    new HashMap<ContainerId, ContainerStatus>();

  /* set of containers that need to be cleaned */
  private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(
      new ContainerIdComparator());

  /* the list of applications that have finished and need to be purged */
  private final List<ApplicationId> finishedApplications = new ArrayList<ApplicationId>();

  private HeartbeatResponse latestHeartBeatResponse = recordFactory
      .newRecordInstance(HeartbeatResponse.class);
  
  private static final StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent> stateMachineFactory 
                 = new StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent>(NodeState.NEW)
  
     //Transitions from NEW state
     .addTransition(NodeState.NEW, NodeState.RUNNING, 
         RMNodeEventType.STARTED, new AddNodeTransition())

     //Transitions from RUNNING state
     .addTransition(NodeState.RUNNING, 
         EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
         RMNodeEventType.STATUS_UPDATE, new StatusUpdateWhenHealthyTransition())
     .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONED,
         RMNodeEventType.DECOMMISSION,
         new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
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
         RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())

     //Transitions from UNHEALTHY state
     .addTransition(NodeState.UNHEALTHY, 
         EnumSet.of(NodeState.UNHEALTHY, NodeState.RUNNING),
         RMNodeEventType.STATUS_UPDATE, new StatusUpdateWhenUnHealthyTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONED,
         RMNodeEventType.DECOMMISSION,
         new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
     .addTransition(NodeState.UNHEALTHY, NodeState.LOST,
         RMNodeEventType.EXPIRE,
         new DeactivateNodeTransition(NodeState.LOST))
     .addTransition(NodeState.UNHEALTHY, NodeState.REBOOTED,
         RMNodeEventType.REBOOTING,
         new DeactivateNodeTransition(NodeState.REBOOTED))
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
         
     // create the topology tables
     .installTopology(); 

  private final StateMachine<NodeState, RMNodeEventType,
                             RMNodeEvent> stateMachine;

  public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
      int cmPort, int httpPort, Node node, Resource capability) {
    this.nodeId = nodeId;
    this.context = context;
    this.hostName = hostName;
    this.commandPort = cmPort;
    this.httpPort = httpPort;
    this.totalCapability = capability; 
    this.nodeAddress = hostName + ":" + cmPort;
    this.httpAddress = hostName + ":" + httpPort;
    this.node = node;
    this.nodeHealthStatus.setIsNodeHealthy(true);
    this.nodeHealthStatus.setHealthReport("Healthy");
    this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());

    this.latestHeartBeatResponse.setResponseId(0);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);
    
    this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();  
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
  public String getRackName() {
    return node.getNetworkLocation();
  }

  @Override
  public Node getNode() {
    return this.node;
  }
  
  @Override
  public NodeHealthStatus getNodeHealthStatus() {
    this.readLock.lock();

    try {
      return this.nodeHealthStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  private void setNodeHealthStatus(NodeHealthStatus status)
  {
    this.writeLock.lock();
    try {
      this.nodeHealthStatus.setHealthReport(status.getHealthReport());
      this.nodeHealthStatus.setIsNodeHealthy(status.getIsNodeHealthy());
      this.nodeHealthStatus.setLastHealthReportTime(status.getLastHealthReportTime());
    } finally {
      this.writeLock.unlock();
    }
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
  public List<ContainerId> getContainersToCleanUp() {

    this.readLock.lock();

    try {
      return new ArrayList<ContainerId>(this.containersToClean);
    } finally {
      this.readLock.unlock();
    }
  };

  @Override
  public HeartbeatResponse getLastHeartBeatResponse() {

    this.readLock.lock();

    try {
      return this.latestHeartBeatResponse;
    } finally {
      this.readLock.unlock();
    }
  }

  public void handle(RMNodeEvent event) {
    LOG.debug("Processing " + event.getNodeId() + " of type " + event.getType());
    try {
      writeLock.lock();
      NodeState oldState = getState();
      try {
         stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + 
            " on Node  " + this.nodeId);
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
    metrics.incrNumActiveNodes();

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
    }
  }

  private void updateMetricsForDeactivatedNode(NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    metrics.decrNumActiveNodes();

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
    }
  }

  public static class AddNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler

      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeAddedSchedulerEvent(rmNode));
      
      String host = rmNode.nodeId.getHost();
      if (rmNode.context.getInactiveRMNodes().containsKey(host)) {
        // Old node rejoining
        RMNode previouRMNode = rmNode.context.getInactiveRMNodes().get(host);
        rmNode.context.getInactiveRMNodes().remove(host);
        rmNode.updateMetricsForRejoinedNode(previouRMNode.getState());
      } else {
        // Increment activeNodes explicitly because this is a new node.
        ClusterMetrics.getMetrics().incrNumActiveNodes();
      }
    }
  }
  
  public static class ReconnectNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Kill containers since node is rejoining.
      rmNode.nodeUpdateQueue.clear();
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeRemovedSchedulerEvent(rmNode));

      RMNode newNode = ((RMNodeReconnectEvent)event).getReconnectedNode();
      if (rmNode.getTotalCapability().equals(newNode.getTotalCapability())
          && rmNode.getHttpPort() == newNode.getHttpPort()) {
        // Reset heartbeat ID since node just restarted.
        rmNode.getLastHeartBeatResponse().setResponseId(0);
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeAddedSchedulerEvent(rmNode));
      } else {
        // Reconnected node differs, so replace old node and start new node
        switch (rmNode.getState()) {
        case RUNNING:
          ClusterMetrics.getMetrics().decrNumActiveNodes();
          break;
        case UNHEALTHY:
          ClusterMetrics.getMetrics().decrNumUnhealthyNMs();
          break;
        }
        rmNode.context.getRMNodes().put(newNode.getNodeID(), newNode);
        rmNode.context.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(newNode.getNodeID(), RMNodeEventType.STARTED));
      }
    }
  }

  public static class CleanUpAppTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.finishedApplications.add(((
          RMNodeCleanAppEvent) event).getAppId());
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

  public static class DeactivateNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    private final NodeState finalState;
    public DeactivateNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler
      rmNode.nodeUpdateQueue.clear();
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeRemovedSchedulerEvent(rmNode));
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodesListManagerEvent(
              NodesListManagerEventType.NODE_UNUSABLE, rmNode));

      // Deactivate the node
      rmNode.context.getRMNodes().remove(rmNode.nodeId);
      LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
          + finalState);
      rmNode.context.getInactiveRMNodes().put(rmNode.nodeId.getHost(), rmNode);

      //Update the metrics
      rmNode.updateMetricsForDeactivatedNode(finalState);
    }
  }

  public static class StatusUpdateWhenHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {
    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {

      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestHeartBeatResponse = statusEvent.getLatestResponse();

      NodeHealthStatus remoteNodeHealthStatus = 
          statusEvent.getNodeHealthStatus();
      rmNode.setNodeHealthStatus(remoteNodeHealthStatus);
      if (!remoteNodeHealthStatus.getIsNodeHealthy()) {
        LOG.info("Node " + rmNode.nodeId + " reported UNHEALTHY with details: "
            + remoteNodeHealthStatus.getHealthReport());
        rmNode.nodeUpdateQueue.clear();
        // Inform the scheduler
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeRemovedSchedulerEvent(rmNode));
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodesListManagerEvent(
                NodesListManagerEventType.NODE_UNUSABLE, rmNode));
        // Update metrics
        rmNode.updateMetricsForDeactivatedNode(NodeState.UNHEALTHY);
        return NodeState.UNHEALTHY;
      }

      // Filter the map to only obtain just launched containers and finished
      // containers.
      List<ContainerStatus> newlyLaunchedContainers = 
          new ArrayList<ContainerStatus>();
      List<ContainerStatus> completedContainers = 
          new ArrayList<ContainerStatus>();
      for (ContainerStatus remoteContainer : statusEvent.getContainers()) {
        ContainerId containerId = remoteContainer.getContainerId();
        
        // Don't bother with containers already scheduled for cleanup, or for
        // applications already killed. The scheduler doens't need to know any
        // more about this container
        if (rmNode.containersToClean.contains(containerId)) {
          LOG.info("Container " + containerId + " already scheduled for " +
          		"cleanup, no further processing");
          continue;
        }
        if (rmNode.finishedApplications.contains(containerId
            .getApplicationAttemptId().getApplicationId())) {
          LOG.info("Container " + containerId
              + " belongs to an application that is already killed,"
              + " no further processing");
          continue;
        }

        // Process running containers
        if (remoteContainer.getState() == ContainerState.RUNNING) {
          if (!rmNode.justLaunchedContainers.containsKey(containerId)) {
            // Just launched container. RM knows about it the first time.
            rmNode.justLaunchedContainers.put(containerId, remoteContainer);
            newlyLaunchedContainers.add(remoteContainer);
          }
        } else {
          // A finished container
          rmNode.justLaunchedContainers.remove(containerId);
          completedContainers.add(remoteContainer);
        }
      }
      if(newlyLaunchedContainers.size() != 0 
          || completedContainers.size() != 0) {
        rmNode.nodeUpdateQueue.add(new UpdatedContainerInfo
            (newlyLaunchedContainers, completedContainers));
      }
      if(rmNode.nextHeartBeat) {
        rmNode.nextHeartBeat = false;
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeUpdateSchedulerEvent(rmNode));
      }
      
      rmNode.context.getDelegationTokenRenewer().updateKeepAliveApplications(
          statusEvent.getKeepAliveAppIds());

      // HeartBeat processing from our end is done, as node pulls the following
      // lists before sending status-updates. Clear data-structures
      // TODO: These lists could go to the NM multiple times, or never.
      rmNode.containersToClean.clear();
      rmNode.finishedApplications.clear();

      return NodeState.RUNNING;
    }
  }

  public static class StatusUpdateWhenUnHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestHeartBeatResponse = statusEvent.getLatestResponse();
      NodeHealthStatus remoteNodeHealthStatus = statusEvent.getNodeHealthStatus();
      rmNode.setNodeHealthStatus(remoteNodeHealthStatus);
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
        rmNode.updateMetricsForRejoinedNode(NodeState.UNHEALTHY);
        return NodeState.RUNNING;
      }

      return NodeState.UNHEALTHY;
    }
  }

  @Override
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    List<UpdatedContainerInfo> latestContainerInfoList = 
        new ArrayList<UpdatedContainerInfo>();
    while(nodeUpdateQueue.peek() != null){
      latestContainerInfoList.add(nodeUpdateQueue.poll());
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
 }
