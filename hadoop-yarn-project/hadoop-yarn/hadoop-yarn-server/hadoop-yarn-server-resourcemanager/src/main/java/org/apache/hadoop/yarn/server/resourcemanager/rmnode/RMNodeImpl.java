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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils.ContainerIdComparator;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

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
  private int httpPort;
  private final String nodeAddress; // The containerManager address
  private String httpAddress;
  private volatile Resource totalCapability;
  private final Node node;

  private String healthReport;
  private long lastHealthReportTime;
  private String nodeManagerVersion;

  /* set of containers that have just launched */
  private final Set<ContainerId> launchedContainers =
    new HashSet<ContainerId>();

  /* set of containers that need to be cleaned */
  private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(
      new ContainerIdComparator());

  /*
   * set of containers to notify NM to remove them from its context. Currently,
   * this includes containers that were notified to AM about their completion
   */
  private final Set<ContainerId> containersToBeRemovedFromNM =
      new HashSet<ContainerId>();

  /* the list of applications that have finished and need to be purged */
  private final List<ApplicationId> finishedApplications = new ArrayList<ApplicationId>();

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
     .addTransition(NodeState.NEW, NodeState.RUNNING, 
         RMNodeEventType.STARTED, new AddNodeTransition())
     .addTransition(NodeState.NEW, NodeState.NEW,
         RMNodeEventType.RESOURCE_UPDATE, 
         new UpdateNodeResourceWhenUnusableTransition())
     .addTransition(NodeState.NEW, NodeState.DECOMMISSIONED,
         RMNodeEventType.DECOMMISSION,
         new DeactivateNodeTransition(NodeState.DECOMMISSIONED))

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
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenRunningTransition())

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
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenUnusableTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())

     // create the topology tables
     .installTopology(); 

  private final StateMachine<NodeState, RMNodeEventType,
                             RMNodeEvent> stateMachine;

  public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
      int cmPort, int httpPort, Node node, Resource capability, String nodeManagerVersion) {
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

    this.latestNodeHeartBeatResponse.setResponseId(0);

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
  public void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response) {
    this.writeLock.lock();

    try {
      response.addAllContainersToCleanup(
          new ArrayList<ContainerId>(this.containersToClean));
      response.addAllApplicationsToCleanup(this.finishedApplications);
      response.addContainersToBeRemovedFromNM(
          new ArrayList<ContainerId>(this.containersToBeRemovedFromNM));
      this.containersToClean.clear();
      this.finishedApplications.clear();
      this.containersToBeRemovedFromNM.clear();
    } finally {
      this.writeLock.unlock();
    }
  };

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
    default:
      LOG.debug("Unexpected previous node state");    
    }
  }

  private void updateMetricsForDeactivatedNode(NodeState initialState,
                                               NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();

    switch (initialState) {
      case RUNNING:
        metrics.decrNumActiveNodes();
        break;
      case UNHEALTHY:
        metrics.decrNumUnhealthyNMs();
        break;
      default:
        LOG.debug("Unexpected inital state");
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
    case NEW:
      break;
    default:
      LOG.debug("Unexpected final state");
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
      return;
    }

    context.getDispatcher().getEventHandler()
        .handle(new RMAppRunningOnNodeEvent(appId, nodeId));
  }
  
  private static void updateNodeResourceFromEvent(RMNodeImpl rmNode, 
     RMNodeResourceUpdateEvent event){
      ResourceOption resourceOption = event.getResourceOption();
      // Set resource on RMNode
      rmNode.totalCapability = resourceOption.getResource();
  }

  public static class AddNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler
      RMNodeStartedEvent startEvent = (RMNodeStartedEvent) event;
      List<NMContainerStatus> containers = null;

      String host = rmNode.nodeId.getHost();
      RMNode previousRMNode = rmNode.context.getInactiveRMNodes().remove(host);
      if (previousRMNode != null) {
        if (previousRMNode.getNodeID().getPort() != -1) {
          // Old node rejoining
          rmNode.updateMetricsForRejoinedNode(previousRMNode.getState());
        } else {
          // An old excluded node rejoining
          ClusterMetrics.getMetrics().decrDecommisionedNMs();
          containers = updateNewNodeMetricsAndContainers(rmNode, startEvent);
        }
      } else {
        // Increment activeNodes explicitly because this is a new node.
        containers = updateNewNodeMetricsAndContainers(rmNode, startEvent);
      }

      if (null != startEvent.getRunningApplications()) {
        for (ApplicationId appId : startEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }
      }

      rmNode.context.getDispatcher().getEventHandler()
        .handle(new NodeAddedSchedulerEvent(rmNode, containers));
      rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_USABLE, rmNode));
    }
  }

  private static List<NMContainerStatus> updateNewNodeMetricsAndContainers(
      RMNodeImpl rmNode, RMNodeStartedEvent startEvent) {
    List<NMContainerStatus> containers;
    ClusterMetrics.getMetrics().incrNumActiveNodes();
    containers = startEvent.getNMContainerStatuses();
    if (containers != null && !containers.isEmpty()) {
      for (NMContainerStatus container : containers) {
        if (container.getContainerState() == ContainerState.RUNNING) {
          rmNode.launchedContainers.add(container.getContainerId());
        }
      }
    }
    return containers;
  }

  public static class ReconnectNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeReconnectEvent reconnectEvent = (RMNodeReconnectEvent) event;
      RMNode newNode = reconnectEvent.getReconnectedNode();
      rmNode.nodeManagerVersion = newNode.getNodeManagerVersion();
      List<ApplicationId> runningApps = reconnectEvent.getRunningApplications();
      boolean noRunningApps = 
          (runningApps == null) || (runningApps.size() == 0);
      
      // No application running on the node, so send node-removal event with 
      // cleaning up old container info.
      if (noRunningApps) {
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
        } else {
          // Reconnected node differs, so replace old node and start new node
          switch (rmNode.getState()) {
            case RUNNING:
              ClusterMetrics.getMetrics().decrNumActiveNodes();
              break;
            case UNHEALTHY:
              ClusterMetrics.getMetrics().decrNumUnhealthyNMs();
              break;
            default:
              LOG.debug("Unexpected Rmnode state");
            }
            rmNode.context.getRMNodes().put(newNode.getNodeID(), newNode);
            rmNode.context.getDispatcher().getEventHandler().handle(
                new RMNodeStartedEvent(newNode.getNodeID(), null, null));
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
    }

    private void handleNMContainerStatus(
        List<NMContainerStatus> nmContainerStatuses, RMNodeImpl rmnode) {
      List<ContainerStatus> containerStatuses =
          new ArrayList<ContainerStatus>();
      for (NMContainerStatus nmContainerStatus : nmContainerStatuses) {
        containerStatuses.add(createContainerStatus(nmContainerStatus));
      }
      rmnode.handleContainerStatus(containerStatuses);
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

  public static class AddContainersToBeRemovedFromNMTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToBeRemovedFromNM.addAll(((
          RMNodeFinishedContainersPulledByAMEvent) event).getContainers());
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
      //check for UnknownNodeId
      if (rmNode.getNodeID().getPort() == -1) {
        rmNode.updateMetricsForDeactivatedNode(rmNode.getState(), finalState);
        return;
      }
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

      // Deactivate the node
      rmNode.context.getRMNodes().remove(rmNode.nodeId);
      LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
          + finalState);
      rmNode.context.getInactiveRMNodes().put(rmNode.nodeId.getHost(), rmNode);

      //Update the metrics
      rmNode.updateMetricsForDeactivatedNode(initialState, finalState);
    }
  }

  public static class StatusUpdateWhenHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {
    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {

      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();

      NodeHealthStatus remoteNodeHealthStatus = 
          statusEvent.getNodeHealthStatus();
      rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
      rmNode.setLastHealthReportTime(
          remoteNodeHealthStatus.getLastHealthReportTime());
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
        rmNode.updateMetricsForDeactivatedNode(rmNode.getState(),
            NodeState.UNHEALTHY);
        return NodeState.UNHEALTHY;
      }

      rmNode.handleContainerStatus(statusEvent.getContainers());

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

      return NodeState.RUNNING;
    }
  }

  public static class StatusUpdateWhenUnHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();
      NodeHealthStatus remoteNodeHealthStatus = statusEvent.getNodeHealthStatus();
      rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
      rmNode.setLastHealthReportTime(
          remoteNodeHealthStatus.getLastHealthReportTime());
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
  public Set<ContainerId> getLaunchedContainers() {
    return this.launchedContainers;
  }

  @Override
  public Set<String> getNodeLabels() {
    RMNodeLabelsManager nlm = context.getNodeLabelManager();
    if (nlm == null || nlm.getLabelsOnNode(nodeId) == null) {
      return CommonNodeLabelsManager.EMPTY_STRING_SET;
    }
    return nlm.getLabelsOnNode(nodeId);
  }

  private void handleContainerStatus(List<ContainerStatus> containerStatuses) {
    // Filter the map to only obtain just launched containers and finished
    // containers.
    List<ContainerStatus> newlyLaunchedContainers =
        new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers =
        new ArrayList<ContainerStatus>();
    int numRemoteRunningContainers = 0;
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
      if (finishedApplications.contains(containerId.getApplicationAttemptId()
          .getApplicationId())) {
        LOG.info("Container " + containerId
            + " belongs to an application that is already killed,"
            + " no further processing");
        continue;
      }

      // Process running containers
      if (remoteContainer.getState() == ContainerState.RUNNING) {
        ++numRemoteRunningContainers;
        if (!launchedContainers.contains(containerId)) {
          // Just launched container. RM knows about it the first time.
          launchedContainers.add(containerId);
          newlyLaunchedContainers.add(remoteContainer);
        }
      } else {
        // A finished container
        launchedContainers.remove(containerId);
        completedContainers.add(remoteContainer);
      }
    }
    completedContainers.addAll(findLostContainers(
          numRemoteRunningContainers, containerStatuses));

    if (newlyLaunchedContainers.size() != 0 || completedContainers.size() != 0) {
      nodeUpdateQueue.add(new UpdatedContainerInfo(newlyLaunchedContainers,
          completedContainers));
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
      if (remoteContainer.getState() == ContainerState.RUNNING) {
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
 }
