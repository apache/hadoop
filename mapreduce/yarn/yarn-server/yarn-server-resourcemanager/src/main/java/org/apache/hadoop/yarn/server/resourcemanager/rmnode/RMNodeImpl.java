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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
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

/**
 * This class is used to keep track of all the applications/containers
 * running on a node.
 *
 */
@Private
@Unstable
public class RMNodeImpl implements RMNode, EventHandler<RMNodeEvent> {

  private static final Log LOG = LogFactory.getLog(RMNodeImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final ReadLock readLock;
  private final WriteLock writeLock;

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
  
  /* set of containers that need to be cleaned */
  private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(
      new ContainerIdComparator());

  /* the list of applications that have finished and need to be purged */
  private final List<ApplicationId> finishedApplications = new ArrayList<ApplicationId>();

  private HeartbeatResponse latestHeartBeatResponse = recordFactory
      .newRecordInstance(HeartbeatResponse.class);

  private static final StateMachineFactory<RMNodeImpl,
                                           RMNodeState,
                                           RMNodeEventType,
                                           RMNodeEvent> stateMachineFactory 
                 = new StateMachineFactory<RMNodeImpl,
                                           RMNodeState,
                                           RMNodeEventType,
                                           RMNodeEvent>(RMNodeState.RUNNING)
  
     //Transitions from RUNNING state
     .addTransition(RMNodeState.RUNNING, 
         EnumSet.of(RMNodeState.RUNNING, RMNodeState.UNHEALTHY),
         RMNodeEventType.STATUS_UPDATE, new StatusUpdateWhenHealthyTransition())
     .addTransition(RMNodeState.RUNNING, RMNodeState.DECOMMISSIONED,
         RMNodeEventType.DECOMMISSION, new RemoveNodeTransition())
     .addTransition(RMNodeState.RUNNING, RMNodeState.LOST,
         RMNodeEventType.EXPIRE, new RemoveNodeTransition())
     .addTransition(RMNodeState.RUNNING, RMNodeState.LOST,
         RMNodeEventType.REBOOTING, new RemoveNodeTransition())
     .addTransition(RMNodeState.RUNNING, RMNodeState.RUNNING,
         RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
     .addTransition(RMNodeState.RUNNING, RMNodeState.RUNNING,
         RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())

     //Transitions from UNHEALTHY state
     .addTransition(RMNodeState.UNHEALTHY, 
         EnumSet.of(RMNodeState.UNHEALTHY, RMNodeState.RUNNING),
         RMNodeEventType.STATUS_UPDATE, new StatusUpdateWhenUnHealthyTransition())
         
     // create the topology tables
     .installTopology(); 

  private final StateMachine<RMNodeState, RMNodeEventType,
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
    this.httpAddress = hostName + ":" + httpPort;;
    this.node = node;
    this.nodeHealthStatus.setIsNodeHealthy(true);
    this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());

    this.latestHeartBeatResponse.setResponseId(0);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);
    
    context.getDispatcher().getEventHandler().handle(
        new NodeAddedSchedulerEvent(this));
  }
  
  @Override
  public String getNodeHostName() {
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

  @Override
  public RMNodeState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ApplicationId> pullAppsToCleanup() {
    this.writeLock.lock();

    try {
      List<ApplicationId> lastfinishedApplications = new ArrayList<ApplicationId>();
      lastfinishedApplications.addAll(this.finishedApplications);
      this.finishedApplications.clear();
      return lastfinishedApplications;
    } finally {
      this.writeLock.unlock();
    }

  }

  @Override
  public List<ContainerId> pullContainersToCleanUp() {

    this.writeLock.lock();

    try {
      List<ContainerId> containersToCleanUp = new ArrayList<ContainerId>();
      containersToCleanUp.addAll(this.containersToClean);
      this.containersToClean.clear();
      return containersToCleanUp;
    } finally {
      this.writeLock.unlock();
    }
  };

  @Override
  public HeartbeatResponse getLastHeartBeatResponse() {

    this.writeLock.lock();

    try {
      return this.latestHeartBeatResponse;
    } finally {
      this.writeLock.unlock();
    }
  }

  public void handle(RMNodeEvent event) {
    LOG.info("Processing " + event.getNodeId() + " of type " + event.getType());
    try {
      writeLock.lock();
      RMNodeState oldState = getState();
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

  public static class RemoveNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeRemovedSchedulerEvent(rmNode));

      // Remove the node from the system.
      rmNode.context.getRMNodes().remove(rmNode.nodeId);
      LOG.info("Removed Node " + rmNode.nodeId);
      
    }
  }

  public static class StatusUpdateWhenHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, RMNodeState> {
    @Override
    public RMNodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {

      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestHeartBeatResponse = statusEvent.getLatestResponse();

      if (!statusEvent.getNodeHealthStatus().getIsNodeHealthy()) {
        // Inform the scheduler
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeRemovedSchedulerEvent(rmNode));
        return RMNodeState.UNHEALTHY;
      }

      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeUpdateSchedulerEvent(rmNode, statusEvent
              .getContainersCollection()));

      return RMNodeState.RUNNING;
    }
  }

  public static class StatusUpdateWhenUnHealthyTransition
 implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, RMNodeState> {

    @Override
    public RMNodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestHeartBeatResponse = statusEvent.getLatestResponse();

      if (statusEvent.getNodeHealthStatus().getIsNodeHealthy()) {
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeAddedSchedulerEvent(rmNode));
        return RMNodeState.RUNNING;
      }

      return RMNodeState.UNHEALTHY;
    }
  }
 }
