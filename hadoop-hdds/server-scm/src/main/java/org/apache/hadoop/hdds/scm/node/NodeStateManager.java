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

package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.*;
import org.apache.hadoop.hdds.scm.node.states.Node2PipelineMap;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine
    .InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL;

/**
 * NodeStateManager maintains the state of all the datanodes in the cluster. All
 * the node state change should happen only via NodeStateManager. It also
 * runs a heartbeat thread which periodically updates the node state.
 * <p>
 * The getNode(byState) functions make copy of node maps and then creates a list
 * based on that. It should be assumed that these get functions always report
 * *stale* information. For example, getting the deadNodeCount followed by
 * getNodes(DEAD) could very well produce totally different count. Also
 * getNodeCount(HEALTHY) + getNodeCount(DEAD) + getNodeCode(STALE), is not
 * guaranteed to add up to the total nodes that we know off. Please treat all
 * get functions in this file as a snap-shot of information that is inconsistent
 * as soon as you read it.
 */
public class NodeStateManager implements Runnable, Closeable {

  /**
   * Node's life cycle events.
   */
  private enum NodeLifeCycleEvent {
    TIMEOUT, RESTORE, RESURRECT, DECOMMISSION, DECOMMISSIONED
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(NodeStateManager.class);

  /**
   * StateMachine for node lifecycle.
   */
  private final StateMachine<NodeState, NodeLifeCycleEvent> stateMachine;
  /**
   * This is the map which maintains the current state of all datanodes.
   */
  private final NodeStateMap nodeStateMap;
  /**
   * Maintains the mapping from node to pipelines a node is part of.
   */
  private final Node2PipelineMap node2PipelineMap;
  /**
   * Used for publishing node state change events.
   */
  private final EventPublisher eventPublisher;
  /**
   * Maps the event to be triggered when a node state us updated.
   */
  private final Map<NodeState, Event<DatanodeDetails>> state2EventMap;
  /**
   * ExecutorService used for scheduling heartbeat processing thread.
   */
  private final ScheduledExecutorService executorService;
  /**
   * The frequency in which we have run the heartbeat processing thread.
   */
  private final long heartbeatCheckerIntervalMs;
  /**
   * The timeout value which will be used for marking a datanode as stale.
   */
  private final long staleNodeIntervalMs;
  /**
   * The timeout value which will be used for marking a datanode as dead.
   */
  private final long deadNodeIntervalMs;

  /**
   * The future is used to pause/unpause the scheduled checks.
   */
  private ScheduledFuture<?> healthCheckFuture;

  /**
   * Test utility - tracks if health check has been paused (unit tests).
   */
  private boolean checkPaused;

  /**
   * timestamp of the latest heartbeat check process.
   */
  private long lastHealthCheck;

  /**
   * number of times the heart beat check was skipped.
   */
  private long skippedHealthChecks;

  /**
   * Constructs a NodeStateManager instance with the given configuration.
   *
   * @param conf Configuration
   */
  public NodeStateManager(Configuration conf, EventPublisher eventPublisher) {
    this.nodeStateMap = new NodeStateMap();
    this.node2PipelineMap = new Node2PipelineMap();
    this.eventPublisher = eventPublisher;
    this.state2EventMap = new HashMap<>();
    initialiseState2EventMap();
    Set<NodeState> finalStates = new HashSet<>();
    finalStates.add(NodeState.DECOMMISSIONED);
    this.stateMachine = new StateMachine<>(NodeState.HEALTHY, finalStates);
    initializeStateMachine();
    heartbeatCheckerIntervalMs = HddsServerUtil
        .getScmheartbeatCheckerInterval(conf);
    staleNodeIntervalMs = HddsServerUtil.getStaleNodeInterval(conf);
    deadNodeIntervalMs = HddsServerUtil.getDeadNodeInterval(conf);
    Preconditions.checkState(heartbeatCheckerIntervalMs > 0,
        OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL + " should be greater than 0.");
    Preconditions.checkState(staleNodeIntervalMs < deadNodeIntervalMs,
        OZONE_SCM_STALENODE_INTERVAL + " should be less than" +
            OZONE_SCM_DEADNODE_INTERVAL);
    executorService = HadoopExecutors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("SCM Heartbeat Processing Thread - %d").build());

    skippedHealthChecks = 0;
    checkPaused = false; // accessed only from test functions

    scheduleNextHealthCheck();
  }

  /**
   * Populates state2event map.
   */
  private void initialiseState2EventMap() {
    state2EventMap.put(NodeState.STALE, SCMEvents.STALE_NODE);
    state2EventMap.put(NodeState.DEAD, SCMEvents.DEAD_NODE);
    state2EventMap
        .put(NodeState.HEALTHY, SCMEvents.NON_HEALTHY_TO_HEALTHY_NODE);
  }

  /*
   *
   * Node and State Transition Mapping:
   *
   * State: HEALTHY         -------------------> STALE
   * Event:                       TIMEOUT
   *
   * State: STALE           -------------------> DEAD
   * Event:                       TIMEOUT
   *
   * State: STALE           -------------------> HEALTHY
   * Event:                       RESTORE
   *
   * State: DEAD            -------------------> HEALTHY
   * Event:                       RESURRECT
   *
   * State: HEALTHY         -------------------> DECOMMISSIONING
   * Event:                     DECOMMISSION
   *
   * State: STALE           -------------------> DECOMMISSIONING
   * Event:                     DECOMMISSION
   *
   * State: DEAD            -------------------> DECOMMISSIONING
   * Event:                     DECOMMISSION
   *
   * State: DECOMMISSIONING -------------------> DECOMMISSIONED
   * Event:                     DECOMMISSIONED
   *
   *  Node State Flow
   *
   *  +--------------------------------------------------------+
   *  |                                     (RESURRECT)        |
   *  |   +--------------------------+                         |
   *  |   |      (RESTORE)           |                         |
   *  |   |                          |                         |
   *  V   V                          |                         |
   * [HEALTHY]------------------->[STALE]------------------->[DEAD]
   *    |         (TIMEOUT)          |         (TIMEOUT)       |
   *    |                            |                         |
   *    |                            |                         |
   *    |                            |                         |
   *    |                            |                         |
   *    | (DECOMMISSION)             | (DECOMMISSION)          | (DECOMMISSION)
   *    |                            V                         |
   *    +------------------->[DECOMMISSIONING]<----------------+
   *                                 |
   *                                 | (DECOMMISSIONED)
   *                                 |
   *                                 V
   *                          [DECOMMISSIONED]
   *
   */

  /**
   * Initializes the lifecycle of node state machine.
   */
  private void initializeStateMachine() {
    stateMachine.addTransition(
        NodeState.HEALTHY, NodeState.STALE, NodeLifeCycleEvent.TIMEOUT);
    stateMachine.addTransition(
        NodeState.STALE, NodeState.DEAD, NodeLifeCycleEvent.TIMEOUT);
    stateMachine.addTransition(
        NodeState.STALE, NodeState.HEALTHY, NodeLifeCycleEvent.RESTORE);
    stateMachine.addTransition(
        NodeState.DEAD, NodeState.HEALTHY, NodeLifeCycleEvent.RESURRECT);
    stateMachine.addTransition(
        NodeState.HEALTHY, NodeState.DECOMMISSIONING,
        NodeLifeCycleEvent.DECOMMISSION);
    stateMachine.addTransition(
        NodeState.STALE, NodeState.DECOMMISSIONING,
        NodeLifeCycleEvent.DECOMMISSION);
    stateMachine.addTransition(
        NodeState.DEAD, NodeState.DECOMMISSIONING,
        NodeLifeCycleEvent.DECOMMISSION);
    stateMachine.addTransition(
        NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED,
        NodeLifeCycleEvent.DECOMMISSIONED);

  }

  /**
   * Adds a new node to the state manager.
   *
   * @param datanodeDetails DatanodeDetails
   *
   * @throws NodeAlreadyExistsException if the node is already present
   */
  public void addNode(DatanodeDetails datanodeDetails)
      throws NodeAlreadyExistsException {
    nodeStateMap.addNode(datanodeDetails, stateMachine.getInitialState());
    eventPublisher.fireEvent(SCMEvents.NEW_NODE, datanodeDetails);
  }

  /**
   * Adds a pipeline in the node2PipelineMap.
   * @param pipeline - Pipeline to be added
   */
  public void addPipeline(Pipeline pipeline) {
    node2PipelineMap.addPipeline(pipeline);
  }

  /**
   * Get the count of pipelines associated to single datanode.
   * @param datanodeDetails single datanode
   * @return number of pipelines associated with it
   */
  public int getPipelinesCount(DatanodeDetails datanodeDetails) {
    return node2PipelineMap.getPipelinesCount(datanodeDetails.getUuid());
  }

  /**
   * Get information about the node.
   *
   * @param datanodeDetails DatanodeDetails
   *
   * @return DatanodeInfo
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public DatanodeInfo getNode(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return nodeStateMap.getNodeInfo(datanodeDetails.getUuid());
  }

  /**
   * Updates the last heartbeat time of the node.
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public void updateLastHeartbeatTime(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    nodeStateMap.getNodeInfo(datanodeDetails.getUuid())
        .updateLastHeartbeatTime();
  }

  /**
   * Returns the current state of the node.
   *
   * @param datanodeDetails DatanodeDetails
   *
   * @return NodeState
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public NodeState getNodeState(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return nodeStateMap.getNodeState(datanodeDetails.getUuid());
  }

  /**
   * Returns all the node which are in healthy state.
   *
   * @return list of healthy nodes
   */
  public List<DatanodeInfo> getHealthyNodes() {
    return getNodes(NodeState.HEALTHY);
  }

  /**
   * Returns all the node which are in stale state.
   *
   * @return list of stale nodes
   */
  public List<DatanodeInfo> getStaleNodes() {
    return getNodes(NodeState.STALE);
  }

  /**
   * Returns all the node which are in dead state.
   *
   * @return list of dead nodes
   */
  public List<DatanodeInfo> getDeadNodes() {
    return getNodes(NodeState.DEAD);
  }

  /**
   * Returns all the node which are in the specified state.
   *
   * @param state NodeState
   *
   * @return list of nodes
   */
  public List<DatanodeInfo> getNodes(NodeState state) {
    List<DatanodeInfo> nodes = new ArrayList<>();
    nodeStateMap.getNodes(state).forEach(
        uuid -> {
          try {
            nodes.add(nodeStateMap.getNodeInfo(uuid));
          } catch (NodeNotFoundException e) {
            // This should not happen unless someone else other than
            // NodeStateManager is directly modifying NodeStateMap and removed
            // the node entry after we got the list of UUIDs.
            LOG.error("Inconsistent NodeStateMap! " + nodeStateMap);
          }
        });
    return nodes;
  }

  /**
   * Returns all the nodes which have registered to NodeStateManager.
   *
   * @return all the managed nodes
   */
  public List<DatanodeInfo> getAllNodes() {
    List<DatanodeInfo> nodes = new ArrayList<>();
    nodeStateMap.getAllNodes().forEach(
        uuid -> {
          try {
            nodes.add(nodeStateMap.getNodeInfo(uuid));
          } catch (NodeNotFoundException e) {
            // This should not happen unless someone else other than
            // NodeStateManager is directly modifying NodeStateMap and removed
            // the node entry after we got the list of UUIDs.
            LOG.error("Inconsistent NodeStateMap! " + nodeStateMap);
          }
        });
    return nodes;
  }

  /**
   * Gets set of pipelineID a datanode belongs to.
   * @param dnId - Datanode ID
   * @return Set of PipelineID
   */
  public Set<PipelineID> getPipelineByDnID(UUID dnId) {
    return node2PipelineMap.getPipelines(dnId);
  }

  /**
   * Returns the count of healthy nodes.
   *
   * @return healthy node count
   */
  public int getHealthyNodeCount() {
    return getNodeCount(NodeState.HEALTHY);
  }

  /**
   * Returns the count of stale nodes.
   *
   * @return stale node count
   */
  public int getStaleNodeCount() {
    return getNodeCount(NodeState.STALE);
  }

  /**
   * Returns the count of dead nodes.
   *
   * @return dead node count
   */
  public int getDeadNodeCount() {
    return getNodeCount(NodeState.DEAD);
  }

  /**
   * Returns the count of nodes in specified state.
   *
   * @param state NodeState
   *
   * @return node count
   */
  public int getNodeCount(NodeState state) {
    return nodeStateMap.getNodeCount(state);
  }

  /**
   * Returns the count of all nodes managed by NodeStateManager.
   *
   * @return node count
   */
  public int getTotalNodeCount() {
    return nodeStateMap.getTotalNodeCount();
  }

  /**
   * Removes a pipeline from the node2PipelineMap.
   * @param pipeline - Pipeline to be removed
   */
  public void removePipeline(Pipeline pipeline) {
    node2PipelineMap.removePipeline(pipeline);
  }

  /**
   * Adds the given container to the specified datanode.
   *
   * @param uuid - datanode uuid
   * @param containerId - containerID
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                        use addDatanodeInContainerMap call.
   */
  public void addContainer(final UUID uuid,
                           final ContainerID containerId)
      throws NodeNotFoundException {
    nodeStateMap.addContainer(uuid, containerId);
  }

  /**
   * Update set of containers available on a datanode.
   * @param uuid - DatanodeID
   * @param containerIds - Set of containerIDs
   * @throws NodeNotFoundException - if datanode is not known.
   */
  public void setContainers(UUID uuid, Set<ContainerID> containerIds)
      throws NodeNotFoundException {
    nodeStateMap.setContainers(uuid, containerIds);
  }

  /**
   * Return set of containerIDs available on a datanode.
   * @param uuid - DatanodeID
   * @return - set of containerIDs
   */
  public Set<ContainerID> getContainers(UUID uuid)
      throws NodeNotFoundException {
    return nodeStateMap.getContainers(uuid);
  }

  /**
   * Move Stale or Dead node to healthy if we got a heartbeat from them.
   * Move healthy nodes to stale nodes if it is needed.
   * Move Stales node to dead if needed.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {

    if (shouldSkipCheck()) {
      skippedHealthChecks++;
      LOG.info("Detected long delay in scheduling HB processing thread. "
          + "Skipping heartbeat checks for one iteration.");
    } else {
      checkNodesHealth();
    }

    // we purposefully make this non-deterministic. Instead of using a
    // scheduleAtFixedFrequency  we will just go to sleep
    // and wake up at the next rendezvous point, which is currentTime +
    // heartbeatCheckerIntervalMs. This leads to the issue that we are now
    // heart beating not at a fixed cadence, but clock tick + time taken to
    // work.
    //
    // This time taken to work can skew the heartbeat processor thread.
    // The reason why we don't care is because of the following reasons.
    //
    // 1. checkerInterval is general many magnitudes faster than datanode HB
    // frequency.
    //
    // 2. if we have too much nodes, the SCM would be doing only HB
    // processing, this could lead to SCM's CPU starvation. With this
    // approach we always guarantee that  HB thread sleeps for a little while.
    //
    // 3. It is possible that we will never finish processing the HB's in the
    // thread. But that means we have a mis-configured system. We will warn
    // the users by logging that information.
    //
    // 4. And the most important reason, heartbeats are not blocked even if
    // this thread does not run, they will go into the processing queue.
    scheduleNextHealthCheck();
  }

  private void checkNodesHealth() {

    /*
     *
     *          staleNodeDeadline                healthyNodeDeadline
     *                 |                                  |
     *      Dead       |             Stale                |     Healthy
     *      Node       |             Node                 |     Node
     *      Window     |             Window               |     Window
     * ----------------+----------------------------------+------------------->
     *                      >>-->> time-line >>-->>
     *
     * Here is the logic of computing the health of a node.
     *
     * 1. We get the current time and look back that the time
     *    when we got a heartbeat from a node.
     * 
     * 2. If the last heartbeat was within the window of healthy node we mark
     *    it as healthy.
     * 
     * 3. If the last HB Time stamp is longer and falls within the window of
     *    Stale Node time, we will mark it as Stale.
     * 
     * 4. If the last HB time is older than the Stale Window, then the node is
     *    marked as dead.
     *
     * The Processing starts from current time and looks backwards in time.
     */
    long processingStartTime = Time.monotonicNow();
    // After this time node is considered to be stale.
    long healthyNodeDeadline = processingStartTime - staleNodeIntervalMs;
    // After this time node is considered to be dead.
    long staleNodeDeadline = processingStartTime - deadNodeIntervalMs;

    Predicate<Long> healthyNodeCondition =
        (lastHbTime) -> lastHbTime >= healthyNodeDeadline;
    // staleNodeCondition is superset of stale and dead node
    Predicate<Long> staleNodeCondition =
        (lastHbTime) -> lastHbTime < healthyNodeDeadline;
    Predicate<Long> deadNodeCondition =
        (lastHbTime) -> lastHbTime < staleNodeDeadline;
    try {
      for (NodeState state : NodeState.values()) {
        List<UUID> nodes = nodeStateMap.getNodes(state);
        for (UUID id : nodes) {
          DatanodeInfo node = nodeStateMap.getNodeInfo(id);
          switch (state) {
          case HEALTHY:
            // Move the node to STALE if the last heartbeat time is less than
            // configured stale-node interval.
            updateNodeState(node, staleNodeCondition, state,
                  NodeLifeCycleEvent.TIMEOUT);
            break;
          case STALE:
            // Move the node to DEAD if the last heartbeat time is less than
            // configured dead-node interval.
            updateNodeState(node, deadNodeCondition, state,
                NodeLifeCycleEvent.TIMEOUT);
            // Restore the node if we have received heartbeat before configured
            // stale-node interval.
            updateNodeState(node, healthyNodeCondition, state,
                NodeLifeCycleEvent.RESTORE);
            break;
          case DEAD:
            // Resurrect the node if we have received heartbeat before
            // configured stale-node interval.
            updateNodeState(node, healthyNodeCondition, state,
                NodeLifeCycleEvent.RESURRECT);
            break;
            // We don't do anything for DECOMMISSIONING and DECOMMISSIONED in
            // heartbeat processing.
          case DECOMMISSIONING:
          case DECOMMISSIONED:
          default:
          }
        }
      }
    } catch (NodeNotFoundException e) {
      // This should not happen unless someone else other than
      // NodeStateManager is directly modifying NodeStateMap and removed
      // the node entry after we got the list of UUIDs.
      LOG.error("Inconsistent NodeStateMap! " + nodeStateMap);
    }
    long processingEndTime = Time.monotonicNow();
    //If we have taken too much time for HB processing, log that information.
    if ((processingEndTime - processingStartTime) >
        heartbeatCheckerIntervalMs) {
      LOG.error("Total time spend processing datanode HB's is greater than " +
              "configured values for datanode heartbeats. Please adjust the" +
              " heartbeat configs. Time Spend on HB processing: {} seconds " +
              "Datanode heartbeat Interval: {} seconds.",
          TimeUnit.MILLISECONDS
              .toSeconds(processingEndTime - processingStartTime),
          heartbeatCheckerIntervalMs);
    }

  }

  private void scheduleNextHealthCheck() {

    if (!Thread.currentThread().isInterrupted() &&
        !executorService.isShutdown()) {
      //BUGBUG: The return future needs to checked here to make sure the
      // exceptions are handled correctly.
      healthCheckFuture = executorService.schedule(this,
          heartbeatCheckerIntervalMs, TimeUnit.MILLISECONDS);
    } else {
      LOG.warn("Current Thread is interrupted, shutting down HB processing " +
          "thread for Node Manager.");
    }

    lastHealthCheck = Time.monotonicNow();
  }

  /**
   * if the time since last check exceeds the stale|dead node interval, skip.
   * such long delays might be caused by a JVM pause. SCM cannot make reliable
   * conclusions about datanode health in such situations.
   * @return : true indicates skip HB checks
   */
  private boolean shouldSkipCheck() {

    long currentTime = Time.monotonicNow();
    long minInterval = Math.min(staleNodeIntervalMs, deadNodeIntervalMs);

    return ((currentTime - lastHealthCheck) >= minInterval);
  }

  /**
   * Updates the node state if the condition satisfies.
   *
   * @param node DatanodeInfo
   * @param condition condition to check
   * @param state current state of node
   * @param lifeCycleEvent NodeLifeCycleEvent to be applied if condition
   *                       matches
   *
   * @throws NodeNotFoundException if the node is not present
   */
  private void updateNodeState(DatanodeInfo node, Predicate<Long> condition,
      NodeState state, NodeLifeCycleEvent lifeCycleEvent)
      throws NodeNotFoundException {
    try {
      if (condition.test(node.getLastHeartbeatTime())) {
        NodeState newState = stateMachine.getNextState(state, lifeCycleEvent);
        nodeStateMap.updateNodeState(node.getUuid(), state, newState);
        if (state2EventMap.containsKey(newState)) {
          eventPublisher.fireEvent(state2EventMap.get(newState), node);
        }
      }
    } catch (InvalidStateTransitionException e) {
      LOG.warn("Invalid state transition of node {}." +
              " Current state: {}, life cycle event: {}",
          node, state, lifeCycleEvent);
    }
  }

  @Override
  public void close() {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }

      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown NodeStateManager properly.");
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Test Utility : return number of times heartbeat check was skipped.
   * @return : count of times HB process was skipped
   */
  @VisibleForTesting
  long getSkippedHealthChecks() {
    return skippedHealthChecks;
  }

  /**
   * Test Utility : Pause the periodic node hb check.
   * @return ScheduledFuture for the scheduled check that got cancelled.
   */
  @VisibleForTesting
  ScheduledFuture pause() {

    if (executorService.isShutdown() || checkPaused) {
      return null;
    }

    checkPaused = healthCheckFuture.cancel(false);

    return healthCheckFuture;
  }

  /**
   * Test utility : unpause the periodic node hb check.
   * @return ScheduledFuture for the next scheduled check
   */
  @VisibleForTesting
  ScheduledFuture unpause() {

    if (executorService.isShutdown()) {
      return null;
    }

    if (checkPaused) {
      Preconditions.checkState(((healthCheckFuture == null)
          || healthCheckFuture.isCancelled()
          || healthCheckFuture.isDone()));

      checkPaused = false;
      /**
       * We do not call scheduleNextHealthCheck because we are
       * not updating the lastHealthCheck timestamp.
       */
      healthCheckFuture = executorService.schedule(this,
          heartbeatCheckerIntervalMs, TimeUnit.MILLISECONDS);
    }

    return healthCheckFuture;
  }
}
