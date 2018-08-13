/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.pipelines;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipelines.ratis.RatisManagerImpl;
import org.apache.hadoop.hdds.scm.pipelines.standalone.StandaloneManagerImpl;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.statemachine
    .InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_PIPELINE_STATE;

/**
 * Sends the request to the right pipeline manager.
 */
public class PipelineSelector {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineSelector.class);
  private final ContainerPlacementPolicy placementPolicy;
  private final NodeManager nodeManager;
  private final Configuration conf;
  private final ContainerStateManager containerStateManager;
  private final EventPublisher eventPublisher;
  private final RatisManagerImpl ratisManager;
  private final StandaloneManagerImpl standaloneManager;
  private final long containerSize;
  private final Node2PipelineMap node2PipelineMap;
  private final LeaseManager<Pipeline> pipelineLeaseManager;
  private final StateMachine<LifeCycleState,
      HddsProtos.LifeCycleEvent> stateMachine;

  /**
   * Constructs a pipeline Selector.
   *
   * @param nodeManager - node manager
   * @param conf - Ozone Config
   */
  public PipelineSelector(NodeManager nodeManager,
      ContainerStateManager containerStateManager, Configuration conf,
      EventPublisher eventPublisher) {
    this.nodeManager = nodeManager;
    this.conf = conf;
    this.eventPublisher = eventPublisher;
    this.placementPolicy = createContainerPlacementPolicy(nodeManager, conf);
    this.containerSize = OzoneConsts.GB * this.conf.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);
    node2PipelineMap = new Node2PipelineMap();
    this.standaloneManager =
        new StandaloneManagerImpl(this.nodeManager, placementPolicy,
            containerSize, node2PipelineMap);
    this.ratisManager =
        new RatisManagerImpl(this.nodeManager, placementPolicy, containerSize,
            conf, node2PipelineMap);
    // Initialize the container state machine.
    Set<HddsProtos.LifeCycleState> finalStates = new HashSet();
    long pipelineCreationLeaseTimeout = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_LEASE_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_LEASE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.containerStateManager = containerStateManager;
    pipelineLeaseManager = new LeaseManager<>("PipelineCreation",
        pipelineCreationLeaseTimeout);
    pipelineLeaseManager.start();

    // These are the steady states of a container.
    finalStates.add(HddsProtos.LifeCycleState.OPEN);
    finalStates.add(HddsProtos.LifeCycleState.CLOSED);

    this.stateMachine = new StateMachine<>(HddsProtos.LifeCycleState.ALLOCATED,
        finalStates);
    initializeStateMachine();
  }

  /**
   * Event and State Transition Mapping:
   *
   * State: ALLOCATED ---------------> CREATING
   * Event:                CREATE
   *
   * State: CREATING  ---------------> OPEN
   * Event:               CREATED
   *
   * State: OPEN      ---------------> CLOSING
   * Event:               FINALIZE
   *
   * State: CLOSING   ---------------> CLOSED
   * Event:                CLOSE
   *
   * State: CREATING  ---------------> CLOSED
   * Event:               TIMEOUT
   *
   *
   * Container State Flow:
   *
   * [ALLOCATED]---->[CREATING]------>[OPEN]-------->[CLOSING]
   *            (CREATE)     | (CREATED)     (FINALIZE)   |
   *                         |                            |
   *                         |                            |
   *                         |(TIMEOUT)                   |(CLOSE)
   *                         |                            |
   *                         +--------> [CLOSED] <--------+
   */
  private void initializeStateMachine() {
    stateMachine.addTransition(HddsProtos.LifeCycleState.ALLOCATED,
        HddsProtos.LifeCycleState.CREATING,
        HddsProtos.LifeCycleEvent.CREATE);

    stateMachine.addTransition(HddsProtos.LifeCycleState.CREATING,
        HddsProtos.LifeCycleState.OPEN,
        HddsProtos.LifeCycleEvent.CREATED);

    stateMachine.addTransition(HddsProtos.LifeCycleState.OPEN,
        HddsProtos.LifeCycleState.CLOSING,
        HddsProtos.LifeCycleEvent.FINALIZE);

    stateMachine.addTransition(HddsProtos.LifeCycleState.CLOSING,
        HddsProtos.LifeCycleState.CLOSED,
        HddsProtos.LifeCycleEvent.CLOSE);

    stateMachine.addTransition(HddsProtos.LifeCycleState.CREATING,
        HddsProtos.LifeCycleState.CLOSED,
        HddsProtos.LifeCycleEvent.TIMEOUT);
  }

  /**
   * Translates a list of nodes, ordered such that the first is the leader, into
   * a corresponding {@link Pipeline} object.
   *
   * @param nodes - list of datanodes on which we will allocate the container.
   * The first of the list will be the leader node.
   * @return pipeline corresponding to nodes
   */
  public static Pipeline newPipelineFromNodes(
      List<DatanodeDetails> nodes, ReplicationType replicationType,
      ReplicationFactor replicationFactor, PipelineID id) {
    Preconditions.checkNotNull(nodes);
    Preconditions.checkArgument(nodes.size() > 0);
    String leaderId = nodes.get(0).getUuidString();
    // A new pipeline always starts in allocated state
    Pipeline pipeline = new Pipeline(leaderId, LifeCycleState.ALLOCATED,
        replicationType, replicationFactor, id);
    for (DatanodeDetails node : nodes) {
      pipeline.addMember(node);
    }
    return pipeline;
  }

  /**
   * Create pluggable container placement policy implementation instance.
   *
   * @param nodeManager - SCM node manager.
   * @param conf - configuration.
   * @return SCM container placement policy implementation instance.
   */
  @SuppressWarnings("unchecked")
  private static ContainerPlacementPolicy createContainerPlacementPolicy(
      final NodeManager nodeManager, final Configuration conf) {
    Class<? extends ContainerPlacementPolicy> implClass =
        (Class<? extends ContainerPlacementPolicy>) conf.getClass(
            ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
            SCMContainerPlacementRandom.class);

    try {
      Constructor<? extends ContainerPlacementPolicy> ctor =
          implClass.getDeclaredConstructor(NodeManager.class,
              Configuration.class);
      return ctor.newInstance(nodeManager, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(implClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
      LOG.error("Unhandled exception occurred, Placement policy will not be " +
          "functional.");
      throw new IllegalArgumentException("Unable to load " +
          "ContainerPlacementPolicy", e);
    }
  }

  /**
   * Return the pipeline manager from the replication type.
   *
   * @param replicationType - Replication Type Enum.
   * @return pipeline Manager.
   * @throws IllegalArgumentException If an pipeline type gets added
   * and this function is not modified we will throw.
   */
  private PipelineManager getPipelineManager(ReplicationType replicationType)
      throws IllegalArgumentException {
    switch (replicationType) {
    case RATIS:
      return this.ratisManager;
    case STAND_ALONE:
      return this.standaloneManager;
    case CHAINED:
      throw new IllegalArgumentException("Not implemented yet");
    default:
      throw new IllegalArgumentException("Unexpected enum found. Does not" +
          " know how to handle " + replicationType.toString());
    }

  }

  /**
   * This function is called by the Container Manager while allocating a new
   * container. The client specifies what kind of replication pipeline is needed
   * and based on the replication type in the request appropriate Interface is
   * invoked.
   */

  public Pipeline getReplicationPipeline(ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor)
      throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Getting replication pipeline forReplicationType {} :" +
            " ReplicationFactor {}", replicationType.toString(),
        replicationFactor.toString());

    /**
     * In the Ozone world, we have a very simple policy.
     *
     * 1. Try to create a pipeline if there are enough free nodes.
     *
     * 2. This allows all nodes to part of a pipeline quickly.
     *
     * 3. if there are not enough free nodes, return already allocated pipeline
     * in a round-robin fashion.
     *
     * TODO: Might have to come up with a better algorithm than this.
     * Create a new placement policy that returns pipelines in round robin
     * fashion.
     */
    Pipeline pipeline =
        manager.createPipeline(replicationFactor, replicationType);
    if (pipeline == null) {
      // try to return a pipeline from already allocated pipelines
      pipeline = manager.getPipeline(replicationFactor, replicationType);
    } else {
      // if a new pipeline is created, initialize its state machine
      updatePipelineState(pipeline,HddsProtos.LifeCycleEvent.CREATE);

      //TODO: move the initialization of pipeline to Ozone Client
      manager.initializePipeline(pipeline);
      updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.CREATED);
    }
    return pipeline;
  }

  /**
   * This function to return pipeline for given pipeline name and replication
   * type.
   */
  public Pipeline getPipeline(PipelineID pipelineID,
      ReplicationType replicationType) throws IOException {
    if (pipelineID == null) {
      return null;
    }
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Getting replication pipeline forReplicationType {} :" +
        " pipelineName:{}", replicationType, pipelineID);
    return manager.getPipeline(pipelineID);
  }

  /**
   * Finalize a given pipeline.
   */
  public void finalizePipeline(Pipeline pipeline) throws IOException {
    PipelineManager manager = getPipelineManager(pipeline.getType());
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Finalizing pipeline. pipelineID: {}", pipeline.getId());
    // Remove the pipeline from active allocation
    manager.finalizePipeline(pipeline);
    updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.FINALIZE);
    closePipelineIfNoOpenContainers(pipeline);
  }

  /**
   * Close a given pipeline.
   */
  public void closePipelineIfNoOpenContainers(Pipeline pipeline) throws IOException {
    if (pipeline.getLifeCycleState() != LifeCycleState.CLOSING) {
      return;
    }
    NavigableSet<ContainerID> containerIDS = containerStateManager
        .getMatchingContainerIDsByPipeline(pipeline.getId());
    if (containerIDS.size() == 0) {
      updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.CLOSE);
      LOG.info("Closing pipeline. pipelineID: {}", pipeline.getId());
    }
  }

  /**
   * Close a given pipeline.
   */
  private void closePipeline(Pipeline pipeline) {
    PipelineManager manager = getPipelineManager(pipeline.getType());
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Closing pipeline. pipelineID: {}", pipeline.getId());
    NavigableSet<ContainerID> containers =
        containerStateManager
            .getMatchingContainerIDsByPipeline(pipeline.getId());
    Preconditions.checkArgument(containers.size() == 0);
    manager.closePipeline(pipeline);
  }

  private void closeContainersByPipeline(Pipeline pipeline) {
    NavigableSet<ContainerID> containers =
        containerStateManager
            .getMatchingContainerIDsByPipeline(pipeline.getId());
    for (ContainerID id : containers) {
      eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, id);
    }
  }

  /**
   * list members in the pipeline .
   */

  public List<DatanodeDetails> getDatanodes(ReplicationType replicationType,
      PipelineID pipelineID) throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Getting data nodes from pipeline : {}", pipelineID);
    return manager.getMembers(pipelineID);
  }

  /**
   * Update the datanodes in the list of the pipeline.
   */

  public void updateDatanodes(ReplicationType replicationType, PipelineID
      pipelineID, List<DatanodeDetails> newDatanodes) throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Updating pipeline: {} with new nodes:{}", pipelineID,
        newDatanodes.stream().map(DatanodeDetails::toString)
            .collect(Collectors.joining(",")));
    manager.updatePipeline(pipelineID, newDatanodes);
  }

  public Node2PipelineMap getNode2PipelineMap() {
    return node2PipelineMap;
  }

  public void removePipeline(UUID dnId) {
    Set<Pipeline> pipelineSet =
        node2PipelineMap.getPipelines(dnId);
    for (Pipeline pipeline : pipelineSet) {
      getPipelineManager(pipeline.getType())
          .closePipeline(pipeline);
    }
    node2PipelineMap.removeDatanode(dnId);
  }

  /**
   * Update the Pipeline State to the next state.
   *
   * @param pipeline - Pipeline
   * @param event - LifeCycle Event
   * @throws SCMException  on Failure.
   */
  public void updatePipelineState(Pipeline pipeline,
      HddsProtos.LifeCycleEvent event) throws IOException {
    HddsProtos.LifeCycleState newState;
    try {
      newState = stateMachine.getNextState(pipeline.getLifeCycleState(), event);
    } catch (InvalidStateTransitionException ex) {
      String error = String.format("Failed to update pipeline state %s, " +
              "reason: invalid state transition from state: %s upon " +
              "event: %s.",
          pipeline.getId(), pipeline.getLifeCycleState(), event);
      LOG.error(error);
      throw new SCMException(error, FAILED_TO_CHANGE_PIPELINE_STATE);
    }

    // This is a post condition after executing getNextState.
    Preconditions.checkNotNull(newState);
    Preconditions.checkNotNull(pipeline);
    try {
      switch (event) {
      case CREATE:
        // Acquire lease on pipeline
        Lease<Pipeline> pipelineLease = pipelineLeaseManager.acquire(pipeline);
        // Register callback to be executed in case of timeout
        pipelineLease.registerCallBack(() -> {
          updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.TIMEOUT);
          return null;
        });
        break;
      case CREATED:
        // Release the lease on pipeline
        pipelineLeaseManager.release(pipeline);
        break;

      case FINALIZE:
        closeContainersByPipeline(pipeline);
        break;

      case CLOSE:
      case TIMEOUT:
        closePipeline(pipeline);
        break;
      default:
        throw new SCMException("Unsupported pipeline LifeCycleEvent.",
            FAILED_TO_CHANGE_PIPELINE_STATE);
      }

      pipeline.setLifeCycleState(newState);
    } catch (LeaseException e) {
      throw new IOException("Lease Exception.", e);
    }
  }

  public void shutdown() {
    if (pipelineLeaseManager != null) {
      pipelineLeaseManager.shutdown();
    }
  }
}
