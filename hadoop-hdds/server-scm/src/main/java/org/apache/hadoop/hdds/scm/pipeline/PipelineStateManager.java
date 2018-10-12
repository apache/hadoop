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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_CHANGE_PIPELINE_STATE;

/**
 * Manages the state of pipelines in SCM. All write operations like pipeline
 * creation, removal and updates should come via SCMPipelineManager.
 * PipelineStateMap class holds the data structures related to pipeline and its
 * state. All the read and write operations in PipelineStateMap are protected
 * by a read write lock.
 */
class PipelineStateManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.hdds.scm.pipelines.PipelineStateManager.class);

  private final PipelineStateMap pipelineStateMap;
  private final StateMachine<LifeCycleState, LifeCycleEvent> stateMachine;
  private final LeaseManager<Pipeline> pipelineLeaseManager;

  PipelineStateManager(Configuration conf) {
    this.pipelineStateMap = new PipelineStateMap();
    Set<LifeCycleState> finalStates = new HashSet<>();
    long pipelineCreationLeaseTimeout = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_LEASE_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_LEASE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    // TODO: Use LeaseManager for creation of pipelines.
    // Add pipeline initialization logic.
    this.pipelineLeaseManager = new LeaseManager<>("PipelineCreation",
        pipelineCreationLeaseTimeout);
    this.pipelineLeaseManager.start();

    finalStates.add(LifeCycleState.CLOSED);
    this.stateMachine = new StateMachine<>(LifeCycleState.ALLOCATED,
        finalStates);
    initializeStateMachine();
  }


  /*
   * Event and State Transition Mapping.
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

  /**
   * Add javadoc.
   */
  private void initializeStateMachine() {
    stateMachine.addTransition(LifeCycleState.ALLOCATED,
        LifeCycleState.CREATING, LifeCycleEvent.CREATE);

    stateMachine.addTransition(LifeCycleState.CREATING,
        LifeCycleState.OPEN, LifeCycleEvent.CREATED);

    stateMachine.addTransition(LifeCycleState.OPEN,
        LifeCycleState.CLOSING, LifeCycleEvent.FINALIZE);

    stateMachine.addTransition(LifeCycleState.CLOSING,
        LifeCycleState.CLOSED, LifeCycleEvent.CLOSE);

    stateMachine.addTransition(LifeCycleState.CREATING,
        LifeCycleState.CLOSED, LifeCycleEvent.TIMEOUT);
  }

  Pipeline updatePipelineState(PipelineID pipelineID, LifeCycleEvent event)
      throws IOException {
    Pipeline pipeline = null;
    try {
      pipeline = pipelineStateMap.getPipeline(pipelineID);
      LifeCycleState newState =
          stateMachine.getNextState(pipeline.getLifeCycleState(), event);
      return pipelineStateMap.updatePipelineState(pipeline.getID(), newState);
    } catch (InvalidStateTransitionException ex) {
      String error = String.format("Failed to update pipeline state %s, "
              + "reason: invalid state transition from state: %s upon "
              + "event: %s.", pipeline.getID(), pipeline.getLifeCycleState(),
          event);
      LOG.error(error);
      throw new SCMException(error, FAILED_TO_CHANGE_PIPELINE_STATE);
    }
  }

  void addPipeline(Pipeline pipeline) throws IOException {
    pipelineStateMap.addPipeline(pipeline);
  }

  void addContainerToPipeline(PipelineID pipelineId, ContainerID containerID)
      throws IOException {
    pipelineStateMap.addContainerToPipeline(pipelineId, containerID);
  }

  Pipeline getPipeline(PipelineID pipelineID) throws IOException {
    return pipelineStateMap.getPipeline(pipelineID);
  }

  List<Pipeline> getPipelines(HddsProtos.ReplicationType type) {
    return pipelineStateMap.getPipelines(type);
  }

  Set<ContainerID> getContainers(PipelineID pipelineID) throws IOException {
    return pipelineStateMap.getContainers(pipelineID);
  }

  void removePipeline(PipelineID pipelineID) throws IOException {
    pipelineStateMap.removePipeline(pipelineID);
  }

  void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    pipelineStateMap.removeContainerFromPipeline(pipelineID, containerID);
  }

  void close() {
    pipelineLeaseManager.shutdown();
  }
}
