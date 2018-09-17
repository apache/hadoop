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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.common.statemachine
    .InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_PIPELINE_STATE;

/**
 * Manages Pipeline states.
 */
public class PipelineStateManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineStateManager.class);

  private final StateMachine<HddsProtos.LifeCycleState,
      HddsProtos.LifeCycleEvent> stateMachine;

  PipelineStateManager() {
    // Initialize the container state machine.
    Set<HddsProtos.LifeCycleState> finalStates = new HashSet<>();
    // These are the steady states of a container.
    finalStates.add(HddsProtos.LifeCycleState.OPEN);
    finalStates.add(HddsProtos.LifeCycleState.CLOSED);

    this.stateMachine = new StateMachine<>(HddsProtos.LifeCycleState.ALLOCATED,
        finalStates);
    initializeStateMachine();
  }

  /**
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
    pipeline.setLifeCycleState(newState);
  }
}
