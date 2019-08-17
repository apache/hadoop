/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.common;

import org.apache.commons.collections.SetUtils;
import org.apache.hadoop.ozone.common.statemachine
    .InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.CLEANUP;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.CLOSED;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.CREATING;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.FINAL;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES.INIT;
import static org.apache.hadoop.ozone.common.TestStateMachine.STATES
    .OPERATIONAL;

/**
 * This class is to test ozone common state machine.
 */
public class TestStateMachine {

  /**
   * STATES used by the test state machine.
   */
  public enum STATES {INIT, CREATING, OPERATIONAL, CLOSED, CLEANUP, FINAL};

  /**
   * EVENTS used by the test state machine.
   */
  public enum EVENTS {ALLOCATE, CREATE, UPDATE, CLOSE, DELETE, TIMEOUT};

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testStateMachineStates() throws InvalidStateTransitionException {
    Set<STATES> finals = new HashSet<>();
    finals.add(FINAL);

    StateMachine<STATES, EVENTS> stateMachine =
        new StateMachine<>(INIT, finals);

    stateMachine.addTransition(INIT, CREATING, EVENTS.ALLOCATE);
    stateMachine.addTransition(CREATING, OPERATIONAL, EVENTS.CREATE);
    stateMachine.addTransition(OPERATIONAL, OPERATIONAL, EVENTS.UPDATE);
    stateMachine.addTransition(OPERATIONAL, CLEANUP, EVENTS.DELETE);
    stateMachine.addTransition(OPERATIONAL, CLOSED, EVENTS.CLOSE);
    stateMachine.addTransition(CREATING, CLEANUP, EVENTS.TIMEOUT);

    // Initial and Final states
    Assert.assertEquals("Initial State", INIT, stateMachine.getInitialState());
    Assert.assertTrue("Final States", SetUtils.isEqualSet(finals,
        stateMachine.getFinalStates()));

    // Valid state transitions
    Assert.assertEquals("STATE should be OPERATIONAL after being created",
        OPERATIONAL, stateMachine.getNextState(CREATING, EVENTS.CREATE));
    Assert.assertEquals("STATE should be OPERATIONAL after being updated",
        OPERATIONAL, stateMachine.getNextState(OPERATIONAL, EVENTS.UPDATE));
    Assert.assertEquals("STATE should be CLEANUP after being deleted",
        CLEANUP, stateMachine.getNextState(OPERATIONAL, EVENTS.DELETE));
    Assert.assertEquals("STATE should be CLEANUP after being timeout",
        CLEANUP, stateMachine.getNextState(CREATING, EVENTS.TIMEOUT));
    Assert.assertEquals("STATE should be CLOSED after being closed",
        CLOSED, stateMachine.getNextState(OPERATIONAL, EVENTS.CLOSE));

    // Negative cases: invalid transition
    expectException();
    stateMachine.getNextState(OPERATIONAL, EVENTS.CREATE);

    expectException();
    stateMachine.getNextState(CREATING, EVENTS.CLOSE);
  }

  /**
   * We expect an InvalidStateTransitionException.
   */
  private void expectException() {
    exception.expect(InvalidStateTransitionException.class);
    exception.expectMessage("Invalid event");
  }

}
