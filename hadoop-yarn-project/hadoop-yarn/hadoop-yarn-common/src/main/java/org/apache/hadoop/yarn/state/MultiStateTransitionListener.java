/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.state;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link StateTransitionListener} that dispatches the pre and post
 * state transitions to multiple registered listeners.
 * NOTE: The registered listeners are called in a for loop. Clients should
 *       know that a listener configured earlier might prevent a later listener
 *       from being called, if for instance it throws an un-caught Exception.
 */
public abstract class MultiStateTransitionListener
    <OPERAND, EVENT, STATE extends Enum<STATE>> implements
    StateTransitionListener<OPERAND, EVENT, STATE> {

  private final List<StateTransitionListener<OPERAND, EVENT, STATE>> listeners =
      new ArrayList<>();

  /**
   * Add a listener to the list of listeners.
   * @param listener A listener.
   */
  public void addListener(StateTransitionListener<OPERAND, EVENT, STATE>
      listener) {
    listeners.add(listener);
  }

  @Override
  public void preTransition(OPERAND op, STATE beforeState,
      EVENT eventToBeProcessed) {
    for (StateTransitionListener<OPERAND, EVENT, STATE> listener : listeners) {
      listener.preTransition(op, beforeState, eventToBeProcessed);
    }
  }

  @Override
  public void postTransition(OPERAND op, STATE beforeState, STATE afterState,
      EVENT processedEvent) {
    for (StateTransitionListener<OPERAND, EVENT, STATE> listener : listeners) {
      listener.postTransition(op, beforeState, afterState, processedEvent);
    }
  }
}
