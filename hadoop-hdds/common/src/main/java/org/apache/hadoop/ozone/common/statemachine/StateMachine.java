/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.common.statemachine;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Template class that wraps simple event driven state machine.
 * @param <STATE> states allowed
 * @param <EVENT> events allowed
 */
public class StateMachine<STATE extends Enum<?>, EVENT extends Enum<?>> {
  private STATE initialState;
  private Set<STATE> finalStates;

  private final LoadingCache<EVENT, Map<STATE, STATE>> transitions =
      CacheBuilder.newBuilder().build(
          CacheLoader.from((Supplier<Map<STATE, STATE>>) () -> new HashMap()));

  public StateMachine(STATE initState, Set<STATE> finalStates) {
    this.initialState = initState;
    this.finalStates = finalStates;
  }

  public STATE getInitialState() {
    return initialState;
  }

  public Set<STATE> getFinalStates() {
    return finalStates;
  }

  public STATE getNextState(STATE from, EVENT e)
      throws InvalidStateTransitionException {
    STATE target = transitions.getUnchecked(e).get(from);
    if (target == null) {
      throw new InvalidStateTransitionException(from, e);
    }
    return target;
  }

  public void addTransition(STATE from, STATE to, EVENT e) {
    transitions.getUnchecked(e).put(from, to);
  }
}