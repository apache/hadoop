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
package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.state.StateTransitionListener;

/**
 * Interface to be used by external cluster operators to implement a
 * State Transition listener that is notified before and after a container
 * state transition.
 * NOTE: The pre and post transition callbacks will be made in the synchronized
 *       block as the call to the instrumented transition - Serially, in the
 *       order: preTransition, transition and postTransition. The implementor
 *       must ensure that the callbacks return in a timely manner to avoid
 *       blocking the state-machine.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ContainerStateTransitionListener extends
    StateTransitionListener<ContainerImpl, ContainerEvent, ContainerState> {

  /**
   * Init method which will be invoked by the Node Manager to inject the
   * NM {@link Context}.
   * @param context NM Context.
   */
  void init(Context context);
}
