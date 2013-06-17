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

package org.apache.hadoop.yarn.state;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Hook for Transition. 
 * Post state is decided by Transition hook. Post state must be one of the 
 * valid post states registered in StateMachine.
 */
@Public
@Evolving
public interface MultipleArcTransition
        <OPERAND, EVENT, STATE extends Enum<STATE>> {

  /**
   * Transition hook.
   * @return the postState. Post state must be one of the 
   *                      valid post states registered in StateMachine.
   * @param operand the entity attached to the FSM, whose internal 
   *                state may change.
   * @param event causal event
   */
  public STATE transition(OPERAND operand, EVENT event);

}
