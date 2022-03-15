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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;

/**
 * This class implements the fallback logic for MappingRuleActions, this can
 * be extended to implement the actual logic of the actions, this should be
 * a base class for most actions.
 */
public abstract class MappingRuleActionBase implements MappingRuleAction {
  /**
   * The default fallback method is reject, so if the action fails
   * We will reject the application. However this behaviour can be overridden
   * on a per rule basis
   */
  private MappingRuleResult fallback = MappingRuleResult.createRejectResult();

  /**
   * Returns the fallback action to be taken if the main action (result returned
   * by the execute method) fails.
   * e.g. Target queue does not exist, or reference is ambiguous
   * @return The fallback action to be taken if the main action fails
   */
  public MappingRuleResult getFallback() {
    return fallback;
  }

  /**
   * Sets the fallback method to reject, if the action cannot be executed the
   * application will get rejected.
   * @return MappingRuleAction The same object for method chaining.
   */
  public MappingRuleAction setFallbackReject() {
    fallback = MappingRuleResult.createRejectResult();
    return this;
  }

  /**
   * Sets the fallback method to skip, if the action cannot be executed
   * We move onto the next rule, ignoring this one.
   * @return MappingRuleAction The same object for method chaining.
   */
  public MappingRuleAction setFallbackSkip() {
    fallback = MappingRuleResult.createSkipResult();
    return this;
  }

  /**
   * Sets the fallback method to place to default, if the action cannot be
   * executed the application will be placed into the default queue, if the
   * default queue does not exist the application will get rejected.
   * @return MappingRuleAction The same object for method chaining.
   */
  public MappingRuleAction setFallbackDefaultPlacement() {
    fallback = MappingRuleResult.createDefaultPlacementResult();
    return this;
  }

  /**
   * This method is the main logic of the action, it shall determine based on
   * the mapping context, what should be the action's result.
   * @param variables The variable context, which contains all the variables
   * @return The result of the action
   */
  public abstract MappingRuleResult execute(VariableContext variables);
}
