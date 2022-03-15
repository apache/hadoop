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

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;

/**
 * This interface represents the action part of a MappingRule, action are
 * responsible to decide what should happen with the actual application
 * submission.
 */
public interface MappingRuleAction {
  /**
   * Returns the fallback action to be taken if the main action (result returned
   * by the execute method) fails.
   * e.g. Target queue does not exist, or reference is ambiguous
   * @return The fallback action to be taken if the main action fails
   */
  MappingRuleResult getFallback();

  /**
   * This method is the main logic of the action, it shall determine based on
   * the mapping context, what should be the action's result.
   * @param variables The variable context, which contains all the variables
   * @return The result of the action
   */
  MappingRuleResult execute(VariableContext variables);


  /**
   * Sets the fallback method to reject, if the action cannot be executed the
   * application will get rejected.
   * @return MappingRuleAction The same object for method chaining.
   */
  MappingRuleAction setFallbackReject();

  /**
   * Sets the fallback method to skip, if the action cannot be executed
   * We move onto the next rule, ignoring this one.
   * @return MappingRuleAction The same object for method chaining.
   */
  MappingRuleAction setFallbackSkip();

  /**
   * Sets the fallback method to place to default, if the action cannot be
   * executed. The application will be placed into the default queue, if the
   * default queue does not exist the application will get rejected
   * @return MappingRuleAction The same object for method chaining.
   */
  MappingRuleAction setFallbackDefaultPlacement();

  /**
   * This method is responsible for config validation, the context contains all
   * information required for validation, method should throw an exception on
   * detectable setup errors.
   * @param ctx Validation context with all the necessary objects and helper
   *            methods required during validation
   * @throws YarnException is thrown on validation error
   */
  void validate(MappingRuleValidationContext ctx) throws YarnException;
}
