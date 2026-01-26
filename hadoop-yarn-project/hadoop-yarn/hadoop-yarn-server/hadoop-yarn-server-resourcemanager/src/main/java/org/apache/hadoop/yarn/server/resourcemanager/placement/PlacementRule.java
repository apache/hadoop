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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

/**
 * Abstract base for all Placement Rules.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PlacementRule {

  /**
   * Set the config based on the passed in argument. This construct is used to
   * not pollute this abstract class with implementation specific references.
   * @param initArg initialization arguments.
   */
  public void setConfig(Object initArg) {
    // Default is a noop
  }

  /**
   * Return the name of the rule.
   * @return The name of the rule, the fully qualified class name.
   */
  public String getName() {
    return this.getClass().getName();
  }

  /**
   * Initialize the rule with the scheduler.
   * @param scheduler the scheduler using the rule
   * @return <code>true</code> or <code>false</code> The outcome of the
   * initialisation, rule dependent response which might not be persisted in
   * the rule.
   * @throws IOException for any errors
   */
  public abstract boolean initialize(ResourceScheduler scheduler)
      throws IOException;

  /**
   * Return the scheduler queue name the application should be placed in
   * wrapped in an {@link ApplicationPlacementContext} object.
   *
   * A non <code>null</code> return value places the application in a queue,
   * a <code>null</code> value means the queue is not yet determined. The
   * next {@link PlacementRule} in the list maintained in the
   * {@link PlacementManager} will be executed.
   *
   * @param asc The context of the application created on submission
   * @param user The name of the user submitting the application
   * 
   * @throws YarnException for any error while executing the rule
   * 
   * @return The queue name wrapped in {@link ApplicationPlacementContext} or
   * <code>null</code> if no queue was resolved
   */
  public abstract ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException;


  /**
   * Return the scheduler queue name the application should be placed in
   * wrapped in an {@link ApplicationPlacementContext} object.
   *
   * A non <code>null</code> return value places the application in a queue,
   * a <code>null</code> value means the queue is not yet determined. The
   * next {@link PlacementRule} in the list maintained in the
   * {@link PlacementManager} will be executed.
   *
   * @param asc The context of the application created on submission
   * @param user The name of the user submitting the application
   * @param recovery Indicates if the submission is a recovery
   *
   * @throws YarnException for any error while executing the rule
   *
   * @return The queue name wrapped in {@link ApplicationPlacementContext} or
   * <code>null</code> if no queue was resolved
   */
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user, boolean recovery)
      throws YarnException {
    return getPlacementForApp(asc, user);
  }
}