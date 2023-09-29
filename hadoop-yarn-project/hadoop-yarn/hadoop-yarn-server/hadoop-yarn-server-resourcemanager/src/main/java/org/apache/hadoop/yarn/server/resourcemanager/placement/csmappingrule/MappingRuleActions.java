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
 * This class contains all the actions and some helper methods to generate them.
 */
public final class MappingRuleActions {
  public static final String DEFAULT_QUEUE_VARIABLE = "%default";

  /**
   * Utility class, hiding constructor.
   */
  private MappingRuleActions() {}

  /**
   * PlaceToQueueAction represents a placement action, contains the pattern of
   * the queue name or path in which the path variables will be substituted
   * with the variable context's respective values.
   */
  public static class PlaceToQueueAction extends MappingRuleActionBase {
    /**
     * We store the queue pattern in this variable, it may contain substitutable
     * variables.
     */
    private String queuePattern;

    /**
     * This flag indicates whether the target queue can be created if it does
     * not exist yet.
     */
    private boolean allowCreate;

    /**
     * Constructor.
     * @param queuePattern The queue pattern in which the application will be
     *                     placed if this action is fired. The pattern may
     *                     contain variables. eg. root.%primary_group.%user
     * @param allowCreate Determines if the target queue should be created if it
     *                    does not exist
     */
    PlaceToQueueAction(String queuePattern, boolean allowCreate) {
      this.allowCreate = allowCreate;
      this.queuePattern = queuePattern == null ? "" : queuePattern;
    }

    /**
     * This method is the main logic of the action, it will replace all the
     * variables in the queuePattern with their respective values, then returns
     * a placementResult with the final queue name.
     *
     * @param variables The variable context, which contains all the variables
     * @return The result of the action
     */
    @Override
    public MappingRuleResult execute(VariableContext variables) {
        String substituted = variables.replacePathVariables(queuePattern);
        return MappingRuleResult.createPlacementResult(
            substituted, allowCreate);
    }

    /**
     * This method is responsible for config validation, we use the validation
     * context's helper method to validate if our path is valid. From the
     * point of the action all paths are valid, that is why we need to use
     * an external component which is aware of the queue structure and know
     * when a queue placement is valid in that context. This way this calass can
     * stay independent of the capacity scheduler's internal queue placement
     * logic, yet it is able to obey it's rules.
     * @param ctx Validation context with all the necessary objects and helper
     *            methods required during validation
     * @throws YarnException is thrown on validation error
     */
    @Override
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      ctx.validateQueuePath(this.queuePattern);
    }

    @Override
    public String toString() {
      return "PlaceToQueueAction{" +
          "queueName='" + queuePattern + "'," +
          "allowCreate=" + allowCreate +
          "}";
    }
  }

  /**
   * RejectAction represents the action when the application is rejected, this
   * simply will throw an error on the user's side letting it know the
   * submission was rejected.
   */
  public static class RejectAction extends MappingRuleActionBase {
    /**
     * Reject action will unconditionally return a reject result.
     * @param variables The variable context, which contains all the variables
     * @return Always a REJECT MappingRuleResut
     */
    @Override
    public MappingRuleResult execute(VariableContext variables) {
      return MappingRuleResult.createRejectResult();
    }

    /**
     * Reject action is always valid, so it is just an empty implementation
     * of the defined interface method.
     * @param ctx Validation context with all the necessary objects and helper
     *            methods required during validation
     * @throws YarnException is thrown on validation error
     */
    @Override
    public void validate(MappingRuleValidationContext ctx) throws
        YarnException {}

    @Override
    public String toString() {
      return "RejectAction";
    }
  }

  /**
   * VariableUpdateAction represents the action which alters one of the
   * mutable variables in the variable context, but doesn't do anything with
   * the application. This can be used to change the default queue or define
   * custom variables to be used later.
   */
  public static class VariableUpdateAction extends MappingRuleActionBase {
    /**
     * Name of the variable to be updated (in it's full form) eg. %custom
     */
    private final String variableName;
    /**
     * The variable's new value pattern, this may contain additional variables
     * which will be evaluated on execution.
     */
    private final String variableValue;

    /**
     * Constructor.
     * @param variableName Name of the variable to be updated in the variable
     *                     context
     * @param variableValue
     */
    VariableUpdateAction(String variableName, String variableValue) {
      this.variableName = variableName;
      this.variableValue = variableValue;
    }

    /**
     * This execute is a bit special, compared to other actions, since it does
     * not affect the placement of the application, but changes the variable
     * context. So it always returns a skip result in order to ensure the
     * rule evalutaion continues after the variable update.
     * The exectute method will do the update to the variable context the
     * variable name stored in variableName will be updated with the value
     * stored in variableValue, but all variables in the variableValue will
     * gets resolved first, so this way dynamic updates are possible.
     * @param variables The variable context, which contains all the variables
     * @return Always a skip result.
     */
    @Override
    public MappingRuleResult execute(VariableContext variables) {
      variables.put(variableName, variables.replaceVariables(variableValue));
      return MappingRuleResult.createSkipResult();
    }

    /**
     * During the validation process we add the variable set by this action
     * to the known variables, to make sure the context is aware that we might
     * introduce a new custom variable. All rules after this may use this
     * variable. If the variable cannot be added (eg. it is already added as
     * immutable), an exception will be thrown, and the validation will fail.
     * @param ctx Validation context with all the necessary objects and helper
     *            methods required during validation
     * @throws YarnException If the variable cannot be added to the context
     */
    @Override
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      ctx.addVariable(this.variableName);
    }

    @Override
    public String toString() {
      return "VariableUpdateAction{" +
          "variableName='" + variableName + '\'' +
          ", variableValue='" + variableValue + '\'' +
          '}';
    }
  }

  /**
   * Convenience method to create an action which changes the default queue.
   * @param queue The new value of the default queue
   * @return VariableUpdateAction which will change the default queue on execute
   */
  public static MappingRuleAction createUpdateDefaultAction(String queue) {
    return new VariableUpdateAction(DEFAULT_QUEUE_VARIABLE, queue);
  }

  /**
   * Convenience method to create an action which places the application to a
   * queue.
   * @param queue The name of the queue the application should be placed to
   * @param allowCreate Determines if the target queue should be created if it
   *                    does not exist
   * @return PlaceToQueueAction which will place the application to the
   * specified queue on execute
   */
  public static MappingRuleAction createPlaceToQueueAction(
      String queue, boolean allowCreate) {
    return new PlaceToQueueAction(queue, allowCreate);
  }

  /**
   * Convenience method to create an action which places the application to the
   * DEFAULT queue.
   * @return PlaceToQueueAction which will place the application to the
   * DEFAULT queue on execute
   */
  public static MappingRuleAction createPlaceToDefaultAction() {
    return createPlaceToQueueAction(DEFAULT_QUEUE_VARIABLE, false);
  }

  /**
   * Convenience method to create an action rejects the application.
   * @return RejectAction which will reject the application on execute
   */
  public static MappingRuleAction createRejectAction() {
    return new RejectAction();
  }
}
